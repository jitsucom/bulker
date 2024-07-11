package main

import (
	"bufio"
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/pg"
	types2 "github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/sync-sidecar/db"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type StreamStat struct {
	EventsCount    int    `json:"events"`
	BytesProcessed int    `json:"bytes"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

type ActiveStream struct {
	name                string
	mode                string
	bufferedEventsCount int
	bufferedBytes       int
	unsavedState        any
	closed              bool

	bulkerStream bulker.BulkerStream
	*StreamStat
}

func NewActiveStream(name, mode string) *ActiveStream {
	return &ActiveStream{name: name, mode: mode, StreamStat: &StreamStat{Status: "RUNNING"}}
}

func (s *ActiveStream) Begin(bulkerStream bulker.BulkerStream) error {
	if s.closed {
		return fmt.Errorf("Stream '%s' is already closed", s.name)
	}
	if s.bulkerStream != nil {
		return fmt.Errorf("Stream '%s' is already started", s.name)
	}
	s.bulkerStream = bulkerStream
	s.Status = "RUNNING"
	return nil
}

func (s *ActiveStream) Abort() (state bulker.State) {
	if s == nil {
		return
	}
	if s.bulkerStream != nil {
		state = s.bulkerStream.Abort(context.Background())
	}
	s.bulkerStream = nil
	return
}

func (s *ActiveStream) Commit() (state bulker.State, err error) {
	if s == nil {
		return
	}
	if s.bulkerStream != nil {
		if s.Error != "" {
			state = s.bulkerStream.Abort(context.Background())
		} else {
			state, err = s.bulkerStream.Complete(context.Background())
			if err != nil {
				s.Error = err.Error()
			} else {
				s.EventsCount += s.bufferedEventsCount
				s.BytesProcessed += s.bufferedBytes
			}
		}
	}
	s.bufferedEventsCount = 0
	s.bufferedBytes = 0
	s.bulkerStream = nil
	return
}

func (s *ActiveStream) Close(complete bool) (state bulker.State, err error) {
	if complete {
		state, _ = s.Commit()
	} else {
		state = s.Abort()
		if s.Error == "" {
			s.Error = "Stream was interrupted"
		}
	}
	if s.Error != "" {
		if s.EventsCount > 0 {
			s.Status = "PARTIAL"
		} else {
			s.Status = "FAILED"
		}
	} else if s.Status == "RUNNING" {
		s.Status = "SUCCESS"
	}
	s.closed = true
	return
}

func (s *ActiveStream) Consume(p *types2.OrderedMap[string, any], originalSize int) error {
	if s.Error != "" {
		return nil
	}
	_, _, err := s.bulkerStream.Consume(context.Background(), p)
	if err == nil {
		s.bufferedEventsCount++
		s.bufferedBytes += originalSize
	}
	return err
}

func (s *ActiveStream) IsActive() bool {
	return s.bulkerStream != nil
}

func (s *ActiveStream) RegisterError(err error) {
	if err != nil && s != nil && s.Error == "" {
		s.Error = err.Error()
		s.bufferedEventsCount = 0
		s.bufferedBytes = 0
	}
}

type ReadSideCar struct {
	*AbstractSideCar
	tableNamePrefix string

	lastMessageTime   atomic.Int64
	lastStateMessage  string
	blk               bulker.Bulker
	lastStream        *ActiveStream
	processedStreams  map[string]*ActiveStream
	catalog           *types2.OrderedMap[string, *Stream]
	destinationConfig map[string]any
	initialState      string
	fullSync          bool
	eventsCounter     int
	bytesCounter      int
}

func (s *ReadSideCar) Run() {
	var err error
	s.lastMessageTime.Store(time.Now().Unix())
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			if time.Now().Unix()-s.lastMessageTime.Load() > 4000 {
				s.panic("No messages from %s for 1 hour. Exiting", s.packageName)
			}
		}
	}()
	s.dbpool, err = pg.NewPGPool(s.databaseURL)
	if err != nil {
		s.panic("Unable to create postgres connection pool: %v", err)
	}
	defer s.dbpool.Close()

	defer func() {
		if r := recover(); r != nil {
			s.registerErr(fmt.Errorf("%v", r))
		}
		s.closeActiveStreams()
		if len(s.processedStreams) > 0 {
			statusMap := types2.NewOrderedMap[string, any]()
			s.catalog.ForEach(func(streamName string, _ *Stream) {
				if stream, ok := s.processedStreams[streamName]; ok {
					statusMap.Set(streamName, stream.StreamStat)
				} else {
					statusMap.Set(streamName, &StreamStat{Status: "FAILED", Error: "Stream was not processed. Check logs for errors."})
				}
			})
			allSuccess := true
			allFailed := true
			statusMap.ForEach(func(_ string, streamStat any) {
				st := streamStat.(*StreamStat)
				if st.Status != "SUCCESS" {
					allSuccess = false
				}
				if st.Status != "FAILED" {
					allFailed = false
				}
			})
			status := "PARTIAL"
			if allSuccess {
				status = "SUCCESS"
			} else if allFailed {
				status = "FAILED"
			}

			processedStreamsJson, _ := jsonorder.Marshal(statusMap)
			s.sendFinalStatus(status, string(processedStreamsJson))
		} else if s.isErr() {
			s.sendFinalStatus("FAILED", "ERROR: "+s.firstErr.Error())
			os.Exit(1)
		} else {
			s.sendFinalStatus("SUCCESS", "")
		}
	}()
	s.log("Sidecar. command: read. syncId: %s, taskId: %s, package: %s:%s startedAt: %s", s.syncId, s.taskId, s.packageName, s.packageVersion, s.startedAt.Format(time.RFC3339))
	s.fullSync = os.Getenv("FULL_SYNC") == "true"
	if s.fullSync {
		s.log("Running in Full Sync mode")
	}
	err = s.loadDestinationConfig()
	if err != nil {
		s.panic("Error loading destination config: %v", err)
	}
	blk, err := bulker.CreateBulker(bulker.Config{
		Id:                fmt.Sprintf("%s_%s", s.syncId, s.taskId),
		BulkerType:        s.destinationConfig["destinationType"].(string),
		DestinationConfig: s.destinationConfig,
	})
	if err != nil {
		s.panic("Error creating bulker: %v", err)
	}
	s.blk = blk
	err = s.loadCatalog()
	if err != nil {
		s.panic("Error loading catalog: %v", err)
	}
	s.log("Catalog loaded. %d streams selected", s.catalog.Len())
	state, ok := s.loadState()
	if ok {
		s.log("State loaded: %s", state)
	}
	var stdOutErrWaitGroup sync.WaitGroup

	errPipe, _ := os.Open(s.stdErrPipeFile)
	defer errPipe.Close()
	stdOutErrWaitGroup.Add(1)
	// read from stderr
	go func() {
		defer stdOutErrWaitGroup.Done()
		scanner := bufio.NewScanner(errPipe)
		scanner.Buffer(make([]byte, 1024*10), 1024*1024*10)
		for scanner.Scan() {
			line := scanner.Text()
			s.sourceLog("ERRSTD", line)
		}
		if err := scanner.Err(); err != nil {
			s.panic("error reading from err pipe: %v", err)
		}
	}()

	s.processedStreams = map[string]*ActiveStream{}
	outPipe, _ := os.Open(s.stdOutPipeFile)
	defer outPipe.Close()
	stdOutErrWaitGroup.Add(1)
	// read from stdout
	go func() {
		defer stdOutErrWaitGroup.Done()

		scanner := bufio.NewScanner(outPipe)
		scanner.Buffer(make([]byte, 1024*10), 1024*1024*10)
		for scanner.Scan() {
			s.lastMessageTime.Store(time.Now().Unix())
			line := scanner.Bytes()
			lineStr := string(line)
			ok = s.checkJsonRow(lineStr)
			if !ok {
				continue
			}
			row := &Row{}
			err := jsonorder.Unmarshal(line, row)
			if err != nil {
				s.panic("error parsing airbyte line %s: %v", lineStr, err)
			}
			switch row.Type {
			case LogType:
				s.sourceLog(row.Log.Level, row.Log.Message)
			case StateType:
				if s.lastStateMessage != lineStr {
					s.processState(row.State)
					s.lastStateMessage = lineStr
				}
			case RecordType:
				s.processRecord(row.Record, len(line))
			case TraceType:
				s.processTrace(row.Trace, lineStr)
			case ControlType:
				s.sourceLog("WARN", "Control messages are not supported and ignored: %s", lineStr)
			default:
				s.panic("not supported Airbyte message type: %s", row.Type)
			}
		}
		if err := scanner.Err(); err != nil {
			s.panic("error reading from pipe: %v", err)
		}
	}()
	stdOutErrWaitGroup.Wait()
}

func (s *ReadSideCar) processState(state *StateRow) {
	//checkpointing. commit to bulker all processed events on saving state
	switch state.Type {
	case "GLOBAL":
		s.checkpointIfNecessary(s.lastStream)
		s.saveState("_GLOBAL_STATE", state.GlobalState)
	case "STREAM":
		streamName := joinStrings(state.StreamState.StreamDescriptor.Namespace, state.StreamState.StreamDescriptor.Name, ".")
		var ok bool
		stream, ok := s.processedStreams[streamName]
		if ok && stream != nil {
			stream.unsavedState = state.StreamState.StreamState
			s.checkpointIfNecessary(stream)
		}
	case "LEGACY", "":
		s.checkpointIfNecessary(s.lastStream)
		s.saveState("_LEGACY_STATE", state.Data)
	}
}

func (s *ReadSideCar) saveState(stream string, data any) {
	if data == nil {
		return
	}
	if stream != "_LEGACY_STATE" && stream != "_GLOBAL_STATE" {
		processed, ok := s.processedStreams[stream]
		if !ok {
			s.err("STATE: cannot save state for stream '%s' because it was not processed", stream)
			return
		}
		if processed.Error != "" {
			s.err("STATE: not saving state for stream '%s' because of previous errors", stream)
			return
		}
	} else {
		if s.isErr() {
			s.err("STATE: not saving '%s' state because of previous errors", stream)
			return
		}
	}
	stateJson, err := jsonorder.Marshal(data)
	if err != nil {
		s.panic("error marshalling state %+v: %v", data, err)
	}
	s.log("SAVING STATE for '%s': %s", stream, stateJson)
	s.storeState(stream, string(stateJson))
}

func (s *ReadSideCar) closeStream(streamName string, complete bool) {
	stream, ok := s.processedStreams[streamName]
	if !ok || stream == nil {
		s.err("Stream '%s' is not in processed streams", streamName)
		return
	}
	s._closeStream(stream, complete)
	s.updateRunningStatus()
}

func (s *ReadSideCar) checkpointIfNecessary(stream *ActiveStream) {
	if stream == nil {
		return
	}
	// for successfully closed stream it is safe to save state
	// otherwise we save state only after commit
	if stream.Status == "SUCCESS" {
		s.saveState(stream.name, stream.unsavedState)
		stream.unsavedState = nil
		return
	}
	if !stream.IsActive() {
		return
	}

	if stream.bufferedEventsCount >= 500000 || (stream.mode == "incremental" && !s.fullSync) {
		state, err := stream.Commit()
		if err != nil {
			s.err("Stream '%s' bulker commit failed: %v", stream.name, err)
		} else {
			s.saveState(stream.name, stream.unsavedState)
			stream.unsavedState = nil
			s.log("Stream '%s' bulker commit: %s rows: %d successful: %d", stream.name, state.Status, state.ProcessedRows, state.SuccessfulRows)
		}
		s.updateRunningStatus()
	}
	return
}

func (s *ReadSideCar) _closeStream(stream *ActiveStream, complete bool) {
	wasActive := stream.IsActive()
	state, err := stream.Close(complete)
	if err != nil {
		s.err("Stream '%s' bulker commit failed: %v", stream.name, err)
	} else if wasActive {
		s.log("Stream '%s' bulker commit: %s rows: %d successful: %d", stream.name, state.Status, state.ProcessedRows, state.SuccessfulRows)
	}
	if complete {
		s.saveState(stream.name, stream.unsavedState)
		stream.unsavedState = nil
	}
	s.log("Stream '%s' closed: status: %s rows: %d", stream.name, stream.Status, stream.EventsCount)
}

func (s *ReadSideCar) closeActiveStreams() {
	for _, stream := range s.processedStreams {
		if stream.Status == "RUNNING" {
			s._closeStream(stream, true)
		}
	}
}

func (s *ReadSideCar) openStream(streamName string) (*ActiveStream, error) {
	// we create pipe. everything that is written to 'streamWriter' will be sent to bulker via 'streamReader' as reader payload
	str, ok := s.catalog.Get(streamName)
	if !ok {
		err := fmt.Errorf("stream '%s' is not in catalog", streamName)
		return &ActiveStream{name: streamName, StreamStat: &StreamStat{Error: err.Error()}}, err
	}
	stream := s.processedStreams[streamName]
	if stream != nil && stream.Error != "" {
		//for incremental streams we ignore all messages if it was error on previously committed chunks.
		//error may be on bulker side (source may not know about it) and we have no way to command source to switch to the next stream
		return stream, nil
	}
	if stream != nil && stream.IsActive() {
		return stream, nil
	}

	mode := bulker.ReplaceTable
	// if there is no initial sync state, we assume that this is first sync and we need to do full sync
	if str.SyncMode == "incremental" && len(s.initialState) > 0 {
		mode = bulker.Batch
	} else if stream != nil && stream.EventsCount > 0 {
		// checkpointing: if there is previous stats that means that we have committed data because and saved state
		// switch from replace_table to batch mode, to continue adding data to the table
		mode = bulker.Batch
	}
	if stream == nil {
		stream = NewActiveStream(streamName, str.SyncMode)
		s.processedStreams[streamName] = stream
	}
	s.lastStream = stream
	tableName := utils.NvlString(str.TableName, s.tableNamePrefix+streamName)
	jobId := fmt.Sprintf("%s_%s_%s", s.syncId, s.taskId, tableName)

	var streamOptions []bulker.StreamOption
	if len(str.GetPrimaryKeys()) > 0 {
		streamOptions = append(streamOptions, bulker.WithPrimaryKey(str.GetPrimaryKeys()...), bulker.WithDeduplicate())
	}
	s.log("Creating bulker stream: %s table: %s mode: %s primary keys: %s", streamName, tableName, mode, str.GetPrimaryKeys())
	schema := str.ToSchema()
	//s.log("Schema: %+v", schema)
	if len(schema.Fields) > 0 {
		streamOptions = append(streamOptions, bulker.WithSchema(schema))
	}
	bulkerStream, err := s.blk.CreateStream(jobId, tableName, mode, streamOptions...)
	if err != nil {
		return stream, fmt.Errorf("error creating bulker stream: %v", err)
	}
	err = stream.Begin(bulkerStream)
	if err != nil {
		return stream, fmt.Errorf("error starting bulker stream: %v", err)
	}

	return stream, nil
}

func (s *ReadSideCar) processTrace(rec *TraceRow, line string) {
	switch rec.Type {
	case "STREAM_STATUS":
		streamStatus := rec.StreamStatus
		streamName := joinStrings(streamStatus.StreamDescriptor.Namespace, streamStatus.StreamDescriptor.Name, ".")
		s.log("Stream '%s' status: %s", streamName, streamStatus.Status)
		switch streamStatus.Status {
		case "STARTED":
			_, err := s.openStream(streamName)
			if err != nil {
				s.err("error opening stream: %v", err)
			}
		case "COMPLETE", "INCOMPLETE":
			s.closeStream(streamName, streamStatus.Status == "COMPLETE")
		}
	case "ERROR":
		r := rec.Error
		s.errprint("TRACE ERROR: %s", r.Message)
		fmt.Printf("ERROR DETAILS: %s\n%s", r.InternalMessage, r.StackTrace)
		if r.StreamDescriptor.Name != "" {
			streamName := joinStrings(r.StreamDescriptor.Namespace, r.StreamDescriptor.Name, ".")
			stream, ok := s.processedStreams[streamName]
			if ok {
				stream.RegisterError(fmt.Errorf("%s", r.Message))
			}
		}
	default:
		s.log("TRACE: %s", line)
	}
}

func (s *ReadSideCar) processRecord(rec *RecordRow, size int) {
	streamName := joinStrings(rec.Namespace, rec.Stream, ".")
	stream, err := s.openStream(streamName)
	if err != nil {
		s.err("error opening stream: %v", err)
		return
	}
	err = stream.Consume(rec.Data, size)
	if err != nil {
		s.err("error producing to bulker stream: %v", err)
		return
	}
}

func (s *ReadSideCar) sourceLog(level, message string, args ...any) {
	if level == "ERROR" || level == "FATAL" {
		s.lastStream.RegisterError(fmt.Errorf(message, args...))
	}
	s.AbstractSideCar.sourceLog(level, message, args...)
}

func (s *ReadSideCar) err(message string, args ...any) {
	s.lastStream.RegisterError(fmt.Errorf(message, args...))
	s.AbstractSideCar.err(message, args...)
}

func (s *ReadSideCar) errprint(message string, args ...any) {
	s.AbstractSideCar.err(message, args...)
}

func (s *ReadSideCar) panic(message string, args ...any) {
	s.lastStream.RegisterError(fmt.Errorf(message, args...))
	s.AbstractSideCar.panic(message, args...)
}

func (s *ReadSideCar) storeState(stream, state string) {
	err := db.UpsertState(s.dbpool, s.syncId, stream, state, time.Now())
	if err != nil {
		s.panic("error updating state: %v", err)
	}
}

func (s *ReadSideCar) updateRunningStatus() {
	statusMap := types2.NewOrderedMap[string, any]()
	s.catalog.ForEach(func(streamName string, _ *Stream) {
		if stream, ok := s.processedStreams[streamName]; ok {
			statusMap.Set(streamName, stream.StreamStat)
		} else {
			statusMap.Set(streamName, &StreamStat{Status: "PENDING"})
		}
	})
	processedStreamsJson, _ := jsonorder.Marshal(statusMap)
	s._sendStatus("RUNNING", string(processedStreamsJson), false)
}

func (s *ReadSideCar) sendFinalStatus(status string, description string) {
	s._sendStatus(status, description, true)
}

func (s *ReadSideCar) _sendStatus(status string, description string, log bool) {
	if log {
		logFunc := s.log
		if status == "FAILED" {
			logFunc = s.err
		}
		logFunc("READ %s", joinStrings(status, description, ": "))
	}
	err := db.UpsertTask(s.dbpool, s.syncId, s.taskId, s.packageName, s.packageVersion, s.startedAt, status, description)
	if err != nil {
		s.panic("error updating task: %v", err)
	}
}

func (s *ReadSideCar) loadState() (string, bool) {
	//load catalog from file /config/catalog.json and parse it
	statePath := "/config/state.json"
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		return "", false
	}
	state, err := os.ReadFile(statePath)
	if err != nil {
		return "", false
	}
	st := string(state)
	if len(st) == 0 || st == "{}" {
		return "", false
	}
	s.initialState = st
	return st, true
}

func (s *ReadSideCar) loadCatalog() error {
	//load catalog from file /config/catalog.json and parse it
	catalogPath := "/config/catalog.json"
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		return fmt.Errorf("catalog file %s doesn't exist", catalogPath)
	}
	catalogFile, err := os.ReadFile(catalogPath)
	if err != nil {
		return fmt.Errorf("error opening catalog file: %v", err)
	}
	//s.log("Catalog: %s", string(catalogFile))
	catalog := Catalog{}
	err = jsonorder.Unmarshal(catalogFile, &catalog)
	if err != nil {
		return fmt.Errorf("error parsing catalog file: %v", err)
	}
	mp := types2.NewOrderedMap[string, *Stream]()
	for _, stream := range catalog.Streams {
		mp.Set(joinStrings(stream.Namespace, stream.Name, "."), stream)
	}
	s.catalog = mp
	return nil
}

func (s *ReadSideCar) loadDestinationConfig() error {
	//load catalog from file /config/catalog.json and parse it
	destinationConfigPath := "/config/destinationConfig.json"
	if _, err := os.Stat(destinationConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("destination config file %s doesn't exist", destinationConfigPath)
	}
	destinationConfigFile, err := os.ReadFile(destinationConfigPath)
	if err != nil {
		return fmt.Errorf("error opening destination config file: %v", err)
	}
	//s.log("Destination config: %s", string(destinationConfigFile))
	destinationConfig := map[string]any{}
	err = jsonorder.Unmarshal(destinationConfigFile, &destinationConfig)
	if err != nil {
		return fmt.Errorf("error parsing destination config file: %v", err)
	}
	s.destinationConfig = destinationConfig
	return nil
}
