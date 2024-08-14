package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/pg"
	types2 "github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/sync-sidecar/db"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const interruptError = "Stream was interrupted. Check logs for errors."
const cancelledError = "Sync job was cancelled"

type ReadSideCar struct {
	*AbstractSideCar
	namespace       string
	tableNamePrefix string

	eventsLogService eventslog.EventsLogService

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
		cancelled := s.cancelled.Load()
		if r := recover(); r != nil {
			s.registerErr(fmt.Errorf("%v", r))
			s.closeActiveStreams(false)
		} else {
			s.closeActiveStreams(!cancelled)
		}
		if len(s.processedStreams) > 0 {
			statusMap := types2.NewOrderedMap[string, any]()
			s.catalog.ForEach(func(streamName string, _ *Stream) {
				if stream, ok := s.processedStreams[streamName]; ok {
					statusMap.Set(streamName, stream.StreamStat)
				} else {
					if cancelled {
						statusMap.Set(streamName, &StreamStat{Status: "CANCELLED"})
					} else {
						statusMap.Set(streamName, &StreamStat{Status: "FAILED", Error: "Stream was not processed. Check logs for errors."})
					}
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
			} else if cancelled {
				status = "CANCELLED"
			}

			processedStreamsJson, _ := jsonorder.Marshal(statusMap)
			s.sendFinalStatus(status, string(processedStreamsJson))
		} else if s.isErr() {
			s.sendFinalStatus("FAILED", "ERROR: "+s.firstErr.Error())
			os.Exit(1)
		} else if cancelled {
			s.sendFinalStatus("CANCELLED", "")
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

	s.errPipe, _ = os.Open(s.stdErrPipeFile)
	defer s.errPipe.Close()
	stdOutErrWaitGroup.Add(1)
	// read from stderr
	go func() {
		defer stdOutErrWaitGroup.Done()
		scanner := bufio.NewScanner(s.errPipe)
		scanner.Buffer(make([]byte, 1024*10), 1024*1024*10)
		for scanner.Scan() {
			line := scanner.Text()
			s.sourceLog("ERRSTD", line)
		}
		if err := scanner.Err(); err != nil && !s.cancelled.Load() {
			s.panic("error reading from err pipe: %v", err)
		}
	}()

	s.processedStreams = map[string]*ActiveStream{}
	s.outPipe, _ = os.Open(s.stdOutPipeFile)
	defer s.outPipe.Close()
	if s.cancelled.Load() {
		return
	}
	stdOutErrWaitGroup.Add(1)
	// read from stdout
	go func() {
		defer stdOutErrWaitGroup.Done()

		scanner := bufio.NewScanner(s.outPipe)
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
				if row.Log.Level == "ERROR" || row.Log.Level == "FATAL" {
					stream, ok := s.getSolelyRunningStream()
					if ok && stream != nil {
						stream.errorFromLogs = row.Log.Message
					}
				}
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
		if err := scanner.Err(); err != nil && !s.cancelled.Load() {
			s.panic("error reading from pipe: %v", err)
		}
	}()
	stdOutErrWaitGroup.Wait()
}

func (s *ReadSideCar) getSolelyRunningStream() (*ActiveStream, bool) {
	var first *ActiveStream
	for _, stream := range s.processedStreams {
		if stream.Status == "RUNNING" {
			if first == nil {
				first = stream
			} else {
				return nil, false
			}
		}
	}
	return first, first != nil
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
			s.errprint("STATE: cannot save state for stream '%s' because it was not processed", stream)
			return
		}
		if processed.Error != "" {
			s.errprint("STATE: not saving state for stream '%s' because of previous errors", stream)
			return
		}
	} else {
		if s.isErr() {
			s.errprint("STATE: not saving '%s' state because of previous errors", stream)
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
		s.errprint("Stream '%s' is not in processed streams", streamName)
		return
	}
	s._closeStream(stream, complete, false)
}

func (s *ReadSideCar) checkpointIfNecessary(stream *ActiveStream) {
	if stream == nil {
		return
	}
	// for successfully closed stream it is safe to save state
	// otherwise we save state only after commit to warehouse
	if stream.Status == "SUCCESS" {
		s.saveState(stream.name, stream.unsavedState)
		stream.unsavedState = nil
		return
	}
	if !stream.IsActive() {
		return
	}

	if stream.bufferedEventsCount >= 500000 || (stream.mode == "incremental" && !s.fullSync) {
		state := stream.Commit(false)
		state.ProcessingTimeSec = time.Since(stream.started).Seconds()
		if stream.Error != "" {
			s.postEventsLog(s.syncId, state, stream.processedObjectSample, stream.Error)
			s.errprint("Stream '%s' bulker commit failed: %v", stream.name, stream.Error)
		} else {
			if state.ProcessedRows > 0 || stream.bulkerMode == bulker.ReplaceTable {
				s.postEventsLog(s.syncId, state, stream.processedObjectSample, "")
			}
			s.saveState(stream.name, stream.unsavedState)
			stream.unsavedState = nil
			s.log("Stream '%s' bulker commit: %s rows: %d successful: %d", stream.name, state.Status, state.ProcessedRows, state.SuccessfulRows)
		}
		s.updateRunningStatus()
	}
	return
}

type BatchState struct {
	bulker.State  `json:",inline"`
	LastMappedRow types2.Json `json:"lastMappedRow"`
}

func (s *ReadSideCar) postEventsLog(destinationId string, state bulker.State, processedObjectSample types2.Json, batchErr string) {
	if batchErr != "" && state.LastError == nil {
		state.SetError(errors.New(batchErr))
	}
	batchState := BatchState{State: state, LastMappedRow: processedObjectSample}
	level := eventslog.LevelInfo
	if batchErr != "" {
		level = eventslog.LevelError
	}
	_, err := s.eventsLogService.PostEvent(&eventslog.ActorEvent{Timestamp: time.Now(), EventType: eventslog.EventTypeBatch, Level: level, ActorId: destinationId, Event: batchState})
	if err != nil {
		s.errprint("Error posting events log: %v", err)
	}

}

func (s *ReadSideCar) _closeStream(stream *ActiveStream, complete bool, strict bool) {
	wasActive := stream.IsActive()
	state := stream.Close(complete, s.cancelled.Load(), strict)
	state.ProcessingTimeSec = time.Since(stream.started).Seconds()
	if stream.Error != "" {
		s.postEventsLog(s.syncId, state, stream.processedObjectSample, stream.Error)
		s.errprint("Stream '%s' bulker commit failed: %v", stream.name, stream.Error)
	} else if wasActive {
		if state.ProcessedRows > 0 || stream.bulkerMode == bulker.ReplaceTable {
			s.postEventsLog(s.syncId, state, stream.processedObjectSample, "")
		}
		s.log("Stream '%s' bulker commit: %s rows: %d successful: %d", stream.name, state.Status, state.ProcessedRows, state.SuccessfulRows)
	}
	if complete {
		s.saveState(stream.name, stream.unsavedState)
		stream.unsavedState = nil
	}
	s.log("Stream '%s' closed: status: %s rows: %d", stream.name, stream.Status, stream.EventsCount)
}

func (s *ReadSideCar) closeActiveStreams(complete bool) {
	for _, stream := range s.processedStreams {
		if stream.Status == "RUNNING" {
			s._closeStream(stream, complete, true)
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
		stream = NewActiveStream(streamName, str.SyncMode, mode)
		s.processedStreams[streamName] = stream
	}
	s.lastStream = stream
	var namespace string
	tableNamePrefix := strings.ReplaceAll(s.tableNamePrefix, "${SOURCE_NAMESPACE}", str.Namespace)
	tableName := utils.NvlString(str.TableName, tableNamePrefix+str.Name)
	if s.namespace == "${LEGACY}" {
		namespace = ""
		tableName = utils.NvlString(str.TableName, tableNamePrefix+streamName)
	} else {
		namespace = strings.TrimSpace(strings.ReplaceAll(s.namespace, "${SOURCE_NAMESPACE}", str.Namespace))
	}

	jobId := fmt.Sprintf("%s_%s_%s", s.syncId, s.taskId, tableName)

	var streamOptions []bulker.StreamOption
	if len(str.GetPrimaryKeys()) > 0 {
		streamOptions = append(streamOptions, bulker.WithPrimaryKey(str.GetPrimaryKeys()...), bulker.WithDeduplicate())
	}
	s.log("Stream '%s' created bulker. table: %s mode: %s primary keys: %s", streamName, tableName, mode, str.GetPrimaryKeys())
	schema := str.ToSchema()
	//s.log("Schema: %+v", schema)
	if len(schema.Fields) > 0 {
		streamOptions = append(streamOptions, bulker.WithSchema(schema))
	}
	if len(str.CursorField) > 0 {
		streamOptions = append(streamOptions, bulker.WithDiscriminatorField(str.CursorField))
	} else if len(str.DefaultCursorField) > 0 {
		streamOptions = append(streamOptions, bulker.WithDiscriminatorField(str.DefaultCursorField))
	}
	if namespace != "" {
		streamOptions = append(streamOptions, bulker.WithNamespace(namespace))
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
		s.log("Stream '%s' received status: %s", streamName, streamStatus.Status)
		switch streamStatus.Status {
		case "STARTED":
			stream, err := s.openStream(streamName)
			if err != nil {
				s.streamErr(stream, "error opening stream: %v", err)
			}
			s.updateRunningStatus()
		case "COMPLETE", "INCOMPLETE":
			s.closeStream(streamName, streamStatus.Status == "COMPLETE")
			s.updateRunningStatus()
		}
	case "ERROR":
		r := rec.Error
		streamName := joinStrings(r.StreamDescriptor.Namespace, r.StreamDescriptor.Name, ".")
		if streamName != "" {
			s.errprint("TRACE ERROR '%s': %s", streamName, r.Message)
		} else {
			s.errprint("TRACE ERROR: %s", r.Message)
		}
		fmt.Printf("ERROR DETAILS: %s\n%s", r.InternalMessage, r.StackTrace)
		if streamName != "" {
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
		s.streamErr(stream, "error opening stream: %v", err)
		return
	}
	err = stream.Consume(rec.Data, size)
	if err != nil {
		s.streamErr(stream, "error producing to bulker stream: %v", err)
		return
	}
}

func (s *ReadSideCar) sourceLog(level, message string, args ...any) {
	s.AbstractSideCar.sourceLog(level, message, args...)
}

func (s *ReadSideCar) streamErr(stream *ActiveStream, message string, args ...any) {
	err := fmt.Errorf(message, args...)
	stream.RegisterError(err)
	s._log("jitsu", "ERROR", err.Error())
}

func (s *ReadSideCar) errprint(message string, args ...any) {
	text := fmt.Sprintf(message, args...)
	s._log("jitsu", "ERROR", text)
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
			logFunc = s.errprint
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

type StreamStat struct {
	EventsCount    int    `json:"events"`
	BytesProcessed int    `json:"bytes"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

type ActiveStream struct {
	name                string
	mode                string
	bulkerMode          bulker.BulkMode
	bufferedEventsCount int
	bufferedBytes       int
	unsavedState        any
	closed              bool

	bulkerStream          bulker.BulkerStream
	processedObjectSample types2.Json
	started               time.Time
	errorFromLogs         string
	noTrustworthyError    bool
	*StreamStat
}

func NewActiveStream(name, mode string, bulkerMode bulker.BulkMode) *ActiveStream {
	return &ActiveStream{name: name, mode: mode, bulkerMode: bulkerMode, StreamStat: &StreamStat{Status: "RUNNING"}}
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
	s.started = time.Now()
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

// Commit strict - if true, we commit only if there are no errors in logs that could be attributed to that stream
// ( was emitted when only this stream was running )
func (s *ActiveStream) Commit(strict bool) (state bulker.State) {
	if s == nil {
		return
	}
	if s.bulkerStream != nil {
		if s.Error != "" {
			state = s.bulkerStream.Abort(context.Background())
		} else if strict && s.errorFromLogs != "" {
			s.Error = s.errorFromLogs
			state = s.bulkerStream.Abort(context.Background())
		} else {
			var err error
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

func (s *ActiveStream) Close(complete, cancelled, strict bool) (state bulker.State) {
	if complete {
		state = s.Commit(strict)
	} else {
		state = s.Abort()
		if s.Error == "" && !cancelled {
			s.Error = utils.NvlString(s.errorFromLogs, interruptError)
			s.noTrustworthyError = true
		}
	}
	if s.Error != "" {
		if s.EventsCount > 0 {
			s.Status = "PARTIAL"
		} else {
			s.Status = "FAILED"
		}
	} else if cancelled {
		if s.EventsCount > 0 {
			s.Status = "PARTIAL"
			s.Error = cancelledError
		} else {
			s.Status = "CANCELLED"
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
	var err error
	_, s.processedObjectSample, err = s.bulkerStream.Consume(context.Background(), p)
	if err == nil {
		s.bufferedEventsCount++
		s.bufferedBytes += originalSize
	}
	return err
}

func (s *ActiveStream) IsActive() bool {
	return s.bulkerStream != nil && s.Error == ""
}

func (s *ActiveStream) RegisterError(err error) {
	if err != nil && s != nil && (s.Error == "" || s.noTrustworthyError) {
		s.Error = err.Error()
		s.bufferedEventsCount = 0
		s.bufferedBytes = 0
	}
}
