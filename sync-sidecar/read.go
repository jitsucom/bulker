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

func (s *StreamStat) Merge(chunk *StreamStat) error {
	switch chunk.Status {
	case "FAILED":
		if s.Status != "FAILED" {
			s.Status = "PARTIAL"
			s.Error = chunk.Error
		}
	case "SUCCESS":
		if s.Status != "SUCCESS" {
			return fmt.Errorf("unexpected status. cannot merge success status with non-success")
		}
		s.EventsCount += chunk.EventsCount
		s.BytesProcessed += chunk.BytesProcessed
	default:
		return fmt.Errorf("unexpected stream status: " + chunk.Status + ". ")
	}
	return nil
}

type ActiveStream struct {
	name string
	mode string

	bulkerStream bulker.BulkerStream
	*StreamStat
}

func NewActiveStream(name, mode string, bulkerStream bulker.BulkerStream) *ActiveStream {
	return &ActiveStream{name: name, mode: mode, bulkerStream: bulkerStream, StreamStat: &StreamStat{Status: "SUCCESS"}}
}

func (s *ActiveStream) Close() (state bulker.State, err error) {
	if s == nil {
		return
	}
	if s.Error != "" {
		state, err = s.bulkerStream.Abort(context.Background())
	} else {
		state, err = s.bulkerStream.Complete(context.Background())
	}
	return
}

func (s *ActiveStream) Consume(p *types2.OrderedMap[string, any]) error {
	_, _, err := s.bulkerStream.Consume(context.Background(), p)
	return err
}

func (s *ActiveStream) RegisterError(err error) {
	if err != nil && s != nil && s.Error == "" {
		s.Error = err.Error()
		s.BytesProcessed = 0
		s.EventsCount = 0
		s.Status = "FAILED"
	}
}

type ReadSideCar struct {
	*AbstractSideCar
	tableNamePrefix string

	lastMessageTime   atomic.Int64
	blk               bulker.Bulker
	currentStream     *ActiveStream
	processedStreams  map[string]*StreamStat
	catalog           map[string]*Stream
	destinationConfig map[string]any
	initialState      string
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
		if len(s.processedStreams) > 0 {
			for cStream, _ := range s.catalog {
				if _, ok := s.processedStreams[cStream]; !ok {
					s.processedStreams[cStream] = &StreamStat{Status: "FAILED", Error: "Stream was not processed. Check logs for errors."}
				}
			}
			allSuccess := true
			allFailed := true
			for _, streamStat := range s.processedStreams {
				if streamStat.Status != "SUCCESS" {
					allSuccess = false
				}
				if streamStat.Status != "FAILED" {
					allFailed = false
				}
			}
			status := "PARTIAL"
			if allSuccess {
				status = "SUCCESS"
			} else if allFailed {
				status = "FAILED"
			}
			processedStreamsJson, _ := jsonorder.Marshal(s.processedStreams)
			s.sendStatus(status, string(processedStreamsJson))
		} else if s.isErr() {
			s.sendStatus("FAILED", "ERROR: "+s.firstErr.Error())
			os.Exit(1)
		} else {
			s.sendStatus("SUCCESS", "")
		}
	}()
	s.log("Sidecar. command: read. syncId: %s, taskId: %s, package: %s:%s startedAt: %s", s.syncId, s.taskId, s.packageName, s.packageVersion, s.startedAt.Format(time.RFC3339))
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
	s.log("Catalog loaded. %d streams selected", len(s.catalog))
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

	s.processedStreams = map[string]*StreamStat{}
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
			ok = s.checkJsonRow(string(line))
			if !ok {
				continue
			}
			row := &Row{}
			err := jsonorder.Unmarshal(line, row)
			if err != nil {
				s.panic("error parsing airbyte line %s: %v", string(line), err)
			}
			switch row.Type {
			case LogType:
				s.sourceLog(row.Log.Level, row.Log.Message)
			case StateType:
				s.processState(row.State)
			case RecordType:
				s.processRecord(row.Record, len(line))
			case TraceType:
				s.processTrace(row.Trace, string(line))
			case ControlType:
				s.sourceLog("WARN", "Control messages are not supported and ignored: %s", string(line))
			default:
				s.panic("not supported Airbyte message type: %s", row.Type)
			}
		}
		if err := scanner.Err(); err != nil {
			s.panic("error reading from pipe: %v", err)
		}
		s.closeCurrentStream()
	}()
	stdOutErrWaitGroup.Wait()
}

func (s *ReadSideCar) processState(state *StateRow) {
	//checkpointing. commit to bulker all processed events on saving state
	s.closeCurrentStream()
	switch state.Type {
	case "GLOBAL":
		s.saveState("_GLOBAL_STATE", state.GlobalState)
	case "STREAM":
		s.saveState(joinStrings(state.StreamState.StreamDescriptor.Namespace, state.StreamState.StreamDescriptor.Name, "."), state.StreamState.StreamState)
	case "LEGACY", "":
		s.saveState("_LEGACY_STATE", state.Data)
	}
}

func (s *ReadSideCar) saveState(stream string, data any) {
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

func (s *ReadSideCar) closeStream(streamName string) {
	if s.currentStream != nil && s.currentStream.name == streamName {
		state, _ := s.currentStream.Close()
		stat, exists := s.processedStreams[s.currentStream.name]
		if exists {
			err := stat.Merge(s.currentStream.StreamStat)
			if err != nil {
				s.panic("error merging stream stats: %v", err)
			}
		} else {
			stat = s.currentStream.StreamStat
			s.processedStreams[s.currentStream.name] = stat
		}
		s.log("Stream %s closed: status: %s bulker: %s rows: %d successful: %d", streamName, stat.Status, state.Status, state.ProcessedRows, state.SuccessfulRows)
		s.currentStream = nil
	}
}

func (s *ReadSideCar) closeCurrentStream() {
	if s.currentStream != nil {
		s.closeStream(s.currentStream.name)
	}
}

func (s *ReadSideCar) changeStreamIfNeeded(streamName string, previousStats *StreamStat) {
	if s.currentStream == nil || s.currentStream.name != streamName {
		s.closeCurrentStream()
		var err error
		s.currentStream, err = s.openStream(streamName, previousStats)
		if err != nil {
			s.err("error opening stream: %v", err)
		}
	}
}

func (s *ReadSideCar) openStream(streamName string, previousStats *StreamStat) (*ActiveStream, error) {
	// we create pipe. everything that is written to 'streamWriter' will be sent to bulker via 'streamReader' as reader payload
	str, ok := s.catalog[streamName]
	if !ok {
		return &ActiveStream{name: streamName}, fmt.Errorf("stream '%s' is not in catalog", streamName)
	}
	mode := bulker.ReplaceTable
	// if there is no initial sync state, we assume that this is first sync and we need to do full sync
	if str.SyncMode == "incremental" && len(s.initialState) > 0 {
		mode = bulker.Batch
	} else if previousStats != nil && previousStats.EventsCount > 0 {
		// checkpointing: if there is previous stats that means that we have committed data because and saved state
		// switch from replace_table to batch mode, to continue adding data to the table
		mode = bulker.Batch
	}

	tableName := utils.NvlString(str.TableName, s.tableNamePrefix+streamName)
	jobId := fmt.Sprintf("%s_%s_%s", s.syncId, s.taskId, tableName)

	var streamOptions []bulker.StreamOption
	if len(str.GetPrimaryKeys()) > 0 {
		streamOptions = append(streamOptions, bulker.WithPrimaryKey(str.GetPrimaryKeys()...))
	}
	s.log("Creating bulker stream: %s table: %s mode: %s primary keys: %s", streamName, tableName, mode, str.GetPrimaryKeys())
	schema := str.ToSchema()
	//s.log("Schema: %+v", schema)
	if len(schema.Fields) > 0 {
		streamOptions = append(streamOptions, bulker.WithSchema(schema))
	}
	bulkerStream, err := s.blk.CreateStream(jobId, tableName, mode, streamOptions...)
	if err != nil {
		s.err("error creating bulker stream: %v", err)
		return &ActiveStream{name: streamName}, fmt.Errorf("error creating bulker stream: %v", err)
	}
	return NewActiveStream(streamName, string(mode), bulkerStream), nil
}

func (s *ReadSideCar) processTrace(rec *TraceRow, line string) {
	if rec.Type == "STREAM_STATUS" {
		streamStatus := rec.StreamStatus
		streamName := joinStrings(streamStatus.StreamDescriptor.Namespace, streamStatus.StreamDescriptor.Name, ".")
		s.log("Stream %s status: %s", streamName, streamStatus.Status)
		switch streamStatus.Status {
		case "STARTED":
			s.changeStreamIfNeeded(streamName, nil)
		case "COMPLETE", "INCOMPLETE":
			s.closeStream(streamName)
		}
	} else {
		s.log("TRACE: %s", line)
	}
}

func (s *ReadSideCar) processRecord(rec *RecordRow, size int) {
	streamName := joinStrings(rec.Namespace, rec.Stream, ".")
	processed, ok := s.processedStreams[streamName]
	if ok && processed.Error != "" {
		//for incremental streams we ignore all messages if it was error on previously committed chunks.
		//error may be on bulker side (source may not know about it) and we have no way to command source to switch to the next stream
		return
	}
	s.changeStreamIfNeeded(streamName, processed)
	if s.currentStream.Error != "" {
		// ignore all messages after stream received error
		return
	}
	err := s.currentStream.Consume(rec.Data)
	if err != nil {
		s.err("error writing to bulk pipe: %v", err)
		return
	}
	s.currentStream.EventsCount++
	s.currentStream.BytesProcessed += size
}

func (s *ReadSideCar) sourceLog(level, message string, args ...any) {
	if level == "ERROR" || level == "FATAL" {
		s.currentStream.RegisterError(fmt.Errorf(message, args...))
	}
	s.AbstractSideCar.sourceLog(level, message, args...)
}

func (s *ReadSideCar) err(message string, args ...any) {
	s.currentStream.RegisterError(fmt.Errorf(message, args...))
	s.AbstractSideCar.err(message, args...)
}

func (s *ReadSideCar) panic(message string, args ...any) {
	s.currentStream.RegisterError(fmt.Errorf(message, args...))
	_, _ = s.currentStream.Close()
	s.AbstractSideCar.panic(message, args...)
}

func (s *ReadSideCar) storeState(stream, state string) {
	err := db.UpsertState(s.dbpool, s.syncId, stream, state, time.Now())
	if err != nil {
		s.panic("error updating state: %v", err)
	}
}

func (s *ReadSideCar) sendStatus(status string, description string) {
	logFunc := s.log
	if status == "FAILED" {
		logFunc = s.err
	}
	logFunc("READ %s", joinStrings(status, description, ": "))
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
	mp := make(map[string]*Stream, len(catalog.Streams))
	for _, stream := range catalog.Streams {
		mp[joinStrings(stream.Namespace, stream.Name, ".")] = stream
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
