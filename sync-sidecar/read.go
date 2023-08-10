package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/sync-sidecar/db"
	"io"
	"net/url"
	"os"
	"sync"
	"time"
)

type StreamStat struct {
	EventsCount    int    `json:"events"`
	BytesProcessed int    `json:"bytes"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

func (s *StreamStat) Merge(chunk *StreamStat) {
	switch chunk.Status {
	case "FAILED":
		if s.Status != "FAILED" {
			s.Status = "PARTIAL"
			s.Error = chunk.Error
		}
	case "SUCCESS":
		if s.Status != "SUCCESS" {
			panic("unexpected status. cannot merge success status with non-success")
		}
		s.EventsCount += chunk.EventsCount
		s.BytesProcessed += chunk.BytesProcessed
	default:
		panic("unexpected stream status: " + chunk.Status + ". ")
	}
}

type ActiveStream struct {
	name string
	mode string
	// write stream of io.Pipe. Bulker reads from this read stream of this pipe
	writer *io.PipeWriter
	// wait group to wait for stream to finish HTTP request to bulker fully completed after closing writer
	waitGroup        sync.WaitGroup
	bulkerConnFunc   func()
	bulkerConnOpened bool
	*StreamStat
}

func NewActiveStream(name, mode string, bulkerConnFunc func(streamReader *io.PipeReader)) *ActiveStream {
	streamReader, streamWriter := io.Pipe()
	as := &ActiveStream{name: name, mode: mode, writer: streamWriter, StreamStat: &StreamStat{Status: "SUCCESS"}}
	as.bulkerConnFunc = func() {
		defer as.Done()
		defer streamReader.Close()
		bulkerConnFunc(streamReader)
	}
	return as
}

func (s *ActiveStream) Close() {
	if s.Error != "" {
		// intentionally break ndjson stream with that error. Bulker will abort this stream
		_ = s.writer.CloseWithError(fmt.Errorf(s.Error))
	} else {
		if !s.bulkerConnOpened && s.mode != "batch" {
			// if bulker connection was not opened yet, then we need to open it to send empty ndjson stream
			// it is important for replace table stream to replace previous table with empty one.
			s.bulkerConnOpened = true
			s.waitGroup.Add(1)
			go s.bulkerConnFunc()
		}
		_ = s.writer.Close()
	}
	s.waitGroup.Wait()
}

func (s *ActiveStream) Write(p []byte) (n int, err error) {
	if !s.bulkerConnOpened {
		// lazily open bulker connection on first event
		s.bulkerConnOpened = true
		s.waitGroup.Add(1)
		go s.bulkerConnFunc()
	}
	return s.writer.Write(p)
}

func (s *ActiveStream) Done() {
	s.waitGroup.Done()
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

	currentStream    *ActiveStream
	processedStreams map[string]*StreamStat
	catalog          map[string]*Stream
	initialState     string
	eventsCounter    int
	bytesCounter     int
}

func (s *ReadSideCar) Run() {
	var err error
	s.dbpool, err = pgxpool.New(context.Background(), s.databaseURL)
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
			processedStreamsJson, _ := json.Marshal(s.processedStreams)
			s.sendStatus(s.command, status, string(processedStreamsJson))
		} else if s.isErr() {
			s.sendStatus(s.command, "FAILED", "ERROR: "+s.firstErr.Error())
			os.Exit(1)
		} else {
			s.sendStatus(s.command, "SUCCESS", "")
		}
	}()
	s.log("Sidecar. command: %s. syncId: %s, taskId: %s, package: %s:%s startedAt: %s", s.command, s.syncId, s.taskId, s.packageName, s.packageVersion, s.startedAt.Format(time.RFC3339))
	//load file from /config/catalog.json and parse it
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
		scanner.Buffer(make([]byte, 1024*100), 1024*1024*10)
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
		scanner.Buffer(make([]byte, 1024*100), 1024*1024*10)
		for scanner.Scan() {
			line := scanner.Bytes()
			row := &Row{}
			err := json.Unmarshal(line, row)
			if err != nil {
				s.panic("error parsing airbyte line %s: %v", string(line), err)
			}
			switch row.Type {
			case LogType:
				s.sourceLog(row.Log.Level, row.Log.Message)
			case StateType:
				s.processState(row.State)
			case RecordType:
				s.processRecord(row.Record)
			case TraceType:
				s.processTrace(row.Trace)
			default:
				s.panic("not supported type: %s", row.Type)
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
	}
	stateJson, err := json.Marshal(data)
	if err != nil {
		s.panic("error marshalling state %+v: %v", data, err)
	}
	s.log("SAVING STATE for '%s': %s", stream, stateJson)
	s.storeState(stream, string(stateJson))
}

func (s *ReadSideCar) closeStream(streamName string) {
	if s.currentStream != nil && s.currentStream.name == streamName {
		s.currentStream.Close()
		stat, exists := s.processedStreams[s.currentStream.name]
		if exists {
			stat.Merge(s.currentStream.StreamStat)
		} else {
			stat = s.currentStream.StreamStat
			s.processedStreams[s.currentStream.name] = stat
		}
		s.log("Stream %s closed: status: %s rows: %d bytes: %d ", streamName, stat.Status, stat.EventsCount, stat.BytesProcessed)
		s.currentStream = nil
	}
}

func (s *ReadSideCar) closeCurrentStream() {
	if s.currentStream != nil {
		s.closeStream(s.currentStream.name)
	}
}

func (s *ReadSideCar) changeStreamIfNeeded(streamName string) {
	if s.currentStream == nil || s.currentStream.name != streamName {
		s.closeCurrentStream()
		s.currentStream = s.openStream(streamName)
	}
}

func (s *ReadSideCar) openStream(streamName string) *ActiveStream {
	// we create pipe. everything that is written to 'streamWriter' will be sent to bulker via 'streamReader' as reader payload
	str, ok := s.catalog[streamName]
	if !ok {
		s.err("stream '%s' is not in catalog", streamName)
		return nil
	}
	mode := "replace_table"
	// if there is no initial sync state, we assume that this is first sync and we need to do full sync
	if str.SyncMode == "incremental" && len(s.initialState) > 0 {
		mode = "batch"
	}

	bulkerConnFunc := func(streamReader *io.PipeReader) {
		s.log("Creating bulker stream: %s mode: %s primary keys: %s", streamName, mode, str.GetPrimaryKeys())
		bulkerUrl := fmt.Sprintf("%s/bulk/%s?tableName=%s&mode=%s&taskId=%s", s.bulkerURL, s.syncId, url.QueryEscape(streamName), mode, s.taskId)
		for _, v := range str.GetPrimaryKeys() {
			bulkerUrl += fmt.Sprintf("&pk=%s", url.QueryEscape(v))
		}
		_, err := s.bulkerRequest(bulkerUrl, streamReader)
		if err != nil {
			s.err("error sending bulk: %v", err)
			return
		} else {
			s.log("Bulker stream %s finished", streamName)
		}
	}
	newStream := NewActiveStream(streamName, mode, bulkerConnFunc)

	return newStream
}

func (s *ReadSideCar) processTrace(rec *TraceRow) {
	if rec.Type == "STREAM_STATUS" {
		streamStatus := rec.StreamStatus
		streamName := joinStrings(streamStatus.StreamDescriptor.Namespace, streamStatus.StreamDescriptor.Name, ".")
		s.log("Stream %s status: %s", streamName, streamStatus.Status)
		switch streamStatus.Status {
		case "STARTED":
			s.changeStreamIfNeeded(streamName)
		case "COMPLETE", "INCOMPLETE":
			s.closeStream(streamName)
		}
	}
}

func (s *ReadSideCar) processRecord(rec *RecordRow) {
	streamName := joinStrings(rec.Namespace, rec.Stream, ".")
	s.changeStreamIfNeeded(streamName)
	if s.currentStream.Error != "" {
		// ignore all messages after stream received error
		return
	}
	processed, ok := s.processedStreams[streamName]
	if ok && processed.Error != "" {
		//for incremental streams we ignore all messages if it was error on previously committed chunks.
		//error may be on bulker side (source may not known about it) and we have no way to command source to switch to the next stream
		return
	}

	data, err := json.Marshal(rec.Data)
	if err != nil {
		s.err("error marshalling record: %v", err)
		return
	}
	_, err = s.currentStream.Write(data)
	if err != nil {
		s.err("error writing to bulk pipe: %v", err)
		return
	}
	_, _ = s.currentStream.Write([]byte("\n"))
	s.currentStream.EventsCount++
	s.currentStream.BytesProcessed += len(data)
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
	s.AbstractSideCar.panic(message, args...)
}

func (s *ReadSideCar) storeState(stream, state string) {
	err := db.UpsertState(s.dbpool, s.syncId, stream, state, time.Now())
	if err != nil {
		s.panic("error updating state: %v", err)
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
	catalog := Catalog{}
	err = json.Unmarshal(catalogFile, &catalog)
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
