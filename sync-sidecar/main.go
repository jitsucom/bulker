package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

const (
	upsertSpecSQL = `INSERT INTO source_spec (package, version, specs, timestamp ,error ) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ON CONSTRAINT source_spec_pk DO UPDATE SET specs = $3, timestamp = $4, error=$5`

	upsertCatalogSQL = `INSERT INTO source_catalog (source_id, package, version, config_hash, catalog, timestamp, error) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT ON CONSTRAINT source_catalog_pk DO UPDATE SET catalog=$5, timestamp = $6, error=$7`

	upsertStateSQL = `INSERT INTO source_state (source_id, state, timestamp) VALUES ($1, $2, $3)
ON CONFLICT ON CONSTRAINT source_state_pk DO UPDATE SET state=$2, timestamp = $3`
)

type SideCar struct {
	sourceId string
	taskId   string

	packageName    string
	packageVersion string

	stdOutPipeFile  string
	stdErrPipeFile  string
	connectionId    string
	tasksConnection string
	stateConnection string

	bulkerURL       string
	bulkerAuthToken string

	databaseURL string

	//first error occurred during sync
	firstErr      error
	currentStream *io.PipeWriter
}

func main() {
	sidecar := &SideCar{
		sourceId:        os.Getenv("SOURCE_ID"),
		taskId:          os.Getenv("TASK_ID"),
		packageName:     os.Getenv("PACKAGE"),
		packageVersion:  os.Getenv("PACKAGE_VERSION"),
		stdOutPipeFile:  os.Getenv("STDOUT_PIPE_FILE"),
		stdErrPipeFile:  os.Getenv("STDERR_PIPE_FILE"),
		connectionId:    os.Getenv("CONNECTION_ID"),
		tasksConnection: os.Getenv("TASKS_CONNECTION_ID"),
		stateConnection: os.Getenv("STATE_CONNECTION_ID"),
		bulkerURL:       os.Getenv("BULKER_URL"),
		bulkerAuthToken: os.Getenv("BULKER_AUTH_TOKEN"),
		databaseURL:     os.Getenv("DATABASE_URL"),
	}

	sidecar.Run()

}

func (s *SideCar) Run() {
	//TODO: read catalog to detect full sync or incremental mode
	syncStarted := false
	s.sendStatus("sidecar_started", "")
	s.log("Sidecar. stdout: %s, stderr: %s, connectionId: %s, bulkerURL: %s", s.stdOutPipeFile, s.stdErrPipeFile, s.connectionId, s.bulkerURL)
	dbpool, err := pgxpool.New(context.Background(), s.databaseURL)
	if err != nil {
		e := fmt.Sprintf("Unable to create postgres connection pool: %v", err)
		s.sendStatus("sidecar_failed", e)
		s.err(e)
		return
	}
	defer dbpool.Close()
	var wg sync.WaitGroup

	errPipe, _ := os.Open(s.stdErrPipeFile)
	defer errPipe.Close()
	wg.Add(1)
	// read from stderr
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(errPipe)
		for scanner.Scan() {
			line := scanner.Text()
			s.err(line)
		}
		if err := scanner.Err(); err != nil {
			s.err("error reading from pipe: %v", err)
		}
	}()

	processedStreams := map[string]int{}
	eventsCounter := 0
	outPipe, _ := os.Open(s.stdOutPipeFile)
	defer outPipe.Close()
	wg.Add(1)
	// read from stdout
	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(outPipe)
		scanner.Buffer(make([]byte, 1024*100), 1024*1024*10)
		currentStreamName := ""
		var streamsWaitGroup sync.WaitGroup
		for scanner.Scan() {
			line := scanner.Bytes()
			row := &Row{}
			err := json.Unmarshal(line, row)
			if err != nil {
				s.err("error parsing airbyte line %s: %v", string(line), err)
			}
			if !syncStarted {
				syncStarted = true
				s.sendStatus("started", "")
			}
			switch row.Type {
			case LogType:
				switch row.Log.Level {
				case "ERROR":
					s.err(row.Log.Message)
				default:
					s.log(row.Log.Message)
				}
			case StateType:
				//TODO: support STATE
				s.log("state: %v\n", row.State.Data)
				if !s.isErr() {
					err := s.sendState(row.State.Data)
					if err != nil {
						s.err("error sending state: %v", err)
					}
				}
			case RecordType:
				if s.isErr() {
					//don't send records after first error
					continue
				}
				rec := row.Record
				streamName := rec.Stream
				if streamName != currentStreamName {
					if s.currentStream != nil {
						processedStreams[currentStreamName] = eventsCounter
						eventsCounter = 0
						_ = s.currentStream.Close()
					}
					if _, ok := processedStreams[streamName]; ok {
						s.err("stream '%s' was already processed. We assume that airbyte doesn't mix streams", streamName)
						continue
					}
					r, w := io.Pipe()
					s.currentStream = w
					currentStreamName = streamName
					streamsWaitGroup.Add(1)
					go func() {
						defer streamsWaitGroup.Done()
						s.log("creating stream: %s", streamName)
						bd, err := s.bulkerRequest(fmt.Sprintf("%s/bulk/%s?tableName=%s", s.bulkerURL, s.connectionId, url.QueryEscape(streamName)), r)
						if err != nil {
							s.err("error sending bulk: %v", err)
							return
						}
						s.log("bulk response: %s", string(bd))
					}()
				}
				data, err := json.Marshal(rec.Data)
				if err != nil {
					s.err("error marshalling record: %v", err)
					break
				}
				_, err = s.currentStream.Write(data)
				if err != nil {
					s.err("error writing to bulk pipe: %v", err)
					break
				}
				_, _ = s.currentStream.Write([]byte("\n"))
				eventsCounter++
			case SpecType:
				spec, _ := json.Marshal(row.Spec)
				dbpool.Exec(context.Background(), upsertSpecSQL, s.packageName, s.packageVersion, spec, time.Now(), nil)
				s.log("spec: %v", row.Spec)
			case ConnectionStatusType:
				s.log("Connection Status: %s: %v", row.ConnectionStatus.Status, row.ConnectionStatus.Message)
			case CatalogType:
				catalog, _ := json.Marshal(row.Catalog)
				s.log("Catalog: %s", string(catalog))
			case TraceType:
			default:
				s.err("not supported type: %s", row.Type)
			}
		}
		if err := scanner.Err(); err != nil {
			s.err("error reading from pipe: %v", err)
		}
		if s.currentStream != nil {
			processedStreams[currentStreamName] = eventsCounter
			_ = s.currentStream.Close()
		}
		streamsWaitGroup.Wait()
	}()

	wg.Wait()
	if s.isErr() {
		s.sendStatus("failed", s.firstErr.Error())
	} else {
		s.sendStatus("completed", fmt.Sprint(processedStreams))
	}
	s.log("Sidecar finished")
}
func (s *SideCar) log(message string, args ...any) {
	s._log(fmt.Sprintf(message, args...))
}

func (s *SideCar) err(message string, args ...any) {
	text := fmt.Sprintf(message, args...)
	if s.firstErr == nil {
		s.firstErr = errors.New(text)
		if s.currentStream != nil {
			_, _ = s.currentStream.Write([]byte("ABORT with error: " + text + "\n"))
			_ = s.currentStream.Close()
		}
	}
	s._err(text)
}

func (s *SideCar) isErr() bool {
	return s.firstErr != nil
}

func (s *SideCar) _log(message string) {
	fmt.Printf("INFO : %s\n", message)
	err := s.sendLog("INFO", message)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}

func (s *SideCar) _err(message string) {
	fmt.Printf("ERROR: %s\n", message)
	err := s.sendLog("ERROR", message)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}

func (s *SideCar) sendLog(level string, message string) error {
	logMessage := map[string]any{
		"timestamp": time.Now(),
		"source_id": s.sourceId,
		"task_id":   s.taskId,
		"level":     level,
		"message":   message,
	}
	return s.bulkerEvent(s.tasksConnection, "task_log", logMessage)
}

func (s *SideCar) sendStatus(status string, description string) {
	statusMessage := map[string]any{
		"timestamp":   time.Now(),
		"source_id":   s.sourceId,
		"task_id":     s.taskId,
		"status":      status,
		"description": description,
	}
	err := s.bulkerEvent(s.tasksConnection, "task_status", statusMessage)
	if err != nil {
		s.err("failed to send status: %v", err)
	}
}

func (s *SideCar) sendState(state any) error {
	bt, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("error marshalling state: %v", err)
	}
	stateObj := map[string]any{
		"timestamp": time.Now(),
		"source_id": s.sourceId,
		"state":     string(bt),
	}
	return s.bulkerEvent(s.stateConnection, "task_state", stateObj)
}
func (s *SideCar) bulkerEvent(connection, tableName string, payload any) error {
	v, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshalling event payload %v for %s: %v", payload, tableName, err)
	}
	body := bytes.NewReader(v)
	_, err = s.bulkerRequest(fmt.Sprintf("%s/post/%s?tableName=%s", s.bulkerURL, connection, url.QueryEscape(tableName)), body)
	if err != nil {
		return fmt.Errorf("error sending event to %s: %v", tableName, err)
	}
	return nil
}

func (s *SideCar) bulkerRequest(url string, payload io.Reader) ([]byte, error) {
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return nil, fmt.Errorf("error creating POST %s request: %v", url, err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.bulkerAuthToken))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s error: %v", url, err)
	}
	defer res.Body.Close()
	bd, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("POST %s read response error: %v", url, res.Status)
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("POST %s error %v: %s", url, res.Status, string(bd))
	}
	return bd, nil
}
