package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/sync-sidecar/db"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type AbstractSideCar struct {
	syncId     string
	taskId     string
	storageKey string
	command    string

	packageName    string
	packageVersion string

	stdOutPipeFile string
	stdErrPipeFile string

	bulkerURL       string
	bulkerAuthToken string

	databaseURL string
	dbpool      *pgxpool.Pool

	startedBy string
	startedAt time.Time

	//first error occurred during command
	firstErr error
}

func main() {
	startedAt, err := time.Parse(time.RFC3339, os.Getenv("STARTED_AT"))
	if err != nil {
		startedAt = time.Now()
	}

	command := os.Getenv("COMMAND")
	abstract := &AbstractSideCar{
		syncId:          os.Getenv("SYNC_ID"),
		taskId:          os.Getenv("TASK_ID"),
		command:         os.Getenv("COMMAND"),
		storageKey:      os.Getenv("STORAGE_KEY"),
		packageName:     os.Getenv("PACKAGE"),
		packageVersion:  os.Getenv("PACKAGE_VERSION"),
		stdOutPipeFile:  os.Getenv("STDOUT_PIPE_FILE"),
		stdErrPipeFile:  os.Getenv("STDERR_PIPE_FILE"),
		bulkerURL:       os.Getenv("BULKER_URL"),
		bulkerAuthToken: os.Getenv("BULKER_AUTH_TOKEN"),
		databaseURL:     os.Getenv("DATABASE_URL"),
		startedAt:       startedAt,
	}
	if command == "read" {
		sidecar := &ReadSideCar{AbstractSideCar: abstract, tableNamePrefix: os.Getenv("TABLE_NAME_PREFIX")}
		sidecar.Run()
	} else {
		sidecar := SpecCatalogSideCar{AbstractSideCar: abstract}
		sidecar.Run()
	}

}

func (s *AbstractSideCar) log(message string, args ...any) {
	s._log("jitsu", "INFO", fmt.Sprintf(message, args...))
}

func (s *AbstractSideCar) sourceLog(level, message string, args ...any) {
	message = strings.TrimPrefix(message, "INFO ")
	message = strings.TrimPrefix(message, "ERROR ")
	message = strings.TrimPrefix(message, "WARN ")
	message = strings.TrimPrefix(message, "DEBUG ")
	message = strings.TrimPrefix(message, "FATAL ")

	text := fmt.Sprintf(message, args...)
	if level == "ERROR" || level == "FATAL" {
		s.registerErr(errors.New(text))
	}
	s._log(s.packageName, level, text)
}

func (s *AbstractSideCar) err(message string, args ...any) {
	text := fmt.Sprintf(message, args...)
	s.registerErr(errors.New(text))
	s._log("jitsu", "ERROR", text)
}

func (s *AbstractSideCar) panic(message string, args ...any) {
	text := fmt.Sprintf(message, args...)
	s.registerErr(errors.New(text))
	s._log("jitsu", "ERROR", text)
	panic(text)
}

func (s *AbstractSideCar) isErr() bool {
	return s.firstErr != nil
}

func (s *AbstractSideCar) registerErr(err error) {
	if s.firstErr == nil {
		s.firstErr = err
	}
}

func (s *AbstractSideCar) checkJsonRow(json string) bool {
	if strings.HasPrefix(json, "{") && strings.HasSuffix(json, "}") {
		return true
	}
	//get first word
	logLine := strings.SplitN(json, " ", 2)
	firstWord := logLine[0]
	rest := ""
	if len(logLine) > 1 {
		rest = logLine[1]
	}
	switch strings.ToLower(firstWord) {
	case "info", "error", "warn", "debug", "fatal", "trace":
		s._log(s.packageName, firstWord, rest)
	default:
		s._log(s.packageName, "ERROR", json)
	}
	return false
}

func (s *AbstractSideCar) _log(logger, level, message string) {
	fmt.Printf("%s : %s\n", level, message)
	err := s.sendLog(logger, level, message)
	if err != nil {
		fmt.Printf("%s: %v\n", level, err)
	}
}

func (s *AbstractSideCar) sendLog(logger, level string, message string) error {
	return db.InsertTaskLog(s.dbpool, uuid.New().String(), level, logger, message, s.syncId, s.taskId, time.Now())
}

func (s *AbstractSideCar) bulkerRequest(url string, payload io.Reader, schema types.Schema) ([]byte, error) {
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return nil, fmt.Errorf("error creating POST %s request: %v", url, err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.bulkerAuthToken))
	if !schema.IsEmpty() {
		schemaBytes, err := json.Marshal(schema)
		if err != nil {
			return nil, fmt.Errorf("error marshalling schema: %v", err)
		}
		req.Header.Set("X-Jitsu-Schema", string(schemaBytes))
	}
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

func joinStrings(str1, str2, sep string) string {
	if str1 == "" {
		return str2
	} else if str2 == "" {
		return str1
	}

	return str1 + sep + str2
}
