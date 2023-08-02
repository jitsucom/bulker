package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/sync-sidecar/db"
	"io"
	"net/http"
	"net/url"
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
	logsConnection string

	bulkerURL       string
	bulkerAuthToken string

	databaseURL string
	dbpool      *pgxpool.Pool

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
		logsConnection:  os.Getenv("LOGS_CONNECTION_ID"),
		bulkerURL:       os.Getenv("BULKER_URL"),
		bulkerAuthToken: os.Getenv("BULKER_AUTH_TOKEN"),
		databaseURL:     os.Getenv("DATABASE_URL"),
		startedAt:       startedAt,
	}
	if command == "read" {
		sidecar := &ReadSideCar{AbstractSideCar: abstract}
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

func (s *AbstractSideCar) _log(logger, level, message string) {
	fmt.Printf("%s : %s\n", level, message)
	err := s.sendLog(logger, level, message)
	if err != nil {
		fmt.Printf("%s: %v\n", level, err)
	}
}

func (s *AbstractSideCar) sendLog(logger, level string, message string) error {
	logMessage := map[string]any{
		"id":        uuid.New().String(),
		"timestamp": time.Now().Format(time.RFC3339Nano),
		"sync_id":   s.syncId,
		"task_id":   s.taskId,
		"logger":    logger,
		"level":     level,
		"message":   message,
	}
	return s.bulkerEvent(s.logsConnection, "task_log", logMessage)
}

func (s *AbstractSideCar) sendStatus(command string, status string, description string) {
	s.log("%s %s", strings.ToUpper(command), joinStrings(status, description, ": "))
	if command == "read" && s.dbpool != nil {
		err := db.UpsertTask(s.dbpool, s.syncId, s.taskId, s.packageName, s.packageVersion, s.startedAt, status, description)
		if err != nil {
			s.panic("error updating task: %v", err)
		}
	}
}

func (s *AbstractSideCar) bulkerEvent(connection, tableName string, payload any) error {
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

func (s *AbstractSideCar) bulkerRequest(url string, payload io.Reader) ([]byte, error) {
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

func joinStrings(str1, str2, sep string) string {
	if str1 == "" {
		return str2
	} else if str2 == "" {
		return str1
	}

	return str1 + sep + str2
}
