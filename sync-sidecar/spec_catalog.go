package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/pg"
	"github.com/jitsucom/bulker/sync-sidecar/db"
	"os"
	"strings"
	"sync"
	"time"
)

type SpecCatalogSideCar struct {
	*AbstractSideCar
}

func (s *SpecCatalogSideCar) Run() {
	var err error
	s.dbpool, err = pg.NewPGPool(s.databaseURL)
	if err != nil {
		s.panic("Unable to create postgres connection pool: %v", err)
	}
	defer s.dbpool.Close()

	defer func() {
		if r := recover(); r != nil {
			s.sendStatus(s.command, "FAILED", fmt.Sprint(r))
			os.Exit(1)
		} else if s.isErr() {
			s.sendStatus(s.command, "FAILED", s.firstErr.Error())
			os.Exit(1)
		} else {
			s.sendStatus(s.command, "SUCCESS", "")
		}
	}()
	s.log("Sidecar. command: %s, taskId: %s, package: %s:%s startedAt: %s", s.command, s.taskId, s.packageName, s.packageVersion, s.startedAt.Format(time.RFC3339))
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

	outPipe, _ := os.Open(s.stdOutPipeFile)
	defer outPipe.Close()
	stdOutErrWaitGroup.Add(1)
	// read from stdout
	go func() {
		defer stdOutErrWaitGroup.Done()

		scanner := bufio.NewScanner(outPipe)
		scanner.Buffer(make([]byte, 1024*10), 1024*1024*10)
		for scanner.Scan() {
			line := scanner.Bytes()
			ok := s.checkJsonRow(string(line))
			if !ok {
				continue
			}
			row := &Row{}
			err := json.Unmarshal(line, row)
			if err != nil {
				s.panic("error parsing airbyte line %s: %v", string(line), err)
			}
			switch row.Type {
			case LogType:
				s.sourceLog(row.Log.Level, row.Log.Message)
			case SpecType:
				s.processSpec(row.Spec)
			case ConnectionStatusType:
				s.processConnectionStatus(row.ConnectionStatus)
			case CatalogType:
				s.processCatalog(row.Catalog)
			case ControlType:
				s.sourceLog("WARN", "Control messages are not supported and ignored: %s", string(line))
			case TraceType:
			default:
				s.panic("not supported type: %s", row.Type)
			}
		}
		if err := scanner.Err(); err != nil {
			s.panic("error reading from pipe: %v", err)
		}
	}()
	stdOutErrWaitGroup.Wait()
}

func (s *SpecCatalogSideCar) processSpec(spec map[string]any) {
	// ignore previous error messages since we got result
	s.firstErr = nil
	specJson, _ := json.Marshal(spec)
	err := db.UpsertSpec(s.dbpool, s.packageName, s.packageVersion, string(specJson), s.startedAt, "")
	if err != nil {
		s.panic("error updating spec for %s:%s: %v", s.packageName, s.packageVersion, err)
	}
	s.log("Spec: %s", specJson)
}

func (s *SpecCatalogSideCar) processConnectionStatus(status *StatusRow) {
	// ignore previous error messages since we got result
	s.firstErr = nil
	s.log("CONNECTION STATUS: %s", joinStrings(status.Status, status.Message, ": "))
	st := strings.ReplaceAll(status.Status, "SUCCEEDED", "SUCCESS")
	err := db.UpsertCheck(s.dbpool, s.packageName, s.packageVersion, s.storageKey, st, status.Message, s.startedAt)
	if err != nil {
		s.panic("error updating connection status for: %s: %v", s.storageKey, err)
	}
}

func (s *SpecCatalogSideCar) processCatalog(catalog map[string]any) {
	// ignore previous error messages since we got result
	s.firstErr = nil
	catalogJson, _ := json.Marshal(catalog)
	s.log("CATALOG: %s", catalogJson)
	err := db.UpsertCatalogSuccess(s.dbpool, s.packageName, s.packageVersion, s.storageKey, string(catalogJson), s.startedAt, "SUCCESS", "")
	if err != nil {
		s.panic("error updating catalog for: %s: %v", s.storageKey, err)
	}
}

func (s *SpecCatalogSideCar) sendStatus(command string, status string, description string) {
	logFunc := s.log
	if status == "FAILED" {
		logFunc = s.err
	}
	logFunc("%s %s", strings.ToUpper(command), joinStrings(status, description, ": "))
	if status == "FAILED" {
		switch command {
		case "spec":
			err := db.InsertSpecError(s.dbpool, s.packageName, s.packageVersion, s.startedAt, description)
			if err != nil {
				s.panic("error updating specs: %v", err)
			}
		case "discover":
			err := db.UpsertCatalogStatus(s.dbpool, s.packageName, s.packageVersion, s.storageKey, s.startedAt, "FAILED", description)
			if err != nil {
				s.panic("error updating catalog status: %v", err)
			}
		case "check":
			err := db.InsertCheckError(s.dbpool, s.packageName, s.packageVersion, s.storageKey, "FAILED", "FAILED: "+description, s.startedAt)
			if err != nil {
				s.panic("error updating connection status: %v", err)
			}
		}
	}
}
