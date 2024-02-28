package eventslog

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"slices"

	"sync"
	"time"
)

const chEventsLogServiceName = "ch_events_log"

type ClickhouseEventsLog struct {
	sync.Mutex
	appbase.Service
	conn                  driver.Conn
	eventsBuffer          []*ActorEvent
	periodicFlushInterval time.Duration
	closeChan             chan struct{}
}

func NewClickhouseEventsLog(config EventsLogConfig) (EventsLogService, error) {
	base := appbase.NewServiceBase(chEventsLogServiceName)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.ClickhouseURL},
		Auth: clickhouse.Auth{
			Database: config.ClickhouseDatabase,
			Username: utils.NvlString(config.ClickhouseUsername, "default"),
			Password: config.ClickhousePassword,
		},
		TLS: &tls.Config{},
		Settings: clickhouse.Settings{
			"async_insert":                 1,
			"wait_for_async_insert":        0,
			"async_insert_max_data_size":   "5000000",
			"async_insert_busy_timeout_ms": 5000,
			"date_time_input_format":       "best_effort",
		},
		DialTimeout: time.Second * 30,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionZSTD,
		},
	})
	if err != nil {
		return nil, err
	}
	c := ClickhouseEventsLog{
		Service:               base,
		conn:                  conn,
		eventsBuffer:          make([]*ActorEvent, 0, 1000),
		periodicFlushInterval: time.Second * 5,
		closeChan:             make(chan struct{}),
	}
	c.Start()
	return &c, nil
}

func (r *ClickhouseEventsLog) Start() {
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(r.periodicFlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.flush()
			case <-r.closeChan:
				return
			}
		}
	})
}

func (r *ClickhouseEventsLog) flush() {
	r.Lock()
	if len(r.eventsBuffer) == 0 {
		r.Unlock()
		return
	}
	tm := time.Now()
	bufferCopy := slices.Clone(r.eventsBuffer)
	clear(r.eventsBuffer)
	r.eventsBuffer = r.eventsBuffer[:0]
	r.Unlock()
	batch, err := r.conn.PrepareBatch(context.Background(), "INSERT INTO events_log")
	if err != nil {
		r.Errorf("Error preparing batch: %v", err)
		return
	}
	dt := time.Now()
	for _, event := range bufferCopy {
		bytes, _ := json.Marshal(event.Event)
		err = batch.Append(
			dt,
			event.ActorId,
			string(event.EventType),
			string(event.Level),
			string(bytes),
		)
		if err != nil {
			r.Errorf("Error appending to batch: %v", err)
			continue
		}
	}
	err = batch.Send()
	if err != nil {
		r.Errorf("Error sending batch: %v", err)
	} else {
		r.Infof("Inserted %d events in %v", len(bufferCopy), time.Since(tm))
	}
}

func (r *ClickhouseEventsLog) PostAsync(event *ActorEvent) {
	if event == nil {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.eventsBuffer = append(r.eventsBuffer, event)
}

func (r *ClickhouseEventsLog) PostEvent(event *ActorEvent) (id EventsLogRecordId, err error) {
	return "", fmt.Errorf("not implemented")
}

func (r *ClickhouseEventsLog) GetEvents(eventType EventType, actorId string, level string, filter *EventsLogFilter, limit int) ([]EventsLogRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *ClickhouseEventsLog) Close() error {
	r.closeChan <- struct{}{}
	_ = r.conn.Close()
	return nil
}
