package app

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	jsoniter "github.com/json-iterator/go"
	"io"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type EventType string
type EventStatus string

type EventsLogRecordId string

const redisEventsLogServiceName = "redis_events_log"

const redisEventsLogStreamKey = "events_log:%s#%s"

const (
	EventTypeIncomingAll   EventType = "incoming.all"
	EventTypeIncomingError EventType = "incoming.error"

	EventTypeProcessedAll   EventType = "bulker_stream.all"
	EventTypeProcessedError EventType = "bulker_stream.error"

	EventTypeBatchAll   EventType = "bulker_batch.all"
	EventTypeBatchError EventType = "bulker_batch.error"
)

var redisStreamIdTimestampPart = regexp.MustCompile(`^\d{13}`)

type EventsLogFilter struct {
	Start    time.Time
	End      time.Time
	BeforeId EventsLogRecordId
	Filter   func(event any) bool
}

type EventsLogRecord struct {
	Id      EventsLogRecordId `json:"id"`
	Date    time.Time         `json:"date"`
	Content any               `json:"content"`
}

type ActorEvent struct {
	EventType EventType
	ActorId   string
	Event     any
}

type EventsLogService interface {
	io.Closer
	// PostEvent posts event to the events log
	// actorId – id of entity of event origin. E.g. for 'incoming' event - id of site, for 'processed' event - id of destination
	PostEvent(event *ActorEvent) (id EventsLogRecordId, err error)

	PostAsync(event *ActorEvent)

	GetEvents(eventType EventType, actorId string, filter *EventsLogFilter, limit int) ([]EventsLogRecord, error)
}

type RedisEventsLog struct {
	sync.Mutex
	appbase.Service
	redisPool             *redis.Pool
	maxSize               int
	eventsBuffer          map[string][]*ActorEvent
	periodicFlushInterval time.Duration
	closeChan             chan struct{}
}

func NewRedisEventsLog(config *Config, redisUrl string) (*RedisEventsLog, error) {
	base := appbase.NewServiceBase(redisEventsLogServiceName)
	base.Debugf("Creating RedisEventsLog with redisURL: %s", redisUrl)
	redisPool := newPool(redisUrl, config.RedisTLSCA)
	r := RedisEventsLog{
		Service:               base,
		redisPool:             redisPool,
		maxSize:               config.EventsLogMaxSize,
		eventsBuffer:          make(map[string][]*ActorEvent),
		periodicFlushInterval: time.Second * 5,
		closeChan:             make(chan struct{}),
	}
	r.Start()
	return &r, nil
}

func (r *RedisEventsLog) Start() {
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(r.periodicFlushInterval)
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

func (r *RedisEventsLog) flush() {
	r.Lock()
	defer r.Unlock()
	if len(r.eventsBuffer) == 0 {
		return
	}
	connection := r.redisPool.Get()
	defer connection.Close()
	_ = connection.Send("MULTI")
	for streamKey, events := range r.eventsBuffer {
		for i, event := range events {
			serialized, ok := event.Event.([]byte)
			if !ok {
				var err error
				serialized, err = jsoniter.Marshal(event.Event)
				if err != nil {
					metrics.EventsLogError("marshal_error").Inc()
					r.Errorf("failed to serialize event entity [%v]: %v", event.Event, err)
					continue
				}
			}
			if i == len(events)-1 {
				r.Debugf("Posting %d events to stream [%s]", len(events), streamKey)
				_ = connection.Send("XADD", streamKey, "MAXLEN", "~", r.maxSize, "*", "event", serialized)
			} else {
				_ = connection.Send("XADD", streamKey, "*", "event", serialized)
			}
		}
	}
	_, err := connection.Do("EXEC")
	if err != nil {
		metrics.EventsLogError(RedisError(err)).Inc()
		r.Errorf("failed to post events: %v", err)
	} else {
		clear(r.eventsBuffer)
	}
}

func (r *RedisEventsLog) PostAsync(event *ActorEvent) {
	if event == nil {
		return
	}
	r.Lock()
	defer r.Unlock()
	key := fmt.Sprintf(redisEventsLogStreamKey, event.EventType, event.ActorId)
	buf, ok := r.eventsBuffer[key]
	if !ok {
		buf = []*ActorEvent{event}
		r.eventsBuffer[key] = buf
	} else if len(buf) < r.maxSize {
		buf = append(buf, event)
		r.eventsBuffer[key] = buf
	}
}

func (r *RedisEventsLog) PostEvent(event *ActorEvent) (id EventsLogRecordId, err error) {
	if event == nil {
		return "", nil
	}
	connection := r.redisPool.Get()
	defer connection.Close()

	streamKey := fmt.Sprintf(redisEventsLogStreamKey, event.EventType, event.ActorId)
	serialized, ok := event.Event.([]byte)
	if !ok {
		serialized, err = jsoniter.Marshal(event.Event)
		if err != nil {
			metrics.EventsLogError("marshal_error").Inc()
			return "", r.NewError("failed to serialize event entity [%v]: %v", event.Event, err)
		}
	}
	idString, err := redis.String(connection.Do("XADD", streamKey, "MAXLEN", "~", r.maxSize, "*", "event", serialized))
	if err != nil {
		metrics.EventsLogError(RedisError(err)).Inc()
		return "", r.NewError("failed to post event to stream [%s]: %v", streamKey, err)
	}
	return EventsLogRecordId(idString), nil
}

func (r *RedisEventsLog) GetEvents(eventType EventType, actorId string, filter *EventsLogFilter, limit int) ([]EventsLogRecord, error) {
	streamKey := fmt.Sprintf(redisEventsLogStreamKey, eventType, actorId)

	start, end, err := filter.GetStartAndEndIds()
	if err != nil {
		metrics.EventsLogError("filter_error").Inc()
		return nil, r.NewError("%v", err)
	}
	args := []interface{}{streamKey, end, start}
	if limit > 0 {
		args = append(args, "COUNT", limit)
	}
	connection := r.redisPool.Get()
	defer connection.Close()

	recordsRaw, err := connection.Do("XREVRANGE", args...)
	if err != nil {
		metrics.EventsLogError(RedisError(err)).Inc()
		return nil, r.NewError("failed to get events from stream [%s]: %v", streamKey, err)
	}
	records := recordsRaw.([]any)
	//r.Infof("Got %d events from stream [%s]", len(records), streamKey)
	results := make([]EventsLogRecord, 0, len(records))
	for _, record := range records {
		rec := record.([]any)
		id, _ := redis.String(rec[0], nil)
		mp, _ := redis.StringMap(rec[1], nil)
		//r.Infof("id: %s mp: %+v", id, mp)
		var event map[string]interface{}
		err = jsoniter.Unmarshal([]byte(mp["event"]), &event)
		if err != nil {
			metrics.EventsLogError("unmarshal_error").Inc()
			return nil, r.NewError("failed to unmarshal event from stream [%s] %s: %v", streamKey, mp["event"], err)
		}
		date, err := parseTimestamp(id)
		if err != nil {
			metrics.EventsLogError("parse_timestamp_error").Inc()
			return nil, r.NewError("failed to parse timestamp from id [%s]: %v", id, err)
		}
		if (filter == nil || filter.Filter == nil) || filter.Filter(event) {
			results = append(results, EventsLogRecord{
				Id:      EventsLogRecordId(id),
				Content: event,
				Date:    date,
			})
		}

	}
	return results, nil
}

// GetStartAndEndIds returns end and start ids for the stream
func (f *EventsLogFilter) GetStartAndEndIds() (start, end string, err error) {
	end = "+"
	start = "-"
	if f == nil {
		return
	}
	var endTime int64
	if f.BeforeId != "" {
		end = fmt.Sprintf("(%s", f.BeforeId)
		tsTime, err := parseTimestamp(string(f.BeforeId))
		if err != nil {
			return "", "", err
		}
		endTime = tsTime.UnixMilli()
	}
	if !f.End.IsZero() {
		if endTime == 0 || f.End.UnixMilli() < endTime {
			end = fmt.Sprint(f.End.UnixMilli())
		}
	}
	if !f.Start.IsZero() {
		start = fmt.Sprint(f.Start.UnixMilli())
	}

	return
}

func (r *RedisEventsLog) Close() error {
	r.closeChan <- struct{}{}
	r.redisPool.Close()
	return nil
}

func parseTimestamp(id string) (time.Time, error) {
	match := redisStreamIdTimestampPart.FindStringSubmatch(id)
	if match == nil {
		return time.Time{}, fmt.Errorf("failed to parse beforeId [%s] it is expected to start with timestamp", id)
	}
	ts, _ := strconv.ParseInt(match[0], 10, 64)
	return time.UnixMilli(ts), nil
}

type DummyEventsLogService struct{}

func (d *DummyEventsLogService) PostAsync(_ *ActorEvent) {
}

func (d *DummyEventsLogService) PostEvent(_ *ActorEvent) (id EventsLogRecordId, err error) {
	return "", nil
}

func (d *DummyEventsLogService) GetEvents(_ EventType, _ string, _ *EventsLogFilter, _ int) ([]EventsLogRecord, error) {
	return nil, nil
}

func (d *DummyEventsLogService) Close() error {
	return nil
}
