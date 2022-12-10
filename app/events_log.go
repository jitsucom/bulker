package app

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/jitsucom/bulker/app/metrics"
	"github.com/jitsucom/bulker/base/objects"
	jsoniter "github.com/json-iterator/go"
	"io"
	"regexp"
	"strconv"
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

	EventTypeProcessedAll   EventType = "processed.all"
	EventTypeProcessedError EventType = "processed.error"

	EventTypeBatchAll   EventType = "batch.all"
	EventTypeBatchError EventType = "batch.error"
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

type EventsLogService interface {
	io.Closer
	// PostEvent posts event to the events log
	// actorId – id of entity of event origin. E.g. for 'incoming' event - id of site, for 'processed' event - id of destination
	PostEvent(eventType EventType, actorId string, event any) (id EventsLogRecordId, err error)

	GetEvents(eventType EventType, actorId string, filter *EventsLogFilter, limit int) ([]EventsLogRecord, error)
}

type RedisEventsLog struct {
	objects.ServiceBase
	redisPool *redis.Pool
	maxSize   int
}

func NewRedisEventsLog(config *AppConfig) (*RedisEventsLog, error) {
	base := objects.NewServiceBase(redisEventsLogServiceName)
	base.Debugf("Creating RedisEventsLog with redisURL: %s", config.EventsLogRedisURL)
	redisPool := newPool(config.EventsLogRedisURL, config.RedisTLSCA)
	r := RedisEventsLog{
		ServiceBase: base,
		redisPool:   redisPool,
		maxSize:     config.EventsLogMaxSize,
	}
	return &r, nil
}

func (r *RedisEventsLog) PostEvent(eventType EventType, actorId string, event any) (id EventsLogRecordId, err error) {
	serialized, err := jsoniter.Marshal(event)
	if err != nil {
		metrics.EventsLogError("marshal_error").Inc()
		return "", r.NewError("failed to serialize event entity [%v]: %w", event, err)
	}

	connection := r.redisPool.Get()
	defer connection.Close()

	streamKey := fmt.Sprintf(redisEventsLogStreamKey, eventType, actorId)

	idString, err := redis.String(connection.Do("XADD", streamKey, "MAXLEN", "~", r.maxSize, "*", "event", serialized))
	if err != nil {
		metrics.EventsLogError(RedisError(err)).Inc()
		return "", r.NewError("failed to post event to stream [%s]: %w", streamKey, err)
	}
	return EventsLogRecordId(idString), nil
}

func (r *RedisEventsLog) GetEvents(eventType EventType, actorId string, filter *EventsLogFilter, limit int) ([]EventsLogRecord, error) {
	streamKey := fmt.Sprintf(redisEventsLogStreamKey, eventType, actorId)

	start, end, err := filter.GetStartAndEndIds()
	if err != nil {
		metrics.EventsLogError("filter_error").Inc()
		return nil, r.NewError("%w", err)
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
		return nil, r.NewError("failed to get events from stream [%s]: %w", streamKey, err)
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
			return nil, r.NewError("failed to unmarshal event from stream [%s] %s: %w", streamKey, mp["event"], err)
		}
		date, err := parseTimestamp(id)
		if err != nil {
			metrics.EventsLogError("parse_timestamp_error").Inc()
			return nil, r.NewError("failed to parse timestamp from id [%s]: %w", id, err)
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

func (d *DummyEventsLogService) PostEvent(eventType EventType, actorId string, event any) (id EventsLogRecordId, err error) {
	return "", nil
}

func (d *DummyEventsLogService) GetEvents(eventType EventType, actorId string, filter *EventsLogFilter, limit int) ([]EventsLogRecord, error) {
	return nil, nil
}

func (d *DummyEventsLogService) Close() error {
	return nil
}
