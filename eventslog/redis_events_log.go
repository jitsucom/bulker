package eventslog

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	jsoniter "github.com/json-iterator/go"
	"regexp"
	"strings"
	"sync"
	"time"
)

const redisEventsLogServiceName = "redis_events_log"

const redisEventsLogStreamKey = "events_log:%s.%s#%s"

var redisStreamIdTimestampPart = regexp.MustCompile(`^\d{13}`)

type RedisEventsLog struct {
	sync.Mutex
	appbase.Service
	redisPool             *redis.Pool
	maxSize               int
	eventsBuffer          map[string][]*ActorEvent
	periodicFlushInterval time.Duration
	closeChan             chan struct{}
}

func NewRedisEventsLog(redisUrl, redisTLSCA string, maxLogSize int) (EventsLogService, error) {
	base := appbase.NewServiceBase(redisEventsLogServiceName)
	base.Debugf("Creating RedisEventsLog with redisURL: %s", redisUrl)
	redisPool := newPool(redisUrl, redisTLSCA)
	r := RedisEventsLog{
		Service:               base,
		redisPool:             redisPool,
		maxSize:               maxLogSize,
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
					EventsLogError("marshal_error").Inc()
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
		EventsLogError(RedisError(err)).Inc()
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
	levels := mapLevel(string(event.Level))
	for _, level := range levels {
		key := fmt.Sprintf(redisEventsLogStreamKey, event.EventType, level, event.ActorId)
		buf, ok := r.eventsBuffer[key]
		if !ok {
			buf = []*ActorEvent{event}
			r.eventsBuffer[key] = buf
		} else if len(buf) < r.maxSize {
			buf = append(buf, event)
			r.eventsBuffer[key] = buf
		}
	}
}

func (r *RedisEventsLog) PostEvent(event *ActorEvent) (id EventsLogRecordId, err error) {
	if event == nil {
		return "", nil
	}
	connection := r.redisPool.Get()
	defer connection.Close()
	level := mapLevel(string(event.Level))[0]
	streamKey := fmt.Sprintf(redisEventsLogStreamKey, event.EventType, level, event.ActorId)
	serialized, ok := event.Event.([]byte)
	if !ok {
		serialized, err = jsoniter.Marshal(event.Event)
		if err != nil {
			EventsLogError("marshal_error").Inc()
			return "", r.NewError("failed to serialize event entity [%v]: %v", event.Event, err)
		}
	}
	idString, err := redis.String(connection.Do("XADD", streamKey, "MAXLEN", "~", r.maxSize, "*", "event", serialized))
	if err != nil {
		EventsLogError(RedisError(err)).Inc()
		return "", r.NewError("failed to post event to stream [%s]: %v", streamKey, err)
	}
	return EventsLogRecordId(idString), nil
}

func mapLevel(level string) []string {
	if level == "error" {
		return []string{"error", "all"}
	}
	return []string{"all"}
}

func (r *RedisEventsLog) GetEvents(eventType EventType, actorId string, level string, filter *EventsLogFilter, limit int) ([]EventsLogRecord, error) {
	level = mapLevel(level)[0]
	streamKey := fmt.Sprintf(redisEventsLogStreamKey, eventType, level, actorId)

	start, end, err := filter.GetStartAndEndIds()
	if err != nil {
		EventsLogError("filter_error").Inc()
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
		EventsLogError(RedisError(err)).Inc()
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
			EventsLogError("unmarshal_error").Inc()
			return nil, r.NewError("failed to unmarshal event from stream [%s] %s: %v", streamKey, mp["event"], err)
		}
		date, err := parseTimestamp(id)
		if err != nil {
			EventsLogError("parse_timestamp_error").Inc()
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

func (r *RedisEventsLog) Close() error {
	r.closeChan <- struct{}{}
	r.redisPool.Close()
	return nil
}

func newPool(redisURL string, ca string) *redis.Pool {
	opts := make([]redis.DialOption, 0)
	if ca != "" || strings.HasPrefix(redisURL, "rediss://") {
		tlsConfig := tls.Config{InsecureSkipVerify: true}
		if ca != "" {
			rootCAs, _ := x509.SystemCertPool()
			if rootCAs == nil {
				rootCAs = x509.NewCertPool()
			}
			rootCAs.AppendCertsFromPEM([]byte(ca))
			tlsConfig.RootCAs = rootCAs
		}
		opts = append(opts, redis.DialUseTLS(true), redis.DialTLSConfig(&tlsConfig))
	}

	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		// Dial or DialContext must be set. When both are set, DialContext takes precedence over Dial.
		Dial: func() (redis.Conn, error) { return redis.DialURL(redisURL, opts...) },
	}
}

func RedisError(err error) string {
	if err == nil {
		return ""
	}
	if err == redis.ErrPoolExhausted {
		return "redis error: ERR_POOL_EXHAUSTED"
	} else if err == redis.ErrNil {
		return "redis error: ERR_NIL"
	} else if strings.Contains(strings.ToLower(err.Error()), "timeout") {
		return "redis error: ERR_TIMEOUT"
	} else {
		return "redis error: UNKNOWN"
	}
}
