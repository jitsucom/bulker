package app

import (
	"context"
	"github.com/jitsucom/bulker/bulkerapp/app/testcontainers"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEventsLog(t *testing.T) {
	t.Parallel()
	reqr := require.New(t)

	redis, err := testcontainers.NewRedisContainer(context.Background())
	reqr.NoError(err)
	defer redis.Close()

	//1519073278252
	//1668686118735
	appConfig := Config{}
	appConfig.EventsLogMaxSize = 1000
	appConfig.EventsLogRedisURL = redis.URL()
	redisEl, err := NewRedisEventsLog(&appConfig, redis.URL())
	reqr.NoError(err)

	var tsStart, tsEnd time.Time
	var idBefore EventsLogRecordId
	for i := 0; i < 100; i++ {
		ts := time.Now()
		event := map[string]any{
			"test":       "test",
			"_timestamp": ts,
			"id":         i,
		}
		id, err := redisEl.PostEvent(EventTypeIncomingAll, "test", event)
		reqr.NoError(err)
		logging.Infof("Posted event %s", id)
		// for latter testing of interval filters
		if i == 35 {
			tsStart, _ = parseTimestamp(string(id))
		}
		if i == 55 {
			idBefore = id
		}
		if i == 65 {
			tsEnd, _ = parseTimestamp(string(id))
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Get last 10 events
	events, err := redisEl.GetEvents(EventTypeIncomingAll, "test", nil, 10)
	reqr.NoError(err)
	reqr.Len(events, 10)
	for _, event := range events {
		logging.Infof("Id: %s Content: %+v Date: %s", event.Id, event.Content, event.Date)
	}

	// Get consequent 10 events
	lastEvent := events[len(events)-1]
	beforeId := lastEvent.Id
	lastEventId := lastEvent.Content.(map[string]any)["id"].(float64)
	//test beforeId filter
	events, err = redisEl.GetEvents(EventTypeIncomingAll, "test", &EventsLogFilter{BeforeId: beforeId}, 10)
	reqr.NoError(err)
	reqr.Len(events, 10)
	event := events[0]
	firstEventId := event.Content.(map[string]any)["id"].(float64)
	logging.Infof("BeforeId filter => Id: %s Content: %+v Date: %s", event.Id, event.Content, event.Date)
	reqr.Equal(lastEventId-1, firstEventId)

	// Test interval filter
	logging.Infof("Test interval filter from %d to %d", tsStart.UnixMilli(), tsEnd.UnixMilli())
	events, err = redisEl.GetEvents(EventTypeIncomingAll, "test", &EventsLogFilter{Start: tsStart, End: tsEnd}, 0)
	reqr.NoError(err)
	reqr.Len(events, 31)
	firstEventId = events[0].Content.(map[string]any)["id"].(float64)
	reqr.Equal(65, int(firstEventId))
	lastEventId = events[len(events)-1].Content.(map[string]any)["id"].(float64)
	reqr.Equal(35, int(lastEventId))

	// Test interval filter with beforeId
	logging.Infof("Test interval filter from %d to %d", tsStart.UnixMilli(), tsEnd.UnixMilli())
	events, err = redisEl.GetEvents(EventTypeIncomingAll, "test", &EventsLogFilter{Start: tsStart, End: tsEnd, BeforeId: idBefore}, 0)
	reqr.NoError(err)
	reqr.Len(events, 20)
	firstEventId = events[0].Content.(map[string]any)["id"].(float64)
	reqr.Equal(54, int(firstEventId))
	lastEventId = events[len(events)-1].Content.(map[string]any)["id"].(float64)
	reqr.Equal(35, int(lastEventId))

}
