package main

import (
	"testing"
	"time"

	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/stretchr/testify/assert"
)

func TestDeduplicateBatch(t *testing.T) {
	tests := []struct {
		name     string
		batch    []types.Json
		gapMs    int
		expected int // expected number of events after deduplication
	}{
		{
			name:     "empty batch",
			batch:    []types.Json{},
			gapMs:    1000,
			expected: 0,
		},
		{
			name: "single event",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), nil, nil),
			},
			gapMs:    1000,
			expected: 1,
		},
		{
			name: "duplicate events within gap",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), types.JsonFromKV("prop", "value", "prop1", "value1"), types.JsonFromKV("trait", "value", "trait1", "value1")),
				createEvent("anon1", "user1", "track", "test_event", time.Now().Add(500*time.Millisecond), types.JsonFromKV("prop", "value", "prop1", "value1"), types.JsonFromKV("trait", "value", "trait1", "value1")),
			},
			gapMs:    1000,
			expected: 1, // second event should be filtered
		},
		{
			name: "duplicate events outside gap",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), types.JsonFromKV("prop", "value", "prop1", "value1"), types.JsonFromKV("trait", "value", "trait1", "value1")),
				createEvent("anon1", "user1", "track", "test_event", time.Now().Add(1500*time.Millisecond), types.JsonFromKV("prop", "value", "prop1", "value1"), types.JsonFromKV("trait", "value", "trait1", "value1")),
			},
			gapMs:    1000,
			expected: 2, // both events should be kept
		},
		{
			name: "different anonymousId",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), nil, nil),
				createEvent("anon2", "user1", "track", "test_event", time.Now(), nil, nil),
			},
			gapMs:    1000,
			expected: 2, // different anonymousId means different events
		},
		{
			name: "different userId",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), nil, nil),
				createEvent("anon1", "user2", "track", "test_event", time.Now(), nil, nil),
			},
			gapMs:    1000,
			expected: 2, // different userId means different events
		},
		{
			name: "different event type",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), nil, nil),
				createEvent("anon1", "user1", "identify", "test_event", time.Now(), nil, nil),
			},
			gapMs:    1000,
			expected: 2, // different type means different events
		},
		{
			name: "different event name",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "event1", time.Now(), nil, nil),
				createEvent("anon1", "user1", "track", "event2", time.Now(), nil, nil),
			},
			gapMs:    1000,
			expected: 2, // different event name means different events
		},
		{
			name: "different properties",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), types.JsonFromKV("prop", "value1"), nil),
				createEvent("anon1", "user1", "track", "test_event", time.Now(), types.JsonFromKV("prop", "value2"), nil),
			},
			gapMs:    1000,
			expected: 2, // different properties means different events
		},
		{
			name: "different traits",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), nil, types.JsonFromKV("trait", "value1")),
				createEvent("anon1", "user1", "track", "test_event", time.Now(), nil, types.JsonFromKV("trait", "value2")),
			},
			gapMs:    1000,
			expected: 2, // different traits means different events
		},
		{
			name: "multiple duplicates with one unique",
			batch: []types.Json{
				createEvent("anon1", "user1", "track", "test_event", time.Now(), types.JsonFromKV("prop", "value"), nil),
				createEvent("anon1", "user1", "track", "test_event", time.Now().Add(100*time.Millisecond), types.JsonFromKV("prop", "value"), nil),
				createEvent("anon1", "user1", "track", "test_event", time.Now().Add(200*time.Millisecond), types.JsonFromKV("prop", "value"), nil),
				createEvent("anon2", "user1", "track", "test_event", time.Now(), types.JsonFromKV("prop", "value"), nil),
			},
			gapMs:    1000,
			expected: 2, // 3 duplicates -> 1, plus 1 unique = 2 total
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deduplicateBatch(tt.batch, tt.gapMs)
			assert.Equal(t, tt.expected, len(result), "unexpected number of events after deduplication")
		})
	}
}

// Helper function to create test events
func createEvent(anonymousId, userId, eventType, eventName string, ts time.Time, properties types.Json, traits types.Json) types.Json {
	event := types.NewJson(10)
	if anonymousId != "" {
		event.Set("anonymousId", anonymousId)
	}
	if userId != "" {
		event.Set("userId", userId)
	}
	if eventType != "" {
		event.Set("type", eventType)
	}
	if eventName != "" {
		event.Set("event", eventName)
	}
	event.Set("timestamp", ts.Format(time.RFC3339Nano))

	if properties != nil {
		event.Set("properties", properties)
	}
	if traits != nil {
		event.Set("traits", traits)
	}

	return event
}
