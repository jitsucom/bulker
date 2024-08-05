package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeTopicId(t *testing.T) {
	testCases := []struct {
		desc          string
		destinationId string
		mode          string
		tableName     string
		prefix        string
		checkLength   bool
		expected      string
		requireErr    require.ErrorAssertionFunc
	}{
		{
			desc:          "should return valid topic ID",
			destinationId: "clyzlw-",
			mode:          "batch",
			tableName:     "events",
			expected:      "in.id.clyzlw-.m.batch.t.events",
			requireErr:    require.NoError,
		},
		{
			desc:          "should return valid topic ID with prefix",
			destinationId: "clyzlw-",
			mode:          "batch",
			tableName:     "events",
			prefix:        "some.prefix.",
			expected:      "some.prefix.in.id.clyzlw-.m.batch.t.events",
			requireErr:    require.NoError,
		},
		{
			desc:          "should return error when checkLength is true and topic ID is too long",
			destinationId: "clyzlw-",
			mode:          "batch",
			tableName:     "events",
			prefix:        "too.looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong",
			checkLength:   true,
			requireErr:    require.Error,
		},
	}
	for _, tC := range testCases {
		tc := tC

		t.Run(tc.desc, func(t *testing.T) {
			topicId, err := MakeTopicId(tc.destinationId, tc.mode, tc.tableName, tc.prefix, tc.checkLength)

			tc.requireErr(t, err)

			assert.Equal(t, tc.expected, topicId)
		})
	}
}

func TestParseTopicId(t *testing.T) {
	testCases := []struct {
		desc                  string
		topic                 string
		expectedDestinationId string
		expectedMode          string
		expectedTableName     string
		requireErr            require.ErrorAssertionFunc
	}{
		{
			desc:                  "should parse a batch events topic",
			topic:                 "in.id.clyzlw9m10006az0lhwqg34u9-pq7h-vjyr-gKxdFT.m.batch.t.events",
			expectedDestinationId: "clyzlw9m10006az0lhwqg34u9-pq7h-vjyr-gKxdFT",
			expectedMode:          "batch",
			expectedTableName:     "events",
			requireErr:            require.NoError,
		},
		{
			desc:                  "should parse a batch events topic with prefix",
			topic:                 "prefix.with.dots.in.id.clyzlw9m10006az0lhwqg34u9-pq7h-vjyr-gKxdFT.m.batch.t.events",
			expectedDestinationId: "clyzlw9m10006az0lhwqg34u9-pq7h-vjyr-gKxdFT",
			expectedMode:          "batch",
			expectedTableName:     "events",
			requireErr:            require.NoError,
		},
		{
			desc:       "should return error when 'in.id' is missing",
			topic:      "clyzlw9m10006az0lhwqg34u9-pq7h-vjyr-gKxdFT.m.batch.t.events",
			requireErr: require.Error,
		},
		{
			desc:       "should return error when there are too many sub-strings",
			topic:      "in.id.extra-substring.clyzlw9m10006az0lhwqg34u9-pq7h-vjyr-gKxdFT.m.batch.t.events",
			requireErr: require.Error,
		},
	}
	for _, tC := range testCases {
		tc := tC

		t.Run(tC.desc, func(t *testing.T) {
			destinationId, mode, tableName, err := ParseTopicId(tc.topic)

			tc.requireErr(t, err)

			assert.Equal(t, tc.expectedDestinationId, destinationId)
			assert.Equal(t, tc.expectedMode, mode)
			assert.Equal(t, tc.expectedTableName, tableName)
		})
	}
}
