package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		assertErr     assert.ErrorAssertionFunc
	}{
		{
			desc:          "should return valid topic ID",
			destinationId: "clyzlw-",
			mode:          "batch",
			tableName:     "events",
			expected:      "in.id.clyzlw-.m.batch.t.events",
			assertErr:     assert.NoError,
		},
		{
			desc:          "should return valid topic ID with prefix",
			destinationId: "clyzlw-",
			mode:          "batch",
			tableName:     "events",
			prefix:        "some.prefix.",
			expected:      "some.prefix.in.id.clyzlw-.m.batch.t.events",
			assertErr:     assert.NoError,
		},
		{
			desc:          "should return error when checkLength is true and topic ID is too long",
			destinationId: "clyzlw-",
			mode:          "batch",
			tableName:     "events",
			prefix:        "too.looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong",
			checkLength:   true,
			expected:      "",
			assertErr:     assert.Error,
		},
	}
	for _, tC := range testCases {
		tc := tC

		t.Run(tc.desc, func(t *testing.T) {
			topicId, err := MakeTopicId(tc.destinationId, tc.mode, tc.tableName, tc.prefix, tc.checkLength)

			tc.assertErr(t, err)

			assert.Equal(t, tc.expected, topicId)
		})
	}
}
