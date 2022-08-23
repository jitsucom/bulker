package sql

import (
	"github.com/jitsucom/bulker/bulker"
	"sync"
	"testing"
)

// TestReplaceTableStream sequentially runs 3 replace table streams without dropping table in between
func TestReplaceTableStream(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:                "first_run",
			tableName:           "replace_table_test",
			modes:               []bulker.BulkMode{bulker.ReplaceTable},
			leaveResultingTable: true,
			dataFile:            "test_data/partition1.ndjson",
			expectedRowsCount:   5,
			bulkerTypes:         []string{"postgres"},
		},
		{
			name:                "second_run",
			tableName:           "replace_table_test",
			modes:               []bulker.BulkMode{bulker.ReplaceTable},
			leaveResultingTable: true,
			dataFile:            "test_data/partition2.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 11, "name": "test11"},
				{"_timestamp": constantTime, "id": 12, "name": "test12"},
				{"_timestamp": constantTime, "id": 13, "name": "test13"},
				{"_timestamp": constantTime, "id": 14, "name": "test14"},
				{"_timestamp": constantTime, "id": 15, "name": "test15"},
				{"_timestamp": constantTime, "id": 16, "name": "test16"},
				{"_timestamp": constantTime, "id": 17, "name": "test17"},
				{"_timestamp": constantTime, "id": 18, "name": "test18"},
				{"_timestamp": constantTime, "id": 19, "name": "test19"},
				{"_timestamp": constantTime, "id": 20, "name": "test20"},
			},
			bulkerTypes: []string{"postgres"},
		},
		{
			name:                "empty_run",
			tableName:           "replace_table_test",
			leaveResultingTable: true,
			modes:               []bulker.BulkMode{bulker.ReplaceTable},
			dataFile:            "test_data/empty.ndjson",
			expectedRowsCount:   0,
			bulkerTypes:         []string{"postgres"},
		},
		{
			name:        "dummy_test_table_cleanup",
			tableName:   "replace_table_test",
			modes:       []bulker.BulkMode{bulker.ReplaceTable},
			dataFile:    "test_data/empty.ndjson",
			bulkerTypes: []string{"postgres"},
		},
	}
	sequentialGroup := sync.WaitGroup{}
	sequentialGroup.Add(1)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testStream)
			sequentialGroup.Done()
		})
		sequentialGroup.Wait()
		sequentialGroup.Add(1)
	}
}
