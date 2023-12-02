package sql

import (
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"sync"
	"testing"
)

// TestReplaceTableStream sequentially runs 3 replace table streams without dropping table in between
func TestReplaceTableStream(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			//delete any table leftovers from previous tests
			name:      "dummy_test_table_cleanup",
			tableName: "replace_table_test",
			modes:     []bulker.BulkMode{bulker.ReplaceTable},
			dataFile:  "test_data/empty.ndjson",
			configIds: allBulkerConfigs,
		},
		{
			name:                "first_run",
			tableName:           "replace_table_test",
			modes:               []bulker.BulkMode{bulker.ReplaceTable},
			leaveResultingTable: true,
			dataFile:            "test_data/partition1.ndjson",
			expectedRowsCount:   5,
			configIds:           allBulkerConfigs,
		},
		{
			name:                "second_run",
			tableName:           "replace_table_test",
			modes:               []bulker.BulkMode{bulker.ReplaceTable},
			leaveResultingTable: true,
			dataFile:            "test_data/replace_table.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 11, "name": "test11", "name2": "a"},
				{"_timestamp": constantTime, "id": 12, "name": "test12", "name2": "a"},
				{"_timestamp": constantTime, "id": 13, "name": "test13", "name2": "a"},
				{"_timestamp": constantTime, "id": 14, "name": "test14", "name2": "a"},
				{"_timestamp": constantTime, "id": 15, "name": "test15", "name2": "a"},
				{"_timestamp": constantTime, "id": 16, "name": "test16", "name2": "a"},
				{"_timestamp": constantTime, "id": 17, "name": "test17", "name2": "a"},
				{"_timestamp": constantTime, "id": 18, "name": "test18", "name2": "a"},
				{"_timestamp": constantTime, "id": 19, "name": "test19", "name2": "a"},
				{"_timestamp": constantTime, "id": 20, "name": "test20", "name2": "a"},
			},
			configIds: allBulkerConfigs,
		},
		{
			name:                "empty_run",
			tableName:           "replace_table_test",
			leaveResultingTable: true,
			modes:               []bulker.BulkMode{bulker.ReplaceTable},
			dataFile:            "test_data/empty.ndjson",
			expectedRowsCount:   0,
			configIds:           allBulkerConfigs,
		},
		{
			name:      "dummy_test_table_cleanup",
			tableName: "replace_table_test",
			modes:     []bulker.BulkMode{bulker.ReplaceTable},
			dataFile:  "test_data/empty.ndjson",
			configIds: allBulkerConfigs,
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
