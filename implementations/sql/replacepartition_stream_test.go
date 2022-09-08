package sql

import (
	"github.com/jitsucom/bulker/bulker"
	"sync"
	"testing"
)

// TestReplacePartitionStream sequentially runs 3 replace partition streams without dropping table in between
func TestReplacePartitionStream(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:                "first_partition",
			tableName:           "replace_partition_test",
			modes:               []bulker.BulkMode{bulker.ReplacePartition},
			leaveResultingTable: true,
			dataFile:            "test_data/partition1.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 3, "name": "test3", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 4, "name": "test4", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 5, "name": "test5", "__partition_id": "1"},
			},
			streamOptions: []bulker.StreamOption{WithPartition("1")},
			bulkerTypes:   allBulkerTypes,
		},
		{
			name:                "second_partition",
			tableName:           "replace_partition_test",
			modes:               []bulker.BulkMode{bulker.ReplacePartition},
			leaveResultingTable: true,
			dataFile:            "test_data/partition2.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 3, "name": "test3", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 4, "name": "test4", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 5, "name": "test5", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 11, "name": "test11", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 12, "name": "test12", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 13, "name": "test13", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 14, "name": "test14", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 15, "name": "test15", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 16, "name": "test16", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 17, "name": "test17", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 18, "name": "test18", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 19, "name": "test19", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 20, "name": "test20", "__partition_id": "2"},
			},
			streamOptions: []bulker.StreamOption{WithPartition("2")},
			bulkerTypes:   allBulkerTypes,
		},
		{
			name:                "first_partition_reprocess",
			tableName:           "replace_partition_test",
			modes:               []bulker.BulkMode{bulker.ReplacePartition},
			leaveResultingTable: true,
			dataFile:            "test_data/partition1_1.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 6, "name": "test6", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 7, "name": "test7", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 8, "name": "test8", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 9, "name": "test9", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 10, "name": "test10", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 11, "name": "test11", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 12, "name": "test12", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 13, "name": "test13", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 14, "name": "test14", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 15, "name": "test15", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 16, "name": "test16", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 17, "name": "test17", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 18, "name": "test18", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 19, "name": "test19", "__partition_id": "2"},
				{"_timestamp": constantTime, "id": 20, "name": "test20", "__partition_id": "2"},
			},
			streamOptions: []bulker.StreamOption{WithPartition("1")},
			bulkerTypes:   allBulkerTypes,
		},
		{
			name:                "second_partition_empty",
			tableName:           "replace_partition_test",
			leaveResultingTable: true,
			modes:               []bulker.BulkMode{bulker.ReplacePartition},
			dataFile:            "test_data/empty.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 6, "name": "test6", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 7, "name": "test7", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 8, "name": "test8", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 9, "name": "test9", "__partition_id": "1"},
				{"_timestamp": constantTime, "id": 10, "name": "test10", "__partition_id": "1"},
			},
			streamOptions: []bulker.StreamOption{WithPartition("2")},
			bulkerTypes:   allBulkerTypes,
		},
		{
			name:          "dummy_test_table_cleanup",
			tableName:     "replace_partition_test",
			modes:         []bulker.BulkMode{bulker.ReplacePartition},
			dataFile:      "test_data/empty.ndjson",
			streamOptions: []bulker.StreamOption{WithPartition("1")},
			bulkerTypes:   allBulkerTypes,
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
