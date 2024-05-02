package sql

import (
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"sync"
	"testing"
)

// TestTransactionalStream sequentially runs  transactional stream without dropping table in between
func TestSchemaFreeze(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			//deletes any table leftovers from previous tests
			name:      "dummy_test_table_cleanup",
			tableName: "schema_freeze_test",
			modes:     []bulker.BulkMode{bulker.Batch, bulker.Stream},
			dataFile:  "test_data/empty.ndjson",
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId, MySQLBulkerTypeId}),
		},
		{
			name:                "added_columns_first_run",
			tableName:           "schema_freeze_test",
			modes:               []bulker.BulkMode{bulker.Batch, bulker.Stream},
			leaveResultingTable: true,
			dataFile:            "test_data/columns_added.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("_timestamp", "column1", "column2", "column3", "id", "name"),
			},
			expectedRowsCount: 6,
			configIds:         utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId, MySQLBulkerTypeId}),
		},
		{
			name:                "added_columns_second_run",
			tableName:           "schema_freeze_test",
			modes:               []bulker.BulkMode{bulker.Batch, bulker.Stream},
			leaveResultingTable: true,
			dataFile:            "test_data/columns_added2.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("_timestamp", "column1", "column2", "column3", "_unmapped_data", "id", "name"),
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "column1": nil, "column2": nil, "column3": nil, "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "column1": "data", "column2": nil, "column3": nil, "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 3, "name": "test3", "column1": "data", "column2": "data", "column3": nil, "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 4, "name": "test2", "column1": "data", "column2": nil, "column3": nil, "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 5, "name": "test", "column1": nil, "column2": nil, "column3": nil, "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 6, "name": "test4", "column1": "data", "column2": "data", "column3": "data", "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 7, "name": "test", "column1": nil, "column2": nil, "column3": nil, "_unmapped_data": "{\"column4\": \"data\"}"},
				{"_timestamp": constantTime, "id": 8, "name": "test2", "column1": nil, "column2": nil, "column3": nil, "_unmapped_data": "{\"column5\": \"data\"}"},
			},
			configIds:     utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId, MySQLBulkerTypeId}),
			streamOptions: []bulker.StreamOption{WithSchemaFreeze()},
		},
		{
			name:          "dummy_test_table_cleanup",
			tableName:     "schema_freeze_test",
			modes:         []bulker.BulkMode{bulker.Batch, bulker.Stream},
			dataFile:      "test_data/empty.ndjson",
			configIds:     utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId, MySQLBulkerTypeId}),
			streamOptions: []bulker.StreamOption{WithSchemaFreeze()},
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

func TestMaxColumns(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			name:                "added_columns_first_run",
			tableName:           "max_columns_test",
			modes:               []bulker.BulkMode{bulker.Batch, bulker.Stream},
			leaveResultingTable: true,
			dataFile:            "test_data/columns_added.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("_timestamp", "column1", "_unmapped_data", "id", "name"),
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "column1": nil, "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "column1": "data", "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 3, "name": "test3", "column1": "data", "_unmapped_data": "{\"column2\": \"data\"}"},
				{"_timestamp": constantTime, "id": 4, "name": "test2", "column1": "data", "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 5, "name": "test", "column1": nil, "_unmapped_data": nil},
				{"_timestamp": constantTime, "id": 6, "name": "test4", "column1": "data", "_unmapped_data": "{\"column2\": \"data\", \"column3\": \"data\"}"},
			},
			batchSize:     2,
			streamOptions: []bulker.StreamOption{WithMaxColumnsCount(4)},
			configIds:     utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId, MySQLBulkerTypeId}),
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
