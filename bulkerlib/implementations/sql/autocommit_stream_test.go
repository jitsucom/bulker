package sql

import (
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"sync"
	"testing"
)

// TestAutocommitStream sequentially runs autocommit stream without dropping table in between.
// just to make sure that Complete() logic works fine
func TestAutocommitStream(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			//deletes any table leftovers from previous tests
			name:      "dummy_test_table_cleanup",
			tableName: "autocommit_test",
			modes:     []bulker.BulkMode{bulker.Stream},
			dataFile:  "test_data/empty.ndjson",
			configIds: exceptBigquery,
		},
		{
			name:                "added_columns_first_run",
			tableName:           "autocommit_test",
			modes:               []bulker.BulkMode{bulker.Stream},
			leaveResultingTable: true,
			dataFile:            "test_data/columns_added.ndjson",
			expectedRowsCount:   6,
			configIds:           exceptBigquery,
		},
		{
			name:                "added_columns_second_run",
			tableName:           "autocommit_test",
			modes:               []bulker.BulkMode{bulker.Stream},
			leaveResultingTable: true,
			dataFile:            "test_data/columns_added2.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("_timestamp", "column1", "column2", "column3", "column4", "column5", "id", "name"),
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "column1": nil, "column2": nil, "column3": nil, "column4": nil, "column5": nil},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "column1": "data", "column2": nil, "column3": nil, "column4": nil, "column5": nil},
				{"_timestamp": constantTime, "id": 3, "name": "test3", "column1": "data", "column2": "data", "column3": nil, "column4": nil, "column5": nil},
				{"_timestamp": constantTime, "id": 4, "name": "test2", "column1": "data", "column2": nil, "column3": nil, "column4": nil, "column5": nil},
				{"_timestamp": constantTime, "id": 5, "name": "test", "column1": nil, "column2": nil, "column3": nil, "column4": nil, "column5": nil},
				{"_timestamp": constantTime, "id": 6, "name": "test4", "column1": "data", "column2": "data", "column3": "data", "column4": nil, "column5": nil},
				{"_timestamp": constantTime, "id": 7, "name": "test", "column1": nil, "column2": nil, "column3": nil, "column4": "data", "column5": nil},
				{"_timestamp": constantTime, "id": 8, "name": "test2", "column1": nil, "column2": nil, "column3": nil, "column4": nil, "column5": "data"},
			},
			configIds: exceptBigquery,
		},
		{
			name:      "dummy_test_table_cleanup",
			tableName: "autocommit_test",
			modes:     []bulker.BulkMode{bulker.Stream},
			dataFile:  "test_data/empty.ndjson",
			configIds: exceptBigquery,
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
