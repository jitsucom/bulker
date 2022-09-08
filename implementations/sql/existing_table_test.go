package sql

import (
	"github.com/jitsucom/bulker/bulker"
	"testing"
)

func TestExistingTable(t *testing.T) {
	t.Skip("This test is not finished")
	return
	tests := []bulkerTestConfig{
		{
			name:     "existing_table1",
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile: "test_data/existing_table1.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"id": SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"id": "2022-08-18 14:17:22.841182Z"},
			},
			bulkerTypes: allBulkerTypes,
		},
		{
			name:     "existing_table2",
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile: "test_data/existing_table2.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"id": SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"id": "1"},
				{"id": "2022-08-18 14:17:22.841182Z"},
			},
			bulkerTypes: allBulkerTypes,
		},
		{
			name:     "existing_table3",
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile: "test_data/existing_table3.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"id": SQLColumn{Type: "timestamp"},
				},
			},
			expectedErrors: map[string]any{"consume_object_0": "cause: pq: 22007 invalid input syntax for type timestamp: \"1\""},
			bulkerTypes:    allBulkerTypes,
		},
		{
			name:     "existing_table4",
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile: "test_data/existing_table4.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"id": SQLColumn{Type: "numeric"},
				},
			},
			expectedErrors: map[string]any{"consume_object_0": "cause: pq: 22P02 invalid input syntax for type numeric: \"2022-08-18 14:17:22.841182Z\""},
			bulkerTypes:    allBulkerTypes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testStream)
		})
	}
}
