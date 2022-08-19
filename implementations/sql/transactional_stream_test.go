package sql

import (
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"testing"
	"time"
)

func TestTransactionalStream(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:              "added_columns",
			mode:              bulker.Transactional,
			dataFile:          "test_data/columns_added.ndjson",
			expectedRowsCount: 6,
			expectedTable: &Table{
				Name:     "postgres_added_columns_test",
				Schema:   "bulker",
				PKFields: utils.Set[string]{},
				Columns: Columns{
					"_timestamp": SQLColumn{Type: "timestamp without time zone"},
					"column1":    SQLColumn{Type: "text"},
					"column2":    SQLColumn{Type: "text"},
					"column3":    SQLColumn{Type: "text"},
					"id":         SQLColumn{Type: "bigint"},
					"name":       SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "column1": nil, "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "column1": "data", "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 3, "name": "test3", "column1": "data", "column2": "data", "column3": nil},
				{"_timestamp": constantTime, "id": 4, "name": "test2", "column1": "data", "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 5, "name": "test", "column1": nil, "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 6, "name": "test4", "column1": "data", "column2": "data", "column3": "data"},
			},
			bulkerTypes: []string{"postgres"},
		},
		{
			name:              "types",
			mode:              bulker.Transactional,
			dataFile:          "test_data/types.ndjson",
			expectedRowsCount: 2,
			expectedTable: &Table{
				Name:     "postgres_types_test",
				Schema:   "bulker",
				PKFields: utils.Set[string]{},
				Columns: Columns{
					"bool1":            SQLColumn{Type: "boolean"},
					"bool2":            SQLColumn{Type: "boolean"},
					"float1":           SQLColumn{Type: "double precision"},
					"floatstring":      SQLColumn{Type: "text"},
					"int1":             SQLColumn{Type: "bigint"},
					"intstring":        SQLColumn{Type: "text"},
					"roundfloat":       SQLColumn{Type: "double precision"},
					"roundfloatstring": SQLColumn{Type: "text"},
					"string1":          SQLColumn{Type: "text"},
					"time1":            SQLColumn{Type: "timestamp without time zone"},
					"time2":            SQLColumn{Type: "timestamp without time zone"},
					"date1":            SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"bool1": false, "bool2": true, "float1": 1.2, "floatstring": "1.1", "int1": 1, "intstring": "1", "roundfloat": 1.0, "roundfloatstring": "1.0", "string1": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": "2022-08-18"},
				{"bool1": false, "bool2": true, "float1": 1.0, "floatstring": "1.0", "int1": 1, "intstring": "1", "roundfloat": 1.0, "roundfloatstring": "1.0", "string1": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": "2022-08-18"},
			},
			bulkerTypes: []string{"postgres"},
		},
		{
			name:           "types_collision",
			mode:           bulker.Transactional,
			dataFile:       "test_data/types_collision.ndjson",
			expectedErrors: map[string]any{"consume_object_1": "cause: pq: 22P02 invalid input syntax for type bigint: \"1.1\""},
			bulkerTypes:    []string{"postgres"},
		},
		{
			name:     "existing_table1",
			mode:     bulker.Transactional,
			dataFile: "test_data/existing_table1.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"a": SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"a": "2022-08-18T14:17:22.841182Z"},
			},
			expectedRowsCount: 1,
			bulkerTypes:       []string{"postgres"},
		},
		{
			name:     "existing_table2",
			mode:     bulker.Transactional,
			dataFile: "test_data/existing_table2.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"a": SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"a": "2022-08-18 14:17:22.841182Z"},
				{"a": "1"},
			},
			expectedRowsCount: 2,
			bulkerTypes:       []string{"postgres"},
		},
		{
			name:     "existing_table3",
			mode:     bulker.Transactional,
			dataFile: "test_data/existing_table3.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"a": SQLColumn{Type: "timestamp"},
				},
			},
			expectedErrors: map[string]any{"consume_object_0": "cause: pq: 22007 invalid input syntax for type timestamp: \"1\""},
			bulkerTypes:    []string{"postgres"},
		},
		{
			name:     "existing_table4",
			mode:     bulker.Transactional,
			dataFile: "test_data/existing_table4.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"a": SQLColumn{Type: "numeric"},
				},
			},
			expectedErrors: map[string]any{"consume_object_0": "cause: pq: 22P02 invalid input syntax for type numeric: \"2022-08-18 14:17:22.841182Z\""},
			bulkerTypes:    []string{"postgres"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testStream)
		})
	}
}
