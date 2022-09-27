package sql

import (
	"github.com/jitsucom/bulker/bulker"
	"sync"
	"testing"
)

func TestExistingTable1(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:                "existing_table1_create_table",
			tableName:           "existing_table1_test",
			modes:               []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:            "test_data/existing_table_text.ndjson",
			leaveResultingTable: true,
			expectedErrors:      map[string]any{"create_stream_bigquery_autocommit": BigQueryAutocommitUnsupported},
			bulkerTypes:         allBulkerTypes,
		},
		{
			name:                "existing_table1_add_events",
			tableName:           "existing_table1_test",
			modes:               []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table1.ndjson",
			expectedRows: []map[string]any{
				{"id": "1"},
				{"id": "22.2"},
				{"id": "string_id"},
				{"id": "string_id2"},
			},
			expectedErrors: map[string]any{"create_stream_bigquery_autocommit": BigQueryAutocommitUnsupported},
			bulkerTypes:    allBulkerTypes,
		},
		{
			name:           "existing_table1_cleanup",
			tableName:      "existing_table1_test",
			modes:          []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_autocommit": BigQueryAutocommitUnsupported},
			bulkerTypes:    allBulkerTypes,
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

func TestExistingTable2(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:                "existing_table2_create_table",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:            "test_data/existing_table_num.ndjson",
			leaveResultingTable: true,
			expectedErrors:      map[string]any{"create_stream_bigquery_autocommit": BigQueryAutocommitUnsupported},
			bulkerTypes:         allBulkerTypes,
		},
		{
			name:                "existing_table2_add_events",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table2.ndjson",
			expectedErrors: map[string]any{
				"consume_object_0_postgres_autocommit":     "pq: 22P02 invalid input syntax for type bigint: \"string_id\"",
				"stream_complete_postgres_transactional":   "pq: 22P02 invalid input syntax for type bigint: \"string_id\"",
				"consume_object_0_clickhouse_autocommit":   "error converting string to int",
				"stream_complete_clickhouse_transactional": "error converting string to int",
				"consume_object_0_redshift_autocommit":     "pq: 22P02 invalid input syntax for integer: \"string_id\"",
				"stream_complete_redshift_transactional":   "failed.  Check 'stl_load_errors' system table for details",
				"consume_object_0_mysql_autocommit":        "Error 1366: Incorrect integer value: 'string_id' for column 'id' at row 1",
				"stream_complete_mysql_transactional":      "Error 1366: Incorrect integer value: 'string_id' for column 'id' at row 1",
				"consume_object_0_snowflake_autocommit":    "100038 (22018): Numeric value 'string_id' is not recognized",
				"stream_complete_snowflake_transactional":  "100038 (22018): Numeric value 'string_id' is not recognized",
				"create_stream_bigquery_autocommit":        BigQueryAutocommitUnsupported,
				"stream_complete_bigquery_transactional":   "Could not parse 'string_id' as INT64 for field id",
			},
			bulkerTypes: allBulkerTypes,
		},
		{
			name:           "existing_table2_cleanup",
			tableName:      "existing_table2_test",
			modes:          []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_autocommit": BigQueryAutocommitUnsupported},
			bulkerTypes:    allBulkerTypes,
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
