package sql

import (
	"github.com/jitsucom/bulker/bulker"
	"sync"
	"testing"
)

func TestExistingTable1(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			//delete any table leftovers from previous tests
			name:           "existing_table1_cleanup",
			tableName:      "existing_table1_test",
			modes:          []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
		{
			name:                "existing_table1_create_table",
			tableName:           "existing_table1_test",
			modes:               []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:            "test_data/existing_table_text.ndjson",
			leaveResultingTable: true,
			expectedErrors:      map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:           allBulkerConfigs,
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
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
		{
			name:           "existing_table1_cleanup",
			tableName:      "existing_table1_test",
			modes:          []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
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
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			//delete any table leftovers from previous tests
			name:           "existing_table2_cleanup",
			tableName:      "existing_table2_test",
			modes:          []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
		{
			name:                "existing_table2_create_table",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:            "test_data/existing_table_num.ndjson",
			leaveResultingTable: true,
			expectedErrors:      map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:           allBulkerConfigs,
		},
		{
			name:                "existing_table2_add_events",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table2.ndjson",
			expectedErrors: map[string]any{
				"consume_object_0_postgres_stream":   "pq: 22P02 invalid input syntax for type bigint: \"string_id\"",
				"stream_complete_postgres_batch":     "pq: 22P02 invalid input syntax for type bigint: \"string_id\"",
				"consume_object_0_clickhouse_stream": "error converting string to int",
				"stream_complete_clickhouse_batch":   "error converting string to int",
				"consume_object_0_redshift_stream":   "pq: 22P02 invalid input syntax for integer: \"string_id\"",
				"stream_complete_redshift_batch":     "system table for details",
				"consume_object_0_mysql_stream":      "Error 1366: Incorrect integer value: 'string_id' for column 'id' at row 1",
				"stream_complete_mysql_batch":        "Error 1366: Incorrect integer value: 'string_id' for column 'id' at row 1",
				"consume_object_0_snowflake_stream":  "100038 (22018): Numeric value 'string_id' is not recognized",
				"stream_complete_snowflake_batch":    "100038 (22018): Numeric value 'string_id' is not recognized",
				"create_stream_bigquery_stream":      BigQueryAutocommitUnsupported,
				"stream_complete_bigquery_batch":     "Could not parse 'string_id' as INT64 for field id",
			},
			configIds: allBulkerConfigs,
		},
		{
			name:           "existing_table2_cleanup",
			tableName:      "existing_table2_test",
			modes:          []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
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
