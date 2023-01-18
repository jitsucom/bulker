package sql

import (
	"github.com/jitsucom/bulker/base/utils"
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
			modes:          []bulker.BulkMode{bulker.Batch, bulker.Stream},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
		{
			name:                "existing_table1_create_table",
			tableName:           "existing_table1_test",
			modes:               []bulker.BulkMode{bulker.Batch, bulker.Stream},
			dataFile:            "test_data/existing_table_text.ndjson",
			leaveResultingTable: true,
			expectedErrors:      map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:           allBulkerConfigs,
		},
		{
			name:                "existing_table1_add_events",
			tableName:           "existing_table1_test",
			modes:               []bulker.BulkMode{bulker.Batch, bulker.Stream},
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
			modes:          []bulker.BulkMode{bulker.Batch, bulker.Stream},
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

func TestExistingTableStream2(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			//delete any table leftovers from previous tests
			name:           "existing_table2_cleanup",
			tableName:      "existing_table2_test",
			modes:          []bulker.BulkMode{bulker.Stream},
			dataFile:       "test_data/empty.ndjson",
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
		{
			name:                "existing_table2_create_table",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Stream},
			dataFile:            "test_data/existing_table_num.ndjson",
			leaveResultingTable: true,
			expectedErrors:      map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:           allBulkerConfigs,
		},
		{
			name:                "existing_table2_add_events",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Stream},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table2.ndjson",
			expectedErrors: map[string]any{
				"consume_object_0_postgres_stream":   "pq: 22P02 invalid input syntax for type bigint: \"string_id\"",
				"consume_object_0_clickhouse_stream": []string{"error converting string to int", "Cannot parse string 'string_id' as Int64"},
				"consume_object_0_redshift_stream":   "pq: 22P02 invalid input syntax for integer: \"string_id\"",
				"consume_object_0_mysql_stream":      "Incorrect integer value: 'string_id' for column 'data' at row 1",
				"consume_object_0_snowflake_stream":  "100038 (22018): Numeric value 'string_id' is not recognized",
				"create_stream_bigquery_stream":      BigQueryAutocommitUnsupported,
			},
			configIds: allBulkerConfigs,
		},
		{
			name:           "existing_table2_cleanup",
			tableName:      "existing_table2_test",
			modes:          []bulker.BulkMode{bulker.Stream},
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

func TestExistingTableBatch2(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			//delete any table leftovers from previous tests
			name:      "existing_table2_cleanup",
			tableName: "existing_table2_test",
			modes:     []bulker.BulkMode{bulker.Batch},
			dataFile:  "test_data/empty.ndjson",
			configIds: allBulkerConfigs,
		},
		{
			name:                "existing_table2_create_table",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Batch},
			dataFile:            "test_data/existing_table_num.ndjson",
			leaveResultingTable: true,
			configIds:           allBulkerConfigs,
		},
		{
			name:                "existing_table2_add_events",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Batch},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table2.ndjson",
			expectedRows: []map[string]any{
				{"id": 1, "data": 1, "_unmapped_data": nil},
				{"id": 2, "data": 0, "_unmapped_data": "{\"data\":\"string_id\"}"},
			},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{ClickHouseBulkerTypeId, ClickHouseBulkerTypeId + "_cluster", ClickHouseBulkerTypeId + "_cluster_noshards"}),
		},
		{
			name:                "existing_table2_add_events",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Batch},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table2.ndjson",
			expectedRows: []map[string]any{
				{"id": 1, "data": 1, "_unmapped_data": nil},
				{"id": 2, "data": nil, "_unmapped_data": "{\"data\":\"string_id\"}"},
			},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{RedshiftBulkerTypeId, BigqueryBulkerTypeId}),
		},
		{
			name:                "existing_table2_add_events",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Batch},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table2.ndjson",
			expectedRows: []map[string]any{
				{"id": 1, "data": 1, "_unmapped_data": nil},
				{"id": 2, "data": nil, "_unmapped_data": "{\"DATA\":\"string_id\"}"},
			},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{SnowflakeBulkerTypeId}),
		},
		{
			name:                "existing_table2_add_events",
			tableName:           "existing_table2_test",
			modes:               []bulker.BulkMode{bulker.Batch},
			leaveResultingTable: true,
			dataFile:            "test_data/existing_table2.ndjson",
			expectedRows: []map[string]any{
				{"id": 1, "data": 1, "_unmapped_data": nil},
				{"id": 2, "data": nil, "_unmapped_data": "{\"data\": \"string_id\"}"},
			},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId, MySQLBulkerTypeId}),
		},
		{
			name:           "existing_table2_cleanup",
			tableName:      "existing_table2_test",
			modes:          []bulker.BulkMode{bulker.Batch},
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
