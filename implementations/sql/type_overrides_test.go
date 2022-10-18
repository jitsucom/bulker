package sql

import (
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"testing"
	"time"
)

func TestTypeOverride(t *testing.T) {
	//t.Skip("Temporarily disabled")
	tests := []bulkerTestConfig{
		{
			name:              "types_override_postgres",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: &ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "double precision").
				With("roundfloatstring", "double precision").
				With("boolstring", "boolean").
				With("date1", "date").
				With("intstring", "bigint"))},
			configIds: utils.ArrayIntersection(allBulkerTypes, []string{PostgresBulkerTypeId}),
		},
		{
			name:              "types_override_redshift",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: &ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "double precision").
				With("roundfloatstring", "double precision").
				With("boolstring", "boolean").
				With("date1", "date").
				With("intstring", "bigint"))},
			configIds: utils.ArrayIntersection(allBulkerTypes, []string{RedshiftBulkerTypeId}),
		},
		{
			name:              "types_override_bigquery",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: &ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "FLOAT").
				With("roundfloatstring", "FLOAT").
				With("boolstring", "BOOLEAN").
				With("date1", "DATE").
				With("intstring", "INTEGER"))},
			expectedErrors: map[string]any{"create_stream_bigquery_autocommit": BigQueryAutocommitUnsupported},
			configIds:      utils.ArrayIntersection(allBulkerTypes, []string{BigqueryBulkerTypeId}),
		},
		{
			name:              "types_override_snowflake",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: &ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "double precision").
				With("roundfloatstring", "double precision").
				With("boolstring", "boolean").
				With("date1", "date").
				With("intstring", "bigint"))},
			configIds: utils.ArrayIntersection(allBulkerTypes, []string{SnowflakeBulkerTypeId}),
		},
		{
			name:              "types_override_mysql",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: &ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": "true", "float1": 1.2, "floatstring": 1.1, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": "false", "float1": 1.0, "floatstring": 1.0, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "DOUBLE").
				With("roundfloatstring", "DOUBLE").
				//With("boolstring", "boolean"). //mysql doesnt cast 'true','false' string to boolean
				With("date1", "date").
				With("intstring", "BIGINT"))},
			configIds: utils.ArrayIntersection(allBulkerTypes, []string{MySQLBulkerTypeId}),
		},
		{
			name:              "types_override_clickhouse",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: &ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "Float64").
				With("roundfloatstring", "Float64").
				With("boolstring", "bool").
				With("date1", "Date").
				With("intstring", "Int64"))},
			configIds: utils.ArrayIntersection(allBulkerTypes, []string{ClickHouseBulkerTypeId}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testStream)
		})
	}
}
