package sql

import (
	"cloud.google.com/go/bigquery"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"testing"
	"time"
)

func TestReverseDataTypeMapping(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:                      "data_types_from_sql_types",
			modes:                     []bulker.BulkMode{bulker.Batch},
			dataFile:                  "test_data/data_types.ndjson",
			expectedTableTypeChecking: TypeCheckingDataTypesOnly,
			expectedTable: ExpectedTable{
				Columns: Columns{
					"id":          {DataType: types.INT64},
					"float_type":  {DataType: types.FLOAT64},
					"time_type":   {DataType: types.TIMESTAMP},
					"bool_type":   {DataType: types.BOOL},
					"string_type": {DataType: types.STRING},
				},
			},
			configIds: allBulkerConfigs,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestConfig(t, tt, testStream)
		})
	}
}

func TestSQLTypeHints(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:                      "sql_types_hints_postgres",
			modes:                     []bulker.BulkMode{bulker.Stream},
			expectPartitionId:         true,
			dataFile:                  "test_data/type_hints.ndjson",
			expectedTableTypeChecking: TypeCheckingSQLTypesOnly,
			expectedTable: ExpectedTable{
				Columns: Columns{
					"id":                              {Type: "bigint"},
					"name":                            {Type: "text"},
					"time1":                           {Type: "timestamp without time zone"},
					"int1":                            {Type: "bigint"},
					"nested_json1":                    {Type: "json"},
					"nested_json2":                    {Type: "json"},
					"nested_json3_a":                  {Type: "bigint"},
					"nested_json3_nested_json_nested": {Type: "json"},
					"nested_json4":                    {Type: "json"},
				},
			},
			expectedRows: []map[string]any{
				{"id": 1, "time1": constantTime, "name": "a", "int1": 27, "nested_json1": "{\"a\":1}", "nested_json2": "{\"a\":\"2\"}", "nested_json3_a": 2, "nested_json3_nested_json_nested": "{\"a\":3}", "nested_json4": "{\"a\":\"4\"}"},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("nested_json4", "json"))},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId}),
		}, {
			name:                      "sql_types_hints_bigquery",
			modes:                     []bulker.BulkMode{bulker.Batch},
			expectPartitionId:         true,
			dataFile:                  "test_data/type_hints_bq.ndjson",
			expectedTableTypeChecking: TypeCheckingSQLTypesOnly,
			expectedTable: ExpectedTable{
				Columns: Columns{
					"id":                              {Type: string(bigquery.IntegerFieldType)},
					"name":                            {Type: string(bigquery.StringFieldType)},
					"time1":                           {Type: string(bigquery.TimestampFieldType)},
					"int1":                            {Type: string(bigquery.IntegerFieldType)},
					"nested_json1":                    {Type: string(bigquery.JSONFieldType)},
					"nested_json2":                    {Type: string(bigquery.JSONFieldType)},
					"nested_json3_a":                  {Type: string(bigquery.IntegerFieldType)},
					"nested_json3_nested_json_nested": {Type: string(bigquery.JSONFieldType)},
					"nested_json4":                    {Type: string(bigquery.JSONFieldType)},
				},
			},
			expectedRows: []map[string]any{
				{"id": 1, "time1": constantTime, "name": "a", "int1": 27, "nested_json1": "{\"a\":1}", "nested_json2": "{\"a\":\"2\"}", "nested_json3_a": 2, "nested_json3_nested_json_nested": "{\"a\":3}", "nested_json4": "{\"a\":\"4\"}"},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("nested_json4", "json"))},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{BigqueryBulkerTypeId}),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestConfig(t, tt, testStream)
		})
	}
}

func TestTypeOverride(t *testing.T) {
	//t.Skip("Temporarily disabled")
	tests := []bulkerTestConfig{
		{
			name:              "types_override_postgres",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int_1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 3, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "double precision").
				With("roundfloatstring", "double precision").
				With("boolstring", "boolean").
				With("date1", "date").
				With("int_1", "bigint").
				With("intstring", "bigint"))},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{PostgresBulkerTypeId}),
		},
		{
			name:              "types_override_redshift",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int_1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 3, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "double precision").
				With("roundfloatstring", "double precision").
				With("boolstring", "boolean").
				With("date1", "date").
				With("int_1", "bigint").
				With("intstring", "bigint"))},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{RedshiftBulkerTypeId}),
		},
		{
			name:              "types_override_bigquery",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int_1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 3, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "FLOAT").
				With("roundfloatstring", "FLOAT").
				With("boolstring", "BOOLEAN").
				With("date1", "DATE").
				With("int_1", "INTEGER").
				With("intstring", "INTEGER"))},
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      utils.ArrayIntersection(allBulkerConfigs, []string{BigqueryBulkerTypeId}),
		},
		{
			name:              "types_override_snowflake",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int_1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 3, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "double precision").
				With("roundfloatstring", "double precision").
				With("boolstring", "boolean").
				With("date1", "date").
				With("int_1", "bigint").
				With("intstring", "bigint"))},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{SnowflakeBulkerTypeId}),
		},
		{
			name:              "types_override_mysql",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int_1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": "true", "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": "false", "float1": 1.0, "floatstring": 1.0, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 3, "bool1": false, "bool2": true, "boolstring": "true", "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "DOUBLE").
				With("roundfloatstring", "DOUBLE").
				//With("boolstring", "boolean"). //mysql doesnt cast 'true','false' string to boolean
				With("date1", "date").
				With("int_1", "BIGINT").
				With("intstring", "BIGINT"))},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{MySQLBulkerTypeId}),
		},
		{
			name:              "types_override_clickhouse",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("id", "bool1", "bool2", "boolstring", "float1", "floatstring", "int_1", "intstring", "roundfloat", "roundfloatstring", "name", "time1", "time2", "date1"),
			},
			expectedRows: []map[string]any{
				{"id": 1, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 2, "bool1": false, "bool2": true, "boolstring": false, "float1": 1.0, "floatstring": 1.0, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
				{"id": 3, "bool1": false, "bool2": true, "boolstring": true, "float1": 1.2, "floatstring": 1.1, "int_1": 1, "intstring": 1, "roundfloat": 1.0, "roundfloatstring": 1.0, "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": timestamp.MustParseTime("2006-01-02", "2022-08-18")},
			},
			streamOptions: []bulker.StreamOption{WithColumnTypes(SQLTypes{}.
				With("floatstring", "Float64").
				With("roundfloatstring", "Float64").
				With("boolstring", "bool").
				With("date1", "Date").
				With("int_1", "Int64").
				With("intstring", "Int64"))},
			configIds: utils.ArrayIntersection(allBulkerConfigs, []string{ClickHouseBulkerTypeId, ClickHouseBulkerTypeId + "_cluster"}),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestConfig(t, tt, testStream)
		})
	}
}

func TestTypeCoalesce(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			// this test runs in 2 batches by 4 rows
			// first batch makes sure that table is created with correct types of columns (least common type of types collected from 4 first rows)
			// second batch makes sure that new data is properly converted to column types where applicable
			name:                      "type_coalesce",
			modes:                     []bulker.BulkMode{bulker.Batch},
			dataFile:                  "test_data/types_coalesce.ndjson",
			expectedTableTypeChecking: TypeCheckingDataTypesOnly,
			batchSize:                 4,
			expectedTable: ExpectedTable{
				Columns: Columns{
					"id":      {DataType: types.INT64},
					"str_1":   {DataType: types.STRING},
					"float_1": {DataType: types.FLOAT64},
					"int_1":   {DataType: types.INT64},
					"bool_1":  {DataType: types.BOOL},
					"str_2":   {DataType: types.STRING},
				},
			},
			expectedRows: []map[string]any{
				{"id": 1, "str_1": "7", "float_1": 7.0, "int_1": 7, "bool_1": true, "str_2": "str"},
				{"id": 2, "str_1": "7", "float_1": 7.0, "int_1": 7, "bool_1": false, "str_2": "str"},
				{"id": 3, "str_1": "3.14", "float_1": 3.14, "int_1": 7, "bool_1": true, "str_2": "str"},
				{"id": 4, "str_1": "str", "float_1": 3.14, "int_1": 7, "bool_1": true, "str_2": "str"},
				{"id": 5, "str_1": "str", "float_1": 1.0, "int_1": 9, "bool_1": false, "str_2": "str"},
				{"id": 6, "str_1": "str", "float_1": 0.0, "int_1": 0, "bool_1": true, "str_2": "str"},
				{"id": 7, "str_1": "str", "float_1": 7.0, "int_1": 1, "bool_1": true, "str_2": "str"},
				{"id": 8, "str_1": "str", "float_1": 7.0, "int_1": 1, "bool_1": false, "str_2": "str"},
			},
			configIds: allBulkerConfigs,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestConfig(t, tt, testStream)
		})
	}
}
