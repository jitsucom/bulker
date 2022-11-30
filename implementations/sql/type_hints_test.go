package sql

import (
	"cloud.google.com/go/bigquery"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"testing"
)

func TestSQLTypeHints(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:                      "sql_types_hints_postgres",
			modes:                     []bulker.BulkMode{bulker.Stream},
			expectPartitionId:         true,
			dataFile:                  "test_data/type_hints.ndjson",
			expectedTableTypeChecking: true,
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
			expectedTableTypeChecking: true,
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
