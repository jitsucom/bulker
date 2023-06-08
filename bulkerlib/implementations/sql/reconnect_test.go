package sql

import (
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"testing"
	"time"
)

func TestReconnect(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:      "reconnect_test",
			tableName: "reconnect_test",
			modes:     []bulker.BulkMode{bulker.Stream},
			dataFile:  "test_data/simple2.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test1"},
				{"_timestamp": constantTime, "id": 2, "name": "test2"},
				{"_timestamp": constantTime, "id": 3, "name": "test3"},
				{"_timestamp": constantTime, "id": 7, "name": "test7"},
				{"_timestamp": constantTime, "id": 8, "name": "test8"},
				{"_timestamp": constantTime, "id": 9, "name": "test9"},
			},
			ignoreConsumeErrors: true,
			expectedErrors: map[string]any{
				"consume_object_3": "connection refused",
				"consume_object_4": "connection refused",
				"consume_object_5": "connection refused",
			},
			postStepFunctions: map[string]StepFunction{
				"consume_object_2": func(testConfig bulkerTestConfig, mode bulker.BulkMode) error {
					switch testConfig.config.BulkerType {
					case PostgresBulkerTypeId:
						return postgresContainer.Stop()
					case MySQLBulkerTypeId:
						return mysqlContainer.Stop()
					case ClickHouseBulkerTypeId:
						return clickhouseContainer.Stop()
					}
					return fmt.Errorf("test not expect bulker type: %s", testConfig.config.BulkerType)
				},
				"consume_object_5": func(testConfig bulkerTestConfig, mode bulker.BulkMode) error {
					defer func() {
						time.Sleep(1 * time.Second)
					}()
					switch testConfig.config.BulkerType {
					case PostgresBulkerTypeId:
						return postgresContainer.Start()
					case MySQLBulkerTypeId:
						return mysqlContainer.Start()
					case ClickHouseBulkerTypeId:
						return clickhouseContainer.Start()
					}
					return fmt.Errorf("test not expect bulker type: %s", testConfig.config.BulkerType)
				},
			},
			configIds: []string{PostgresBulkerTypeId, MySQLBulkerTypeId, ClickHouseBulkerTypeId},
		},
		{
			name:      "reconnect_test_transactional",
			tableName: "reconnect_test",
			modes:     []bulker.BulkMode{bulker.Batch},
			dataFile:  "test_data/simple2.ndjson",
			// new transaction every 3 objects
			batchSize: 3,
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test1"},
				{"_timestamp": constantTime, "id": 2, "name": "test2"},
				{"_timestamp": constantTime, "id": 3, "name": "test3"},
				{"_timestamp": constantTime, "id": 7, "name": "test7"},
				{"_timestamp": constantTime, "id": 8, "name": "test8"},
				{"_timestamp": constantTime, "id": 9, "name": "test9"},
			},
			ignoreConsumeErrors: true,
			expectedErrors: map[string]any{
				"consume_object_3":             "connection refused",
				"consume_object_4":             "connection refused",
				"consume_object_5":             "connection refused",
				"stream_complete_1":            "connection refused",
				"consume_object_3_clickhouse":  "connection refused",
				"consume_object_4_clickhouse":  "connection refused",
				"consume_object_5_clickhouse":  "connection refused",
				"stream_complete_1_clickhouse": "connection refused",
			},
			postStepFunctions: map[string]StepFunction{
				"stream_complete_0": func(testConfig bulkerTestConfig, mode bulker.BulkMode) error {
					switch testConfig.config.BulkerType {
					case PostgresBulkerTypeId:
						return postgresContainer.Stop()
					case MySQLBulkerTypeId:
						return mysqlContainer.Stop()
					case ClickHouseBulkerTypeId:
						return clickhouseContainer.Stop()
					}
					return fmt.Errorf("test not expect bulker type: %s", testConfig.config.BulkerType)
				},
				"stream_complete_1": func(testConfig bulkerTestConfig, mode bulker.BulkMode) error {
					defer func() {
						time.Sleep(1 * time.Second)
					}()
					switch testConfig.config.BulkerType {
					case PostgresBulkerTypeId:
						return postgresContainer.Start()
					case MySQLBulkerTypeId:
						return mysqlContainer.Start()
					case ClickHouseBulkerTypeId:
						return clickhouseContainer.Start()
					}
					return fmt.Errorf("test not expect bulker type: %s", testConfig.config.BulkerType)
				},
			},
			configIds: []string{PostgresBulkerTypeId, MySQLBulkerTypeId, ClickHouseBulkerTypeId},
		},
		{
			name:      "init_after_error",
			tableName: "init_after_error_test",
			modes:     []bulker.BulkMode{bulker.Stream},
			dataFile:  "test_data/simple.ndjson",
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 3, "name": "test2", "extra": "extra"},
			},
			ignoreConsumeErrors: true,
			expectedErrors: map[string]any{
				"create_bulker":            "connection refused",
				"consume_object_0":         "connection refused",
				"consume_object_1":         "connection refused",
				"pre_cleanup":              "database connection is not initialized",
				"init_database_clickhouse": "database connection is not initialized",
			},
			postStepFunctions: map[string]StepFunction{
				//stop containers before initing bulker
				"init": func(testConfig bulkerTestConfig, mode bulker.BulkMode) error {
					switch testConfig.config.BulkerType {
					case PostgresBulkerTypeId:
						return postgresContainer.Stop()
					case MySQLBulkerTypeId:
						return mysqlContainer.Stop()
					case ClickHouseBulkerTypeId:
						return clickhouseContainer.Stop()
					}
					return fmt.Errorf("test not expect bulker type: %s", testConfig.config.BulkerType)
				},
				//start containers back after 2nd message
				"consume_object_1": func(testConfig bulkerTestConfig, mode bulker.BulkMode) error {
					defer func() {
						time.Sleep(1 * time.Second)
					}()
					switch testConfig.config.BulkerType {
					case PostgresBulkerTypeId:
						return postgresContainer.Start()
					case MySQLBulkerTypeId:
						return mysqlContainer.Start()
					case ClickHouseBulkerTypeId:
						return clickhouseContainer.Start()
					}
					return fmt.Errorf("test not expect bulker type: %s", testConfig.config.BulkerType)
				},
			},
			configIds: []string{PostgresBulkerTypeId, MySQLBulkerTypeId, ClickHouseBulkerTypeId},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			//t.Parallel()
			runTestConfig(t, tt, testStream)
		})
	}
}
