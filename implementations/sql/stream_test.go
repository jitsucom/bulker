package sql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

var constantTime = timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22.841182Z")

const forceLeaveResultingTables = false

var configRegistry = map[string]any{
	"postgres": os.Getenv("BULKER_TEST_POSTGRES"),
}

type bulkerTestConfig struct {
	//name of the test
	name string
	//bulker config
	config *bulker.Config
	//for which bulker types (destination types) to run test
	bulkerTypes []string
	//create table with provided schema before running the test. name and schema of table are ignored
	//TODO: implement stream_test preExistingTable
	preExistingTable *Table
	//schema of the table expected as result of complete test run
	expectedTable *Table
	//continue test run even after Consume() returned error
	ignoreConsumeErrors bool
	//rows count expected in resulting table
	expectedRowsCount int
	//rows data expected in resulting table
	expectedRows []map[string]any
	//map of expected errors by step name. May be error type or string. String is used for error message partial matching.
	expectedErrors map[string]any
	//don't clean up resulting table before and after test run. See also forceLeaveResultingTables
	leaveResultingTable bool
	//file with objects to consume in ngjson format
	dataFile string
	//bulker stream mode
	mode bulker.BulkMode
	//bulker stream options
	streamOptions []bulker.StreamOption
}

func TestSimple(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:              "autocommit",
			mode:              bulker.AutoCommit,
			dataFile:          "test_data/simple.ndjson",
			expectedRowsCount: 2,
			expectedTable: &Table{
				Name:     "postgres_autocommit_test",
				Schema:   "bulker",
				PKFields: utils.Set[string]{},
				Columns: Columns{
					"_timestamp": SQLColumn{Type: "timestamp without time zone"},
					"extra":      SQLColumn{Type: "text"},
					"id":         SQLColumn{Type: "bigint"},
					"name":       SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "extra": nil},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "extra": "extra"},
			},
			bulkerTypes: []string{"postgres"},
		},
		{
			name:              "transactional",
			mode:              bulker.Transactional,
			dataFile:          "test_data/simple.ndjson",
			expectedRowsCount: 2,
			bulkerTypes:       []string{"postgres"},
			streamOptions:     []bulker.StreamOption{WithPrimaryKey("id"), WithMergeRows()},
		},
		{
			name:              "replacetable",
			mode:              bulker.ReplaceTable,
			dataFile:          "test_data/simple.ndjson",
			expectedRowsCount: 2,
			bulkerTypes:       []string{"postgres"},
			streamOptions:     []bulker.StreamOption{WithPrimaryKey("id"), WithColumnType("id", "text")},
		},
		{
			name:              "replacepartition",
			mode:              bulker.ReplacePartition,
			dataFile:          "test_data/simple.ndjson",
			expectedRowsCount: 2,
			bulkerTypes:       []string{"postgres"},
			streamOptions:     []bulker.StreamOption{WithPartition("partition_id")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testStream)
		})
	}
}

func runTestConfig(t *testing.T, tt bulkerTestConfig, testFunc func(*testing.T, bulkerTestConfig)) {
	if tt.config != nil {
		t.Run(tt.config.Id+"_"+tt.name, func(t *testing.T) {
			testFunc(t, tt)
		})
	} else {
		for _, bulkerType := range tt.bulkerTypes {
			newTd := tt
			newTd.config = &bulker.Config{Id: bulkerType, BulkerType: bulkerType, DestinationConfig: configRegistry[bulkerType]}
			t.Run(bulkerType+"_"+newTd.name, func(t *testing.T) {
				testFunc(t, newTd)
			})
		}
	}
}

func testStream(t *testing.T, testConfig bulkerTestConfig) {
	require := require.New(t)

	blk, err := bulker.CreateBulker(*testConfig.config)
	CheckError("create_bulker", require, testConfig.expectedErrors, err)
	defer func() {
		err = blk.Close()
		CheckError("bulker_close", require, testConfig.expectedErrors, err)
	}()
	sqlAdapter, ok := blk.(SQLAdapter)
	require.True(ok)
	ctx := context.Background()
	tableName := testConfig.config.Id + "_" + testConfig.name + "_test"
	//clean up in case of previous test failure
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		err = sqlAdapter.DropTable(ctx, sqlAdapter.DbWrapper(), tableName, true)
		CheckError("pre_cleanup", require, testConfig.expectedErrors, err)
	}
	//create destination table with predefined schema before running stream
	if testConfig.preExistingTable != nil {
		testConfig.preExistingTable.Name = tableName
		testConfig.preExistingTable.Schema = sqlAdapter.GetConfig().Schema
		err = sqlAdapter.CreateTable(ctx, sqlAdapter.DbWrapper(), testConfig.preExistingTable)
		CheckError("pre_existingtable", require, testConfig.expectedErrors, err)
	}
	//clean up after test run
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		defer func() {
			sqlAdapter.DropTable(ctx, sqlAdapter.DbWrapper(), tableName, true)
		}()
	}
	stream, err := blk.CreateStream(t.Name(), tableName, testConfig.mode, testConfig.streamOptions...)
	CheckError("create_stream", require, testConfig.expectedErrors, err)

	//Abort stream if error occurred
	defer func() {
		if err != nil {
			_, err = stream.Abort(ctx)
			CheckError("stream_abort", require, testConfig.expectedErrors, err)
		}
	}()

	file, err := os.Open(testConfig.dataFile)
	CheckError("open_file", require, testConfig.expectedErrors, err)

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		obj := types.Object{}
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		err = decoder.Decode(&obj)
		CheckError("decode_json", require, testConfig.expectedErrors, err)
		err = stream.Consume(ctx, obj)
		CheckError(fmt.Sprintf("consume_object_%d", i), require, testConfig.expectedErrors, err)
		if err != nil && !testConfig.ignoreConsumeErrors {
			return
		}
		i++
	}
	//Commit stream
	state, err := stream.Complete(ctx)
	CheckError("stream_complete", require, testConfig.expectedErrors, err)

	require.Equal(bulker.Completed, state.Status)
	require.Equal(testConfig.expectedRowsCount, state.SuccessfulRows)
	require.Equal(testConfig.expectedRowsCount, state.ProcessedRows)
	CheckError("state_lasterror", require, testConfig.expectedErrors, state.LastError)

	if testConfig.expectedTable != nil {
		//Check table schema
		table, err := sqlAdapter.GetTableSchema(ctx, sqlAdapter.DbWrapper(), tableName)
		CheckError("get_table", require, testConfig.expectedErrors, err)
		logging.Infof("table: %+v", table)
		require.Equal(testConfig.expectedTable, table)
	}
	if testConfig.expectedRowsCount >= 0 || len(testConfig.expectedRows) > 0 {
		//Check rows count and rows data when provided
		rows, err := sqlAdapter.Select(ctx, tableName, nil)
		CheckError("select_result", require, testConfig.expectedErrors, err)
		if testConfig.expectedRowsCount >= 0 {
			require.Equal(testConfig.expectedRowsCount, len(rows))
		}
		if len(testConfig.expectedRows) > 0 {
			require.Equal(testConfig.expectedRows, rows)
		}
	}
}

func CheckError(step string, require *require.Assertions, expectedErrors map[string]any, err error) {
	switch target := expectedErrors[step].(type) {
	case string:
		require.Containsf(err.Error(), target, "Error text expected to contain: %s\nGot: %s", target, err)
	case error:
		require.ErrorIs(err, target)
	case nil:
		require.NoError(err)
	default:
		panic(fmt.Sprintf("unexpected type of expected error %T", target))
	}
}
