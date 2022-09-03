package sql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations/sql/testcontainers"
	"github.com/jitsucom/bulker/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

var constantTime = timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22.841182Z")

const forceLeaveResultingTables = false

var configRegistry = map[string]any{
	//"postgres": os.Getenv("BULKER_TEST_POSTGRES"),
}

func init() {
	postgresContainer, err := testcontainers.NewPostgresContainer(context.Background())
	if err != nil {
		panic(err)
	}
	configRegistry["postgres"] = DataSourceConfig{
		Host:       postgresContainer.Host,
		Port:       postgresContainer.Port,
		Username:   postgresContainer.Username,
		Password:   postgresContainer.Password,
		Db:         postgresContainer.Database,
		Schema:     postgresContainer.Schema,
		Parameters: map[string]string{"sslmode": "disable"},
	}
}

type bulkerTestConfig struct {
	//name of the test
	name string
	//tableName name of the destination table. Leave empty generate automatically
	tableName string
	//bulker config
	config *bulker.Config
	//for which bulker types (destination types) to run test
	bulkerTypes []string
	//create table with provided schema before running the test. name and schema of table are ignored
	//TODO: implement stream_test preExistingTable
	preExistingTable *Table
	//continue test run even after Consume() returned error
	ignoreConsumeErrors bool
	//expected state of stream Complete() call
	expectedState *bulker.State
	//schema of the table expected as result of complete test run
	expectedTable *Table
	//for configs that runs for multiple modes including bulker.ReplacePartition automatically adds WithPartition to streamOptions and partition id column to expectedTable and expectedRows for that particular mode
	expectPartitionId bool
	//rows count expected in resulting table. don't use with expectedRows. any type to allow nil value meaning not set
	expectedRowsCount any
	//rows data expected in resulting table
	expectedRows []map[string]any
	//map of expected errors by step name. May be error type or string. String is used for error message partial matching.
	expectedErrors map[string]any
	//don't clean up resulting table before and after test run. See also forceLeaveResultingTables
	leaveResultingTable bool
	//file with objects to consume in ngjson format
	dataFile string
	//bulker stream mode-s to test
	modes []bulker.BulkMode
	//bulker stream options
	streamOptions []bulker.StreamOption
}

func TestStreams(t *testing.T) {
	tests := []bulkerTestConfig{
		{
			name:              "added_columns",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/columns_added.ndjson",
			expectedTable: &Table{
				Name:     "added_columns_test",
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
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types.ndjson",
			expectedTable: &Table{
				Name:     "types_test",
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
					"name":             SQLColumn{Type: "text"},
					"time1":            SQLColumn{Type: "timestamp without time zone"},
					"time2":            SQLColumn{Type: "timestamp without time zone"},
					"date1":            SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"bool1": false, "bool2": true, "float1": 1.2, "floatstring": "1.1", "int1": 1, "intstring": "1", "roundfloat": 1.0, "roundfloatstring": "1.0", "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": "2022-08-18"},
				{"bool1": false, "bool2": true, "float1": 1.0, "floatstring": "1.0", "int1": 1, "intstring": "1", "roundfloat": 1.0, "roundfloatstring": "1.0", "name": "test", "time1": constantTime, "time2": timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z"), "date1": "2022-08-18"},
			},
			bulkerTypes: []string{"postgres"},
		},
		{
			name:              "types_collision",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/types_collision.ndjson",
			expectedErrors:    map[string]any{"consume_object_1": "cause: pq: 22P02 invalid input syntax for type bigint: \"1.1\""},
			bulkerTypes:       []string{"postgres"},
		},
		{
			name:     "existing_table1",
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile: "test_data/existing_table1.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"a": SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"a": "2022-08-18 14:17:22.841182Z"},
			},
			bulkerTypes: []string{"postgres"},
		},
		{
			name:     "existing_table2",
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
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
			bulkerTypes: []string{"postgres"},
		},
		{
			name:     "existing_table3",
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
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
			modes:    []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit},
			dataFile: "test_data/existing_table4.ndjson",
			preExistingTable: &Table{
				Columns: Columns{
					"a": SQLColumn{Type: "numeric"},
				},
			},
			expectedErrors: map[string]any{"consume_object_0": "cause: pq: 22P02 invalid input syntax for type numeric: \"2022-08-18 14:17:22.841182Z\""},
			bulkerTypes:    []string{"postgres"},
		},
		{
			name:              "repeated_ids_no_pk",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/repeated_ids.ndjson",
			expectedTable: &Table{
				Name:     "repeated_ids_no_pk_test",
				PKFields: utils.Set[string]{},
				Columns: Columns{
					"_timestamp": SQLColumn{Type: "timestamp without time zone"},
					"id":         SQLColumn{Type: "bigint"},
					"name":       SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test"},
				{"_timestamp": constantTime, "id": 2, "name": "test1"},
				{"_timestamp": constantTime, "id": 3, "name": "test2"},
				{"_timestamp": constantTime, "id": 3, "name": "test3"},
				{"_timestamp": constantTime, "id": 4, "name": "test4"},
				{"_timestamp": constantTime, "id": 4, "name": "test5"},
				{"_timestamp": constantTime, "id": 3, "name": "test6"},
				{"_timestamp": constantTime, "id": 1, "name": "test7"},
			},
			bulkerTypes: []string{"postgres"},
		},
		{
			name:              "repeated_ids_pk",
			modes:             []bulker.BulkMode{bulker.Transactional, bulker.AutoCommit, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/repeated_ids.ndjson",
			expectedState: &bulker.State{
				Status:         bulker.Completed,
				ProcessedRows:  8,
				SuccessfulRows: 8,
			},
			expectedTable: &Table{
				Name:           "repeated_ids_pk_test",
				PrimaryKeyName: "repeated_ids_pk_test_pk",
				PKFields:       utils.NewSet("id"),
				Columns: Columns{
					"_timestamp": SQLColumn{Type: "timestamp without time zone"},
					"id":         SQLColumn{Type: "bigint"},
					"name":       SQLColumn{Type: "text"},
				},
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 2, "name": "test1"},
				{"_timestamp": constantTime, "id": 4, "name": "test5"},
				{"_timestamp": constantTime, "id": 3, "name": "test6"},
				{"_timestamp": constantTime, "id": 1, "name": "test7"},
			},
			bulkerTypes:   []string{"postgres"},
			streamOptions: []bulker.StreamOption{WithPrimaryKey("id"), WithMergeRows()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testStream)
		})
	}
}

func runTestConfig(t *testing.T, tt bulkerTestConfig, testFunc func(*testing.T, bulkerTestConfig, bulker.BulkMode)) {
	if tt.config != nil {
		for _, mode := range tt.modes {
			t.Run(string(mode)+"_"+tt.config.Id+"_"+tt.name, func(t *testing.T) {
				testFunc(t, tt, mode)
			})
		}
	} else {
		for _, bulkerType := range tt.bulkerTypes {
			newTd := tt
			newTd.config = &bulker.Config{Id: bulkerType, BulkerType: bulkerType, DestinationConfig: configRegistry[bulkerType]}
			for _, mode := range newTd.modes {
				t.Run(string(mode)+"_"+bulkerType+"_"+newTd.name, func(t *testing.T) {
					testFunc(t, newTd, mode)
				})
			}
		}
	}
}

func testStream(t *testing.T, testConfig bulkerTestConfig, mode bulker.BulkMode) {
	require := require.New(t)
	adaptConfig(t, &testConfig, mode)
	blk, err := bulker.CreateBulker(*testConfig.config)
	CheckError("create_bulker", require, testConfig.expectedErrors, err)
	defer func() {
		err = blk.Close()
		CheckError("bulker_close", require, testConfig.expectedErrors, err)
	}()
	sqlAdapter, ok := blk.(SQLAdapter)
	require.True(ok)
	ctx := context.Background()
	tableName := testConfig.tableName
	if tableName == "" {
		tableName = testConfig.name + "_test"
	}
	//clean up in case of previous test failure
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		err = sqlAdapter.DropTable(ctx, sqlAdapter.DbWrapper(), tableName, true)
		CheckError("pre_cleanup", require, testConfig.expectedErrors, err)
	}
	//create destination table with predefined schema before running stream
	if testConfig.preExistingTable != nil {
		testConfig.preExistingTable.Name = tableName
		err = sqlAdapter.CreateTable(ctx, sqlAdapter.DbWrapper(), testConfig.preExistingTable)
		CheckError("pre_existingtable", require, testConfig.expectedErrors, err)
	}
	//clean up after test run
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		defer func() {
			sqlAdapter.DropTable(ctx, sqlAdapter.DbWrapper(), tableName, true)
		}()
	}
	stream, err := blk.CreateStream(t.Name(), tableName, mode, testConfig.streamOptions...)
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

	if testConfig.expectedState != nil {
		require.Equal(bulker.Completed, state.Status)
		require.Equal(*testConfig.expectedState, state)
	}
	CheckError("state_lasterror", require, testConfig.expectedErrors, state.LastError)

	if testConfig.expectedTable != nil {
		//Check table schema
		table, err := sqlAdapter.GetTableSchema(ctx, sqlAdapter.DbWrapper(), tableName)
		CheckError("get_table", require, testConfig.expectedErrors, err)
		require.Equal(testConfig.expectedTable, table)
	}
	if testConfig.expectedRowsCount != nil || testConfig.expectedRows != nil {
		//Check rows count and rows data when provided
		rows, err := sqlAdapter.Select(ctx, tableName, nil)
		CheckError("select_result", require, testConfig.expectedErrors, err)
		if testConfig.expectedRows == nil {
			require.Equal(testConfig.expectedRowsCount, len(rows))
		} else {
			require.Equal(testConfig.expectedRows, rows)
		}
	}
}

// adaptConfig since we can use a single config for many modes and db types we may need to
// apply changes for specific modes of dbs
func adaptConfig(t *testing.T, testConfig *bulkerTestConfig, mode bulker.BulkMode) {
	switch mode {
	case bulker.ReplacePartition:
		if testConfig.expectPartitionId {
			partitionId := uuid.New()
			newOptions := make([]bulker.StreamOption, len(testConfig.streamOptions))
			copy(newOptions, testConfig.streamOptions)
			newOptions = append(newOptions, WithPartition(partitionId))
			testConfig.streamOptions = newOptions
			//add partition id column to expectedTable
			if testConfig.expectedTable != nil {
				textColumn, ok := testConfig.expectedTable.Columns["name"]
				if !ok {
					t.Fatalf("test config error: expected table must have a 'name' column of string type to guess what type to expect for %s column", PartitonIdKeyword)
				}
				newExpectedTable := testConfig.expectedTable.Clone()
				newExpectedTable.Columns[PartitonIdKeyword] = textColumn
				testConfig.expectedTable = newExpectedTable
			}
			//add partition id value to all expected rows
			if testConfig.expectedRows != nil {
				newExpectedRows := make([]map[string]any, len(testConfig.expectedRows))
				for i, row := range testConfig.expectedRows {
					newRow := make(map[string]any, len(row)+1)
					utils.MapPutAll(newRow, row)
					newRow[PartitonIdKeyword] = partitionId
					newExpectedRows[i] = newRow
				}
				testConfig.expectedRows = newExpectedRows
			}
		}
	}
}

func CheckError(step string, require *require.Assertions, expectedErrors map[string]any, err error) {
	switch target := expectedErrors[step].(type) {
	case string:
		require.Contains(fmt.Sprintf("%v", err), target)
	case error:
		require.ErrorIs(err, target)
	case nil:
		require.NoError(err)
	default:
		panic(fmt.Sprintf("unexpected type of expected error %T", target))
	}
}
