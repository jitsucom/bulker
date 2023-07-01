package sql

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	testcontainers2 "github.com/jitsucom/bulker/bulkerlib/implementations/sql/testcontainers"
	"github.com/jitsucom/bulker/bulkerlib/implementations/sql/testcontainers/clickhouse"
	"github.com/jitsucom/bulker/bulkerlib/implementations/sql/testcontainers/clickhouse_noshards"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
	"time"
)

var constantTime = timestamp.MustParseTime(time.RFC3339Nano, "2022-08-18T14:17:22Z")

const forceLeaveResultingTables = false

var allBulkerConfigs []string
var exceptBigquery []string

type TestConfig struct {
	//type of bulker destination
	BulkerType string
	//Config     destination config
	Config any
}

type StepFunction func(testConfig bulkerTestConfig, mode bulker.BulkMode) error

var configRegistry = map[string]any{}

type ExpectedTable struct {
	Name     string
	PKFields utils.Set[string]
	Columns  Columns
}

var postgresContainer *testcontainers2.PostgresContainer
var mysqlContainer *testcontainers2.MySQLContainer
var clickhouseContainer *testcontainers2.ClickHouseContainer

func init() {
	bigqueryConfig := os.Getenv("BULKER_TEST_BIGQUERY")
	if bigqueryConfig != "" {
		configRegistry[BigqueryBulkerTypeId] = TestConfig{BulkerType: BigqueryBulkerTypeId, Config: bigqueryConfig}
	}

	redshiftConfig := os.Getenv("BULKER_TEST_REDSHIFT")
	if redshiftConfig != "" {
		configRegistry[RedshiftBulkerTypeId] = TestConfig{BulkerType: RedshiftBulkerTypeId, Config: redshiftConfig}
	}

	redshiftServerlessConfig := os.Getenv("BULKER_TEST_REDSHIFT_SERVERLESS")
	if redshiftServerlessConfig != "" {
		configRegistry[RedshiftBulkerTypeId+"_serverless"] = TestConfig{BulkerType: RedshiftBulkerTypeId, Config: redshiftServerlessConfig}
	}

	snowflakeConfig := os.Getenv("BULKER_TEST_SNOWFLAKE")
	if snowflakeConfig != "" {
		configRegistry[SnowflakeBulkerTypeId] = TestConfig{BulkerType: SnowflakeBulkerTypeId, Config: snowflakeConfig}
	}
	var err error
	postgresContainer, err = testcontainers2.NewPostgresContainer(context.Background(), false)
	if err != nil {
		panic(err)
	}
	configRegistry[PostgresBulkerTypeId] = TestConfig{BulkerType: PostgresBulkerTypeId, Config: PostgresConfig{
		DataSourceConfig: DataSourceConfig{
			Host:       postgresContainer.Host,
			Port:       postgresContainer.Port,
			Username:   postgresContainer.Username,
			Password:   postgresContainer.Password,
			Db:         postgresContainer.Database,
			Schema:     postgresContainer.Schema,
			Parameters: map[string]string{"sslmode": "disable"},
		},
	}}

	mysqlContainer, err = testcontainers2.NewMySQLContainer(context.Background())
	if err != nil {
		panic(err)
	}
	configRegistry[MySQLBulkerTypeId] = TestConfig{BulkerType: MySQLBulkerTypeId, Config: DataSourceConfig{
		Host:       mysqlContainer.Host,
		Port:       mysqlContainer.Port,
		Username:   mysqlContainer.Username,
		Password:   mysqlContainer.Password,
		Db:         mysqlContainer.Database,
		Parameters: map[string]string{"tls": "false", "parseTime": "true"},
	}}

	clickhouseContainer, err = testcontainers2.NewClickhouseContainer(context.Background())
	if err != nil {
		panic(err)
	}
	configRegistry[ClickHouseBulkerTypeId] = TestConfig{BulkerType: ClickHouseBulkerTypeId, Config: ClickHouseConfig{
		Hosts:    clickhouseContainer.Hosts,
		Username: "default",
		Database: clickhouseContainer.Database,
	}}

	clickhouseClusterContainer, err := clickhouse.NewClickhouseClusterContainer(context.Background())
	if err != nil {
		panic(err)
	}
	configRegistry[ClickHouseBulkerTypeId+"_cluster"] = TestConfig{BulkerType: ClickHouseBulkerTypeId, Config: ClickHouseConfig{
		Hosts:    clickhouseClusterContainer.Hosts,
		Username: "default",
		Database: clickhouseClusterContainer.Database,
		Cluster:  clickhouseClusterContainer.Cluster,
	}}
	clickhouseClusterContainerNoShards, err := clickhouse_noshards.NewClickHouseClusterContainerNoShards(context.Background())
	if err != nil {
		panic(err)
	}
	configRegistry[ClickHouseBulkerTypeId+"_cluster_noshards"] = TestConfig{BulkerType: ClickHouseBulkerTypeId, Config: ClickHouseConfig{
		//also test HTTP mode with this config
		Hosts:    clickhouseClusterContainerNoShards.HostsHTTP,
		Protocol: ClickHouseProtocolHTTP,
		Username: "default",
		Database: clickhouseClusterContainerNoShards.Database,
		Cluster:  clickhouseClusterContainerNoShards.Cluster,
	}}

	allBulkerConfigs = make([]string, 0, len(configRegistry))
	exceptBigquery = make([]string, 0, len(configRegistry))
	for k := range configRegistry {
		allBulkerConfigs = append(allBulkerConfigs, k)
		if k != BigqueryBulkerTypeId {
			exceptBigquery = append(exceptBigquery, k)
		}
	}
	////uncomment to run test for single db only
	//allBulkerConfigs = []string{MySQLBulkerTypeId}
	//exceptBigquery = allBulkerConfigs
	logging.Infof("Initialized bulker types: %v", allBulkerConfigs)
}

type TypeCheckingMode int

const (
	//Disabled type checking
	TypeCheckingDisabled TypeCheckingMode = iota
	//DataTypesOnly - check only data types
	TypeCheckingDataTypesOnly
	//SQLTypesOnly - check only data types
	TypeCheckingSQLTypesOnly
	//TypeCheckingFull - strict type checking
	TypeCheckingFull
)

type bulkerTestConfig struct {
	//name of the test
	name string
	//tableName name of the destination table. Leave empty generate automatically
	tableName string
	//bulker config
	config *bulker.Config
	//for which bulker predefined configurations to run test
	configIds []string
	//continue test run even after Consume() returned error
	ignoreConsumeErrors bool
	//expected state of stream Complete() call
	expectedState *bulker.State
	//schema of the table expected as result of complete test run
	expectedTable ExpectedTable
	//control whether to check types of columns fow expectedTable. For test that run against multiple bulker types is required to leave 'false'
	expectedTableTypeChecking TypeCheckingMode
	//control whether to check character case of table and columns names
	expectedTableCaseChecking bool
	//for configs that runs for multiple modes including bulker.ReplacePartition automatically adds WithPartition to streamOptions and partition id column to expectedTable and expectedRows for that particular mode
	expectPartitionId bool
	//orderBy clause for select query to check expectedTable (default: id asc)
	orderBy []string
	//rows count expected in resulting table. don't use with expectedRows. any type to allow nil value meaning not set
	expectedRowsCount any
	//rows data expected in resulting table
	expectedRows []map[string]any
	//map of expected errors by step name. May be error type or string. String is used for error message partial matching.
	expectedErrors map[string]any
	//map of function to run after each step by step name.
	postStepFunctions map[string]StepFunction
	//don't clean up resulting table before and after test run. See also forceLeaveResultingTables
	leaveResultingTable bool
	//file with objects to consume in ndjson format
	dataFile string
	//bulker stream mode-s to test
	modes []bulker.BulkMode
	//bulker stream options
	streamOptions []bulker.StreamOption
	//batchSize for bigdata test commit stream every batchSize rows
	batchSize int
}

func (c *bulkerTestConfig) getIdAndTableName(mode bulker.BulkMode) (id, tableName string) {
	tableName = c.tableName
	if tableName == "" {
		tableName = c.name
	}
	tableName = tableName + "_" + strings.ToLower(string(mode))
	id = fmt.Sprintf("%s_%s", c.config.BulkerType, tableName)
	return
}

func TestBasics(t *testing.T) {
	t.Parallel()
	tests := []bulkerTestConfig{
		{
			name:              "added_columns",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/columns_added.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("_timestamp", "column1", "column2", "column3", "id", "name"),
			},
			expectedRowsCount: 6,
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "column1": nil, "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 2, "name": "test2", "column1": "data", "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 3, "name": "test3", "column1": "data", "column2": "data", "column3": nil},
				{"_timestamp": constantTime, "id": 4, "name": "test2", "column1": "data", "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 5, "name": "test", "column1": nil, "column2": nil, "column3": nil},
				{"_timestamp": constantTime, "id": 6, "name": "test4", "column1": "data", "column2": "data", "column3": "data"},
			},
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
		{
			name:              "repeated_ids_no_pk",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/repeated_ids.ndjson",
			expectedTable: ExpectedTable{
				PKFields: utils.Set[string]{},
				Columns:  justColumns("_timestamp", "id", "name"),
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test"},
				{"_timestamp": constantTime, "id": 1, "name": "test7"},
				{"_timestamp": constantTime, "id": 2, "name": "test1"},
				{"_timestamp": constantTime, "id": 3, "name": "test2"},
				{"_timestamp": constantTime, "id": 3, "name": "test3"},
				{"_timestamp": constantTime, "id": 3, "name": "test6"},
				{"_timestamp": constantTime, "id": 4, "name": "test4"},
				{"_timestamp": constantTime, "id": 4, "name": "test5"},
			},
			orderBy:        []string{"id", "name"},
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
		},
		{
			name:              "repeated_ids_pk",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/repeated_ids.ndjson",
			expectedTable: ExpectedTable{
				PKFields: utils.NewSet("id"),
				Columns:  justColumns("_timestamp", "id", "name"),
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test7"},
				{"_timestamp": constantTime, "id": 2, "name": "test1"},
				{"_timestamp": constantTime, "id": 3, "name": "test6"},
				{"_timestamp": constantTime, "id": 4, "name": "test5"},
			},
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
			streamOptions:  []bulker.StreamOption{bulker.WithPrimaryKey("id"), bulker.WithMergeRows()},
		},
		{
			name:              "multi_pk",
			modes:             []bulker.BulkMode{bulker.Batch, bulker.Stream, bulker.ReplaceTable, bulker.ReplacePartition},
			expectPartitionId: true,
			dataFile:          "test_data/repeated_ids_multi.ndjson",
			expectedTable: ExpectedTable{
				PKFields: utils.NewSet("id", "id2"),
				Columns:  justColumns("_timestamp", "id", "id2", "name"),
			},
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "id2": "a", "name": "test8"},
				{"_timestamp": constantTime, "id": 2, "id2": "b", "name": "test1"},
				{"_timestamp": constantTime, "id": 3, "id2": "c", "name": "test7"},
				{"_timestamp": constantTime, "id": 4, "id2": "d", "name": "test5"},
				{"_timestamp": constantTime, "id": 4, "id2": "dd", "name": "test6"},
			},
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
			orderBy:        []string{"id", "id2"},
			streamOptions:  []bulker.StreamOption{bulker.WithPrimaryKey("id", "id2"), bulker.WithMergeRows()},
		},
		{
			name:              "timestamp_option",
			modes:             []bulker.BulkMode{bulker.Batch},
			expectPartitionId: true,
			dataFile:          "test_data/simple.ndjson",
			expectedTable: ExpectedTable{
				Columns: justColumns("_timestamp", "id", "name", "extra"),
			},
			expectedRowsCount: 6,
			expectedRows: []map[string]any{
				{"_timestamp": constantTime, "id": 1, "name": "test", "extra": nil},
				{"_timestamp": constantTime, "id": 2, "name": "test", "extra": nil},
				{"_timestamp": constantTime, "id": 3, "name": "test2", "extra": "extra"},
			},
			expectedErrors: map[string]any{"create_stream_bigquery_stream": BigQueryAutocommitUnsupported},
			configIds:      allBulkerConfigs,
			streamOptions:  []bulker.StreamOption{bulker.WithTimestamp("_timestamp")},
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

func runTestConfig(t *testing.T, tt bulkerTestConfig, testFunc func(*testing.T, bulkerTestConfig, bulker.BulkMode)) {
	if tt.config != nil {
		for _, mode := range tt.modes {
			t.Run(string(mode)+"_"+tt.config.Id+"_"+tt.name, func(t *testing.T) {
				testFunc(t, tt, mode)
			})
		}
	} else {
		for _, testConfigId := range tt.configIds {
			newTd := tt
			if !utils.ArrayContains(allBulkerConfigs, testConfigId) {
				t.Skipf("Config '%s' is not selected for this test", testConfigId)
			}
			testConfigRaw, ok := configRegistry[testConfigId]
			if !ok {
				t.Fatalf("No config found for %s", testConfigId)
			}
			testConfig := testConfigRaw.(TestConfig)
			newTd.config = &bulker.Config{Id: testConfigId, BulkerType: testConfig.BulkerType, DestinationConfig: testConfig.Config, LogLevel: bulker.Verbose}
			for _, mode := range newTd.modes {
				tc := newTd
				mode := mode
				t.Run(string(mode)+"_"+testConfigId+"_"+newTd.name, func(t *testing.T) {
					testFunc(t, tc, mode)
				})
			}
		}
	}
}

func testStream(t *testing.T, testConfig bulkerTestConfig, mode bulker.BulkMode) {
	reqr := require.New(t)
	adaptConfig(t, &testConfig, mode)
	PostStep("init", testConfig, mode, reqr, nil)
	blk, err := bulker.CreateBulker(*testConfig.config)
	PostStep("create_bulker", testConfig, mode, reqr, err)
	defer func() {
		err = blk.Close()
		PostStep("bulker_close", testConfig, mode, reqr, err)
	}()
	sqlAdapter, ok := blk.(SQLAdapter)
	reqr.True(ok)
	ctx := context.Background()
	id, tableName := testConfig.getIdAndTableName(mode)
	err = sqlAdapter.InitDatabase(ctx)
	PostStep("init_database", testConfig, mode, reqr, err)
	//clean up in case of previous test failure
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		err = sqlAdapter.DropTable(ctx, tableName, true)
		PostStep("pre_cleanup", testConfig, mode, reqr, err)
	}
	//clean up after test run
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		defer func() {
			_ = sqlAdapter.DropTable(ctx, tableName, true)
		}()
	}
	stream, err := blk.CreateStream(id, tableName, mode, testConfig.streamOptions...)
	PostStep("create_stream", testConfig, mode, reqr, err)
	if err != nil {
		return
	}
	//Abort stream if error occurred
	defer func() {
		if err != nil {
			_, _ = stream.Abort(ctx)
			//CheckError("stream_abort", testConfig.config.BulkerType, reqr, testConfig.expectedErrors, err)
		}
	}()

	file, err := os.Open(testConfig.dataFile)
	PostStep("open_file", testConfig, mode, reqr, err)

	scanner := bufio.NewScanner(file)
	i := 0
	streamNum := 0
	for scanner.Scan() {
		if i > 0 && testConfig.batchSize > 0 && i%testConfig.batchSize == 0 {
			_, err := stream.Complete(ctx)
			PostStep(fmt.Sprintf("stream_complete_%d", streamNum), testConfig, mode, reqr, err)
			streamNum++
			logging.Infof("%d. batch is completed", i)
			stream, err = blk.CreateStream(id, tableName, mode, testConfig.streamOptions...)
			PostStep(fmt.Sprintf("create_stream_%d", streamNum), testConfig, mode, reqr, err)
			if err != nil {
				return
			}
		}
		obj := types2.Object{}
		decoder := jsoniter.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		err = decoder.Decode(&obj)
		PostStep("decode_json", testConfig, mode, reqr, err)
		_, _, err = stream.Consume(ctx, obj)
		PostStep(fmt.Sprintf("consume_object_%d", i), testConfig, mode, reqr, err)
		if err != nil && !testConfig.ignoreConsumeErrors {
			break
		}
		i++
	}
	//Commit stream
	state, err := stream.Complete(ctx)
	PostStep("stream_complete", testConfig, mode, reqr, err)

	if testConfig.expectedState != nil {
		reqr.Equal(*testConfig.expectedState, state)
	}
	if err != nil {
		return
	}
	//PostStep("state_lasterror", testConfig, mode, reqr, state.LastError)
	if len(testConfig.expectedTable.Columns) > 0 {
		//Check table schema
		table, err := sqlAdapter.GetTableSchema(ctx, tableName)
		PostStep("get_table", testConfig, mode, reqr, err)
		switch testConfig.expectedTableTypeChecking {
		case TypeCheckingDisabled:
			for k := range testConfig.expectedTable.Columns {
				testConfig.expectedTable.Columns[k] = types2.SQLColumn{Type: "__TEST_type_checking_disabled_by_expectedTableTypeChecking__"}
			}
			for k := range table.Columns {
				table.Columns[k] = types2.SQLColumn{Type: "__TEST_type_checking_disabled_by_expectedTableTypeChecking__"}
			}
		case TypeCheckingDataTypesOnly:
			for k := range testConfig.expectedTable.Columns {
				testConfig.expectedTable.Columns[k] = types2.SQLColumn{DataType: testConfig.expectedTable.Columns[k].DataType}
			}
			for k := range table.Columns {
				table.Columns[k] = types2.SQLColumn{DataType: table.Columns[k].DataType}
			}
		case TypeCheckingSQLTypesOnly:
			for k := range testConfig.expectedTable.Columns {
				testConfig.expectedTable.Columns[k] = types2.SQLColumn{Type: testConfig.expectedTable.Columns[k].Type}
			}
			for k := range table.Columns {
				table.Columns[k] = types2.SQLColumn{Type: table.Columns[k].Type}
			}
		}
		if !testConfig.expectedTableCaseChecking {
			newColumns := make(Columns, len(testConfig.expectedTable.Columns))
			for k := range testConfig.expectedTable.Columns {
				newColumns[strings.ToLower(k)] = testConfig.expectedTable.Columns[k]
			}
			testConfig.expectedTable.Columns = newColumns
			newColumns = make(Columns, len(table.Columns))
			for k := range table.Columns {
				newColumns[strings.ToLower(k)] = table.Columns[k]
			}
			table.Columns = newColumns

			newPKFields := utils.NewSet[string]()
			for k := range testConfig.expectedTable.PKFields {
				newPKFields.Put(strings.ToLower(k))
			}
			testConfig.expectedTable.PKFields = newPKFields
			newPKFields = utils.NewSet[string]()
			for k := range table.PKFields {
				newPKFields.Put(strings.ToLower(k))
			}
			table.PKFields = newPKFields

			testConfig.expectedTable.Name = strings.ToLower(testConfig.expectedTable.Name)
			table.Name = strings.ToLower(table.Name)

			table.PrimaryKeyName = strings.ToLower(table.PrimaryKeyName)
		}
		expectedPKFields := utils.NewSet[string]()
		if len(testConfig.expectedTable.PKFields) > 0 {
			expectedPKFields = testConfig.expectedTable.PKFields
		}
		// don't check table name if not explicitly set
		if testConfig.expectedTable.Name == "" {
			table.Name = ""
		}
		table.PrimaryKeyName = ""
		expectedTable := &Table{
			Name:           testConfig.expectedTable.Name,
			PrimaryKeyName: "",
			PKFields:       expectedPKFields,
			Columns:        testConfig.expectedTable.Columns,
		}
		reqr.Equal(expectedTable, table)
	}
	if testConfig.expectedRowsCount != nil || testConfig.expectedRows != nil {
		time.Sleep(1 * time.Second)
		//Check rows count and rows data when provided
		rows, err := sqlAdapter.Select(ctx, tableName, nil, testConfig.orderBy)
		PostStep("select_result", testConfig, mode, reqr, err)
		if testConfig.expectedRows == nil {
			reqr.Equal(testConfig.expectedRowsCount, len(rows))
		} else {
			reqr.Len(rows, len(testConfig.expectedRows))
			for i, row := range rows {
				exRow := testConfig.expectedRows[i]
				for k, exV := range exRow {
					switch exD := exV.(type) {
					case time.Time:
						reqr.IsType(exV, row[k], "row %d, columns '%s' type mismatch. Expected time.Time", i, k)
						reqr.Equal(exD.Format(time.RFC3339Nano), row[k].(time.Time).Format(time.RFC3339Nano), "row %d, columns '%s' not equals", i, k)
					default:
						reqr.Equal(exV, row[k], "row %d, columns '%s' not equals", i, k)
					}
				}
			}
		}
	}
}

// adaptConfig since we can use a single config for many modes and db types we may need to
// apply changes for specific modes of dbs
func adaptConfig(t *testing.T, testConfig *bulkerTestConfig, mode bulker.BulkMode) {
	if len(testConfig.orderBy) == 0 {
		testConfig.orderBy = []string{"id"}
	}
	switch mode {
	case bulker.ReplacePartition:
		if testConfig.expectPartitionId {
			partitionId := uuid.New()
			newOptions := make([]bulker.StreamOption, len(testConfig.streamOptions))
			copy(newOptions, testConfig.streamOptions)
			newOptions = append(newOptions, bulker.WithPartition(partitionId))
			testConfig.streamOptions = newOptions
			//add partition id column to expectedTable
			if len(testConfig.expectedTable.Columns) > 0 {
				textColumn, ok := testConfig.expectedTable.Columns["name"]
				if !ok {
					textColumn, ok = testConfig.expectedTable.Columns["NAME"]
					if !ok {
						t.Fatalf("test config error: expected table must have a 'name' column of string type to guess what type to expect for %s column", PartitonIdKeyword)
					}
				}
				newExpectedTable := ExpectedTable{Columns: testConfig.expectedTable.Columns.Clone(), PKFields: testConfig.expectedTable.PKFields.Clone()}
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

func PostStep(step string, testConfig bulkerTestConfig, mode bulker.BulkMode, reqr *require.Assertions, err error) {
	bulkerType := testConfig.config.BulkerType
	stepLookupKeys := []string{step + "_" + bulkerType + "_" + strings.ToLower(string(mode)), step + "_" + bulkerType, step}

	//run post step function if any
	stepF := utils.MapNVLKeys(testConfig.postStepFunctions, stepLookupKeys...)
	if stepF != nil {
		err1 := stepF(testConfig, mode)
		if err == nil {
			err = err1
		}
	}

	expectedError := utils.MapNVLKeys(testConfig.expectedErrors, stepLookupKeys...)
	switch target := expectedError.(type) {
	case []string:
		contains := false
		for _, t := range target {
			if strings.Contains(err.Error(), t) {
				contains = true
				break
			}
		}
		if !contains {
			reqr.Fail(fmt.Sprintf("%s", err), "error in step %s doesn't contain one of expected value: %+v", step, target)
		}
	case string:
		reqr.ErrorContainsf(err, target, "error in step %s doesn't contain expected value: %s", step, target)
	case error:
		reqr.ErrorIs(err, target, "error in step %s doesn't match expected error: %s", step, target)
	case nil:
		reqr.NoError(err, "unexpected error in step %s", step)
	default:
		panic(fmt.Sprintf("unexpected type of expected error: %T for step: %s", target, step))
	}
}

// Returns Columns map with no type information
func justColumns(columns ...string) Columns {
	colsMap := make(Columns, len(columns))
	for _, col := range columns {
		colsMap[col] = types2.SQLColumn{}
	}
	return colsMap
}
