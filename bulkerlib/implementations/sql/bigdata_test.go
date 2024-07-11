package sql

import (
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
	"time"
)

const eventsCount = 1_000_000

func TestMillionRows(t *testing.T) {
	configsEnabled := os.Getenv("BULKER_TEST_MILLION_ROWS")
	if configsEnabled == "" {
		t.Skip("This test is disabled by default. To enable it set BULKER_TEST_MILLION_ROWS env variable with comma separated list of bulker config ids")
		return
	}
	configIds := strings.Split(configsEnabled, ",")
	configIds = utils.ArrayIntersection(allBulkerConfigs, configIds)
	if len(configIds) == 0 {
		t.Skipf("Test was skipped. IDs: %v is not among configured configs: %v", configIds, allBulkerConfigs)
		return
	}
	tests := []bulkerTestConfig{
		{
			name:  "one_million_rows",
			modes: []bulker.BulkMode{bulker.Batch},
			expectedTable: ExpectedTable{
				Columns: justColumns("_timestamp", "id", "name"),
			},
			expectedRowsCount: eventsCount,
			configIds:         configIds,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestConfig(t, tt, testLotOfEvents)
		})
	}
}

func TestMillionRowsBatched(t *testing.T) {
	configsEnabled := os.Getenv("BULKER_TEST_MILLION_ROWS_BATCHED")
	if configsEnabled == "" {
		t.Skip("This test is disabled by default. To enable it set BULKER_TEST_MILLION_ROWS_BATCHED env variable with comma separated list of bulker config ids")
		return
	}
	configIds := strings.Split(configsEnabled, ",")
	configIds = utils.ArrayIntersection(allBulkerConfigs, configIds)
	if len(configIds) == 0 {
		t.Skipf("Test was skipped. IDs: %v is not among configured configs: %v", configIds, allBulkerConfigs)
		return
	}
	tests := []bulkerTestConfig{
		{
			name:                "one_million_rows_batched",
			modes:               []bulker.BulkMode{bulker.Batch},
			batchSize:           100_000,
			expectedRowsCount:   eventsCount,
			leaveResultingTable: false,
			configIds:           configIds,
			streamOptions:       []bulker.StreamOption{bulker.WithPrimaryKey("id"), bulker.WithDeduplicate()},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runTestConfig(t, tt, testLotOfEvents)
		})
	}
}
func testLotOfEvents(t *testing.T, testConfig bulkerTestConfig, mode bulker.BulkMode) {
	reqr := require.New(t)
	blk, err := bulker.CreateBulker(*testConfig.config)
	PostStep("create_bulker", testConfig, mode, reqr, err)
	defer func() {
		err = blk.Close()
		PostStep("bulker_close", testConfig, mode, reqr, err)
	}()
	sqlAdapter, ok := blk.(SQLAdapter)
	reqr.True(ok)
	ctx := context.Background()
	tableName := testConfig.tableName
	if tableName == "" {
		tableName = testConfig.name + "_test"
	}
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
	stream, err := blk.CreateStream(t.Name(), tableName, mode, testConfig.streamOptions...)
	PostStep("create_stream", testConfig, mode, reqr, err)
	if err != nil {
		return
	}
	//Abort stream if error occurred
	defer func() {
		if err != nil {
			_ = stream.Abort(ctx)
			PostStep("stream_abort", testConfig, mode, reqr, err)
		}
	}()

	startTime := timestamp.Now()
	i := 0
	for ; i < eventsCount; i++ {
		if i > 0 && i%testConfig.batchSize == 0 {
			_, err := stream.Complete(ctx)
			PostStep("stream_complete", testConfig, mode, reqr, err)
			logging.Infof("%d. batch is completed in %s", i, time.Since(startTime))
			_ = blk.Close()
			blk, err = bulker.CreateBulker(*testConfig.config)
			PostStep("create_bulker", testConfig, mode, reqr, err)
			stream, err = blk.CreateStream(t.Name(), tableName, mode, testConfig.streamOptions...)
			PostStep("create_stream", testConfig, mode, reqr, err)
			if err != nil {
				return
			}
			startTime = timestamp.Now()
		}
		obj := types.ObjectFromMap(map[string]any{"_timestamp": constantTime, "id": i, "name": "test"})
		_, _, err = stream.Consume(ctx, obj)
		PostStep(fmt.Sprintf("consume_object_%d", i), testConfig, mode, reqr, err)
		if err != nil && !testConfig.ignoreConsumeErrors {
			return
		}
	}
	//Commit stream
	state, err := stream.Complete(ctx)
	sqlAdapter = blk.(SQLAdapter)
	PostStep("stream_complete", testConfig, mode, reqr, err)
	logging.Infof("%d. batch is completed in %s", i, time.Since(startTime))

	if testConfig.expectedState != nil {
		reqr.Equal(*testConfig.expectedState, state)
	}
	if err != nil {
		return
	}
	PostStep("state_lasterror", testConfig, mode, reqr, state.LastError)

	if testConfig.expectedTable.Columns != nil && testConfig.expectedTable.Columns.Len() > 0 {
		//Check table schema
		table, err := sqlAdapter.GetTableSchema(ctx, tableName)
		PostStep("get_table", testConfig, mode, reqr, err)
		reqr.Equal(testConfig.expectedTable, table)
	}
	if testConfig.expectedRowsCount != nil {
		time.Sleep(1 * time.Second)
		//Check rows count and rows data when provided
		count, err := sqlAdapter.Count(ctx, tableName, nil)
		PostStep("select_count", testConfig, mode, reqr, err)
		reqr.Equal(testConfig.expectedRowsCount, count)
	}
}
