package sql

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMillionRows(t *testing.T) {
	t.Skip("This test is too slow")
	tests := []bulkerTestConfig{
		{
			name:  "one_million_rows",
			modes: []bulker.BulkMode{bulker.Transactional},
			expectedTable: &ExpectedTable{
				Columns: justColumns("_timestamp", "id", "name"),
			},
			expectedState: &bulker.State{
				Status:         bulker.Completed,
				ProcessedRows:  1_000_000,
				SuccessfulRows: 1_000_000,
			},
			expectedRowsCount: 1_000_000,
			bulkerTypes:       []string{"bigquery"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testOneMillion)
		})
	}
}

func TestMillionRowsBatched(t *testing.T) {
	t.Skip("This test is too slow")
	tests := []bulkerTestConfig{
		{
			name:              "one_million_rows_batched",
			modes:             []bulker.BulkMode{bulker.Transactional},
			batchSize:         10_000,
			expectedRowsCount: 1_000_000,
			bulkerTypes:       []string{"postgres"},
			streamOptions:     []bulker.StreamOption{WithPrimaryKey("id"), WithMergeRows()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testOneMillion)
		})
	}
}
func testOneMillion(t *testing.T, testConfig bulkerTestConfig, mode bulker.BulkMode) {
	reqr := require.New(t)
	blk, err := bulker.CreateBulker(*testConfig.config)
	CheckError("create_bulker", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
	defer func() {
		err = blk.Close()
		CheckError("bulker_close", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
	}()
	sqlAdapter, ok := blk.(SQLAdapter)
	reqr.True(ok)
	ctx := context.Background()
	tableName := testConfig.tableName
	if tableName == "" {
		tableName = testConfig.name + "_test"
	}
	err = sqlAdapter.InitDatabase(ctx)
	CheckError("init_database", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
	//clean up in case of previous test failure
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		err = sqlAdapter.DropTable(ctx, tableName, true)
		CheckError("pre_cleanup", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
	}
	//clean up after test run
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		defer func() {
			sqlAdapter.DropTable(ctx, tableName, true)
		}()
	}
	stream, err := blk.CreateStream(t.Name(), tableName, mode, testConfig.streamOptions...)
	CheckError("create_stream", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
	if err != nil {
		return
	}
	//Abort stream if error occurred
	defer func() {
		if err != nil {
			_, err = stream.Abort(ctx)
			CheckError("stream_abort", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
		}
	}()

	startTime := timestamp.Now()
	for i := 0; i < 1_000_000; i++ {
		if i > 0 && i%10000 == 0 {
			_, err := stream.Complete(ctx)
			CheckError("stream_complete", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
			logging.Infof("%d. batch is completed in %s", i, time.Since(startTime))
			stream, err = blk.CreateStream(t.Name(), tableName, mode, testConfig.streamOptions...)
			CheckError("create_stream", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
			if err != nil {
				return
			}
			startTime = timestamp.Now()
		}
		obj := types.Object{"_timestamp": constantTime, "id": i, "name": "test"}
		err = stream.Consume(ctx, obj)
		CheckError(fmt.Sprintf("consume_object_%d", i), testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
		if err != nil && !testConfig.ignoreConsumeErrors {
			return
		}
	}
	//Commit stream
	state, err := stream.Complete(ctx)
	CheckError("stream_complete", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)

	if testConfig.expectedState != nil {
		reqr.Equal(*testConfig.expectedState, state)
	}
	if err != nil {
		return
	}
	CheckError("state_lasterror", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, state.LastError)

	if testConfig.expectedTable != nil {
		//Check table schema
		table, err := sqlAdapter.GetTableSchema(ctx, tableName)
		CheckError("get_table", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
		reqr.Equal(testConfig.expectedTable, table)
	}
	if testConfig.expectedRowsCount != nil {
		//Check rows count and rows data when provided
		count, err := sqlAdapter.Count(ctx, tableName, nil)
		CheckError("select_count", testConfig.config.BulkerType, mode, reqr, testConfig.expectedErrors, err)
		reqr.Equal(testConfig.expectedRowsCount, count)
	}
}