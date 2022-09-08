package sql

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMillionRows(t *testing.T) {
	t.Skip("This test is too slow")
	tests := []bulkerTestConfig{
		{
			name:  "one_million_rows",
			modes: []bulker.BulkMode{bulker.Transactional},
			expectedTable: &Table{
				Name:     "one_million_rows_test",
				PKFields: utils.Set[string]{},
				Columns: Columns{
					"_timestamp": SQLColumn{Type: "timestamp without time zone"},
					"id":         SQLColumn{Type: "bigint"},
					"name":       SQLColumn{Type: "text"},
				},
			},
			expectedState: &bulker.State{
				Status:         bulker.Completed,
				ProcessedRows:  1_000_000,
				SuccessfulRows: 1_000_000,
			},
			expectedRowsCount: 1_000_000,
			bulkerTypes:       []string{"postgres"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestConfig(t, tt, testOneMillion)
		})
	}
}

func testOneMillion(t *testing.T, testConfig bulkerTestConfig, mode bulker.BulkMode) {
	require := require.New(t)
	blk, err := bulker.CreateBulker(*testConfig.config)
	CheckError("create_bulker", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)
	defer func() {
		err = blk.Close()
		CheckError("bulker_close", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)
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
		err = sqlAdapter.DropTable(ctx, tableName, true)
		CheckError("pre_cleanup", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)
	}
	//clean up after test run
	if !testConfig.leaveResultingTable && !forceLeaveResultingTables {
		defer func() {
			sqlAdapter.DropTable(ctx, tableName, true)
		}()
	}
	stream, err := blk.CreateStream(t.Name(), tableName, mode, testConfig.streamOptions...)
	CheckError("create_stream", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)

	//Abort stream if error occurred
	defer func() {
		if err != nil {
			_, err = stream.Abort(ctx)
			CheckError("stream_abort", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)
		}
	}()

	for i := 0; i < 1_000_000; i++ {
		obj := types.Object{"_timestamp": constantTime, "id": i, "name": "test"}
		err = stream.Consume(ctx, obj)
		CheckError(fmt.Sprintf("consume_object_%d", i), testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)
		if err != nil && !testConfig.ignoreConsumeErrors {
			return
		}
	}
	//Commit stream
	state, err := stream.Complete(ctx)
	CheckError("stream_complete", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)

	if testConfig.expectedState != nil {
		require.Equal(bulker.Completed, state.Status)
		require.Equal(*testConfig.expectedState, state)
	}
	CheckError("state_lasterror", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, state.LastError)

	if testConfig.expectedTable != nil {
		//Check table schema
		table, err := sqlAdapter.GetTableSchema(ctx, tableName)
		CheckError("get_table", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)
		require.Equal(testConfig.expectedTable, table)
	}
	if testConfig.expectedRowsCount != nil {
		//Check rows count and rows data when provided
		count, err := sqlAdapter.Count(ctx, tableName, nil)
		CheckError("select_count", testConfig.config.BulkerType, mode, require, testConfig.expectedErrors, err)
		require.Equal(testConfig.expectedRowsCount, count)
	}
}
