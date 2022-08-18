package sql

import (
	"context"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

type bulkerTestConfig struct {
	name          string
	config        bulker.Config
	tableName     string
	mode          bulker.BulkMode
	streamOptions []bulker.StreamOption
}

func TestBulker(t *testing.T) {
	var bulkerTestData = []bulkerTestConfig{
		{mode: bulker.AutoCommit, name: "postgres_autocommit",
			config:    bulker.Config{Id: "postgres", BulkerType: "postgres", DestinationConfig: os.Getenv("BULKER_TEST_POSTGRES")},
			tableName: "autocommit_test"},
		{mode: bulker.Transactional, name: "postgres_transactional",
			config:    bulker.Config{Id: "postgres", BulkerType: "postgres", DestinationConfig: os.Getenv("BULKER_TEST_POSTGRES")},
			tableName: "transactional_test", streamOptions: []bulker.StreamOption{WithPrimaryKey("id"), WithMergeRows()}},
		{mode: bulker.ReplaceTable, name: "postgres_replacetable",
			config:    bulker.Config{Id: "postgres", BulkerType: "postgres", DestinationConfig: os.Getenv("BULKER_TEST_POSTGRES")},
			tableName: "replacetable_test", streamOptions: []bulker.StreamOption{WithPrimaryKey("id"), WithColumnType("id", "text")}},
		{mode: bulker.ReplacePartition, name: "postgres_replacepartition",
			config:    bulker.Config{Id: "postgres", BulkerType: "postgres", DestinationConfig: os.Getenv("BULKER_TEST_POSTGRES")},
			tableName: "replacepartition_test", streamOptions: []bulker.StreamOption{WithPartition("partition_id")}},
	}
	logging.Infof("BULKER_TEST_POSTGRES: %s", os.Getenv("BULKER_TEST_POSTGRES"))
	for _, td := range bulkerTestData {
		t.Run(td.name, func(t *testing.T) {
			testStreamSimple(t, td)
		})
	}
}

func testStreamSimple(t *testing.T, testConfig bulkerTestConfig) {
	require := require.New(t)

	blk, err := bulker.CreateBulker(testConfig.config)
	require.NoError(err)

	stream, err := blk.CreateStream(t.Name(), testConfig.tableName, testConfig.mode, testConfig.streamOptions...)
	require.NoError(err)

	objects := []types.Object{
		{"_timestamp": time.Now(), "id": 1, "name": "test"},
		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
	}
	ctx := context.Background()
	for _, obj := range objects {
		err = stream.Consume(ctx, obj)
		require.NoError(err)
	}

	state, err := stream.Complete(ctx)
	require.NoError(err)
	require.Equal(bulker.Completed, state.Status)
	require.Equal(2, state.SuccessfulRows)
	require.Equal(2, state.ProcessedRows)
	require.NoError(state.LastError)

	sqlAdapter, ok := blk.(SQLAdapter)
	require.True(ok)
	rows, err := sqlAdapter.Select(ctx, testConfig.tableName, nil)
	require.NoError(err)
	require.Equal(2, len(rows))
}
