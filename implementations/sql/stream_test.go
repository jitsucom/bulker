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
}

//
//func TestTransactionalStream(t *testing.T) {
//	cfg := sql.DataSourceConfig{
//		Host:     "pg1.eventnative.com",
//		Port:     5432,
//		Db:       "db_ku4muzdna5tuzctiffw4_cec63eda",
//		Username: "u_ku4muzdna5tuzctiffw4",
//		Password: "VTRwyv33H31GqN2F",
//		Schema:   "bulker",
//	}
//	assert := assert.New(t)
//
//	queryLogger := logging.NewQueryLogger("test", os.Stderr, os.Stderr)
//	p, err := sql.NewPostgres(context.Background(), &cfg, queryLogger, types.SQLTypes{})
//	assert.NoError(err)
//
//	stream, err := sql.NewTransactionalStream("test", p, "bulker_transactional_test", bulker.WithPrimaryKey("id"))
//	assert.NoError(err)
//
//	objects := []types.Object{
//		{"_timestamp": time.Now(), "id": 1, "name": "test"},
//		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
//	}
//	for _, obj := range objects {
//		err = stream.Consume(obj)
//		assert.NoError(err)
//	}
//
//	state, err := stream.Complete()
//	assert.NoError(err)
//	assert.Equal(bulker.Completed, state.Status)
//	assert.Equal(2, state.SuccessfulRows)
//	assert.Equal(2, state.ProcessedRows)
//	assert.NoError(state.LastError)
//}
//
//func TestReplaceTableStream(t *testing.T) {
//	cfg := sql.DataSourceConfig{
//		Host:     "pg1.eventnative.com",
//		Port:     5432,
//		Db:       "db_ku4muzdna5tuzctiffw4_cec63eda",
//		Username: "u_ku4muzdna5tuzctiffw4",
//		Password: "VTRwyv33H31GqN2F",
//		Schema:   "bulker",
//	}
//	assert := assert.New(t)
//
//	queryLogger := logging.NewQueryLogger("test", os.Stderr, os.Stderr)
//	p, err := sql.NewPostgres(context.Background(), &cfg, queryLogger, types.SQLTypes{})
//	assert.NoError(err)
//
//	stream, err := sql.NewReplaceTableStream("test", p, "bulker_replace_table_test", bulker.WithPrimaryKey("id"))
//	assert.NoError(err)
//
//	objects := []types.Object{
//		{"_timestamp": time.Now(), "id": 1, "name": "test"},
//		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
//	}
//	for _, obj := range objects {
//		err = stream.Consume(obj)
//		assert.NoError(err)
//	}
//
//	state, err := stream.Complete()
//	assert.NoError(err)
//	assert.Equal(bulker.Completed, state.Status)
//	assert.Equal(2, state.SuccessfulRows)
//	assert.Equal(2, state.ProcessedRows)
//	assert.NoError(state.LastError)
//}
//
//func TestReplacePartitionStream(t *testing.T) {
//	cfg := sql.DataSourceConfig{
//		Host:     "pg1.eventnative.com",
//		Port:     5432,
//		Db:       "db_ku4muzdna5tuzctiffw4_cec63eda",
//		Username: "u_ku4muzdna5tuzctiffw4",
//		Password: "VTRwyv33H31GqN2F",
//		Schema:   "bulker",
//	}
//	assert := assert.New(t)
//
//	queryLogger := logging.NewQueryLogger("test", os.Stderr, os.Stderr)
//	p, err := sql.NewPostgres(context.Background(), &cfg, queryLogger, types.SQLTypes{})
//	assert.NoError(err)
//
//	stream, err := sql.NewReplacePartitionStream("test", p, "bulker_replace_partition_test", bulker.WithPartition("tadam"))
//	assert.NoError(err)
//
//	objects := []types.Object{
//		{"_timestamp": time.Now(), "id": 1, "name": "test"},
//		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
//	}
//	for _, obj := range objects {
//		err = stream.Consume(obj)
//		assert.NoError(err)
//	}
//
//	state, err := stream.Complete()
//	assert.NoError(err)
//	assert.Equal(bulker.Completed, state.Status)
//	assert.Equal(2, state.SuccessfulRows)
//	assert.Equal(2, state.ProcessedRows)
//	assert.NoError(state.LastError)
//}
