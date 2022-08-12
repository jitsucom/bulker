package implementations

import (
	"context"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestAutoCommitStream(t *testing.T) {
	cfg := types.DataSourceConfig{
		Host:     "pg1.eventnative.com",
		Port:     5432,
		Db:       "db_ku4muzdna5tuzctiffw4_cec63eda",
		Username: "u_ku4muzdna5tuzctiffw4",
		Password: "VTRwyv33H31GqN2F",
		Schema:   "bulker",
	}
	assert := assert.New(t)

	queryLogger := logging.NewQueryLogger("test", os.Stderr, os.Stderr)
	p, err := NewPostgres(context.Background(), &cfg, queryLogger, types.SQLTypes{})
	assert.NoError(err)

	stream, err := NewAutoCommitStream("test", p, "bulker_autocommit_test")
	assert.NoError(err)

	objects := []types.Object{
		{"_timestamp": time.Now(), "id": 1, "name": "test"},
		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
	}
	for _, obj := range objects {
		err = stream.Consume(obj)
		assert.NoError(err)
	}

	state, err := stream.Complete()
	assert.NoError(err)
	assert.Equal(bulker.Completed, state.Status)
	assert.Equal(2, state.SuccessfulRows)
	assert.Equal(2, state.ProcessedRows)
	assert.NoError(state.LastError)
}

func TestTransactionalStream(t *testing.T) {
	cfg := types.DataSourceConfig{
		Host:     "pg1.eventnative.com",
		Port:     5432,
		Db:       "db_ku4muzdna5tuzctiffw4_cec63eda",
		Username: "u_ku4muzdna5tuzctiffw4",
		Password: "VTRwyv33H31GqN2F",
		Schema:   "bulker",
	}
	assert := assert.New(t)

	queryLogger := logging.NewQueryLogger("test", os.Stderr, os.Stderr)
	p, err := NewPostgres(context.Background(), &cfg, queryLogger, types.SQLTypes{})
	assert.NoError(err)

	stream, err := NewTransactionalStream("test", p, "bulker_transactional_test", bulker.WithPrimaryKey("id"))
	assert.NoError(err)

	objects := []types.Object{
		{"_timestamp": time.Now(), "id": 1, "name": "test"},
		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
	}
	for _, obj := range objects {
		err = stream.Consume(obj)
		assert.NoError(err)
	}

	state, err := stream.Complete()
	assert.NoError(err)
	assert.Equal(bulker.Completed, state.Status)
	assert.Equal(2, state.SuccessfulRows)
	assert.Equal(2, state.ProcessedRows)
	assert.NoError(state.LastError)
}

func TestReplaceTableStream(t *testing.T) {
	cfg := types.DataSourceConfig{
		Host:     "pg1.eventnative.com",
		Port:     5432,
		Db:       "db_ku4muzdna5tuzctiffw4_cec63eda",
		Username: "u_ku4muzdna5tuzctiffw4",
		Password: "VTRwyv33H31GqN2F",
		Schema:   "bulker",
	}
	assert := assert.New(t)

	queryLogger := logging.NewQueryLogger("test", os.Stderr, os.Stderr)
	p, err := NewPostgres(context.Background(), &cfg, queryLogger, types.SQLTypes{})
	assert.NoError(err)

	stream, err := NewReplaceTableStream("test", p, "bulker_replace_table_test", bulker.WithPrimaryKey("id"))
	assert.NoError(err)

	objects := []types.Object{
		{"_timestamp": time.Now(), "id": 1, "name": "test"},
		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
	}
	for _, obj := range objects {
		err = stream.Consume(obj)
		assert.NoError(err)
	}

	state, err := stream.Complete()
	assert.NoError(err)
	assert.Equal(bulker.Completed, state.Status)
	assert.Equal(2, state.SuccessfulRows)
	assert.Equal(2, state.ProcessedRows)
	assert.NoError(state.LastError)
}

func TestReplacePartitionStream(t *testing.T) {
	cfg := types.DataSourceConfig{
		Host:     "pg1.eventnative.com",
		Port:     5432,
		Db:       "db_ku4muzdna5tuzctiffw4_cec63eda",
		Username: "u_ku4muzdna5tuzctiffw4",
		Password: "VTRwyv33H31GqN2F",
		Schema:   "bulker",
	}
	assert := assert.New(t)

	queryLogger := logging.NewQueryLogger("test", os.Stderr, os.Stderr)
	p, err := NewPostgres(context.Background(), &cfg, queryLogger, types.SQLTypes{})
	assert.NoError(err)

	stream, err := NewReplacePartitionStream("test", p, "bulker_replace_partition_test", bulker.WithPartition("tadam"))
	assert.NoError(err)

	objects := []types.Object{
		{"_timestamp": time.Now(), "id": 1, "name": "test"},
		{"_timestamp": time.Now(), "id": 2, "name": "test2", "extra": "extra"},
	}
	for _, obj := range objects {
		err = stream.Consume(obj)
		assert.NoError(err)
	}

	state, err := stream.Complete()
	assert.NoError(err)
	assert.Equal(bulker.Completed, state.Status)
	assert.Equal(2, state.SuccessfulRows)
	assert.Equal(2, state.ProcessedRows)
	assert.NoError(state.LastError)
}
