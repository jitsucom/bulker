package sql

import (
	"context"
	"database/sql"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

type AutoCommitStream struct {
	AbstractSQLStream
}

func newAutoCommitStream(id string, p SQLAdapter, dataSource *sql.DB, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := AutoCommitStream{}
	var err error
	ps.AbstractSQLStream, err = newAbstractStream(id, p, dataSource, tableName, bulker.AutoCommit, streamOptions...)
	if err != nil {
		return nil, err
	}

	return &ps, nil
}

func (ps *AutoCommitStream) Consume(ctx context.Context, object types.Object) (err error) {
	defer func() {
		err = ps.postConsume(err)
	}()
	if err = ps.init(ctx); err != nil {
		return err
	}
	table, processedObjects, err := ps.preprocess(object)
	if err != nil {
		return err
	}
	table, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, table)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure table")
	}
	return ps.sqlAdapter.Insert(ctx, table, ps.merge, processedObjects)
}

func (ps *AutoCommitStream) Complete(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}
