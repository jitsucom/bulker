package sql

import (
	"context"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

type AutoCommitStream struct {
	AbstractSQLStream
}

func newAutoCommitStream(id string, p SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := AutoCommitStream{}
	var err error
	ps.AbstractSQLStream, err = newAbstractStream(id, p, tableName, bulker.AutoCommit, streamOptions...)
	if err != nil {
		return nil, err
	}

	return &ps, nil
}

func (ps *AutoCommitStream) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObjects []types.Object, err error) {
	defer func() {
		err = ps.postConsume(err)
		state = ps.state
	}()
	if err = ps.init(ctx); err != nil {
		return
	}
	table, processedObjects, err := ps.preprocess(object)
	if err != nil {
		ps.updateRepresentationTable(table)
		return
	}
	dstTable, err := ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, table)
	if err != nil {
		ps.updateRepresentationTable(table)
		err = errorj.Decorate(err, "failed to ensure table")
		return
	}
	ps.updateRepresentationTable(dstTable)
	return ps.state, processedObjects, ps.sqlAdapter.Insert(ctx, dstTable, ps.merge, processedObjects)
}

func (ps *AutoCommitStream) Complete(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}
