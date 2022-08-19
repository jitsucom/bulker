package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

type AutoCommitStream struct {
	AbstractSQLStream
	db    *TxOrDBWrapper
	merge bool
}

func newAutoCommitStream(id string, p SQLAdapter, dataSource *sql.DB, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := AutoCommitStream{db: p.DbWrapper()}
	ps.AbstractSQLStream = NewAbstractStream(id, p, dataSource, tableName, bulker.AutoCommit, streamOptions...)
	ps.merge = mergeRowsOption.Get(&ps.options)
	if ps.merge && len(primaryKeyOption.Get(&ps.options)) == 0 {
		return nil, fmt.Errorf("MergeRows option requires primary key in the destination table. Please provide WithPrimaryKey option")
	}
	return &ps, nil
}

func (ps *AutoCommitStream) Consume(ctx context.Context, object types.Object) (err error) {
	defer func() {
		err = ps.postConsume(err)
	}()
	table, processedObjects, err := ps.preprocess(object)
	if err != nil {
		return err
	}
	table, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, table)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure table")
	}
	return ps.p.Insert(ctx, ps.db, table, ps.merge, processedObjects)
}

func (ps *AutoCommitStream) Complete(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}
