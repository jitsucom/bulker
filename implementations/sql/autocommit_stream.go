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
	datasource *sql.DB
}

func newAutoCommitStream(id string, p SQLAdapter, dataSource *sql.DB, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := AutoCommitStream{datasource: dataSource}
	ps.AbstractSQLStream = NewAbstractStream(id, p, dataSource, tableName, bulker.AutoCommit, streamOptions...)
	if ps.options.MergeRows && len(ps.options.PrimaryKeyFields) == 0 {
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
	return ps.p.Insert(ctx, ps.datasource, table, ps.options.MergeRows, processedObjects)
}

func (ps *AutoCommitStream) Complete(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}
