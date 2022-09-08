package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

// TODO: Use real temporary tables
// TODO: Prebuffer inserts
// TODO: User prepared statements for insert
// TODO: Use driver specific bulk/batch approaches
type TransactionalStream struct {
	AbstractTransactionalSQLStream
}

func newTransactionalStream(id string, p SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := TransactionalStream{}
	var err error
	ps.AbstractTransactionalSQLStream, err = newAbstractTransactionalStream(id, p, tableName, bulker.Transactional, streamOptions...)
	if err != nil {
		return nil, err
	}
	return &ps, nil
}

func (ps *TransactionalStream) init(ctx context.Context) (err error) {
	if ps.inited {
		return nil
	}
	//localBatchFile := localBatchFileOption.Get(&ps.options)
	//if localBatchFile != "" && ps.tmpFile == nil {
	//	ps.tmpFile, err = os.CreateTemp("", localBatchFile)
	//	if err != nil {
	//		return err
	//	}
	//}
	return ps.AbstractTransactionalSQLStream.init(ctx)
}

func (ps *TransactionalStream) Consume(ctx context.Context, object types.Object) (err error) {
	defer func() {
		err = ps.postConsume(err)
	}()
	if err = ps.init(ctx); err != nil {
		return err
	}

	//type mapping, flattening => table schema
	tableForObject, processedObjects, err := ps.preprocess(object)
	if err != nil {
		return err
	}
	ps.ensureSchema(ctx, &ps.tmpTable, tableForObject, func(ctx context.Context) (*Table, error) {
		//if destination table already exist
		//init tmp table with columns=union(table.columns, cachedTable.columns)
		//to avoid unnecessary alters of tmp table during transaction
		dstTable, err := ps.tx.GetTableSchema(ctx, ps.tableName)
		if err != nil {
			return nil, errorj.Decorate(err, "failed to check for existing destination table schema")
		}
		if dstTable.Exists() {
			utils.MapPutAll(tableForObject.Columns, dstTable.Columns)
		}
		ps.dstTable = tableForObject
		return &Table{
			Name:           fmt.Sprintf("jitsu_tmp_%s", uuid.NewLettersNumbers()[:8]),
			PrimaryKeyName: fmt.Sprintf("jitsu_tmp_pk_%s", uuid.NewLettersNumbers()[:8]),
			PKFields:       tableForObject.PKFields,
			Columns:        tableForObject.Columns,
		}, nil
	})

	return ps.insert(ctx, ps.tmpTable, processedObjects)
}

func (ps *TransactionalStream) Complete(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		state, err = ps.postComplete(ctx, err)
	}()
	//if at least one object was inserted
	if ps.state.SuccessfulRows > 0 {
		if ps.tmpFile != nil {
			if err = ps.flushTmpFile(ctx, ps.tmpTable, true); err != nil {
				return ps.state, err
			}
		}
		//tmp table accumulates all schema changes happened during transaction
		ps.dstTable.Columns = ps.tmpTable.Columns
		ps.dstTable, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, ps.dstTable)
		if err != nil {
			return ps.state, errorj.Decorate(err, "failed to ensure destination table")
		}
		//copy data from tmp table to destination table
		err = ps.tx.CopyTables(ctx, ps.dstTable, ps.tmpTable, ps.merge)
		if err != nil {
			return ps.state, err
		}
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}
