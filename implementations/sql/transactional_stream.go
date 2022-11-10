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
	ps.tmpTableFunc = func(ctx context.Context, tableForObject *Table, batchFile bool) *Table {
		dstTable := tableForObject
		if !batchFile {
			existingTable, _ := ps.tx.GetTableSchema(ctx, ps.tableName)
			if existingTable.Exists() {
				dstTable = existingTable
				dstTable.Columns = utils.MapPutAll(tableForObject.Columns, dstTable.Columns)
			}
		}
		tmpTableName := fmt.Sprintf("jitsu_tmp_%s", uuid.NewLettersNumbers()[:8])
		return &Table{
			Name:           tmpTableName,
			Columns:        dstTable.Columns,
			PKFields:       dstTable.PKFields,
			PrimaryKeyName: BuildConstraintName(tmpTableName),
			Temporary:      true,
		}
	}
	return &ps, nil
}

func (ps *TransactionalStream) init(ctx context.Context) (err error) {
	if ps.inited {
		return nil
	}
	//localBatchFile := localBatchFileOption.Get(&ps.options)
	//if localBatchFile != "" && ps.batchFile == nil {
	//	ps.batchFile, err = os.CreateTemp("", localBatchFile)
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
	if ps.batchFile != nil {
		return ps.writeToBatchFile(ctx, tableForObject, processedObjects)
	} else {
		return ps.insert(ctx, tableForObject, processedObjects)
	}
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
		if ps.batchFile != nil {
			if err = ps.flushBatchFile(ctx); err != nil {
				return ps.state, err
			}
		}
		//ensure that dstTable contains all columns from tmpTable
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
