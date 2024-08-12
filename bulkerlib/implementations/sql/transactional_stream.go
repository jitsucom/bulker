package sql

import (
	"context"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"time"
)

// TODO: Use real temporary tables
type TransactionalStream struct {
	*AbstractTransactionalSQLStream
}

func newTransactionalStream(id string, p SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := TransactionalStream{}
	var err error
	ps.AbstractTransactionalSQLStream, err = newAbstractTransactionalStream(id, p, tableName, bulker.Batch, streamOptions...)
	if err != nil {
		return nil, err
	}
	ps.existingTable, _ = ps.sqlAdapter.GetTableSchema(context.Background(), ps.namespace, ps.tableName)
	ps.tmpTableFunc = func(ctx context.Context, tableForObject *Table, object types.Object) (table *Table) {
		dstTable := utils.Ternary(ps.existingTable.Exists(), ps.existingTable, tableForObject).Clone()
		ps.adjustTableColumnTypes(dstTable, ps.existingTable, tableForObject, object)
		if ps.schemaFromOptions != nil {
			ps.adjustTableColumnTypes(dstTable, ps.existingTable, ps.schemaFromOptions, object)
		}
		tmpTableName := fmt.Sprintf("%s_tmp%s", utils.ShortenString(tableName, 47), time.Now().Format("060102150405"))
		return &Table{
			Namespace:       p.TmpNamespace(ps.namespace),
			Name:            tmpTableName,
			Columns:         dstTable.Columns,
			Temporary:       true,
			TimestampColumn: tableForObject.TimestampColumn,
		}
	}
	return &ps, nil
}

func (ps *TransactionalStream) init(ctx context.Context) (err error) {
	if ps.inited {
		return nil
	}
	return ps.AbstractTransactionalSQLStream.init(ctx)
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
		ps.state.AddWarehouseState(bulker.WarehouseState{
			Name:            "consume",
			TimeProcessedMs: time.Since(ps.startTime).Milliseconds(),
		})
		if ps.batchFile != nil {
			ws, err := ps.flushBatchFile(ctx)
			ps.state.AddWarehouseState(ws)
			if err != nil {
				return ps.state, err
			}
		}
		var dstTable *Table
		dstTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithoutCaching(ctx, ps.tx, ps.id, ps.dstTable)
		if err != nil {
			ps.updateRepresentationTable(ps.dstTable)
			return ps.state, errorj.Decorate(err, "failed to ensure destination table")
		}
		ps.dstTable = dstTable
		ps.updateRepresentationTable(ps.dstTable)
		//copy data from tmp table to destination table
		ws, err := ps.tx.CopyTables(ctx, ps.dstTable, ps.tmpTable, ps.mergeWindow)
		ps.state.AddWarehouseState(ws)
		if err != nil {
			return ps.state, err
		}
		return ps.state, nil
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}
