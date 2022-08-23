package sql

import (
	"context"
	"database/sql"
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
	dstTable *Table
	tmpTable *Table
}

func newTransactionalStream(id string, p SQLAdapter, dataSource *sql.DB, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := TransactionalStream{}

	var err error
	ps.AbstractTransactionalSQLStream, err = newAbstractTransactionalStream(id, p, dataSource, tableName, bulker.Transactional, streamOptions...)
	if err != nil {
		return nil, err
	}
	return &ps, nil
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
	//first object
	if ps.tmpTable == nil {
		//if destination table already exist
		//init tmp table with columns=union(table.columns, cachedTable.columns)
		//to avoid unnecessary alters of tmp table during transaction
		dstTable, err := ps.p.GetTableSchema(ctx, ps.tx, ps.tableName)
		if err != nil {
			return errorj.Decorate(err, "failed to check for existing destination table schema")
		}
		if dstTable.Exists() {
			utils.MapPutAll(tableForObject.Columns, dstTable.Columns)
		}
		ps.dstTable = tableForObject
		ps.tmpTable = &Table{
			Name:           fmt.Sprintf("jitsu_tmp_%s", uuid.NewLettersNumbers()[:8]),
			PrimaryKeyName: fmt.Sprintf("jitsu_tmp_pk_%s", uuid.NewLettersNumbers()[:8]),
			PKFields:       tableForObject.PKFields,
			Columns:        tableForObject.Columns,
		}
	} else {
		ps.tmpTable.Columns = tableForObject.Columns
	}
	//adapt tmp table for new object columns if any
	ps.tmpTable, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, ps.tmpTable)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure temporary table")
	}
	return ps.p.Insert(ctx, ps.tx, ps.tmpTable, ps.merge, processedObjects)
}

func (ps *TransactionalStream) Complete(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		if err != nil {
			ps.state.SuccessfulRows = 0
			if ps.tx != nil {
				_ = ps.p.DropTable(ctx, ps.tx, ps.tmpTable.Name, true)
				_ = ps.tx.Rollback()
			}
		}
		state, err = ps.postComplete(err)
	}()
	//if at least one object was inserted
	if ps.state.SuccessfulRows > 0 {
		//tmp table accumulates all schema changes happened during transaction
		ps.dstTable.Columns = ps.tmpTable.Columns
		ps.dstTable, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, ps.dstTable)
		if err != nil {
			return ps.state, errorj.Decorate(err, "failed to ensure destination table")
		}
		//copy data from tmp table to destination table
		err = ps.p.CopyTables(ctx, ps.tx, ps.dstTable, ps.tmpTable, ps.merge)
		if err != nil {
			return ps.state, err
		}
		//drop tmp table if exists
		_ = ps.p.DropTable(ctx, ps.tx, ps.tmpTable.Name, true)
		err = ps.tx.Commit()
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}

func (ps *TransactionalStream) Abort(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	if ps.tx != nil {
		_ = ps.p.DropTable(ctx, ps.tx, ps.tmpTable.Name, true)
		_ = ps.tx.Rollback()
	}
	ps.state.Status = bulker.Aborted
	return ps.state, err
}
