package implementations

import (
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

type TransactionalStream struct {
	AbstractStream
	tx       *types.Transaction
	dstTable *types.Table
	tmpTable *types.Table
}

func NewTransactionalStream(id string, p bulker.SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := TransactionalStream{}

	var err error
	ps.tx, err = p.OpenTx()
	if err != nil {
		return nil, err
	}
	ps.AbstractStream = NewAbstractStream(id, p, ps.tx, tableName, bulker.Transactional, streamOptions...)

	return &ps, nil
}

func (ps *TransactionalStream) Consume(object types.Object) (err error) {
	defer func() {
		err = ps.PostConsume(err)
	}()
	//type mapping, flattening => table schema
	tableForObject, processedObjects, err := ps.Preprocess(object)
	if err != nil {
		return err
	}
	//first object
	if ps.tmpTable == nil {
		//if destination table already exist and cached
		//init tmp table with columns=union(table.columns, cachedTable.columns)
		//to avoid unnecessary alters of tmp table during transaction
		cachedTable, ok := ps.tableHelper.getCached(ps.tableName)
		if ok {
			utils.MapPutAll(tableForObject.Columns, cachedTable.Columns)
		}
		ps.dstTable = tableForObject
		ps.tmpTable = &types.Table{
			Name:    fmt.Sprintf("jitsu_tmp_%s", uuid.NewLettersNumbers()[:5]),
			Columns: ps.dstTable.Columns,
		}
	} else {
		ps.tmpTable.Columns = tableForObject.Columns
	}
	//adapt tmp table for new object columns if any
	ps.tmpTable, err = ps.tableHelper.EnsureTableWithCaching(ps.id, ps.tmpTable)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure temporary table")
	}
	return ps.p.Insert(ps.tx, ps.tmpTable, false, processedObjects)
}

func (ps *TransactionalStream) Complete() (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		if err != nil {
			ps.state.SuccessfulRows = 0
			_ = ps.tx.Rollback()
		}
		state, err = ps.PostComplete(err)
	}()
	//if at least one object was inserted
	if ps.state.SuccessfulRows > 0 {
		//tmp table accumulates all schema changes happened during transaction
		ps.dstTable.Columns = ps.tmpTable.Columns
		ps.dstTable, err = ps.tableHelper.EnsureTableWithCaching(ps.id, ps.dstTable)
		if err != nil {
			return ps.state, errorj.Decorate(err, "failed to ensure destination table")
		}
		//copy data from tmp table to destination table
		err = ps.p.MergeTables(ps.tx, ps.dstTable, ps.tmpTable)
		if err != nil {
			return ps.state, err
		}
		//drop tmp table if exists
		_ = ps.p.DropTable(ps.tx, ps.tmpTable, true)
		err = ps.tx.Commit()
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}

func (ps *TransactionalStream) Abort() (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	_ = ps.p.DropTable(ps.tx, ps.tmpTable, true)
	_ = ps.tx.Rollback()
	ps.state.Status = bulker.Aborted
	return ps.state, err
}
