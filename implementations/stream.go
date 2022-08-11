package implementations

import (
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/coordination"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/errorj"
	"github.com/jitsucom/bulker/types"
)

type AbstractStream struct {
	id        string
	p         bulker.SQLAdapter
	mode      bulker.BulkMode
	options   bulker.StreamOptions
	tableName string

	state       bulker.State
	tableHelper *TableHelper
}

func NewAbstractStream(id string, p bulker.SQLAdapter, tx *types.Transaction, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) AbstractStream {
	ps := AbstractStream{id: id, p: p, tableName: tableName, mode: mode}
	options := bulker.DefaultStreamOptions
	for _, option := range streamOptions {
		option(&options)
	}
	//TODO: max column?
	ps.tableHelper = NewTableHelper(p, tx, coordination.DummyCoordinationService{}, options.PrimaryKeyFields, 1000)
	ps.state = bulker.State{RowsErrors: map[int]error{}, Status: bulker.Active}
	return ps
}

func (ps *AbstractStream) Preprocess(object types.Object) (*types.Table, []types.Object, error) {
	if ps.state.Status != bulker.Active {
		return nil, nil, fmt.Errorf("stream is not active. Status: %s", ps.state.Status)
	}
	processedObjects, err := ProcessEvents(ps.tableName, []types.Object{object})
	if err != nil {
		return nil, nil, err
	}
	if len(ps.options.CustomTypes) > 0 {
		// enrich overridden schema types
		processedObjects.BatchHeader.Fields.OverrideTypes(ps.options.CustomTypes)
	}
	table := ps.tableHelper.MapTableSchema(processedObjects.BatchHeader)
	return table, processedObjects.GetPayload(), nil
}

func (ps *AbstractStream) PostConsume(err error) error {
	if err != nil {
		ps.tableHelper.ClearCache(ps.tableName)
		ps.state.RowsErrors[ps.state.ProcessedRows] = err
		ps.state.LastError = err
		return err
	} else {
		ps.state.SuccessfulRows++
	}
	ps.state.ProcessedRows++
	return nil
}

func (ps *AbstractStream) PostComplete(err error) (bulker.State, error) {
	if err != nil {
		ps.state.LastError = err
		ps.state.Status = bulker.Failed
	} else {
		ps.state.Status = bulker.Completed
	}
	return ps.state, err
}

type AutoCommitStream struct {
	AbstractStream
}

func NewAutoCommitStream(id string, p bulker.SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := AutoCommitStream{}
	ps.AbstractStream = NewAbstractStream(id, p, p.GetAutoCommitTx(), tableName, bulker.AutoCommit, streamOptions...)

	//var err error
	//if mode != bulker.NewAutoCommitTx {
	//	ps.tx, err = p.OpenTx()
	//	if err != nil {
	//		return nil, err
	//	}
	//} else {
	//	ps.tx = NewAutoCommitTx()
	//}
	return &ps, nil
}

func (ps *AutoCommitStream) Consume(object types.Object) (err error) {
	defer func() {
		err = ps.PostConsume(err)
	}()
	table, processedObjects, err := ps.Preprocess(object)
	if err != nil {
		return err
	}
	table, err = ps.tableHelper.EnsureTableWithCaching(ps.id, table)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure table")
	}
	return ps.p.Insert(ps.p.GetAutoCommitTx(), table, ps.options.MergeRows, processedObjects)
}

func (ps *AutoCommitStream) Abort() (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}

func (ps *AutoCommitStream) Complete() (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

// TODO: use real temporary tables: YES
// TODO: prebuffer table ?
// TODO: check whether COPY is transactional ?
// TODO: pk conflict on Redshift file storage ?
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

func (ps *TransactionalStream) Abort() (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	_ = ps.p.DropTable(ps.tx, ps.tmpTable, true)
	_ = ps.tx.Rollback()
	ps.state.Status = bulker.Aborted
	return ps.state, err
}

func (ps *TransactionalStream) Complete() (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		if err != nil {
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
	}
	//drop tmp table if exists
	_ = ps.p.DropTable(ps.tx, ps.tmpTable, true)
	err = ps.tx.Commit()
	return
}
