package implementations

import (
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/joomcode/errorx"
)

type ReplaceTableStream struct {
	AbstractStream
	tx       *types.Transaction
	tmpTable *types.Table
}

func NewReplaceTableStream(id string, p bulker.SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := ReplaceTableStream{}

	var err error
	ps.tx, err = p.OpenTx()
	if err != nil {
		return nil, err
	}
	ps.AbstractStream = NewAbstractStream(id, p, ps.tx, tableName, bulker.ReplaceTable, streamOptions...)

	return &ps, nil
}

func (ps *ReplaceTableStream) Consume(object types.Object) (err error) {
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
		ps.tmpTable = &types.Table{
			Name:    fmt.Sprintf("%s_tmp_%s", ps.tableName, timestamp.Now().Format("060102_150405")),
			Columns: tableForObject.Columns,
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

func (ps *ReplaceTableStream) Complete() (state bulker.State, err error) {
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
		err = ps.p.ReplaceTable(ps.tx, ps.tableName, ps.tmpTable.Name, true)
		if errorx.IsOfType(err, errorj.DropError) {
			err = ps.p.ReplaceTable(ps.tx, ps.tableName, ps.tmpTable.Name, false)
		}
		if err != nil {
			return ps.state, err
		}
		err = ps.tx.Commit()
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}

func (ps *ReplaceTableStream) Abort() (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	_ = ps.p.DropTable(ps.tx, ps.tmpTable, true)
	_ = ps.tx.Rollback()
	ps.state.Status = bulker.Aborted
	return ps.state, err
}
