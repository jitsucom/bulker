package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/joomcode/errorx"
)

type ReplaceTableStream struct {
	AbstractSQLStream
	tmpTable *Table
}

func newReplaceTableStream(id string, p SQLAdapter, dataSource *sql.DB, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := ReplaceTableStream{}

	ps.AbstractSQLStream = NewAbstractStream(id, p, dataSource, tableName, bulker.ReplaceTable, streamOptions...)

	return &ps, nil
}

func (ps *ReplaceTableStream) Consume(ctx context.Context, object types.Object) (err error) {
	defer func() {
		err = ps.postConsume(err)
	}()
	if err = ps.initTx(ctx); err != nil {
		return err
	}
	//type mapping, flattening => table schema
	tableForObject, processedObjects, err := ps.preprocess(object)
	if err != nil {
		return err
	}
	//first object
	if ps.tmpTable == nil {
		ps.tmpTable = &Table{
			Name:    fmt.Sprintf("%s_tmp_%s", ps.tableName, timestamp.Now().Format("060102_150405")),
			Columns: tableForObject.Columns,
		}
	} else {
		ps.tmpTable.Columns = tableForObject.Columns
	}
	//adapt tmp table for new object columns if any
	ps.tmpTable, err = ps.tableHelper.EnsureTableWithCaching(ctx, ps.id, ps.tmpTable)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure temporary table")
	}
	return ps.p.Insert(ctx, ps.tx, ps.tmpTable, false, processedObjects)
}

func (ps *ReplaceTableStream) Complete(ctx context.Context) (state bulker.State, err error) {
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
	//TODO: replace with empty table if no objects
	if ps.state.SuccessfulRows > 0 {
		err = ps.p.ReplaceTable(ctx, ps.tx, ps.tableName, ps.tmpTable.Name, true)
		if errorx.IsOfType(err, errorj.DropError) {
			err = ps.p.ReplaceTable(ctx, ps.tx, ps.tableName, ps.tmpTable.Name, false)
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

func (ps *ReplaceTableStream) Abort(ctx context.Context) (state bulker.State, err error) {
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
