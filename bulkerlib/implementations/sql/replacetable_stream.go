package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/joomcode/errorx"
	"time"
)

type ReplaceTableStream struct {
	*AbstractTransactionalSQLStream
}

func newReplaceTableStream(id string, p SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := ReplaceTableStream{}

	var err error
	ps.AbstractTransactionalSQLStream, err = newAbstractTransactionalStream(id, p, tableName, bulker.ReplaceTable, streamOptions...)
	if err != nil {
		return nil, err
	}
	ps.tmpTableFunc = func(ctx context.Context, tableForObject *Table, object types.Object) (table *Table) {
		tmpTable := &Table{
			Name:           fmt.Sprintf("%s_tmp%s", utils.ShortenString(ps.tableName, 47), time.Now().Format("060102150405")),
			PrimaryKeyName: tableForObject.PrimaryKeyName,
			//PrimaryKeyName: fmt.Sprintf("%s_%s", tableForObject.PrimaryKeyName, time.Now().Format("060102_150405")),
			PKFields:        tableForObject.PKFields,
			Columns:         tableForObject.Columns,
			TimestampColumn: tableForObject.TimestampColumn,
		}
		if ps.schemaFromOptions != nil {
			ps.adjustTableColumnTypes(tmpTable, nil, ps.schemaFromOptions, object)
		}
		return tmpTable
	}
	return &ps, nil
}

func (ps *ReplaceTableStream) Complete(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		state, err = ps.postComplete(ctx, err)
	}()
	if ps.state.LastError == nil {
		//if at least one object was inserted
		if ps.state.SuccessfulRows > 0 {
			if ps.batchFile != nil {
				ws, err := ps.flushBatchFile(ctx)
				ps.state.AddWarehouseState(ws)
				if err != nil {
					return ps.state, err
				}
			}
			r, ok := ps.state.Representation.(RepresentationTable)
			if ok {
				r.Name = ps.tableName
				r.Temporary = false
				ps.state.Representation = r
			}
			err1 := ps.tx.ReplaceTable(ctx, ps.tableName, ps.tmpTable, true)
			if err1 != nil {
				logging.Errorf("[%s] Error replacing table: %v", ps.id, err1)
			}
			if errorx.IsOfType(err1, errorj.DropError) {
				err = ps.tx.ReplaceTable(ctx, ps.tableName, ps.tmpTable, false)
				if err != nil {
					err = multierror.Append(err1, err).ErrorOrNil()
				}
			} else {
				err = err1
			}
			if err != nil {
				return ps.state, err
			}
		} else {
			//when no objects were consumed. we need to replace table with empty one.
			//truncation seems like a more straightforward approach.
			//no transaction was opened yet and not needed that is why we use ps.sqlAdapter instead of tx
			err = ps.init(ctx)
			if err != nil {
				var table *Table
				table, err = ps.sqlAdapter.GetTableSchema(ctx, ps.tableName)
				if table.Exists() {
					err = ps.sqlAdapter.TruncateTable(ctx, ps.tableName)
				}
			}
		}
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}
