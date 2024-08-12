package sql

import (
	"context"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"time"
)

type ReplacePartitionStream struct {
	*AbstractTransactionalSQLStream
	partitionId string
}

func newReplacePartitionStream(id string, p SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (stream bulker.BulkerStream, err error) {
	ps := ReplacePartitionStream{}
	so := bulker.StreamOptions{}
	for _, opt := range streamOptions {
		so.Add(opt)
	}
	partitionId := bulker.PartitionIdOption.Get(&so)
	if partitionId == "" {
		return nil, errors.New("WithPartition is required option for ReplacePartitionStream")
	}
	ps.AbstractTransactionalSQLStream, err = newAbstractTransactionalStream(id, p, tableName, bulker.ReplacePartition, streamOptions...)
	if err != nil {
		return nil, err
	}
	ps.partitionId = partitionId
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

func (ps *ReplacePartitionStream) ConsumeJSON(ctx context.Context, json []byte) (state bulker.State, processedObject types.Object, err error) {
	var obj types.Object
	err = jsonorder.Unmarshal(json, &obj)
	if err != nil {
		return ps.state, nil, fmt.Errorf("Error parsing JSON: %v", err)
	}
	return ps.Consume(ctx, obj)
}

func (ps *ReplacePartitionStream) ConsumeMap(ctx context.Context, mp map[string]any) (state bulker.State, processedObject types.Object, err error) {
	return ps.Consume(ctx, types.ObjectFromMap(mp))
}

func (ps *ReplacePartitionStream) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObjects types.Object, err error) {
	objCopy := types.NewObject()
	objCopy.Set(PartitonIdKeyword, ps.partitionId)
	objCopy.SetAll(object)
	return ps.AbstractTransactionalSQLStream.Consume(ctx, objCopy)
}

func (ps *ReplacePartitionStream) Complete(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		state, err = ps.postComplete(ctx, err)
	}()
	//if no error happened during inserts. empty stream is valid - means no data for sync period
	if ps.state.LastError == nil {
		//we have to clear all previous data even if no objects was consumed
		//if stream was empty we need to open transaction.
		if err = ps.init(ctx); err != nil {
			return
		}
		err = ps.clearPartition(ctx, ps.tx)
		if err == nil && ps.state.SuccessfulRows > 0 {
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
		}
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}

func (ps *ReplacePartitionStream) clearPartition(ctx context.Context, tx *TxSQLAdapter) error {
	//check if destination table already exists
	table, err := tx.GetTableSchema(ctx, ps.namespace, ps.tableName)
	if err != nil {
		return fmt.Errorf("couldn't start ReplacePartitionStream: failed to check existence of table: %s error: %s", ps.tableName, err)
	}
	if table.Exists() {
		//if table exists we need to delete previous data associated with partitionId,
		//but we need to check if partitionId column exists in table first
		_, ok := table.Columns.Get(tx.ColumnName(PartitonIdKeyword))
		if !ok {
			return fmt.Errorf("couldn't start ReplacePartitionStream: destination table [%s] exist but it is not managed by ReplacePartitionStream: %s column is missing", ps.tableName, tx.ColumnName(PartitonIdKeyword))
		}
		//delete previous data by provided partition id
		err = tx.Delete(ctx, ps.namespace, ps.tableName, ByPartitionId(ps.partitionId))
		if err != nil {
			return fmt.Errorf("couldn't start ReplacePartitionStream: failed to delete data for partitionId: %s error: %s", ps.partitionId, err)
		}
	}
	return nil
}
