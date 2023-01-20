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

type ReplacePartitionStream struct {
	AbstractTransactionalSQLStream
	partitionId string
}

func newReplacePartitionStream(id string, p SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (stream bulker.BulkerStream, err error) {
	ps := ReplacePartitionStream{}
	so := bulker.StreamOptions{}
	for _, opt := range streamOptions {
		so.Add(opt)
	}
	partitionId := PartitionIdOption.Get(&so)
	if partitionId == "" {
		return nil, errors.New("WithPartition is required option for ReplacePartitionStream")
	}
	ps.AbstractTransactionalSQLStream, err = newAbstractTransactionalStream(id, p, tableName, bulker.ReplacePartition, streamOptions...)
	if err != nil {
		return nil, err
	}
	ps.partitionId = partitionId
	ps.tmpTableFunc = func(ctx context.Context, tableForObject *Table, object types.Object) (table *Table) {
		dstTable := tableForObject
		existingTable, _ := ps.tx.GetTableSchema(ctx, ps.tableName)
		if existingTable.Exists() {
			dstTable = existingTable
			ps.adjustTableColumnTypes(dstTable, tableForObject, object)
		}
		tmpTableName := fmt.Sprintf("jitsu_tmp_%s", uuid.NewLettersNumbers()[:8])
		pkName := ""
		if len(dstTable.PKFields) > 0 {
			pkName = BuildConstraintName(tmpTableName)
		}
		return &Table{
			Name:            tmpTableName,
			Columns:         dstTable.Columns,
			PKFields:        dstTable.PKFields,
			PrimaryKeyName:  pkName,
			Temporary:       true,
			TimestampColumn: tableForObject.TimestampColumn,
		}
	}
	return &ps, nil
}

func (ps *ReplacePartitionStream) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObjects []types.Object, err error) {
	objCopy := utils.MapCopy(object)
	objCopy[PartitonIdKeyword] = ps.partitionId
	return ps.AbstractTransactionalSQLStream.Consume(ctx, objCopy)
}

func (ps *ReplacePartitionStream) Complete(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		if err != nil {
			ps.state.SuccessfulRows = 0
			_ = ps.tx.Rollback()
		}
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
				if err = ps.flushBatchFile(ctx); err != nil {
					return ps.state, err
				}
			}
			var dstTable *Table
			dstTable, err = ps.tableHelper.EnsureTableWithoutCaching(ctx, ps.id, ps.dstTable)
			if err != nil {
				ps.updateRepresentationTable(ps.dstTable)
				return ps.state, errorj.Decorate(err, "failed to ensure destination table")
			}
			ps.dstTable = dstTable
			ps.updateRepresentationTable(ps.dstTable)
			//copy data from tmp table to destination table
			err = ps.tx.CopyTables(ctx, ps.dstTable, ps.tmpTable, ps.merge)
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
	table, err := tx.GetTableSchema(ctx, ps.tableName)
	if err != nil {
		return fmt.Errorf("couldn't start ReplacePartitionStream: failed to check existence of table: %s error: %s", ps.tableName, err)
	}
	if table.Exists() {
		//if table exists we need to delete previous data associated with partitionId,
		//but we need to check if partitionId column exists in table first
		_, ok := table.Columns[tx.ColumnName(PartitonIdKeyword)]
		if !ok {
			return fmt.Errorf("couldn't start ReplacePartitionStream: destination table [%s] exist but it is not managed by ReplacePartitionStream: %s column is missing", ps.tableName, tx.ColumnName(PartitonIdKeyword))
		}
		//delete previous data by provided partition id
		err = tx.Delete(ctx, ps.tableName, ByPartitionId(ps.partitionId))
		if err != nil {
			return fmt.Errorf("couldn't start ReplacePartitionStream: failed to delete data for partitionId: %s error: %s", ps.partitionId, err)
		}
	}
	return nil
}
