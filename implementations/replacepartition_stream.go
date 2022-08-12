package implementations

import (
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

type ReplacePartitionStream struct {
	AbstractStream
	tx *types.Transaction
}

func NewReplacePartitionStream(id string, p bulker.SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (stream bulker.BulkerStream, err error) {
	ps := ReplacePartitionStream{}
	defer func() {
		if err != nil && ps.tx != nil {
			_ = ps.tx.Rollback()
		}
	}()
	so := bulker.StreamOptions{}
	for _, opt := range streamOptions {
		opt(&so)
	}
	if so.PartitionId == "" {
		return nil, errors.New("WithPartition is required option for ReplacePartitionStream")
	}
	//opening transaction
	ps.tx, err = p.OpenTx()
	if err != nil {
		return nil, err
	}
	//check if destination table already exists
	table, err := p.GetTableSchema(ps.tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("couldn't start ReplacePartitionStream: failed to check existence of table: %s error: %s", tableName, err)
	}
	if table.Exists() {
		//if table exists we need to delete previous data associated with partitionId,
		//but we need to check if partitionId column exists in table first
		_, ok := table.Columns[types.PartitonIdKeyword]
		if !ok {
			return nil, fmt.Errorf("couldn't start ReplacePartitionStream: destination table [%s] exist but it is not managed by ReplacePartitionStream: %s column is missing", tableName, types.PartitonIdKeyword)
		}
		//delete previous data by provided partition id
		err = p.Delete(ps.tx, tableName, types.DeleteByPartitionId(so.PartitionId))
		if err != nil {
			return nil, fmt.Errorf("couldn't start ReplacePartitionStream: failed to delete data for partitionId: %s error: %s", so.PartitionId, err)
		}
	}
	ps.AbstractStream = NewAbstractStream(id, p, ps.tx, tableName, bulker.ReplacePartition, streamOptions...)
	return &ps, nil
}

func (ps *ReplacePartitionStream) Consume(object types.Object) (err error) {
	defer func() {
		err = ps.PostConsume(err)
	}()
	//mark rows by setting __partition_id column with value of partitionId option
	object[types.PartitonIdKeyword] = ps.options.PartitionId

	//type mapping, flattening => table schema
	tableForObject, processedObjects, err := ps.Preprocess(object)
	if err != nil {
		return err
	}
	//ensure destination table schema
	tableForObject, err = ps.tableHelper.EnsureTableWithCaching(ps.id, tableForObject)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure destination table")
	}
	return ps.p.Insert(ps.tx, tableForObject, false, processedObjects)
}

func (ps *ReplacePartitionStream) Complete() (state bulker.State, err error) {
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
	//if no error happened during inserts. empty stream is valid - means no data for sync period
	if ps.state.LastError == nil {
		err = ps.tx.Commit()
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}

func (ps *ReplacePartitionStream) Abort() (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	_ = ps.tx.Rollback()
	ps.state.Status = bulker.Aborted
	return ps.state, err
}
