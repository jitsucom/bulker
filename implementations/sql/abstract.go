package sql

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/base/coordination"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

// TODO: use real temporary tables: YES
// TODO: prebuffer table ?
// TODO: check whether COPY is transactional ?
// TODO: pk conflict on Redshift file storage ?

type AbstractSQLStream struct {
	id        string
	p         SQLAdapter
	tx        *Transaction
	mode      bulker.BulkMode
	options   bulker.StreamOptions
	tableName string

	state       bulker.State
	tableHelper *TableHelper
}

func NewAbstractStream(id string, p SQLAdapter, tx TxOrDatasource, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) AbstractSQLStream {
	ps := AbstractSQLStream{id: id, p: p, tableName: tableName, mode: mode}
	ps.options = bulker.StreamOptions{}
	for _, option := range streamOptions {
		option(&ps.options)
	}
	//TODO: max column?
	ps.tableHelper = NewTableHelper(p, tx, coordination.DummyCoordinationService{}, primaryKeyOption.Get(&ps.options), 1000)
	ps.state = bulker.State{RowsErrors: map[int]error{}, Status: bulker.Active}
	return ps
}

func (ps *AbstractSQLStream) preprocess(object types.Object) (*Table, []types.Object, error) {
	if ps.state.Status != bulker.Active {
		return nil, nil, fmt.Errorf("stream is not active. Status: %s", ps.state.Status)
	}
	batchHeader, processedObjects, err := ProcessEvents(ps.tableName, []types.Object{object})
	if err != nil {
		return nil, nil, err
	}
	var customFields = columnTypesOption.Get(&ps.options)
	if len(customFields) > 0 {
		// enrich overridden schema types
		batchHeader.Fields.OverrideTypes(customFields)
	}
	table := ps.tableHelper.MapTableSchema(batchHeader)
	ps.state.ProcessedRows++
	return table, processedObjects, nil
}

func (ps *AbstractSQLStream) postConsume(err error) error {
	if err != nil {
		ps.tableHelper.ClearCache(ps.tableName)
		ps.state.RowsErrors[ps.state.ProcessedRows] = err
		ps.state.LastError = err
		return err
	} else {
		ps.state.SuccessfulRows++
	}
	return nil
}

func (ps *AbstractSQLStream) postComplete(err error) (bulker.State, error) {
	if err != nil {
		ps.state.LastError = err
		ps.state.Status = bulker.Failed
	} else {
		ps.state.Status = bulker.Completed
	}
	return ps.state, err
}

func (ps *AbstractSQLStream) initTx(ctx context.Context) error {
	if ps.tx == nil {
		tx, err := ps.p.OpenTx(ctx)
		if err != nil {
			return err
		}
		ps.tableHelper.SetTx(tx)
		ps.tx = tx
		return nil
	}
	return nil
}
