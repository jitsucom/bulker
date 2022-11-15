package sql

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/base/coordination"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

// TODO: tableHelper not sure that using cache is always applicable. Transaction rollback may lead to inconsistency.
// TODO: check whether COPY is transactional ?
// TODO: pk conflict on Redshift file storage ?

type AbstractSQLStream struct {
	id         string
	sqlAdapter SQLAdapter
	mode       bulker.BulkMode
	options    bulker.StreamOptions
	tableName  string
	merge      bool

	state       bulker.State
	tableHelper *TableHelper
	inited      bool

	customTypes SQLTypes
}

func newAbstractStream(id string, p SQLAdapter, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (AbstractSQLStream, error) {
	ps := AbstractSQLStream{id: id, sqlAdapter: p, tableName: tableName, mode: mode}
	ps.options = bulker.StreamOptions{}
	for _, option := range streamOptions {
		option(&ps.options)
	}
	ps.merge = MergeRowsOption.Get(&ps.options)
	if ps.merge && len(PrimaryKeyOption.Get(&ps.options)) == 0 {
		return AbstractSQLStream{}, fmt.Errorf("MergeRows option requires primary key in the destination table. Please provide WithPrimaryKey option")
	}
	var customFields = ColumnTypesOption.Get(&ps.options)
	adaptedCustomFields := make(SQLTypes, len(customFields))
	for k, v := range customFields {
		adaptedCustomFields[p.ColumnName(k)] = v
	}
	//TODO: max column?
	ps.tableHelper = NewTableHelper(p, coordination.DummyCoordinationService{}, PrimaryKeyOption.Get(&ps.options), 1000)
	ps.state = bulker.State{Status: bulker.Active}
	ps.customTypes = adaptedCustomFields
	return ps, nil
}

func (ps *AbstractSQLStream) preprocess(object types.Object) (*Table, []types.Object, error) {
	if ps.state.Status != bulker.Active {
		return nil, nil, fmt.Errorf("stream is not active. Status: %s", ps.state.Status)
	}
	batchHeader, processedObjects, err := ProcessEvents(ps.tableName, []types.Object{object}, ps.sqlAdapter)
	if err != nil {
		return nil, nil, err
	}
	if len(ps.customTypes) > 0 {
		// enrich overridden schema types
		batchHeader.Fields.OverrideTypes(ps.customTypes)
	}
	table := ps.tableHelper.MapTableSchema(batchHeader)
	ps.state.ProcessedRows++
	return table, processedObjects, nil
}

func (ps *AbstractSQLStream) postConsume(err error) error {
	if err != nil {
		ps.state.ErrorRowIndex = ps.state.ProcessedRows
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

func (ps *AbstractSQLStream) init(ctx context.Context) error {
	if ps.inited {
		return nil
	}
	//setup required db object like 'schema' or 'dataset' if doesn't exist
	err := ps.sqlAdapter.InitDatabase(ctx)
	if err != nil {
		return err
	}
	ps.inited = true
	return nil
}
