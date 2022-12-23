package sql

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/base/coordination"
	"github.com/jitsucom/bulker/base/utils"
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
	pkColumns   utils.Set[string]
}

func newAbstractStream(id string, p SQLAdapter, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (AbstractSQLStream, error) {
	ps := AbstractSQLStream{id: id, sqlAdapter: p, tableName: tableName, mode: mode}
	ps.options = bulker.StreamOptions{}
	for _, option := range streamOptions {
		ps.options.Add(option)
	}
	ps.merge = MergeRowsOption.Get(&ps.options)
	pkColumns := PrimaryKeyOption.Get(&ps.options)
	if ps.merge && len(pkColumns) == 0 {
		return AbstractSQLStream{}, fmt.Errorf("MergeRows option requires primary key in the destination table. Please provide WithPrimaryKey option")
	}
	var customFields = ColumnTypesOption.Get(&ps.options)
	adaptedPkColumns := utils.NewSet[string]()
	for k, _ := range pkColumns {
		adaptedPkColumns.Put(p.ColumnName(k))
	}
	ps.pkColumns = adaptedPkColumns
	timestampColumn := TimestampOption.Get(&ps.options)
	if timestampColumn != "" {
		timestampColumn = p.ColumnName(timestampColumn)
	}
	//TODO: max column?
	ps.tableHelper = NewTableHelper(p, coordination.DummyCoordinationService{}, ps.pkColumns, timestampColumn, 1000)
	ps.state = bulker.State{Status: bulker.Active}
	ps.customTypes = customFields
	return ps, nil
}

func (ps *AbstractSQLStream) preprocess(object types.Object) (*Table, []types.Object, error) {
	if ps.state.Status != bulker.Active {
		return nil, nil, fmt.Errorf("stream is not active. Status: %s", ps.state.Status)
	}
	batchHeader, processedObjects, err := ProcessEvents(ps.tableName, []types.Object{object}, ps.sqlAdapter, ps.customTypes)
	if err != nil {
		return nil, nil, err
	}
	table := ps.tableHelper.MapTableSchema(batchHeader)
	ps.state.ProcessedRows++
	return table, processedObjects, nil
}

func (ps *AbstractSQLStream) postConsume(err error) error {
	if err != nil {
		ps.state.ErrorRowIndex = ps.state.ProcessedRows
		ps.state.SetError(err)
		return err
	} else {
		ps.state.SuccessfulRows++
	}
	return nil
}

func (ps *AbstractSQLStream) postComplete(err error) (bulker.State, error) {
	if err != nil {
		ps.state.SetError(err)
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

func (ps *AbstractSQLStream) updateRepresentationTable(table *Table) {
	if ps.state.Representation == nil ||
		ps.state.Representation.(RepresentationTable).Name != table.Name ||
		len(ps.state.Representation.(RepresentationTable).Schema) != len(table.Columns) {
		ps.state.Representation = RepresentationTable{
			Name:             table.Name,
			Schema:           table.Columns,
			PrimaryKeyFields: table.PKFields.ToSlice(),
			PrimaryKeyName:   table.PrimaryKeyName,
			Temporary:        table.Temporary,
		}
	}
}

type RepresentationTable struct {
	Name             string   `json:"name"`
	Schema           Columns  `json:"schema"`
	PrimaryKeyFields []string `json:"primaryKeyFields,omitempty"`
	PrimaryKeyName   string   `json:"primaryKeyName,omitempty"`
	Temporary        bool     `json:"temporary,omitempty"`
}
