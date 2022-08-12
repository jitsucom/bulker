package implementations

import (
	"fmt"
	"github.com/jitsucom/bulker/base/coordination"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

// TODO: use real temporary tables: YES
// TODO: prebuffer table ?
// TODO: check whether COPY is transactional ?
// TODO: pk conflict on Redshift file storage ?

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
	ps.options = bulker.DefaultStreamOptions
	for _, option := range streamOptions {
		option(&ps.options)
	}
	//TODO: max column?
	ps.tableHelper = NewTableHelper(p, tx, coordination.DummyCoordinationService{}, ps.options.PrimaryKeyFields, 1000)
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
	ps.state.ProcessedRows++
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
