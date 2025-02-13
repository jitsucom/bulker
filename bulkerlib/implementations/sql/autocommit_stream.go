package sql

import (
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
)

type AutoCommitStream struct {
	*AbstractSQLStream
}

func newAutoCommitStream(id string, p SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := AutoCommitStream{}
	var err error
	ps.AbstractSQLStream, err = newAbstractStream(id, p, tableName, bulker.Stream, streamOptions...)
	if err != nil {
		return nil, err
	}

	return &ps, nil
}

func (ps *AutoCommitStream) ConsumeJSON(ctx context.Context, json []byte) (state bulker.State, processedObject types.Object, err error) {
	var obj types.Object
	err = jsonorder.Unmarshal(json, &obj)
	if err != nil {
		return ps.state, nil, fmt.Errorf("Error parsing JSON: %v", err)
	}
	return ps.Consume(ctx, obj)
}

func (ps *AutoCommitStream) ConsumeMap(ctx context.Context, mp map[string]any) (state bulker.State, processedObject types.Object, err error) {
	return ps.Consume(ctx, types.ObjectFromMap(mp))
}

func (ps *AutoCommitStream) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObject types.Object, err error) {
	defer func() {
		err = ps.postConsume(err)
		state = ps.state
	}()
	if err = ps.init(ctx); err != nil {
		return
	}
	table, processedObject, err := ps.preprocess(object, false)
	if err != nil {
		return
	}
	existingTable, err := ps.sqlAdapter.TableHelper().Get(ctx, ps.sqlAdapter, table.Namespace, table.Name, true)
	if err != nil {
		err = errorj.Decorate(err, "failed to get current table table")
		return
	}
	if existingTable.Exists() {
		if ps.initialColumnsCount == 0 {
			ps.initialColumnsCount = existingTable.ColumnsCount()
		}
		currentTable := table.WithoutColumns()
		if ps.schemaFromOptions != nil {
			//just to convert values to schema data types
			ps.adjustTableColumnTypes(currentTable, existingTable, ps.schemaFromOptions, object)
		}
		columnsAdded := ps.adjustTableColumnTypes(currentTable, existingTable, table, processedObject)
		if columnsAdded || !currentTable.PKFields.Equals(existingTable.PKFields) {
			ps.updateRepresentationTable(currentTable)
			// if new columns were added - update table. (for _unmapped_data column)
			existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithCaching(ctx, ps.sqlAdapter, ps.id, currentTable)
			if err != nil {
				// give another try without using table cache
				existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithoutCaching(ctx, ps.sqlAdapter, ps.id, currentTable)
				if err != nil {
					ps.updateRepresentationTable(currentTable)
					err = errorj.Decorate(err, "failed to ensure table")
					return
				}
				// here this method only tries to convert values to existing column types
				columnsAdded = ps.adjustTableColumnTypes(currentTable, existingTable, table, processedObject)
				if columnsAdded {
					ps.updateRepresentationTable(currentTable)
					// if new columns were added - update table. (for _unmapped_data column)
					existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithCaching(ctx, ps.sqlAdapter, ps.id, currentTable)
					if err != nil {
						err = errorj.Decorate(err, "failed to ensure table")
						return
					}
				}
			}
		}
		ps.updateRepresentationTable(currentTable)
		// remove columns with missed data from Insert statements
		for el := currentTable.Columns.Front(); el != nil; {
			name := el.Key
			curEl := el
			// move to the next element before deleting the current one. otherwise iteration will be broken
			el = el.Next()
			v, ok := processedObject.Get(name)
			if !ok || v == nil {
				currentTable.Columns.DeleteElement(curEl)
			}
		}
		currentTable.PrimaryKeyName = existingTable.PrimaryKeyName
		err = ps.sqlAdapter.Insert(ctx, currentTable, ps.merge, processedObject)
	} else {
		if ps.schemaFromOptions != nil {
			//just to convert values to schema data types
			ps.adjustTableColumnTypes(table, nil, ps.schemaFromOptions, object)
		}
		existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithCaching(ctx, ps.sqlAdapter, ps.id, table)
		if err != nil {
			err = errorj.Decorate(err, "failed to ensure table")
			return
		}
		ps.updateRepresentationTable(existingTable)
		// remove columns with missed data from Insert statements
		for el := existingTable.Columns.Front(); el != nil; {
			name := el.Key
			curEl := el
			// move to the next element before deleting the current one. otherwise iteration will be broken
			el = el.Next()
			v, ok := processedObject.Get(name)
			if !ok || v == nil {
				existingTable.Columns.DeleteElement(curEl)
			}
		}
		err = ps.sqlAdapter.Insert(ctx, existingTable, ps.merge, processedObject)
	}

	return ps.state, processedObject, err
}

func (ps *AutoCommitStream) Complete(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort(ctx context.Context) (state bulker.State) {
	ps.state.Status = bulker.Aborted
	return ps.state
}
