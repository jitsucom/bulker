package sql

import (
	"context"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
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

func (ps *AutoCommitStream) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObject types.Object, err error) {
	defer func() {
		err = ps.postConsume(err)
		state = ps.state
	}()
	if err = ps.init(ctx); err != nil {
		return
	}
	table, processedObject, err := ps.preprocess(object)
	if ps.schemaFromOptions != nil {
		//just to convert values to schema data types
		ps.adjustTableColumnTypes(table, nil, ps.schemaFromOptions, object)
	}
	if err != nil {
		return
	}
	existingTable, err := ps.sqlAdapter.TableHelper().Get(ctx, ps.sqlAdapter, table.Name, true)
	if err != nil {
		err = errorj.Decorate(err, "failed to get current table table")
		return
	}
	if existingTable.Exists() {
		currentTable := existingTable.Clone()
		columnsAdded := ps.adjustTableColumnTypes(currentTable, existingTable, table, processedObject)
		if columnsAdded {
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
				currentTable = existingTable.Clone()
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
					currentTable = existingTable
				}
			}
		}
		ps.updateRepresentationTable(currentTable)
		err = ps.sqlAdapter.Insert(ctx, currentTable, ps.merge, processedObject)
	} else {
		existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithCaching(ctx, ps.sqlAdapter, ps.id, table)
		err = ps.sqlAdapter.Insert(ctx, existingTable, ps.merge, processedObject)
	}

	return ps.state, processedObject, err
}

func (ps *AutoCommitStream) Complete(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}
