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
		ps.adjustTableToSchema(table, nil, ps.schemaFromOptions)
	}
	if err != nil {
		return
	}
	existingTable, err := ps.sqlAdapter.TableHelper().EnsureTableWithCaching(ctx, ps.sqlAdapter, ps.id, table)
	if err == nil {
		// for autocommit mode this method only tries to convert values to existing column types
		columnsAdded := ps.adjustTableColumnTypes(table, existingTable, table, processedObject)
		if columnsAdded {
			ps.updateRepresentationTable(existingTable)
			// if new columns were added - update table. (for _unmapped_data column)
			existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithCaching(ctx, ps.sqlAdapter, ps.id, table)
		}
		if err == nil {
			existingTable.Columns = table.Columns
			ps.updateRepresentationTable(existingTable)
			err = ps.sqlAdapter.Insert(ctx, existingTable, ps.merge, processedObject)
		}
	}
	if err != nil {
		// give another try without using table cache
		existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithoutCaching(ctx, ps.sqlAdapter, ps.id, table)
		if err != nil {
			ps.updateRepresentationTable(table)
			err = errorj.Decorate(err, "failed to ensure table")
			return
		}
		// for autocommit mode this method only tries to convert values to existing column types
		columnsAdded := ps.adjustTableColumnTypes(table, existingTable, table, processedObject)
		if columnsAdded {
			ps.updateRepresentationTable(existingTable)
			// if new columns were added - update table. (for _unmapped_data column)
			existingTable, err = ps.sqlAdapter.TableHelper().EnsureTableWithCaching(ctx, ps.sqlAdapter, ps.id, table)
			if err != nil {
				err = errorj.Decorate(err, "failed to ensure table")
				return
			}
		}
		existingTable.Columns = table.Columns
		ps.updateRepresentationTable(existingTable)
		return ps.state, processedObject, ps.sqlAdapter.Insert(ctx, existingTable, ps.merge, processedObject)
	}
	return ps.state, processedObject, nil
}

func (ps *AutoCommitStream) Complete(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort(ctx context.Context) (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}
