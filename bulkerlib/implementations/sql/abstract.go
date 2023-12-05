package sql

import (
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/utils"
	jsoniter "github.com/json-iterator/go"
)

// TODO: check whether COPY is transactional ?
// TODO: pk conflict on Redshift file storage ?

const unmappedDataColumn = "_unmapped_data"

type AbstractSQLStream struct {
	id          string
	sqlAdapter  SQLAdapter
	mode        bulker.BulkMode
	options     bulker.StreamOptions
	tableName   string
	merge       bool
	mergeWindow int
	omitNils    bool

	state  bulker.State
	inited bool

	customTypes     types.SQLTypes
	pkColumns       []string
	timestampColumn string
}

func newAbstractStream(id string, p SQLAdapter, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (*AbstractSQLStream, error) {
	ps := AbstractSQLStream{id: id, sqlAdapter: p, tableName: tableName, mode: mode}
	ps.options = bulker.StreamOptions{}
	for _, option := range streamOptions {
		ps.options.Add(option)
	}
	ps.merge = bulker.DeduplicateOption.Get(&ps.options)
	pkColumns := bulker.PrimaryKeyOption.Get(&ps.options)
	if ps.merge && len(pkColumns) == 0 {
		return nil, fmt.Errorf("MergeRows option requires primary key in the destination table. Please provide WithPrimaryKey option")
	}
	if ps.merge {
		ps.mergeWindow = DeduplicateWindow.Get(&ps.options)
	}

	var customFields = ColumnTypesOption.Get(&ps.options)
	ps.pkColumns = pkColumns.ToSlice()
	ps.timestampColumn = bulker.TimestampOption.Get(&ps.options)
	ps.omitNils = OmitNilsOption.Get(&ps.options)

	//TODO: max column?
	ps.state = bulker.State{Status: bulker.Active}
	ps.customTypes = customFields
	return &ps, nil
}

func (ps *AbstractSQLStream) preprocess(object types.Object) (*Table, types.Object, error) {
	if ps.state.Status != bulker.Active {
		return nil, nil, fmt.Errorf("stream is not active. Status: %s", ps.state.Status)
	}
	batchHeader, processedObject, err := ProcessEvents(ps.tableName, object, ps.customTypes, ps.omitNils)
	if err != nil {
		return nil, nil, err
	}
	table, processedObject := ps.sqlAdapter.TableHelper().MapTableSchema(ps.sqlAdapter, batchHeader, processedObject, ps.pkColumns, ps.timestampColumn)
	ps.state.ProcessedRows++
	return table, processedObject, nil
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
	if err := ps.sqlAdapter.Ping(ctx); err != nil {
		return err
	}
	//setup required db object like 'schema' or 'dataset' if doesn't exist
	err := ps.sqlAdapter.InitDatabase(ctx)
	if err != nil {
		return err
	}
	ps.inited = true
	return nil
}

// adjustTableColumnTypes modify currentTable with extra new columns from desiredTable if such exists
// if some column already exists in the database, no problems if its DataType is castable to DataType of existing column
// if some new column is being added but with different DataTypes - type of this column will be changed to a common ancestor type
// object values that can't be casted will be added to '_unmaped_data' column of JSON type as an json object
// returns true if new column was added to the currentTable as a result of this function call
func (ps *AbstractSQLStream) adjustTableColumnTypes(currentTable, existingTable, desiredTable *Table, values types.Object) bool {
	columnsAdded := false
	current := currentTable.Columns.Clone()
	unmappedObj := map[string]any{}
	for name, newCol := range desiredTable.Columns {
		var existingCol types.SQLColumn
		ok := false
		if existingTable != nil {
			existingCol, ok = existingTable.Columns[name]
		}
		if !ok {
			existingCol, ok = current[name]
			if !ok {
				//column not exist in database - adding as New
				newCol.New = true
				current[name] = newCol
				columnsAdded = true
				continue
			}
		} else {
			current[name] = existingCol
		}
		if existingCol.DataType == newCol.DataType {
			continue
		}
		if newCol.Override {
			//if column sql type is overridden by user - leave it this way
			current[name] = newCol
			continue
		}
		if !existingCol.New {
			//column exists in database - check if its DataType is castable to DataType of existing column
			if types.IsConvertible(newCol.DataType, existingCol.DataType) {
				newVal, err := types.Convert(existingCol.DataType, values[name])
				if err != nil {
					//logging.Warnf("Can't convert '%s' value '%v' from %s to %s: %v", name, values[name], newCol.DataType.String(), existingCol.DataType.String(), err)
					unmappedObj[name] = values[name]
					delete(values, name)
					continue
				} else {
					//logging.Infof("Converted '%s' value '%v' from %s to %s: %v", name, values[name], newCol.DataType.String(), existingCol.DataType.String(), newVal)
					values[name] = newVal
				}
			} else {
				//logging.Warnf("Can't convert '%s' value '%v' from %s to %s", name, values[name], newCol.DataType.String(), existingCol.DataType.String())
				unmappedObj[name] = values[name]
				delete(values, name)
				continue
			}
		} else {
			common := types.GetCommonAncestorType(existingCol.DataType, newCol.DataType)
			if common != existingCol.DataType {
				//logging.Warnf("Changed '%s' type from %s to %s because of %s", name, existingCol.DataType.String(), common.String(), newCol.DataType.String())
				sqlType, ok := ps.sqlAdapter.GetSQLType(common)
				if ok {
					existingCol.DataType = common
					existingCol.Type = sqlType
					current[name] = existingCol
				} else {
					logging.SystemErrorf("Unknown column type %s mapping for %s", common, ps.sqlAdapter.Type())
				}
			}
		}
	}
	if len(unmappedObj) > 0 {
		jsonSQLType, _ := ps.sqlAdapter.GetSQLType(types.JSON)
		added := utils.MapPutIfAbsent(current, ps.sqlAdapter.ColumnName(unmappedDataColumn), types.SQLColumn{DataType: types.JSON, Type: jsonSQLType})
		columnsAdded = columnsAdded || added
		b, _ := jsoniter.Marshal(unmappedObj)
		values[ps.sqlAdapter.ColumnName(unmappedDataColumn)] = string(b)
	}
	currentTable.Columns = current
	return columnsAdded
}

func (ps *AbstractSQLStream) updateRepresentationTable(table *Table) {
	if ps.state.Representation == nil ||
		ps.state.Representation.(RepresentationTable).Name != table.Name ||
		len(ps.state.Representation.(RepresentationTable).Schema) != len(table.Columns) {
		ps.state.Representation = RepresentationTable{
			Name:             table.Name,
			Schema:           table.Columns.ToSimpleMap(),
			PrimaryKeyFields: table.GetPKFields(),
			PrimaryKeyName:   table.PrimaryKeyName,
			Temporary:        table.Temporary,
		}
	}
}

type RepresentationTable struct {
	Name             string            `json:"name"`
	Schema           map[string]string `json:"schema"`
	PrimaryKeyFields []string          `json:"primaryKeyFields,omitempty"`
	PrimaryKeyName   string            `json:"primaryKeyName,omitempty"`
	Temporary        bool              `json:"temporary,omitempty"`
}
