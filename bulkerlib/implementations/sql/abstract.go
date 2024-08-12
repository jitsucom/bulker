package sql

import (
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/jsoniter"
	"github.com/jitsucom/bulker/jitsubase/logging"
	types2 "github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"time"
)

// TODO: check whether COPY is transactional ?
// TODO: pk conflict on Redshift file storage ?

const unmappedDataColumn = "_unmapped_data"

type AbstractSQLStream struct {
	id                string
	sqlAdapter        SQLAdapter
	mode              bulker.BulkMode
	options           bulker.StreamOptions
	tableName         string
	namespace         string
	merge             bool
	mergeWindow       int
	omitNils          bool
	schemaFreeze      bool
	schemaOptions     types.Schema
	schemaFromOptions *Table

	state  bulker.State
	inited bool

	customTypes     types.SQLTypes
	pkColumns       []string
	timestampColumn string

	unmappedDataColumn string

	startTime time.Time
}

func newAbstractStream(id string, p SQLAdapter, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (*AbstractSQLStream, error) {
	ps := AbstractSQLStream{id: id, sqlAdapter: p, tableName: tableName, mode: mode}
	ps.options = bulker.StreamOptions{}
	for _, option := range streamOptions {
		ps.options.Add(option)
	}
	ps.merge = bulker.DeduplicateOption.Get(&ps.options)
	pkColumns := bulker.PrimaryKeyOption.Get(&ps.options)
	if ps.merge && pkColumns.Empty() {
		return nil, fmt.Errorf("MergeRows option requires primary key in the destination table. Please provide WithPrimaryKey option")
	}
	if ps.merge {
		ps.mergeWindow = DeduplicateWindow.Get(&ps.options)
	}

	var customFields = ColumnTypesOption.Get(&ps.options)
	ps.pkColumns = utils.ArrayMap(pkColumns.ToSlice(), p.ColumnName)
	ps.timestampColumn = bulker.TimestampOption.Get(&ps.options)
	if ps.timestampColumn != "" {
		ps.timestampColumn = p.ColumnName(ps.timestampColumn)
	}
	ps.namespace = bulker.NamespaceOption.Get(&ps.options)
	if ps.namespace != "" {
		ps.namespace = p.TableName(ps.namespace)
	}
	ps.omitNils = OmitNilsOption.Get(&ps.options)
	ps.schemaFreeze = SchemaFreezeOption.Get(&ps.options)

	schema := bulker.SchemaOption.Get(&ps.options)
	if !schema.IsEmpty() {
		ps.schemaOptions = schema
		ps.schemaFromOptions = ps.sqlAdapter.TableHelper().MapSchema(ps.sqlAdapter, schema)
	}

	ps.unmappedDataColumn = p.ColumnName(unmappedDataColumn)

	//TODO: max column?
	ps.state = bulker.State{Status: bulker.Active, Mode: mode}
	ps.customTypes = customFields
	ps.startTime = time.Now()
	return &ps, nil
}

func (ps *AbstractSQLStream) preprocess(object types.Object) (*Table, types.Object, error) {
	if ps.state.Status != bulker.Active {
		return nil, nil, fmt.Errorf("stream is not active. Status: %s", ps.state.Status)
	}
	var notFlatteningKeys types2.Set[string]
	if !ps.schemaOptions.IsEmpty() {
		notFlatteningKeys = types2.NewSet[string]()
		for _, field := range ps.schemaOptions.Fields {
			if field.Type == types.JSON {
				notFlatteningKeys.Put(field.Name)
			}
		}
	}
	batchHeader, processedObject, err := ProcessEvents(ps.tableName, object, ps.customTypes, ps.omitNils, ps.sqlAdapter.StringifyObjects(), notFlatteningKeys)
	if err != nil {
		return nil, nil, err
	}
	table, processedObject := ps.sqlAdapter.TableHelper().MapTableSchema(ps.sqlAdapter, batchHeader, processedObject, ps.pkColumns, ps.timestampColumn, ps.namespace)
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
	if ps.state.Representation == nil {
		ps.updateRepresentationTable(&Table{
			Name: ps.sqlAdapter.TableName(ps.tableName),
		})
	}
	return ps.state, err
}

func (ps *AbstractSQLStream) init(ctx context.Context) error {
	if err := ps.sqlAdapter.Ping(ctx); err != nil {
		return err
	}
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

// adjustTableColumnTypes modify currentTable with extra new columns from desiredTable if such exists
// if some column already exists in the database, no problems if its DataType is castable to DataType of existing column
// if some new column is being added but with different DataTypes - type of this column will be changed to a common ancestor type
// object values that can't be casted will be added to '_unmaped_data' column of JSON type as an json object
// returns true if new column was added to the currentTable as a result of this function call
func (ps *AbstractSQLStream) adjustTableColumnTypes(currentTable, existingTable, desiredTable *Table, values types.Object) bool {
	columnsAdded := false
	current := currentTable.Columns
	unmappedObj := map[string]any{}
	for el := desiredTable.Columns.Front(); el != nil; el = el.Next() {
		name := el.Key
		newCol := el.Value
		var existingCol types.SQLColumn
		ok := false
		if existingTable != nil && existingTable.Columns != nil {
			existingCol, ok = existingTable.Columns.Get(name)
		}
		if !ok {
			if ps.schemaFreeze {
				// when schemaFreeze=true all new columns values go to _unmapped_data
				v, ok := values.Get(name)
				if ok {
					unmappedObj[name] = v
				}
				current.Delete(name)
				values.Delete(name)
			} else {
				existingCol, ok = current.Get(name)
				if !ok {
					// column doesn't exist in database and in current batch - adding as New
					if !newCol.Override && !newCol.Important {
						newCol.New = true
					}
					current.Set(name, newCol)
					columnsAdded = true
					continue
				}
			}
		} else {
			current.Set(name, existingCol)
		}
		if existingCol.DataType == newCol.DataType {
			continue
		}
		if newCol.Override || newCol.Important {
			//if column sql type is overridden by user - leave it this way
			current.Set(name, newCol)
			continue
		}
		if existingCol.Type != "" {
			// if column exists in db (existingTable) or in current batch (currentTable)
			if !existingCol.New {
				//column exists in database - check if its DataType is castable to DataType of existing column
				v, ok := values.Get(name)
				if ok && v != nil {
					if types.IsConvertible(newCol.DataType, existingCol.DataType) {
						newVal, _, err := types.Convert(existingCol.DataType, v)
						if err != nil {
							//logging.Warnf("Can't convert '%s' value '%v' from %s to %s: %v", name, values[name], newCol.DataType.String(), existingCol.DataType.String(), err)
							unmappedObj[name] = v
							values.Delete(name)
							continue
						} else {
							//logging.Infof("Converted '%s' value '%v' from %s to %s: %v", name, values[name], newCol.DataType.String(), existingCol.DataType.String(), newVal)
							values.Set(name, newVal)
						}
					} else {
						//logging.Warnf("Can't convert '%s' value '%v' from %s to %s", name, values[name], newCol.DataType.String(), existingCol.DataType.String())
						unmappedObj[name] = v
						values.Delete(name)
						continue
					}
				}
			} else {
				// column exists only in current batch - we have chance to change new column type to common ancestor
				common := types.GetCommonAncestorType(existingCol.DataType, newCol.DataType)
				if common != existingCol.DataType {
					//logging.Warnf("Changed '%s' type from %s to %s because of %s", name, existingCol.DataType.String(), common.String(), newCol.DataType.String())
					sqlType, ok := ps.sqlAdapter.GetSQLType(common)
					if ok {
						existingCol.DataType = common
						existingCol.Type = sqlType
						current.Set(name, existingCol)
					} else {
						logging.SystemErrorf("Unknown column type %s mapping for %s", common, ps.sqlAdapter.Type())
					}
				}
			}
		}
	}
	if len(unmappedObj) > 0 {
		jsonSQLType, _ := ps.sqlAdapter.GetSQLType(types.JSON)
		added := current.SetIfAbsent(ps.unmappedDataColumn, types.SQLColumn{DataType: types.JSON, Type: jsonSQLType})
		columnsAdded = columnsAdded || added
		if ps.sqlAdapter.StringifyObjects() {
			b, _ := jsoniter.Marshal(unmappedObj)
			values.Set(ps.unmappedDataColumn, string(b))
		} else {
			values.Set(ps.unmappedDataColumn, unmappedObj)
		}
	}
	return columnsAdded
}

func (ps *AbstractSQLStream) updateRepresentationTable(table *Table) {
	if ps.state.Representation == nil ||
		ps.state.Representation.(RepresentationTable).Name != table.Name ||
		ps.state.Representation.(RepresentationTable).Schema.Len() != table.ColumnsCount() {
		ps.state.Representation = RepresentationTable{
			Name:             table.Name,
			Schema:           table.ToSimpleMap(),
			PrimaryKeyFields: table.GetPKFields(),
			PrimaryKeyName:   table.PrimaryKeyName,
			Temporary:        table.Temporary,
		}
	}
}

type RepresentationTable struct {
	Name             string                          `json:"name"`
	Schema           *types2.OrderedMap[string, any] `json:"schema"`
	PrimaryKeyFields []string                        `json:"primaryKeyFields,omitempty"`
	PrimaryKeyName   string                          `json:"primaryKeyName,omitempty"`
	Temporary        bool                            `json:"temporary,omitempty"`
}
