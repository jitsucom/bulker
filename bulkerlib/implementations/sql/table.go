package sql

import (
	"github.com/jitsucom/bulker/bulkerlib/types"
	types2 "github.com/jitsucom/bulker/jitsubase/types"
	"strings"
)

const BulkerManagedPkConstraintPrefix = "jitsu_pk_"

// Columns is a list of columns representation
type Columns = *types2.OrderedMap[string, types.SQLColumn]

func NewColumns() Columns {
	return types2.NewOrderedMap[string, types.SQLColumn]()
}

func NewColumnsFromMap(mp map[string]types.SQLColumn) Columns {
	m := types2.NewOrderedMap[string, types.SQLColumn]()
	m.SetAllMap(mp)
	return m
}

func NewColumnsFromArrays(arr []types2.El[string, types.SQLColumn]) Columns {
	m := types2.NewOrderedMap[string, types.SQLColumn]()
	for _, a := range arr {
		m.Set(a.Key, a.Value)
	}
	return m
}

// TableField is a table column representation
type TableField struct {
	Field string `json:"field,omitempty"`
	Type  string `json:"type,omitempty"`
	Value any    `json:"value,omitempty"`
}

// Table is a dto for DWH Table representation
type Table struct {
	Name string
	// database or schema depending on warehouse
	Namespace string
	Temporary bool
	Cached    bool

	Columns         Columns
	PKFields        types2.OrderedSet[string]
	PrimaryKeyName  string
	TimestampColumn string

	Partition DatePartition

	DeletePrimaryKeyNamed string
}

// Exists returns true if there is at least one column
func (t *Table) Exists() bool {
	if t == nil {
		return false
	}

	return (t.Columns != nil && t.Columns.Len() > 0) || t.PKFields.Size() > 0 || t.DeletePrimaryKeyNamed != ""
}

// ColumnNames return column names as array
func (t *Table) ColumnNames() []string {
	return t.Columns.Keys()
}

func (t *Table) ToArray() []types.SQLColumn {
	var columns []types.SQLColumn
	t.Columns.ForEach(func(key string, value types.SQLColumn) {
		columns = append(columns, value)
	})
	return columns
}

func (t *Table) MappedColumns(f func(string, types.SQLColumn) string) []string {
	columns := make([]string, 0, t.Columns.Len())
	for el := t.Columns.Front(); el != nil; el = el.Next() {
		columns = append(columns, f(el.Key, el.Value))
	}
	return columns
}

func (t *Table) MappedColumnNames(f func(string) string) []string {
	columns := make([]string, 0, t.Columns.Len())
	for el := t.Columns.Front(); el != nil; el = el.Next() {
		columns = append(columns, f(el.Key))
	}
	return columns
}

// CleanClone returns clone of current table w/o 'New' or 'Override' flags
func (t *Table) CleanClone() *Table {
	if t == nil {
		return nil
	}
	clonedColumns := NewColumns()
	for el := t.Columns.Front(); el != nil; el = el.Next() {
		v := el.Value
		clonedColumns.Set(el.Key, types.SQLColumn{
			Type:     v.Type,
			DdlType:  v.DdlType,
			DataType: v.DataType,
		})
	}

	clonedPkFields := t.PKFields.Clone()

	return &Table{
		Namespace:             t.Namespace,
		Name:                  t.Name,
		Columns:               clonedColumns,
		PKFields:              clonedPkFields,
		PrimaryKeyName:        t.PrimaryKeyName,
		Temporary:             t.Temporary,
		TimestampColumn:       t.TimestampColumn,
		Partition:             t.Partition,
		Cached:                t.Cached,
		DeletePrimaryKeyNamed: t.DeletePrimaryKeyNamed,
	}
}

func (t *Table) CloneIfNeeded() *Table {
	if t == nil {
		return nil
	}
	if t.Cached {
		return t
	}
	return t.Clone()
}

func (t *Table) WithoutColumns() *Table {
	if t == nil {
		return nil
	}
	return t.clone(true)
}

func (t *Table) Clone() *Table {
	if t == nil {
		return nil
	}
	return t.clone(false)
}

// Clone returns clone of current table
func (t *Table) clone(omitColumns bool) *Table {
	clonedColumns := NewColumns()
	if !omitColumns {
		for el := t.Columns.Front(); el != nil; el = el.Next() {
			v := el.Value
			clonedColumns.Set(el.Key, types.SQLColumn{
				Type:     v.Type,
				DdlType:  v.DdlType,
				DataType: v.DataType,
				New:      v.New,
				Override: v.Override,
			})
		}
	}

	clonedPkFields := t.PKFields.Clone()

	return &Table{
		Namespace:             t.Namespace,
		Name:                  t.Name,
		Columns:               clonedColumns,
		PKFields:              clonedPkFields,
		PrimaryKeyName:        t.PrimaryKeyName,
		Temporary:             t.Temporary,
		TimestampColumn:       t.TimestampColumn,
		Partition:             t.Partition,
		Cached:                t.Cached,
		DeletePrimaryKeyNamed: t.DeletePrimaryKeyNamed,
	}
}

// GetPKFields returns primary keys list
func (t *Table) GetPKFields() []string {
	return t.PKFields.ToSlice()
}

func (t *Table) GetPKFieldsSet() types2.OrderedSet[string] {
	return t.PKFields
}

// Diff calculates diff between current schema and another one.
// Return schema to add to current schema (for being equal) or empty if
// 1) another one is empty
// 2) all fields from another schema exist in current schema
// NOTE: Diff method doesn't take types into account
func (t *Table) Diff(sqlAdapter SQLAdapter, another *Table) *Table {
	diff := &Table{Name: t.Name, Namespace: t.Namespace, Columns: NewColumns(), PKFields: types2.NewOrderedSet[string]()}

	if !another.Exists() {
		return diff
	}

	for el := another.Columns.Front(); el != nil; el = el.Next() {
		name := el.Key
		_, ok := t.Columns.Get(name)
		if !ok {
			diff.Columns.Set(name, el.Value)
		}
	}

	jitsuPrimaryKeyName := sqlAdapter.BuildConstraintName(t.Name)
	//check if primary key is maintained by Jitsu (for Postgres and Redshift)
	if t.PrimaryKeyName != "" && !strings.HasPrefix(strings.ToLower(t.PrimaryKeyName), BulkerManagedPkConstraintPrefix) {
		//primary key isn't maintained by Jitsu: do nothing
		return diff
	}

	//primary keys logic
	if t.PKFields.Size() > 0 {
		if !t.PKFields.Equals(another.PKFields) {
			//re-create or delete if another.PKFields is empty
			diff.DeletePrimaryKeyNamed = t.PrimaryKeyName
			diff.PKFields = another.PKFields
			if another.PKFields.Size() > 0 {
				diff.PrimaryKeyName = jitsuPrimaryKeyName
			}
		}
	} else if another.PKFields.Size() > 0 {
		//create
		diff.PKFields = another.PKFields
		diff.PrimaryKeyName = jitsuPrimaryKeyName
	}

	return diff
}

func (t *Table) ToSimpleMap() *types2.OrderedMap[string, any] {
	simple := types2.NewOrderedMap[string, any]()
	for el := t.Columns.Front(); el != nil; el = el.Next() {
		simple.Set(el.Key, el.Value.Type)
	}
	return simple
}

func (t *Table) ColumnsCount() int {
	if t == nil {
		return 0
	}
	return t.Columns.Len()
}
