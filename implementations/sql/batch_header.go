package sql

import (
	"github.com/jitsucom/bulker/types"
	"sort"
)

type Fields map[string]Field

// TypesHeader is the schema result of parsing JSON objects
type TypesHeader struct {
	TableName string
	Fields    Fields
	Partition DatePartition
}

// Exists returns true if there is at least one field
func (bh *TypesHeader) Exists() bool {
	return bh != nil && len(bh.Fields) > 0
}

// Merge adds all fields from other to current instance or merge if exists
func (f Fields) Merge(other Fields) {
	for otherName, otherField := range other {
		f[otherName] = otherField
	}
}

// Clone copies fields into a new Fields object
func (f Fields) Clone() Fields {
	clone := Fields{}

	for fieldName, fieldPayload := range f {

		clone[fieldName] = Field{
			dataType: fieldPayload.dataType,
		}
	}

	return clone
}

func (f Fields) OverrideTypes(newTypes SQLTypes) {
	for column, newType := range newTypes {
		if currentField, ok := f[column]; ok {
			//override type occurrences
			currentField.suggestedType = &newType
			f[column] = currentField
		}
	}
}

// Add all new fields from other to current instance
// if field exists - skip it
func (f Fields) Add(other Fields) {
	for otherName, otherField := range other {
		if _, ok := f[otherName]; !ok {
			f[otherName] = otherField
		}
	}
}

// Header return fields names as a string slice
func (f Fields) Header() (header []string) {
	for fieldName := range f {
		header = append(header, fieldName)
	}
	sort.Strings(header)
	return
}

// Field is a data type holder with sql type suggestion
type Field struct {
	dataType      *types.DataType
	suggestedType *SQLColumn
}

// NewField returns Field instance
func NewField(t types.DataType) Field {
	return Field{
		dataType: &t,
	}
}

// NewFieldWithSQLType returns Field instance with configured suggested sql types
func NewFieldWithSQLType(t types.DataType, suggestedType *SQLColumn) Field {
	return Field{
		dataType:      &t,
		suggestedType: suggestedType,
	}
}

// GetSuggestedSQLType returns suggested SQL type if configured
func (f Field) GetSuggestedSQLType() (SQLColumn, bool) {
	if f.suggestedType != nil {
		return SQLColumn{Type: f.suggestedType.Type, DdlType: f.suggestedType.DdlType, Override: true, New: true}, true
	}

	return SQLColumn{}, false
}

// GetType get field type based on occurrence in one file
// lazily get common ancestor type (typing.GetCommonAncestorType)
func (f Field) GetType() types.DataType {
	return *f.dataType
}
