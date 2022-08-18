package sql

import (
	"github.com/jitsucom/bulker/types"
	"sort"
)

type Fields map[string]Field

// BatchHeader is the schema result of parsing JSON objects
type BatchHeader struct {
	TableName string
	Fields    Fields
	Partition DatePartition
}

// Exists returns true if there is at least one field
func (bh *BatchHeader) Exists() bool {
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

// OverrideTypes check if field exists in other then put its type
func (f Fields) OverrideTypes(other SQLTypes) {
	for otherName, otherField := range other {
		if currentField, ok := f[otherName]; ok {
			//override type occurrences
			currentField.sqlTypeSuggestion = NewSQLTypeSuggestion(otherField, map[string]SQLColumn{})
			f[otherName] = currentField
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

// SQLTypeSuggestion is a struct which keeps certain SQL types per certain destination type
type SQLTypeSuggestion struct {
	sqlType               SQLColumn
	sqlTypePerDestination map[string]SQLColumn
}

// NewSQLTypeSuggestion returns configured SQLTypeSuggestion instance
func NewSQLTypeSuggestion(sqlType SQLColumn, sqlTypePerDestination map[string]SQLColumn) *SQLTypeSuggestion {
	return &SQLTypeSuggestion{
		sqlType:               sqlType,
		sqlTypePerDestination: sqlTypePerDestination,
	}
}

// Field is a data type holder with occurrences
type Field struct {
	dataType          *types.DataType
	sqlTypeSuggestion *SQLTypeSuggestion
}

// NewField returns Field instance
func NewField(t types.DataType) Field {
	return Field{
		dataType: &t,
	}
}

// NewFieldWithSQLType returns Field instance with configured suggested sql types
func NewFieldWithSQLType(t types.DataType, sqlTypeSuggestion *SQLTypeSuggestion) Field {
	return Field{
		dataType:          &t,
		sqlTypeSuggestion: sqlTypeSuggestion,
	}
}

// GetSuggestedSQLType returns suggested SQL type if configured
// is used in case when source overrides destination type
func (f Field) GetSuggestedSQLType(destinationType string) (SQLColumn, bool) {
	if f.sqlTypeSuggestion != nil {
		sqlType, ok := f.sqlTypeSuggestion.sqlTypePerDestination[destinationType]
		if !ok && f.sqlTypeSuggestion.sqlType.Type != "" {
			sqlType = f.sqlTypeSuggestion.sqlType
			ok = true
		}
		if ok {
			return SQLColumn{Type: sqlType.Type, DdlType: sqlType.DdlType, Override: true}, ok
		}
	}

	return SQLColumn{}, false
}

// GetType get field type based on occurrence in one file
// lazily get common ancestor type (typing.GetCommonAncestorType)
func (f Field) GetType() types.DataType {
	return *f.dataType
}
