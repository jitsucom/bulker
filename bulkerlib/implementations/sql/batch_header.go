package sql

import (
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
)

type Fields = []Field

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

// Field is a data type holder with sql type suggestion
type Field struct {
	Name          string
	dataType      *types2.DataType
	suggestedType *types2.SQLColumn
}

// NewField returns Field instance
func NewField(name string, t types2.DataType) Field {
	return Field{
		Name:     name,
		dataType: &t,
	}
}

// NewFieldWithSQLType returns Field instance with configured suggested sql types
func NewFieldWithSQLType(name string, t types2.DataType, suggestedType *types2.SQLColumn) Field {
	return Field{
		Name:          name,
		dataType:      &t,
		suggestedType: suggestedType,
	}
}

// GetSuggestedSQLType returns suggested SQL type if configured
func (f Field) GetSuggestedSQLType() (types2.SQLColumn, bool) {
	if f.suggestedType != nil {
		return types2.SQLColumn{Type: f.suggestedType.Type, DdlType: f.suggestedType.DdlType, Override: true}, true
	}

	return types2.SQLColumn{}, false
}

// GetType get field type based on occurrence in one file
// lazily get common ancestor type (typing.GetCommonAncestorType)
func (f Field) GetType() types2.DataType {
	return *f.dataType
}
