package sql

import (
	"fmt"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
)

var DefaultTypeResolver = NewTypeResolver()

// TypeResolver resolves types.Fields from input object
type TypeResolver interface {
	Resolve(object map[string]any, sqlTypeHints types2.SQLTypes) (Fields, error)
}

// DummyTypeResolver doesn't do anything
type DummyTypeResolver struct {
}

// NewDummyTypeResolver return DummyTypeResolver
func NewDummyTypeResolver() *DummyTypeResolver {
	return &DummyTypeResolver{}
}

// Resolve return one dummy field and types.Fields becomes not empty. (it is used in Facebook destination)
func (dtr *DummyTypeResolver) Resolve(object map[string]any, sqlTypeHints types2.SQLTypes) (Fields, error) {
	return []Field{NewField("dummy", types2.UNKNOWN)}, nil
}

// TypeResolverImpl resolves types based on converter.go rules
type TypeResolverImpl struct {
}

// NewTypeResolver returns TypeResolverImpl
func NewTypeResolver() *TypeResolverImpl {
	return &TypeResolverImpl{}
}

// Resolve return types.Fields representation of input object
// apply default typecast and define column types
// reformat from json.Number into int64 or float64 and put back
// reformat from string with timestamp into time.Time and put back
func (tr *TypeResolverImpl) Resolve(object types2.Object, sqlTypeHints types2.SQLTypes) (Fields, error) {
	fields := make(Fields, 0, object.Len())
	//apply default typecast and define column types
	for el := object.Front(); el != nil; el = el.Next() {
		v, ok := types2.ReformatValue(el.Value)
		k := el.Key
		if ok {
			el.Value = v
		}
		field, err := ResolveType(k, v, sqlTypeHints)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	return fields, nil
}

func ResolveType(name string, value any, sqlTypeHints types2.SQLTypes) (Field, error) {
	resultColumnType, err := types2.TypeFromValue(value)
	if err != nil {
		return Field{}, fmt.Errorf("Error getting type of field [%s] value %v: %v", name, value, err)
	}

	if sqlTypeHints == nil {
		return Field{
			Name:     name,
			dataType: &resultColumnType,
		}, nil
	} else {
		if sqlType, ok := sqlTypeHints[name]; ok {
			return Field{
				Name:          name,
				dataType:      &resultColumnType,
				suggestedType: &sqlType,
			}, nil
		} else {
			return Field{
				Name:     name,
				dataType: &resultColumnType,
			}, nil
		}
	}
}
