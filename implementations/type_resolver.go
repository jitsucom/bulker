package implementations

import (
	"fmt"
	"github.com/jitsucom/bulker/types"
	"strings"
)

const SqlTypeKeyword = "__sql_type_"

var DefaultTypeResolver = NewTypeResolver()

//TypeResolver resolves types.Fields from input object
type TypeResolver interface {
	Resolve(map[string]interface{}) (types.Fields, error)
}

//DummyTypeResolver doesn't do anything
type DummyTypeResolver struct {
}

//NewDummyTypeResolver return DummyTypeResolver
func NewDummyTypeResolver() *DummyTypeResolver {
	return &DummyTypeResolver{}
}

//Resolve return one dummy field and types.Fields becomes not empty. (it is used in Facebook destination)
func (dtr *DummyTypeResolver) Resolve(object map[string]interface{}) (types.Fields, error) {
	return types.Fields{"dummy": types.NewField(types.UNKNOWN)}, nil
}

//TypeResolverImpl resolves types based on converter.go rules
type TypeResolverImpl struct {
}

//NewTypeResolver returns TypeResolverImpl
func NewTypeResolver() *TypeResolverImpl {
	return &TypeResolverImpl{}
}

//Resolve return types.Fields representation of input object
//apply default typecast and define column types
//reformat from json.Number into int64 or float64 and put back
//reformat from string with timestamp into time.Time and put back
func (tr *TypeResolverImpl) Resolve(object map[string]interface{}) (types.Fields, error) {
	mappedTypes := make(map[string]types.SQLColumn)
	for k, v := range object {
		if strings.Contains(k, SqlTypeKeyword) {
			delete(object, k)
			key := strings.ReplaceAll(k, SqlTypeKeyword, "")
			switch val := v.(type) {
			case []interface{}:
				if len(val) > 1 {
					mappedTypes[key] = types.SQLColumn{Type: fmt.Sprint(val[0]), ColumnType: fmt.Sprint(val[1])}
				} else {
					mappedTypes[key] = types.SQLColumn{Type: fmt.Sprint(val[0])}
				}
			case string:
				mappedTypes[key] = types.SQLColumn{Type: val}
			default:
				return nil, fmt.Errorf("incorred type of value for __sql_type_: %T", v)
			}
		}
	}
	Fields := types.Fields{}
	//apply default typecast and define column types
	for k, v := range object {
		v = types.ReformatValue(v)

		object[k] = v
		//value type
		resultColumnType, err := types.TypeFromValue(v)
		if err != nil {
			return nil, fmt.Errorf("Error getting type of field [%s]: %v", k, err)
		}

		//default typecast
		if defaultType, ok := types.DefaultTypes[k]; ok {
			converted, err := types.Convert(defaultType, v)
			if err != nil {
				return nil, fmt.Errorf("Error default converting field [%s]: %v", k, err)
			}

			resultColumnType = defaultType
			object[k] = converted
		}
		if sqlType, ok := mappedTypes[k]; ok {
			Fields[k] = types.NewFieldWithSQLType(resultColumnType, types.NewSQLTypeSuggestion(sqlType, nil))
		} else {
			Fields[k] = types.NewField(resultColumnType)
		}
	}

	return Fields, nil
}
