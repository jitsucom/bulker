package sql

import (
	"fmt"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/types"
	"strings"
)

const SqlTypePrefix = "__sql_type"

// ProcessEvents processes events objects without applying mapping rules
// returns table headerm array of processed objects
// or error if at least 1 was occurred
func ProcessEvents(tableName string, event types.Object, customTypes SQLTypes) (*TypesHeader, types.Object, error) {
	sqlTypesHints, err := extractSQLTypesHints(event)
	if err != nil {
		return nil, nil, err
	}
	for k, v := range customTypes {
		sqlTypesHints[k] = v
	}
	flatObject, err := DefaultFlattener.FlattenObject(event, sqlTypesHints)
	if err != nil {
		return nil, nil, err
	}
	fields, err := DefaultTypeResolver.Resolve(flatObject, sqlTypesHints)
	if err != nil {
		return nil, nil, err
	}
	bh := &TypesHeader{TableName: tableName, Fields: fields}

	return bh, flatObject, nil
}

func extractSQLTypesHints(object map[string]any) (SQLTypes, error) {
	result := SQLTypes{}
	err := _extractSQLTypesHints("", object, result)
	return result, err
}

func _extractSQLTypesHints(key string, object map[string]any, result SQLTypes) error {
	for k, v := range object {
		//if column has __sql_type_ prefix
		if columnName := strings.TrimPrefix(k, SqlTypePrefix); columnName != k {
			delete(object, k)
			columnName = strings.TrimPrefix(columnName, "_")
			//when columnName is empty it means that provided sql type is meant for the whole object
			//e.g. to map nested object to sql JSON type you can add the following property to nested object: "__sql_type_": "JSON" )
			mappedColumnName := utils.JoinNonEmptyStrings("_", key, columnName)
			switch val := v.(type) {
			case []any:
				if len(val) > 1 {
					result[mappedColumnName] = SQLColumn{Type: fmt.Sprint(val[0]), DdlType: fmt.Sprint(val[1]), Override: true}
				} else {
					result[mappedColumnName] = SQLColumn{Type: fmt.Sprint(val[0]), Override: true}
				}
			case string:
				result[mappedColumnName] = SQLColumn{Type: val, Override: true}
			default:
				return fmt.Errorf("incorrect type of value for '__sql_type_' hint: %T", v)
			}
		} else if val, ok := v.(map[string]any); ok {
			err := _extractSQLTypesHints(utils.JoinNonEmptyStrings("_", key, k), val, result)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

//
//// foldLongFields replace all column names with truncated values if they exceed the limit
//// uses cutName under the hood
//func foldLongFields(header *TypesHeader, object types.Object) (*TypesHeader, types.Object, error) {
//	//TODO: Get maxColumnNameLen from storage config
//	maxColumnNameLen := 0
//
//	if maxColumnNameLen <= 0 {
//		return header, object, nil
//	}
//
//	changes := map[string]string{}
//	for name := range header.Fields {
//		if len(name) > maxColumnNameLen {
//			newName := cutName(name, maxColumnNameLen)
//			if name != newName {
//				changes[name] = newName
//			}
//		}
//	}
//
//	for oldName, newName := range changes {
//		field, _ := header.Fields[oldName]
//		delete(header.Fields, oldName)
//		header.Fields[newName] = field
//
//		if value, ok := object[oldName]; ok {
//			delete(object, oldName)
//			object[newName] = value
//		}
//	}
//
//	return header, object, nil
//}
//
//// cutName converts input name that exceeds maxLen to lower length string by cutting parts between '_' to 2 symbols.
//// if name len is still greater than returns maxLen symbols from the end of the name
//func cutName(name string, maxLen int) string {
//	if len(name) <= maxLen {
//		return name
//	}
//
//	//just cut from the beginning
//	if !strings.Contains(name, "_") {
//		return name[len(name)-maxLen:]
//	}
//
//	var replaced bool
//	replace := ""
//	for _, part := range strings.Split(name, "_") {
//		if replace != "" {
//			replace += "_"
//		}
//
//		if len(part) > 2 {
//			newPart := part[:2]
//			name = strings.ReplaceAll(name, replace+part, replace+newPart)
//			replaced = true
//			break
//		} else {
//			replace += part
//		}
//	}
//
//	if !replaced {
//		//case when ab_ac_ad and maxLen = 6
//		return name[len(name)-maxLen:]
//	}
//
//	return cutName(name, maxLen)
//}
