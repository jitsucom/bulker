package sql

import (
	"fmt"
	"github.com/jitsucom/bulker/bulkerlib/implementations"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"strings"
)

// ProcessEvents processes events objects without applying mapping rules
// returns table headerm array of processed objects
// or error if at least 1 was occurred
func ProcessEvents(tableName string, event types.Object, customTypes types.SQLTypes, omitNils bool, stringifyObjects bool) (*TypesHeader, types.Object, error) {
	sqlTypesHints, err := extractSQLTypesHints(event)
	if err != nil {
		return nil, nil, err
	}
	for k, v := range customTypes {
		sqlTypesHints[k] = v
	}
	flatObject, err := implementations.NewFlattener(omitNils, stringifyObjects).FlattenObject(event, sqlTypesHints)
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

func extractSQLTypesHints(object types.Object) (types.SQLTypes, error) {
	result := types.SQLTypes{}
	err := _extractSQLTypesHints("", object, result)
	return result, err
}

func _extractSQLTypesHints(key string, object types.Object, result types.SQLTypes) error {
	var toDelete []string
	for el := object.Front(); el != nil; el = el.Next() {
		k := el.Key
		v := el.Value
		//if column has __sql_type_ prefix
		if strings.HasPrefix(k, implementations.SqlTypePrefix) {
			toDelete = append(toDelete, k)
			columnName := strings.TrimPrefix(k[len(implementations.SqlTypePrefix):], "_")
			//when columnName is empty it means that provided sql type is meant for the whole object
			//e.g. to map nested object to sql JSON type you can add the following property to nested object: "__sql_type_": "JSON" )
			mappedColumnName := utils.JoinNonEmptyStrings("_", key, columnName)
			switch val := v.(type) {
			case []any:
				if len(val) > 1 {
					result[mappedColumnName] = types.SQLColumn{Type: fmt.Sprint(val[0]), DdlType: fmt.Sprint(val[1]), Override: true}
				} else {
					result[mappedColumnName] = types.SQLColumn{Type: fmt.Sprint(val[0]), Override: true}
				}
			case string:
				result[mappedColumnName] = types.SQLColumn{Type: val, Override: true}
			default:
				return fmt.Errorf("incorrect type of value for '__sql_type_' hint: %T", v)
			}
		} else if val, ok := v.(types.Object); ok {
			err := _extractSQLTypesHints(utils.JoinNonEmptyStrings("_", key, k), val, result)
			if err != nil {
				return err
			}
		}
	}
	for _, k := range toDelete {
		object.Delete(k)
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
