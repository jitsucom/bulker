package sql

import (
	"github.com/jitsucom/bulker/types"
	"strings"
)

const SqlTypeKeyword = "__sql_type_"

// ProcessEvents processes events objects without applying mapping rules
// returns table headerm array of processed objects
// or error if at least 1 was occurred
func ProcessEvents(tableName string, objects []types.Object, sqlAdapter SQLAdapter) (*BatchHeader, []types.Object, error) {

	var batchHeader *BatchHeader
	var payload []types.Object
	for _, event := range objects {
		flatObject, err := DefaultFlattener.FlattenObject(event)
		if err != nil {
			return nil, nil, err
		}
		sqlAdaptedObject := make(types.Object)
		for k, v := range flatObject {
			sqlAdaptedObject[sqlAdapter.ColumnName(k)] = v
		}
		fields, err := DefaultTypeResolver.Resolve(sqlAdaptedObject)
		if err != nil {
			return nil, nil, err
		}
		bh := &BatchHeader{TableName: sqlAdapter.TableName(tableName), Fields: fields}

		//don't process empty and skipped object
		if !bh.Exists() {
			continue
		}

		foldedBatchHeader, foldedObject, _ := foldLongFields(bh, sqlAdaptedObject)

		if len(payload) == 0 {
			payload = []types.Object{foldedObject}
			batchHeader = foldedBatchHeader
		} else {
			batchHeader.Fields.Merge(foldedBatchHeader.Fields)
			payload = append(payload, foldedObject)
		}
	}

	return batchHeader, payload, nil
}

// foldLongFields replace all column names with truncated values if they exceed the limit
// uses cutName under the hood
func foldLongFields(header *BatchHeader, object types.Object) (*BatchHeader, types.Object, error) {
	//TODO: Get maxColumnNameLen from storage config
	maxColumnNameLen := 0

	if maxColumnNameLen <= 0 {
		return header, object, nil
	}

	changes := map[string]string{}
	for name := range header.Fields {
		if len(name) > maxColumnNameLen {
			newName := cutName(name, maxColumnNameLen)
			if name != newName {
				changes[name] = newName
			}
		}
	}

	for oldName, newName := range changes {
		field, _ := header.Fields[oldName]
		delete(header.Fields, oldName)
		header.Fields[newName] = field

		if value, ok := object[oldName]; ok {
			delete(object, oldName)
			object[newName] = value
		}
	}

	return header, object, nil
}

// cutName converts input name that exceeds maxLen to lower length string by cutting parts between '_' to 2 symbols.
// if name len is still greater than returns maxLen symbols from the end of the name
func cutName(name string, maxLen int) string {
	if len(name) <= maxLen {
		return name
	}

	//just cut from the beginning
	if !strings.Contains(name, "_") {
		return name[len(name)-maxLen:]
	}

	var replaced bool
	replace := ""
	for _, part := range strings.Split(name, "_") {
		if replace != "" {
			replace += "_"
		}

		if len(part) > 2 {
			newPart := part[:2]
			name = strings.ReplaceAll(name, replace+part, replace+newPart)
			replaced = true
			break
		} else {
			replace += part
		}
	}

	if !replaced {
		//case when ab_ac_ad and maxLen = 6
		return name[len(name)-maxLen:]
	}

	return cutName(name, maxLen)
}
