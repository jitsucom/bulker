package implementations

import (
	"fmt"
	"github.com/jitsucom/bulker/bulkerlib/types"
	jsoniter "github.com/json-iterator/go"
	"reflect"
)

type Flattener interface {
	FlattenObject(object map[string]any, sqlTypeHints types.SQLTypes) (map[string]any, error)
}

type FlattenerImpl struct {
	omitNilValues bool
	// stringifyObjects objects types like JSON, array will be stringified before sent to warehouse (warehouse will parse them back)
	stringifyObjects bool
}

func NewFlattener(omitNilValues, stringifyObjects bool) Flattener {
	return &FlattenerImpl{
		omitNilValues:    omitNilValues,
		stringifyObjects: stringifyObjects,
	}
}

// FlattenObject flatten object e.g. from {"key1":{"key2":123}} to {"key1_key2":123}
// from {"$key1":1} to {"_key1":1}
// from {"(key1)":1} to {"_key1_":1}
func (f *FlattenerImpl) FlattenObject(object map[string]any, sqlTypeHints types.SQLTypes) (map[string]any, error) {
	flattenMap := make(map[string]any)

	err := f.flatten("", object, flattenMap, sqlTypeHints)
	if err != nil {
		return nil, err
	}
	emptyKeyValue, hasEmptyKey := flattenMap[""]
	if hasEmptyKey {
		flattenMap["_unnamed"] = emptyKeyValue
		delete(flattenMap, "")
	}
	return flattenMap, nil
}

// recursive function for flatten key (if value is inner object -> recursion call)
// Reformat key
func (f *FlattenerImpl) flatten(key string, value any, destination map[string]any, sqlTypeHints types.SQLTypes) error {
	t := reflect.ValueOf(value)
	switch t.Kind() {
	case reflect.Slice:
		b, err := jsoniter.Marshal(value)
		if err != nil {
			return fmt.Errorf("error marshaling array with key %s: %v", key, err)
		}
		destination[key] = string(b)
	case reflect.Map:
		unboxed := value.(map[string]any)
		if _, ok := sqlTypeHints[key]; ok {
			if f.stringifyObjects {
				// if there is sql type hint for nested object - we don't flatten it.
				// Instead, we marshal it to json string hoping that database cast function will do the job
				b, err := jsoniter.Marshal(value)
				if err != nil {
					return fmt.Errorf("error marshaling json object with key %s: %v", key, err)
				}
				destination[key] = string(b)
			} else {
				destination[key] = unboxed
			}
			return nil
		}
		for k, v := range unboxed {
			newKey := k
			if key != "" {
				newKey = key + "_" + newKey
			}
			if err := f.flatten(newKey, v, destination, sqlTypeHints); err != nil {
				return err
			}
		}
	default:
		if !f.omitNilValues || value != nil {
			destination[key] = value
		}
	}

	return nil
}

type DummyFlattener struct {
}

func NewDummyFlattener() *DummyFlattener {
	return &DummyFlattener{}
}

// FlattenObject return the same json object
func (df *DummyFlattener) FlattenObject(object map[string]any, sqlTypeHints types.SQLTypes) (map[string]any, error) {
	return object, nil
}
