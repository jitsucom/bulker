package implementations

import (
	"fmt"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	types2 "github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"reflect"
)

const SqlTypePrefix = "__sql_type"

type Flattener interface {
	FlattenObject(object types.Object, notFlatteningKeys types2.Set[string]) (types.Object, error)
}

type FlattenerImpl struct {
	nameTransformer func(string) string
	omitNilValues   bool
	// stringifyObjects objects types like JSON, array will be stringified before sent to warehouse (warehouse will parse them back)
	stringifyObjects bool
}

func NewFlattener(nameTransformer func(string) string, omitNilValues, stringifyObjects bool) Flattener {
	if nameTransformer == nil {
		nameTransformer = func(s string) string {
			return s
		}
	}
	return &FlattenerImpl{
		nameTransformer:  nameTransformer,
		omitNilValues:    omitNilValues,
		stringifyObjects: stringifyObjects,
	}
}

// FlattenObject flatten object e.g. from {"key1":{"key2":123}} to {"key1_key2":123}
// from {"$key1":1} to {"_key1":1}
// from {"(key1)":1} to {"_key1_":1}
func (f *FlattenerImpl) FlattenObject(object types.Object, notFlatteningKeys types2.Set[string]) (types.Object, error) {
	flattenMap := types.NewObject()

	err := f.flatten("", object, flattenMap, notFlatteningKeys)
	if err != nil {
		return nil, err
	}
	emptyKeyValue, hasEmptyKey := flattenMap.Get("")
	if hasEmptyKey {
		flattenMap.Set("_unnamed", emptyKeyValue)
		flattenMap.Delete("")
	}
	return flattenMap, nil
}

// recursive function for flatten key (if value is inner object -> recursion call)
// Reformat key
func (f *FlattenerImpl) flatten(key string, value types.Object, destination types.Object, notFlatteningKeys types2.Set[string]) error {
	if _, ok := notFlatteningKeys[key]; ok {
		if f.stringifyObjects {
			// if there is sql type hint for nested object - we don't flatten it.
			// Instead, we marshal it to json string hoping that database cast function will do the job
			b, err := jsonorder.MarshalToString(value)
			if err != nil {
				return fmt.Errorf("error marshaling json object with key %s: %v", key, err)
			}
			destination.Set(key, b)
		} else {
			destination.Set(key, types.ObjectToMap(value))
		}
		return nil
	}
	for el := value.Front(); el != nil; el = el.Next() {
		var newKey string
		if key != "" {
			newKey = key + "_" + f.nameTransformer(el.Key)
		} else {
			newKey = f.nameTransformer(el.Key)
		}
		elv := el.Value
		if elv == nil {
			if !f.omitNilValues {
				destination.Set(newKey, elv)
			} else {
				continue
			}
		} else {
			k := reflect.TypeOf(elv).Kind()
			switch k {
			case reflect.Slice, reflect.Array:
				if f.stringifyObjects {
					b, err := jsonorder.Marshal(elv)
					if err != nil {
						return fmt.Errorf("error marshaling array with key %s: %v", key, err)
					}
					destination.Set(newKey, string(b))
				} else {
					switch vv := elv.(type) {
					case []types.Object:
						destination.Set(newKey, utils.ArrayMap(vv, func(obj types.Object) map[string]any {
							return types.ObjectToMap(obj)
						}))
					case []any:
						destination.Set(newKey, utils.ArrayMap(vv, func(obj any) any {
							o, ok := obj.(types.Object)
							if ok {
								return types.ObjectToMap(o)
							}
							return obj
						}))
					default:
						destination.Set(newKey, elv)
					}
				}
			case reflect.Map:
				return fmt.Errorf("flattener doesn't support map. Object is required")
			default:
				obj, ok := elv.(types.Object)
				if ok {
					if err := f.flatten(newKey, obj, destination, notFlatteningKeys); err != nil {
						return err
					}
				} else {
					destination.Set(newKey, elv)
				}
			}
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
