package types

import (
	"encoding/json"
	"fmt"
	"io"
)

type Json = *OrderedMap[string, any]

func NewJson() Json {
	return NewOrderedMap[string, any]()
}

func JsonFromReader(r io.Reader) (Json, error) {
	return FromReader[string, any](r)
}

func JsonFromBytes(b []byte) (Json, error) {
	return FromBytes[string, any](b)
}

func JsonFromDecoder(dec *json.Decoder) (Json, error) {
	obj, err := FromDecoder[string, any](dec)
	if err != nil {
		return nil, err
	}
	m, ok := obj.(Json)
	if !ok {
		return nil, fmt.Errorf("Incorrect type: %T, expected object", obj)
	}
	return m, nil
}

func JsonFromMap(mp map[string]any) Json {
	om := NewJson()
	for k, v := range mp {
		nested, ok := v.(map[string]any)
		if ok {
			om.Set(k, JsonFromMap(nested))
		} else {
			om.Set(k, v)
		}
	}
	return om
}

func JsonToMap(j Json) map[string]any {
	return JsonToMapFilter(j, nil)
}

func JsonToMapFilter(j Json, filter func(string) bool) map[string]any {
	mp := make(map[string]any, j.Len())
	for el := j.Front(); el != nil; el = el.Next() {
		key := el.Key
		if filter != nil && !filter(key) {
			continue
		}
		value := el.Value
		js, ok := value.(Json)
		if ok {
			mp[key] = JsonToMapFilter(js, filter)
		} else {
			mp[key] = value
		}
	}
	return mp
}
