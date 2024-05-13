package types

type Json = *OrderedMap[string, any]

func NewJson() Json {
	return NewOrderedMap[string, any]()
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
	mp := make(map[string]any, j.Len())
	for el := j.Front(); el != nil; el = el.Next() {
		key := el.Key
		value := el.Value
		js, ok := value.(Json)
		if ok {
			mp[key] = JsonToMap(js)
		} else {
			mp[key] = value
		}
	}
	return mp
}
