package types

import "strings"

const SqlTypePrefix = "__sql_type"

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

func FilterEvent(event Json) {
	_ = event.Delete("JITSU_TABLE_NAME")
	_ = event.Delete("JITSU_PROFILE_ID")
	filterEvent(event)
}

func filterEvent(event any) {
	switch v := event.(type) {
	case Json:
		for el := v.Front(); el != nil; {
			curEl := el
			// move to the next element before deleting the current one. otherwise iteration will be broken
			el = el.Next()
			if strings.HasPrefix(curEl.Key, SqlTypePrefix) {
				v.DeleteElement(curEl)
			} else {
				switch v2 := curEl.Value.(type) {
				case Json, []any:
					filterEvent(v2)
				}
			}
		}
	case []any:
		for _, a := range v {
			switch v2 := a.(type) {
			case Json, []any:
				filterEvent(v2)
			}
		}
	}
}
