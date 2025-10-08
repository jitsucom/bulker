package types

import (
	"strings"

	"github.com/jitsucom/bulker/jitsubase/jsonorder"
)

const SqlTypePrefix = "__sql_type"

type Json = *jsonorder.OrderedMap[string, any]

func NewJson(defaultCapacity int) Json {
	return jsonorder.NewOrderedMap[string, any](defaultCapacity)
}

func JsonFromMap(mp map[string]any) Json {
	om := NewJson(len(mp))
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

func JsonFromKV(kv ...any) Json {
	om := NewJson(len(kv) / 2)
	for i := 0; i < len(kv)-1; i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			continue
		}
		om.Set(key, kv[i+1])
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
	_ = event.Delete("SALESFORCE_OPERATION")
	_ = event.Delete("SALESFORCE_SOBJECT")
	_ = event.Delete("SALESFORCE_MATCHERS_OPERATOR")
	_ = event.Delete("SALESFORCE_MATCHERS")
	_ = event.Delete("SALESFORCE_PAYLOAD")
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
