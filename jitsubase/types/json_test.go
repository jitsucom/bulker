package types

import (
	"testing"

	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/stretchr/testify/require"
)

const jDirty = `{
  "JITSU_TABLE_NAME": "blabla",
  "type": "track",
  "__sql_type_type": "blabla",
  "event": "test",
  "properties": {
    "title": "Jitsu : ABC",
	"__sql_type_title": "blabla",
    "url": "https://jitsu.com/abc?utm_source=campaign",
    "path": "/start?utm_source=campaign",
    "hash": "",
    "search": "",
    "currency": "USD",
    "width": 1458,
    "height": 1186,
    "newCol1": 1,
    "newCol2": 2
  },
  "anonymousId": "anon_6",
  "context": {
    "library": {
      "name": "@jitsu/js",
      "version": "2.0.1",
      "nested": {
        "zzz": 999,
        "yyy": 888,
        "xxx": 777,
        "kkk": 555,
        "aaa": 111,
        "__sql_type": "blabla"
      },
      "arr1": [1,2,3,4,6],
      "arr2": [{
        "a": 1, "b": 2, "c": 3, "__sql_type_c": "blabla"
      },{
        "x": 1, "y": 2, "z": 3
      }]
    }
  }
}`

const j = `{
  "type": "track",
  "event": "test",
  "properties": {
    "title": "Jitsu : ABC",
    "url": "https://jitsu.com/abc?utm_source=campaign",
    "path": "/start?utm_source=campaign",
    "hash": "",
    "search": "",
    "currency": "USD",
    "width": 1458,
    "height": 1186,
    "newCol1": 1,
    "newCol2": 2
  },
  "anonymousId": "anon_6",
  "context": {
    "library": {
      "name": "@jitsu/js",
      "version": "2.0.1",
      "nested": {
        "zzz": 999,
        "yyy": 888,
        "xxx": 777,
        "kkk": 555,
        "aaa": 111
      },
      "arr1": [1,2,3,4,6],
      "arr2": [{
        "a": 1, "b": 2, "c": 3
      },{
        "x": 1, "y": 2, "z": 3
      }]
    }
  }
}`

const expectedJson = `{"type":"track","event":"test","properties":{"title":"Jitsu : ABC","url":"https://jitsu.com/abc?utm_source=campaign","path":"/start?utm_source=campaign","hash":"","search":"","currency":"USD","width":1458,"height":1186,"newCol1":1,"newCol2":2},"anonymousId":"anon_6","context":{"library":{"name":"@jitsu/js","version":"2.0.1","nested":{"zzz":999,"yyy":888,"xxx":777,"kkk":555,"aaa":111},"arr1":[1,2,3,4,6],"arr2":[{"a":1,"b":2,"c":3},{"x":1,"y":2,"z":3}]}}}`

func TestEventFilter(t *testing.T) {
	var obj *jsonorder.OrderedMap[string, any]
	_ = jsonorder.Unmarshal([]byte(jDirty), &obj)
	t.Log(obj.GetS("__sql_type_type"))
	FilterEvent(obj)
	ja, err := jsonorder.Marshal(obj)
	require.NoError(t, err)
	t.Logf("JSON: %s", ja)
	require.Equal(t, expectedJson, string(ja))
	require.JSONEq(t, j, string(ja))
}
