package jsonorder

import (
	"encoding/json"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/stretchr/testify/require"
	"testing"
)

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

const expectedJson = `{"type":"track","event":"test","properties":{"title":"Jitsu : ABC","url":"https://jitsu.com/abc?utm_source=campaign","path":"/start?utm_source=campaign","hash":"","search":"","currency":"USD","width":1458,"height":1186,"newCol1":1,"newCol2":2},"anonymousId":"anon_6","context":{"library":{"name":"@jitsu/js","version":"2.0.1","nested":{"zzz":999,"yyy":888,"xxx":777,"kkk":555,"aaa":111},"arr1":[1,2,3,4,6],"arr2":[{"a":1,"b":2,"c":3},{"x":1,"y":2,"z":3}]}}}`

func TestOrderedJSON(t *testing.T) {
	//var m map[string]any
	//iter1 := ConfigWithNumbers.BorrowIterator([]byte(j))
	//defer ConfigWithNumbers.ReturnIterator(iter1)
	//iter1.ReadVal(&m)
	var obj *types.OrderedMap[string, any]
	_ = Unmarshal([]byte(j), &obj)
	t.Log(obj.GetS("type"))
	ja, err := Marshal(obj)
	require.NoError(t, err)
	t.Logf("JSON: %s", ja)
	require.Equal(t, expectedJson, string(ja))
	require.JSONEq(t, j, string(ja))

	mExpected := map[string]any{}
	err = json.Unmarshal([]byte(j), &mExpected)
	require.NoError(t, err)
	mActual := map[string]any{}
	err = json.Unmarshal([]byte(ja), &mActual)
	require.NoError(t, err)

	require.Equal(t, mExpected, mActual)

	require.Equal(t, "@jitsu/js", obj.GetPathS("context.library.name"))
	token := obj.Front()
	require.Equal(t, "type", token.Key)
	require.Equal(t, "track", token.Value)
	token = token.Next()
	require.Equal(t, "event", token.Key)
	require.Equal(t, "test", token.Value)
	token = token.Next()
	mp, ok := token.Value.(*types.OrderedMap[string, any])
	require.True(t, ok)
	token = mp.Front()
	require.Equal(t, "title", token.Key)
	require.Equal(t, "Jitsu : ABC", token.Value)
	token = token.Next()
	require.Equal(t, "url", token.Key)
	require.Equal(t, "https://jitsu.com/abc?utm_source=campaign", token.Value)
	token = token.Next()
	require.Equal(t, "path", token.Key)
	require.Equal(t, "/start?utm_source=campaign", token.Value)
	token = token.Next()
	require.Equal(t, "hash", token.Key)
	require.Equal(t, "", token.Value)
	token = token.Next()
	require.Equal(t, "search", token.Key)
	require.Equal(t, "", token.Value)
	token = token.Next()
	require.Equal(t, "currency", token.Key)
	require.Equal(t, "USD", token.Value)
	t.Log("OK")
}

func TestEventFilter(t *testing.T) {
	var obj *types.OrderedMap[string, any]
	_ = Unmarshal([]byte(jDirty), &obj)
	t.Log(obj.GetS("__sql_type_type"))
	types.FilterEvent(obj)
	ja, err := Marshal(obj)
	require.NoError(t, err)
	t.Logf("JSON: %s", ja)
	require.Equal(t, expectedJson, string(ja))
	require.JSONEq(t, j, string(ja))
}
