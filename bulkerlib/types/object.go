package types

import (
	"encoding/json"
	"github.com/jitsucom/bulker/jitsubase/types"
	"io"
)

type Object = types.Json

func NewObject() Object {
	return types.NewJson()
}

func ObjectFromReader(r io.Reader) (Object, error) {
	return types.JsonFromReader(r)
}

func ObjectFromBytes(b []byte) (Object, error) {
	return types.JsonFromBytes(b)
}

func ObjectFromMap(mp map[string]any) Object {
	return types.JsonFromMap(mp)
}

func ObjectFromDecoder(dec *json.Decoder) (Object, error) {
	return types.JsonFromDecoder(dec)
}

func ObjectToMap(o Object) map[string]any {
	return types.JsonToMap(o)
}

func ObjectToMapFilter(o Object, filter func(string) bool) map[string]any {
	return types.JsonToMapFilter(o, filter)
}
