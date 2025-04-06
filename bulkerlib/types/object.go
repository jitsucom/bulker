package types

import (
	"github.com/jitsucom/bulker/jitsubase/types"
)

type Object = types.Json

func NewObject(defaultCapacity int) Object {
	return types.NewJson(defaultCapacity)
}

func ObjectFromMap(mp map[string]any) Object {
	return types.JsonFromMap(mp)
}

func ObjectToMap(o Object) map[string]any {
	return types.JsonToMap(o)
}
