package types

import (
	"github.com/jitsucom/bulker/jitsubase/types"
)

type Object = types.Json

func NewObject() Object {
	return types.NewJson()
}

func ObjectFromMap(mp map[string]any) Object {
	return types.JsonFromMap(mp)
}

func ObjectToMap(o Object) map[string]any {
	return types.JsonToMap(o)
}
