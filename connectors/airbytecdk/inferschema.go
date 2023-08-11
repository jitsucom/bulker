package airbyte

import (
	"encoding/json"
	"fmt"
	"github.com/jitsucom/bulker/airbytecdk/schema"
	"reflect"
)

// Infer schema translates golang structs to JSONSchema format
func InferSchemaFromStruct(i interface{}, logTracker LogTracker) Properties {
	var prop Properties

	s, err := schema.Generate(reflect.TypeOf(i))
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("generate schema error: %v", err))
		return prop
	}

	b, err := json.Marshal(s)
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("json marshal schema error: %v", err))
		return prop
	}

	err = json.Unmarshal(b, &prop)
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("unmarshal schema to propspec error: %v", err))
		return prop
	}

	return prop
}
