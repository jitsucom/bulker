package main

import (
	"fmt"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
)

const (
	LogType              = "LOG"
	ConnectionStatusType = "CONNECTION_STATUS"
	StateType            = "STATE"
	RecordType           = "RECORD"
	TraceType            = "TRACE"
	CatalogType          = "CATALOG"
	ControlType          = "CONTROL"
	SpecType             = "SPEC"
)

// Row is a dto for airbyte output row representation
type Row struct {
	Type             string         `json:"type"`
	Log              *LogRow        `json:"log,omitempty"`
	ConnectionStatus *StatusRow     `json:"connectionStatus,omitempty"`
	State            *StateRow      `json:"state,omitempty"`
	Record           *RecordRow     `json:"record,omitempty"`
	Trace            *TraceRow      `json:"trace,omitempty"`
	Catalog          map[string]any `json:"catalog,omitempty"`
	Spec             map[string]any `json:"spec,omitempty"`
}

// LogRow is a dto for airbyte logs serialization
type LogRow struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

type StreamDescriptor struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type StreamStatus struct {
	StreamDescriptor StreamDescriptor `json:"stream_descriptor"`
	Status           string           `json:"status"`
}

// TraceRow is a dto for airbyte trace serialization
type TraceRow struct {
	Type         string       `json:"type"`
	StreamStatus StreamStatus `json:"stream_status,omitempty"`
}

// StatusRow is a dto for airbyte result status serialization
type StatusRow struct {
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

type StreamState struct {
	StreamDescriptor StreamDescriptor `json:"stream_descriptor"`
	StreamState      map[string]any   `json:"stream_state,omitempty"`
}

type GlobalState struct {
	SharedState  map[string]any `json:"shared_state,omitempty"`
	StreamStates []StreamState  `json:"stream_states,omitempty"`
}

// StateRow is a dto for airbyte state serialization
type StateRow struct {
	Type        string         `json:"type,omitempty"`
	StreamState *StreamState   `json:"stream,omitempty"`
	GlobalState *GlobalState   `json:"global,omitempty"`
	Data        map[string]any `json:"data,omitempty"`
}

// RecordRow is a dto for airbyte record serialization
type RecordRow struct {
	Stream    string                 `json:"stream,omitempty"`
	Namespace string                 `json:"namespace,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

type Catalog struct {
	Streams []*Stream `json:"streams,omitempty"`
}

type StreamMeta struct {
	Name        string           `json:"name"`
	Namespace   string           `json:"namespace"`
	TableName   string           `json:"table_name,omitempty"`
	JSONSchema  StreamJsonSchema `json:"json_schema"`
	PrimaryKeys [][]string       `json:"source_defined_primary_key"`
}

type StreamJsonSchema struct {
	Properties map[string]StreamSchemaProperty `json:"properties"`
}

func (s *StreamMeta) ToSchema() types.Schema {
	fields := make([]types.SchemaField, 0, len(s.JSONSchema.Properties))
	for name, prop := range s.JSONSchema.Properties {
		fields = append(fields, types.SchemaField{
			Name: name,
			Type: prop.ToDataType(),
		})
	}
	return types.Schema{
		Name:   s.Name,
		Fields: fields,
	}
}

type StreamSchemaProperty struct {
	Type        any    `json:"type"`
	Format      string `json:"format"`
	AirbyteType string `json:"airbyte_type"`
	OneOf       []any  `json:"oneOf"`
}

func (ssp *StreamSchemaProperty) ToDataType() types.DataType {
	if len(ssp.OneOf) > 0 {
		return types.STRING
	}
	var tp string
	switch v := ssp.Type.(type) {
	case string:
		tp = v
	case []string:
		a := utils.ArrayExcluding(v, "null")
		if len(a) > 0 {
			tp = a[0]
		}
	case []any:
		a := utils.ArrayExcluding(v, "null")
		if len(a) > 0 {
			tp = fmt.Sprint(a[0])
		}
	}
	switch tp {
	case "string":
		if ssp.Format == "date-time" {
			return types.TIMESTAMP
		}
		return types.STRING
	case "boolean":
		return types.BOOL
	case "integer":
		return types.INT64
	case "number":
		if ssp.AirbyteType == "integer" {
			return types.INT64
		}
		return types.FLOAT64
	case "array":
		return types.JSON
	case "object":
		return types.JSON
	default:
		return types.STRING
	}
}

func (s *StreamMeta) GetPrimaryKeys() []string {
	if len(s.PrimaryKeys) == 0 {
		return []string{}
	}
	pks := make([]string, 0, len(s.PrimaryKeys))
	for _, pk := range s.PrimaryKeys {
		pks = append(pks, pk...)
	}
	return pks
}

type Stream struct {
	*StreamMeta `json:"stream,omitempty"`
	SyncMode    string   `json:"sync_mode"`
	CursorField []string `json:"cursor_field"`
}
