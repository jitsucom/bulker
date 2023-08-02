package main

const (
	LogType              = "LOG"
	ConnectionStatusType = "CONNECTION_STATUS"
	StateType            = "STATE"
	RecordType           = "RECORD"
	TraceType            = "TRACE"
	CatalogType          = "CATALOG"
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
	Name        string     `json:"name"`
	Namespace   string     `json:"namespace"`
	PrimaryKeys [][]string `json:"source_defined_primary_key"`
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
