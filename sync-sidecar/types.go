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
	Type             string                 `json:"type"`
	Log              *LogRow                `json:"log,omitempty"`
	ConnectionStatus *StatusRow             `json:"connectionStatus,omitempty"`
	State            *StateRow              `json:"state,omitempty"`
	Record           *RecordRow             `json:"record,omitempty"`
	Catalog          string                 `json:"catalog,omitempty"`
	Spec             map[string]interface{} `json:"spec,omitempty"`
}

// LogRow is a dto for airbyte logs serialization
type LogRow struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

// StatusRow is a dto for airbyte result status serialization
type StatusRow struct {
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

// StateRow is a dto for airbyte state serialization
type StateRow struct {
	Data map[string]interface{} `json:"data,omitempty"`
}

// RecordRow is a dto for airbyte record serialization
type RecordRow struct {
	Stream string                 `json:"stream,omitempty"`
	Data   map[string]interface{} `json:"data,omitempty"`
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
