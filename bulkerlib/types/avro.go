package types

type AvroType struct {
	Type    any    `json:"type"`
	Name    string `json:"name"`
	Default any    `json:"default"`
}

type AvroSchema struct {
	Type      string              `json:"type"`
	Name      string              `json:"name"`
	Fields    []AvroType          `json:"fields"`
	DataTypes map[string]DataType `json:"-"`
}
