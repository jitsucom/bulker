package types

type Schema struct {
	Name   string        `json:"name"`
	Fields []SchemaField `json:"fields"`
}

type SchemaField struct {
	Name string   `json:"name"`
	Type DataType `json:"type"`
}

func (s Schema) IsEmpty() bool {
	return len(s.Fields) == 0
}

func (s Schema) ColumnsCount() int {
	return len(s.Fields)
}
