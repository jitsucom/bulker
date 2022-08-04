package types

type SQLTypes map[string]SQLColumn

type SQLColumn struct {
	Type       string
	ColumnType string
	Override   bool
}

func (c SQLColumn) DDLType() string {
	if c.ColumnType != "" {
		return c.ColumnType
	}
	return c.Type
}
