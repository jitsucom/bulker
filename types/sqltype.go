package types

const SqlTypeKeyword = "__sql_type_"

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
