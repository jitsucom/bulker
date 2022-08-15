package sql

type SQLTypes map[string]SQLColumn

type SQLColumn struct {
	Type     string
	ddlType  string
	Override bool
}

func (c SQLColumn) DDLType() string {
	if c.ddlType != "" {
		return c.ddlType
	}
	return c.Type
}
