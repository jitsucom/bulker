package sql

type SQLTypes map[string]SQLColumn

type SQLColumn struct {
	Type     string
	DdlType  string
	Override bool
}

func (c SQLColumn) GetDDLType() string {
	if c.DdlType != "" {
		return c.DdlType
	}
	return c.Type
}

func (s SQLTypes) With(name, sqlType string) SQLTypes {
	return s.WithDDL(name, sqlType, "")
}

func (s SQLTypes) WithDDL(name, sqlType, ddlType string) SQLTypes {
	if sqlType == "" {
		return s
	} else if ddlType == "" {
		s[name] = SQLColumn{Type: sqlType, DdlType: sqlType}
	} else {
		s[name] = SQLColumn{Type: sqlType, DdlType: ddlType}
	}
	return s
}
