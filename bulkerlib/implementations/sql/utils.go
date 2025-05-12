package sql

import (
	"database/sql"
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/lib/pq"
	"math/big"
	"strconv"
	"strings"
)

type ColumnScanner struct {
	ColumnType *sql.ColumnType
	value      any
}

func (s *ColumnScanner) Scan(src any) error {
	//logging.Debugf("Scanning %s of %s => %v (%T)", s.ColumnType.Name(), s.ColumnType.DatabaseTypeName(), src, src)
	switch v := src.(type) {
	case []byte:
		s.value = string(v)
	case int64:
		if s.ColumnType.DatabaseTypeName() == "TINYINT" && (v == 1 || v == 0) {
			//hack for mysql where boolean is represented as tinyint(1)
			s.value = v == 1
		} else {
			s.value = int(v)
		}
	case uint8:
		if s.ColumnType.DatabaseTypeName() == "UInt8" && (v == 1 || v == 0) {
			//hack for ClickHouse where boolean is represented as UInt8
			s.value = v == 1
		} else {
			s.value = int(v)
		}
	case big.Int:
		s.value = int(v.Int64())
	case big.Float:
		s.value, _ = v.Float64()
	case *chcol.JSON:
		b, _ := v.MarshalJSON()
		s.value = string(b)
	case string:
		nullable, _ := s.ColumnType.Nullable()
		if !nullable && v == "" {
			s.value = nil
		} else {
			s.value = src
		}
	default:
		s.value = src
	}
	return nil
}

func (s *ColumnScanner) Get() any {
	return s.value
}

type ParameterPlaceholder func(i int, name string) string

var IndexParameterPlaceholder = func(i int, name string) string {
	return "$" + strconv.Itoa(i)
}

var QuestionMarkParameterPlaceholder = func(i int, name string) string {
	return "?"
}

var NamedParameterPlaceholder = func(i int, name string) string {
	return "@" + name
}

func rowToMap(rows *sql.Rows) (map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	data := make([]any, len(columns))
	for i := range columns {
		data[i] = &ColumnScanner{ColumnType: columnTypes[i]}
	}
	if err = rows.Scan(data...); err != nil {
		return nil, err
	}
	row := make(map[string]any, len(columns))
	for i, v := range data {
		row[strings.ToLower(columns[i])] = v.(*ColumnScanner).Get()
	}
	return row, nil
}

func removeLastComma(str string) string {
	if last := len(str) - 1; last >= 0 && str[last] == ',' {
		str = str[:last]
	}

	return str
}

// checkErr checks and extracts parsed pq.Error and extract code,message,details
func checkErr(err error) error {
	if err == nil {
		return nil
	}

	if pgErr, ok := err.(*pq.Error); ok {
		msgParts := []string{"pq:"}
		if pgErr.Code != "" {
			msgParts = append(msgParts, string(pgErr.Code))
		}
		if pgErr.Message != "" {
			msgParts = append(msgParts, pgErr.Message)
		}
		if pgErr.Detail != "" {
			msgParts = append(msgParts, pgErr.Detail)
		}
		if pgErr.Schema != "" {
			msgParts = append(msgParts, "schema:"+pgErr.Schema)
		}
		if pgErr.Table != "" {
			msgParts = append(msgParts, "table:"+pgErr.Table)
		}
		if pgErr.Column != "" {
			msgParts = append(msgParts, "column:"+pgErr.Column)
		}
		if pgErr.DataTypeName != "" {
			msgParts = append(msgParts, "data_type:"+pgErr.DataTypeName)
		}
		if pgErr.Constraint != "" {
			msgParts = append(msgParts, "constraint:"+pgErr.Constraint)
		}
		return errors.New(strings.Join(msgParts, " "))
	}

	return err
}
