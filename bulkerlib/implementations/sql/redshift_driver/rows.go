package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

type redshiftRows struct {
	ctx context.Context
	id  string
	p   *redshiftdata.GetStatementResultPaginator

	page        *redshiftdata.GetStatementResultOutput
	columns     []types.ColumnMetadata
	columnNames []string
	index       int
}

func newRows(ctx context.Context, id string, p *redshiftdata.GetStatementResultPaginator) (*redshiftRows, error) {
	rows := &redshiftRows{
		ctx: ctx,
		id:  id,
		p:   p,
	}
	return rows, rows.getStatementResult()
}

func (rows *redshiftRows) Close() (err error) {
	return nil
}

func (rows *redshiftRows) Columns() []string {
	return rows.columnNames
}

func (rows *redshiftRows) ColumnTypeDatabaseTypeName(index int) string {
	return strings.ToUpper(*rows.columns[index].TypeName)
}

func (rows *redshiftRows) getStatementResult() error {
	if rows.p == nil {
		return io.EOF
	}
	var err error
	rows.page, err = rows.p.NextPage(rows.ctx)
	if err != nil {
		return err
	}
	rows.columns = rows.page.ColumnMetadata
	rows.columnNames = make([]string, 0, len(rows.columns))
	for _, meta := range rows.columns {
		rows.columnNames = append(rows.columnNames, *meta.Name)
	}
	rows.index = 0
	return nil
}

func (rows *redshiftRows) Next(dest []driver.Value) error {
	if rows.page == nil || rows.index >= len(rows.page.Records) {
		if !rows.p.HasMorePages() {
			return io.EOF
		}
		if err := rows.getStatementResult(); err != nil {
			return err
		}
		rows.index = 0
		if len(rows.page.Records) == 0 {
			return io.EOF
		}
	}
	record := rows.page.Records[rows.index]
	for i := range dest {
		if i < len(record) {
			switch field := record[i].(type) {
			case *types.FieldMemberIsNull:
				dest[i] = nil
			case *types.FieldMemberStringValue:
				dataType := strings.ToLower(*rows.page.ColumnMetadata[i].TypeName)
				switch dataType {
				case "timestamp":
					t, err := time.Parse("2006-01-02 15:04:05.999999", field.Value)
					if err != nil {
						dest[i] = field.Value
					} else {
						dest[i] = t
					}
				case "timestamptz":
					t, err := time.Parse("2006-01-02 15:04:05.999999-07", field.Value)
					if err != nil {
						dest[i] = field.Value
					} else {
						dest[i] = t
					}
				case "date":
					t, err := time.Parse("2006-01-02", field.Value)
					if err != nil {
						dest[i] = field.Value
					} else {
						dest[i] = t
					}
				case "time", "time without time zone":
					t, err := time.Parse("15:04:05.999999", field.Value)
					if err != nil {
						dest[i] = field.Value
					} else {
						dest[i] = t
					}
				case "timetz", "time with time zone":
					t, err := time.Parse("15:04:05.999999-07", field.Value)
					if err != nil {
						dest[i] = field.Value
					} else {
						dest[i] = t
					}

				default:
					dest[i] = field.Value
				}
			case *types.FieldMemberLongValue:
				dest[i] = field.Value
			case *types.FieldMemberBooleanValue:
				dest[i] = field.Value
			case *types.FieldMemberDoubleValue:
				dest[i] = field.Value
			case *types.FieldMemberBlobValue:
				dest[i] = field.Value
			}
		}
	}
	rows.index++
	return nil
}
