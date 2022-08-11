package types

import (
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/lib/pq"
	"strings"
)

type ErrorPayload struct {
	Dataset         string
	Bucket          string
	Project         string
	Database        string
	Cluster         string
	Schema          string
	Table           string
	Partition       string
	PrimaryKeys     []string
	Statement       string
	Values          []interface{}
	ValuesMapString string
	TotalObjects    int
}

func (ep *ErrorPayload) String() string {
	var msgParts []string
	if ep.Dataset != "" {
		msgParts = append(msgParts, fmt.Sprintf("dataset: %s", ep.Dataset))
	}
	if ep.Bucket != "" {
		msgParts = append(msgParts, fmt.Sprintf("bucket: %s", ep.Bucket))
	}
	if ep.Project != "" {
		msgParts = append(msgParts, fmt.Sprintf("project: %s", ep.Project))
	}
	if ep.Database != "" {
		msgParts = append(msgParts, fmt.Sprintf("database: %s", ep.Database))
	}
	if ep.Cluster != "" {
		msgParts = append(msgParts, fmt.Sprintf("cluster: %s", ep.Cluster))
	}
	if ep.Schema != "" {
		msgParts = append(msgParts, fmt.Sprintf("schema: %s", ep.Schema))
	}
	if ep.Table != "" {
		msgParts = append(msgParts, fmt.Sprintf("table: %s", ep.Table))
	}
	if ep.Partition != "" {
		msgParts = append(msgParts, fmt.Sprintf("partition: %s", ep.Partition))
	}
	if len(ep.PrimaryKeys) > 0 {
		msgParts = append(msgParts, fmt.Sprintf("primary keys: %v", ep.PrimaryKeys))
	}
	if ep.Statement != "" {
		msgParts = append(msgParts, fmt.Sprintf("statement: %s", utils.ShortenStringWithEllipsis(ep.Statement, 1000)))
	}
	if len(ep.Values) > 0 {
		msgParts = append(msgParts, fmt.Sprintf("values: %v", ep.Values))
	}
	if ep.TotalObjects > 1 {
		msgParts = append(msgParts, fmt.Sprintf("objects count: %d", ep.TotalObjects))
	}
	if ep.ValuesMapString != "" {
		msgParts = append(msgParts, fmt.Sprintf("values of 1st object: %s", ep.ValuesMapString))
	}
	if len(msgParts) > 0 {
		return "\n" + strings.Join(msgParts, "\n") + "\n"
	} else {
		return ""
	}
}

func ObjectValuesToString(header []string, valueArgs []interface{}) string {
	var firstObjectValues strings.Builder
	firstObjectValues.WriteString("{")
	for i, name := range header {
		if i != 0 {
			firstObjectValues.WriteString(", ")
		}
		firstObjectValues.WriteString(name + ": " + fmt.Sprint(valueArgs[i]))
	}
	firstObjectValues.WriteString("}")
	return firstObjectValues.String()
}

// CheckErr checks and extracts parsed pg.Error and extract code,message,details
func CheckErr(err error) error {
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
