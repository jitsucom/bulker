package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

type redshiftResult struct {
	affectedRows int64
}

func newResult(output *redshiftdata.DescribeStatementOutput) *redshiftResult {
	return &redshiftResult{
		affectedRows: output.ResultRows,
	}
}

func newResultWithSubStatementData(st types.SubStatementData) *redshiftResult {
	return &redshiftResult{
		affectedRows: st.ResultRows,
	}
}

func (r *redshiftResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("LastInsertId %w", ErrNotSupported)
}

func (r *redshiftResult) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}

type redshiftDelayedResult struct {
	driver.Result
}

func (r *redshiftDelayedResult) LastInsertId() (int64, error) {
	if r.Result != nil {
		return r.Result.LastInsertId()
	}
	return 0, ErrBeforeCommit
}

func (r *redshiftDelayedResult) RowsAffected() (int64, error) {
	if r.Result != nil {
		return r.Result.RowsAffected()
	}
	return 0, ErrBeforeCommit
}
