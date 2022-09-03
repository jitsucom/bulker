package sql

import (
	"context"
	"database/sql"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
)

// TxOrDBWrapper is sql transaction wrapper. Used for handling and log errors with db type (postgres, mySQL, redshift or snowflake)
// on Commit() and Rollback() calls
type TxOrDBWrapper struct {
	dbType       string
	db           *sql.DB
	tx           *sql.Tx
	queryLogger  *logging.QueryLogger
	errorAdapter ErrorAdapter
}

// ErrorAdapter is used to extract implementation specific payload and adapt to standard error
type ErrorAdapter func(error) error

type TxOrDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Exec(query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryRow(query string, args ...any) *sql.Row
}

func NewTxWrapper(dbType string, tx *sql.Tx, queryLogger *logging.QueryLogger, errorAdapter ErrorAdapter) *TxOrDBWrapper {
	return &TxOrDBWrapper{dbType: dbType, tx: tx, queryLogger: queryLogger, errorAdapter: errorAdapter}
}

func NewDbWrapper(dbType string, db *sql.DB, queryLogger *logging.QueryLogger, errorAdapter ErrorAdapter) *TxOrDBWrapper {
	return &TxOrDBWrapper{dbType: dbType, db: db, queryLogger: queryLogger, errorAdapter: errorAdapter}
}

func NewDummyTxWrapper(dbType string) *TxOrDBWrapper {
	return &TxOrDBWrapper{dbType: dbType}
}

func wrap[R any](ctx context.Context,
	t *TxOrDBWrapper, queryFunction func(tx TxOrDB, query string, args ...any) (R, error),
	query string, args ...any,
) (res R, err error) {
	tx := t.tx
	if tx == nil {
		res, err = queryFunction(t.db, query, args...)
	} else {
		res, err = queryFunction(tx, query, args...)
	}
	if t.errorAdapter != nil {
		err = t.errorAdapter(err)
	}
	if t.queryLogger != nil {
		t.queryLogger.LogQuery(query, err, args...)
	}
	return res, err
}

// ExecContext executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
func (t *TxOrDBWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return wrap(ctx, t, func(tx TxOrDB, query string, args ...any) (sql.Result, error) {
		return tx.ExecContext(ctx, query, args...)
	}, query, args...)
}

// Exec executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
//
// Exec uses context.Background internally; to specify the context, use
// ExecContext.
func (t *TxOrDBWrapper) Exec(query string, args ...any) (sql.Result, error) {
	return wrap(context.Background(), t, func(tx TxOrDB, query string, args ...any) (sql.Result, error) {
		return tx.Exec(query, args...)
	}, query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
func (t *TxOrDBWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return wrap(ctx, t, func(tx TxOrDB, query string, args ...any) (*sql.Rows, error) {
		return tx.QueryContext(ctx, query, args...)
	}, query, args...)
}

// Query executes a query that returns rows, typically a SELECT.
//
// Query uses context.Background internally; to specify the context, use
// QueryContext.
func (t *TxOrDBWrapper) Query(query string, args ...any) (*sql.Rows, error) {
	return wrap(context.Background(), t, func(tx TxOrDB, query string, args ...any) (*sql.Rows, error) {
		return tx.Query(query, args...)
	}, query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (t *TxOrDBWrapper) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row, _ := wrap(ctx, t, func(tx TxOrDB, query string, args ...any) (*sql.Row, error) {
		return tx.QueryRowContext(ctx, query, args...), nil
	}, query, args...)
	return row
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
//
// QueryRow uses context.Background internally; to specify the context, use
// QueryRowContext.
func (t *TxOrDBWrapper) QueryRow(query string, args ...any) *sql.Row {
	row, _ := wrap(context.Background(), t, func(tx TxOrDB, query string, args ...any) (*sql.Row, error) {
		return tx.QueryRow(query, args...), nil
	}, query, args...)
	return row
}

// Commit commits underlying transaction and returns err if occurred
func (t *TxOrDBWrapper) Commit() error {
	if t.tx != nil {
		if err := t.tx.Commit(); err != nil {
			if t.errorAdapter != nil {
				err = t.errorAdapter(err)
			}
			return errorj.CommitTransactionError.Wrap(err, "failed to commit transaction")
		}
	}
	return nil
}

// Rollback cancels underlying transaction and logs system err if occurred
func (t *TxOrDBWrapper) Rollback() error {
	if t.tx != nil {
		if err := t.tx.Rollback(); err != nil {
			//TODO: uncomment?
			//if !(t.dbType == "MySQL" && (strings.HasSuffix(err.Error(), mysql.ErrInvalidConn.Error()) || strings.HasSuffix(err.Error(), "bad connection"))) {
			//	err = CheckErr(err)
			//	return errorj.RollbackTransactionError.Wrap(err, "failed to rollback transaction").
			//		WithProperty(errorj.SystemErrorFlag, true)
			//} else {
			if t.errorAdapter != nil {
				err = t.errorAdapter(err)
			}
			return errorj.RollbackTransactionError.Wrap(err, "failed to rollback transaction")
			//}
		}
	}
	return nil
}
