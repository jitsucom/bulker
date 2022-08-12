package types

import (
	"context"
	"database/sql"
	"github.com/jitsucom/bulker/base/errorj"
)

// Transaction is sql transaction wrapper. Used for handling and log errors with db type (postgres, mySQL, redshift or snowflake)
// on Commit() and Rollback() calls
type Transaction struct {
	dbType     string
	dataSource *sql.DB
	tx         *sql.Tx
}

type TxOrDatasource interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Exec(query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryRow(query string, args ...any) *sql.Row
}

func NewAutoCommitTx(dataSource *sql.DB) *Transaction {
	return &Transaction{dataSource: dataSource}
}

func NewTransaction(dbType string, tx *sql.Tx) *Transaction {
	return &Transaction{dbType: dbType, tx: tx}
}

func wrap[R interface{}](ctx context.Context, t *Transaction, queryFunction func(tx TxOrDatasource) (R, error)) (res R, err error) {
	tx := t.tx
	//var nilValue R
	if tx == nil {
		//AUTOCOMMIT MODE
		//tx, err = t.dataSource.BeginTx(ctx, nil)
		//if err != nil {
		//	return nilValue, err
		//}
		//defer func() {
		//	if err != nil {
		//		_ = tx.Rollback()
		//	} else {
		//		err = tx.Commit()
		//		if err != nil {
		//			res = nilValue
		//		}
		//	}
		//}()
		return queryFunction(t.dataSource)
	}
	return queryFunction(tx)
}

// ExecContext executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
func (t *Transaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return wrap(ctx, t, func(tx TxOrDatasource) (sql.Result, error) {
		return tx.ExecContext(ctx, query, args...)
	})
}

// Exec executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
//
// Exec uses context.Background internally; to specify the context, use
// ExecContext.
func (t *Transaction) Exec(query string, args ...any) (sql.Result, error) {
	return wrap(context.Background(), t, func(tx TxOrDatasource) (sql.Result, error) {
		return tx.Exec(query, args...)
	})
}

// QueryContext executes a query that returns rows, typically a SELECT.
func (t *Transaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return wrap(ctx, t, func(tx TxOrDatasource) (*sql.Rows, error) {
		return tx.QueryContext(ctx, query, args...)
	})
}

// Query executes a query that returns rows, typically a SELECT.
//
// Query uses context.Background internally; to specify the context, use
// QueryContext.
func (t *Transaction) Query(query string, args ...any) (*sql.Rows, error) {
	return wrap(context.Background(), t, func(tx TxOrDatasource) (*sql.Rows, error) {
		return tx.Query(query, args...)
	})
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (t *Transaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row, _ := wrap(ctx, t, func(tx TxOrDatasource) (*sql.Row, error) {
		return tx.QueryRowContext(ctx, query, args...), nil
	})
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
func (t *Transaction) QueryRow(query string, args ...any) *sql.Row {
	row, _ := wrap(context.Background(), t, func(tx TxOrDatasource) (*sql.Row, error) {
		return tx.QueryRow(query, args...), nil
	})
	return row
}

// Commit commits underlying transaction and returns err if occurred
func (t *Transaction) Commit() error {
	if t.tx != nil {
		if err := t.tx.Commit(); err != nil {
			err = CheckErr(err)
			return errorj.CommitTransactionError.Wrap(err, "failed to commit transaction")
		}
	}
	return nil
}

// Rollback cancels underlying transaction and logs system err if occurred
func (t *Transaction) Rollback() error {
	if t.tx != nil {
		if err := t.tx.Rollback(); err != nil {
			//TODO: uncomment?
			//if !(t.dbType == "MySQL" && (strings.HasSuffix(err.Error(), mysql.ErrInvalidConn.Error()) || strings.HasSuffix(err.Error(), "bad connection"))) {
			//	err = CheckErr(err)
			//	return errorj.RollbackTransactionError.Wrap(err, "failed to rollback transaction").
			//		WithProperty(errorj.SystemErrorFlag, true)
			//} else {
			err = CheckErr(err)
			return errorj.RollbackTransactionError.Wrap(err, "failed to rollback transaction")
			//}
		}
	}
	return nil
}
