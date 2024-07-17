package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"io"
)

// TxWrapper is sql transaction wrapper. Used for handling and log errors with db type (postgres, mySQL, redshift or snowflake)
// on Commit() and Rollback() calls
type TxWrapper struct {
	dbType       string
	db           DB
	tx           *sql.Tx
	custom       io.Closer
	queryLogger  *logging.QueryLogger
	errorAdapter ErrorAdapter
	closeDb      bool
	error        error
}

type TxOrDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

type DB interface {
	TxOrDB
	io.Closer
}

func NewTxWrapper(dbType string, tx *sql.Tx, queryLogger *logging.QueryLogger, errorAdapter ErrorAdapter) *TxWrapper {
	return &TxWrapper{dbType: dbType, tx: tx, queryLogger: queryLogger, errorAdapter: errorAdapter}
}

func NewDbWrapper(dbType string, db DB, queryLogger *logging.QueryLogger, errorAdapter ErrorAdapter, closeDb bool) *TxWrapper {
	return &TxWrapper{dbType: dbType, db: db, queryLogger: queryLogger, errorAdapter: errorAdapter, closeDb: closeDb}
}

func NewDummyTxWrapper(dbType string) *TxWrapper {
	return &TxWrapper{dbType: dbType}
}

func NewCustomWrapper(dbType string, custom io.Closer, err error) *TxWrapper {
	return &TxWrapper{dbType: dbType, custom: custom, error: err, closeDb: true}
}

func wrap[R any](ctx context.Context,
	t *TxWrapper, queryFunction func(tx TxOrDB, query string, args ...any) (R, error),
	query string, args ...any,
) (res R, err error) {
	tx := t.tx
	if tx == nil {
		if t.db == nil {
			err = fmt.Errorf("database connection is not initialized. Run Ping method to attempt reinit connection")
			return
		}
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
func (t *TxWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return wrap(ctx, t, func(tx TxOrDB, query string, args ...any) (sql.Result, error) {
		return tx.ExecContext(ctx, query, args...)
	}, query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
func (t *TxWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return wrap(ctx, t, func(tx TxOrDB, query string, args ...any) (*sql.Rows, error) {
		return tx.QueryContext(ctx, query, args...)
	}, query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
func (t *TxWrapper) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row, _ := wrap(ctx, t, func(tx TxOrDB, query string, args ...any) (*sql.Row, error) {
		return tx.QueryRowContext(ctx, query, args...), nil
	}, query, args...)
	return row
}

func (t *TxWrapper) GetCustom() (io.Closer, error) {
	return t.custom, t.error
}

// PrepareContext creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and will be closed
// when the transaction has been committed or rolled back.
//
// To use an existing prepared statement on this transaction, see Tx.Stmt.
//
// The provided context will be used for the preparation of the context, not
// for the execution of the returned statement. The returned statement
// will run in the transaction context.
func (t *TxWrapper) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return wrap(ctx, t, func(tx TxOrDB, query string, args ...any) (*sql.Stmt, error) {
		return tx.PrepareContext(ctx, query)
	}, query)
}

// Commit commits underlying transaction and returns err if occurred
func (t *TxWrapper) Commit() error {
	if t.closeDb {
		defer func() {
			if err := t.db.Close(); err != nil {
				logging.Errorf("failed to close db: %v", err)
			}
		}()
	}
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
func (t *TxWrapper) Rollback() error {
	if t.closeDb {
		if t.db != nil {
			defer func() {
				if err := t.db.Close(); err != nil {
					logging.Errorf("failed to close db: %v", err)
				}
			}()
		}
		if t.custom != nil {
			defer func() {
				if err := t.custom.Close(); err != nil {
					logging.Errorf("failed to close db: %v", err)
				}
			}()
		}
	}
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

type ConWithDB struct {
	db *sql.DB
	//con *sql.Conn
}

func NewConWithDB(db *sql.DB) *ConWithDB {
	return &ConWithDB{db: db}
}

func (c *ConWithDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

func (c *ConWithDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

func (c *ConWithDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

func (c *ConWithDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.db.PrepareContext(ctx, query)
}

func (c *ConWithDB) Close() error {
	//_ = c.con.Close()
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
