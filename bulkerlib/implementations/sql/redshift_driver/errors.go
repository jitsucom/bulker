package driver

import "errors"

var (
	ErrNotSupported = errors.New("not supported")
	ErrDSNEmpty     = errors.New("dsn is empty")
	ErrConnClosed   = errors.New("connection closed")
	ErrBeforeCommit = errors.New("transaction is not committed")
	ErrNotInTx      = errors.New("not in transaction")
	ErrInTx         = errors.New("already in transaction")
)
