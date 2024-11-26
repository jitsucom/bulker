package driver

type redshiftTx struct {
	onCommit   func() error
	onRollback func() error
}

func (tx *redshiftTx) Commit() error {
	return tx.onCommit()
}

func (tx *redshiftTx) Rollback() error {
	return tx.onRollback()
}
