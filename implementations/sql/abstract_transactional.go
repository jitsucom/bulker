package sql

import (
	"context"
	"github.com/jitsucom/bulker/bulker"
)

type AbstractTransactionalSQLStream struct {
	AbstractSQLStream
	tx *TxSQLAdapter
}

func newAbstractTransactionalStream(id string, p SQLAdapter, tx TxOrDB, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (AbstractTransactionalSQLStream, error) {
	ps := AbstractTransactionalSQLStream{}
	abs, err := newAbstractStream(id, p, tx, tableName, mode, streamOptions...)
	if err != nil {
		return ps, err
	}
	ps.AbstractSQLStream = abs
	return ps, nil
}

func (ps *AbstractTransactionalSQLStream) init(ctx context.Context) error {
	err := ps.AbstractSQLStream.init(ctx)
	if err != nil {
		return err
	}
	if ps.tx == nil {
		ps.tx, err = ps.sqlAdapter.OpenTx(ctx)
		if err != nil {
			return err
		}
		//set transactional adapter so all table modification will be performed inside transaction
		ps.tableHelper.SetSQLAdapter(ps.tx)
	}
	return nil
}
