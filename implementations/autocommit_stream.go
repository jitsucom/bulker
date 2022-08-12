package implementations

import (
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

type AutoCommitStream struct {
	AbstractStream
}

func NewAutoCommitStream(id string, p bulker.SQLAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := AutoCommitStream{}
	ps.AbstractStream = NewAbstractStream(id, p, p.GetAutoCommitTx(), tableName, bulker.AutoCommit, streamOptions...)

	//var err error
	//if mode != bulker.NewAutoCommitTx {
	//	ps.tx, err = p.OpenTx()
	//	if err != nil {
	//		return nil, err
	//	}
	//} else {
	//	ps.tx = NewAutoCommitTx()
	//}
	return &ps, nil
}

func (ps *AutoCommitStream) Consume(object types.Object) (err error) {
	defer func() {
		err = ps.PostConsume(err)
	}()
	table, processedObjects, err := ps.Preprocess(object)
	if err != nil {
		return err
	}
	table, err = ps.tableHelper.EnsureTableWithCaching(ps.id, table)
	if err != nil {
		return errorj.Decorate(err, "failed to ensure table")
	}
	return ps.p.Insert(ps.p.GetAutoCommitTx(), table, ps.options.MergeRows, processedObjects)
}

func (ps *AutoCommitStream) Complete() (state bulker.State, err error) {
	ps.state.Status = bulker.Completed
	return ps.state, nil
}

func (ps *AutoCommitStream) Abort() (state bulker.State, err error) {
	ps.state.Status = bulker.Aborted
	return ps.state, nil
}
