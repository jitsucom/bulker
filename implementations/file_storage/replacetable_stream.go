package file_storage

import (
	"context"
	"errors"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
)

type ReplaceTableStream struct {
	AbstractFileStorageStream
}

func NewReplaceTableStream(id string, p implementations.FileAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := ReplaceTableStream{}

	var err error
	ps.AbstractFileStorageStream, err = newAbstractFileStorageStream(id, p, func() string {
		return tableName
	}, bulker.ReplaceTable, streamOptions...)
	if err != nil {
		return nil, err
	}
	return &ps, nil
}

func (ps *ReplaceTableStream) Complete(ctx context.Context) (state bulker.State, err error) {
	if ps.state.Status != bulker.Active {
		return ps.state, errors.New("stream is not active")
	}
	defer func() {
		state, err = ps.postComplete(err)
	}()
	if ps.state.LastError == nil {
		//if at least one object was inserted
		if ps.state.SuccessfulRows > 0 {
			if ps.batchFile != nil {
				if err = ps.flushBatchFile(ctx); err != nil {
					return ps.state, err
				}
			}
		} else {
			//for ReplaceTable stream we should replace existing file with empty one
			if err = ps.fileAdapter.UploadBytes(ps.filenameFunc(), []byte{}); err != nil {
				return ps.state, err
			}
		}
		return
	} else {
		//if was any error - it will trigger transaction rollback in defer func
		err = ps.state.LastError
		return
	}
}
