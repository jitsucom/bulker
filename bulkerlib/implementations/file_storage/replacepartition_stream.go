package file_storage

import (
	"context"
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/implementations"
)

type ReplacePartitionStream struct {
	AbstractFileStorageStream
}

func NewReplacePartitionStream(id string, p implementations.FileAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := ReplacePartitionStream{}
	so := bulker.StreamOptions{}
	for _, opt := range streamOptions {
		so.Add(opt)
	}
	partitionId := bulker.PartitionIdOption.Get(&so)
	if partitionId == "" {
		return nil, errors.New("WithPartition is required option for ReplacePartitionStream")
	}
	var err error
	filenameFunc := func(ctx context.Context) string {
		return fmt.Sprintf("%s/%s", tableName, partitionId)
	}
	ps.AbstractFileStorageStream, err = newAbstractFileStorageStream(id, p, filenameFunc, bulker.ReplacePartition, streamOptions...)
	if err != nil {
		return nil, err
	}
	return &ps, nil
}

func (ps *ReplacePartitionStream) Complete(ctx context.Context) (state bulker.State, err error) {
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
			//for ReplacePartitionStream  we should replace existing file with empty one
			if err = ps.fileAdapter.UploadBytes(ps.filename(ctx), []byte{}); err != nil {
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
