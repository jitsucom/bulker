package file_storage

import (
	"fmt"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
)

const FilenameDate = "2006_01_02T15_04_05"

type TransactionalStream struct {
	AbstractFileStorageStream
}

func NewTransactionalStream(id string, p implementations.FileAdapter, tableName string, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	ps := TransactionalStream{}
	var err error
	streamStartDate := timestamp.Now()
	filenameFunc := func() string {
		return fmt.Sprintf("%s_%s", tableName, streamStartDate.Format(FilenameDate))
	}
	ps.AbstractFileStorageStream, err = newAbstractFileStorageStream(id, p, filenameFunc, bulker.Batch, streamOptions...)
	if err != nil {
		return nil, err
	}
	return &ps, nil
}
