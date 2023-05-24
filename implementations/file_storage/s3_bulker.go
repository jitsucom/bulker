package file_storage

import (
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
)

const S3BulkerTypeId = "s3"
const S3AutocommitUnsupported = "Stream mode is not supported for GCS. Please use 'batch' mode"

func init() {
	bulker.RegisterBulker(S3BulkerTypeId, NewS3Bulker)
}

type S3Bulker struct {
	implementations.S3
}

func NewS3Bulker(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	s3Config := &implementations.S3Config{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, s3Config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	s3adapter, err := implementations.NewS3(s3Config)
	if err != nil {
		return nil, err
	}
	return &S3Bulker{*s3adapter}, nil
}

func (s3 *S3Bulker) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	switch mode {
	case bulker.Stream:
		return nil, errors.New(S3AutocommitUnsupported)
	case bulker.Batch:
		return NewTransactionalStream(id, s3, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return NewReplaceTableStream(id, s3, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return NewReplacePartitionStream(id, s3, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}
