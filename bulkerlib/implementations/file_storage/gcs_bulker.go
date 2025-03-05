package file_storage

import (
	"errors"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	implementations2 "github.com/jitsucom/bulker/bulkerlib/implementations"
	"github.com/jitsucom/bulker/jitsubase/utils"
)

const GCSAutocommitUnsupported = "Stream mode is not supported for GCS. Please use 'batch' mode"

func init() {
	bulker.RegisterBulker(implementations2.GCSBulkerTypeId, NewGCSBulker)
}

type GCSConfig struct {
	implementations2.FileConfig `mapstructure:",squash" json:",inline" yaml:",inline"`
	Bucket                      string `mapstructure:"bucket,omitempty" json:"bucket,omitempty" yaml:"bucket,omitempty"`
	AccessKey                   any    `mapstructure:"accessKey,omitempty" json:"accessKey,omitempty" yaml:"accessKey,omitempty"`
}
type GCSBulker struct {
	implementations2.GoogleCloudStorage
}

func (gcs *GCSBulker) Type() string {
	return implementations2.GCSBulkerTypeId
}

func NewGCSBulker(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	gcsConfig := &GCSConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, gcsConfig); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}
	googleConfig := implementations2.GoogleConfig{
		FileConfig: gcsConfig.FileConfig,
		Bucket:     gcsConfig.Bucket,
		KeyFile:    gcsConfig.AccessKey,
	}
	//TODO: auto reconnect the same way as in SQL bulkers
	gcsAdapter, err := implementations2.NewGoogleCloudStorage(&googleConfig)
	if err != nil {
		return nil, err
	}
	return &GCSBulker{*gcsAdapter}, nil
}

func (gcs *GCSBulker) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	switch mode {
	case bulker.Stream:
		return nil, errors.New(GCSAutocommitUnsupported)
	case bulker.Batch:
		return NewTransactionalStream(id, gcs, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return NewReplaceTableStream(id, gcs, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return NewReplacePartitionStream(id, gcs, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}
