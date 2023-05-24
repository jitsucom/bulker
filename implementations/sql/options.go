package sql

import (
	"fmt"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
)

var (
	ColumnTypesOption = bulker.ImplementationOption[types.SQLTypes]{
		Key:          "columnTypes",
		DefaultValue: types.SQLTypes{},
		AdvancedParseFunc: func(o *bulker.ImplementationOption[types.SQLTypes], serializedValue any) (bulker.StreamOption, error) {
			switch v := serializedValue.(type) {
			case map[string]any:
				sqlTypes := types.SQLTypes{}
				for key, value := range v {
					switch t := value.(type) {
					case string:
						sqlTypes.With(key, t)
					case []string:
						if len(t) == 1 {
							sqlTypes.With(key, t[0])
						} else if len(t) == 2 {
							sqlTypes.WithDDL(key, t[0], t[1])
						} else {
							return nil, fmt.Errorf("failed to parse 'columnTypes' option: %v incorrect number of elements. expected 1 or 2", v)
						}
					}
				}
				return withColumnTypes(o, sqlTypes), nil
			default:
				return nil, fmt.Errorf("failed to parse 'columnTypes' option: %v incorrect type: %T expected map[string]any", v, v)
			}
		},
	}

	localBatchFileOption = bulker.ImplementationOption[string]{Key: "BULKER_OPTION_LOCAL_BATCH_FILE"}

	s3BatchFileOption = bulker.ImplementationOption[*S3OptionConfig]{Key: "BULKER_OPTION_S3_BATCH_FILE"}
)

func init() {
	bulker.RegisterOption(&ColumnTypesOption)
}

type S3OptionConfig struct {
	AccessKeyID string `mapstructure:"accessKeyId,omitempty" json:"accessKeyId,omitempty" yaml:"accessKeyId,omitempty"`
	SecretKey   string `mapstructure:"secretAccessKey,omitempty" json:"secretAccessKey,omitempty" yaml:"secretAccessKey,omitempty"`
	Bucket      string `mapstructure:"bucket,omitempty" json:"bucket,omitempty" yaml:"bucket,omitempty"`
	Region      string `mapstructure:"region,omitempty" json:"region,omitempty" yaml:"region,omitempty"`
	Folder      string `mapstructure:"folder,omitempty" json:"folder,omitempty" yaml:"folder,omitempty"`
}

//func withBatchSize(o *bulker.ImplementationOption[int], batchSize int) bulker.StreamOption {
//	return func(options *bulker.StreamOptions) {
//		o.Set(options, batchSize)
//	}
//}
//
//// WithBatchSize setting just for Kafka consumer for batch mode stream
//// consumer will read messages from kafka topic and send them to destinations in batches of batchSize
//func WithBatchSize(batchSize int) bulker.StreamOption {
//	return withBatchSize(&BatchSizeOption, batchSize)
//}

func withColumnTypes(o *bulker.ImplementationOption[types.SQLTypes], fields types.SQLTypes) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		sqlTypes := o.Get(options)
		if len(sqlTypes) == 0 {
			o.Set(options, fields)
		} else {
			utils.MapPutAll(sqlTypes, fields)
		}
	}
}

// WithColumnTypes provides overrides for column types of current BulkerStream object fields
func WithColumnTypes(fields types.SQLTypes) bulker.StreamOption {
	return withColumnTypes(&ColumnTypesOption, fields)
}

// WithColumnType provides overrides for column type of single column for current BulkerStream object fields
func WithColumnType(columnName, sqlType string) bulker.StreamOption {
	return withColumnTypes(&ColumnTypesOption, types.SQLTypes{}.With(columnName, sqlType))
}

// WithColumnTypeDDL provides overrides for column type and DDL type of single column for current BulkerStream object fields
func WithColumnTypeDDL(columnName, sqlType, ddlType string) bulker.StreamOption {
	return withColumnTypes(&ColumnTypesOption, types.SQLTypes{}.WithDDL(columnName, sqlType, ddlType))
}

// WithLocalBatchFile setting for all modes except bulker.Stream
// Not every database solution supports this option
// fileName - name of tmp file that will be used to collection event batches before sending them to destination
func withLocalBatchFile(fileName string) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		localBatchFileOption.Set(options, fileName)
	}
}

func withS3BatchFile(s3OptionConfig *S3OptionConfig) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		s3BatchFileOption.Set(options, s3OptionConfig)
	}
}
