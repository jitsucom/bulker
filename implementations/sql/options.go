package sql

import (
	"fmt"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
)

var (
	PrimaryKeyOption = bulker.ImplementationOption[utils.Set[string]]{
		Key:          "primary_key",
		DefaultValue: utils.Set[string]{},
		ParseFunc: func(o *bulker.ImplementationOption[utils.Set[string]], serializedValue any) (bulker.StreamOption, error) {
			switch v := serializedValue.(type) {
			case []string:
				return withPrimaryKey(o, v...), nil
			case string:
				return withPrimaryKey(o, v), nil
			default:
				return nil, fmt.Errorf("failed to parse 'primary_ke'y' option: %v incorrect type: %T expected string or []string", v, v)
			}
		},
	}
	MergeRowsOption = bulker.ImplementationOption[bool]{
		Key:          "deduplicate",
		DefaultValue: true,
		ParseFunc: func(o *bulker.ImplementationOption[bool], serializedValue any) (bulker.StreamOption, error) {
			b, err := utils.ParseBool(serializedValue)
			if err != nil {
				return nil, fmt.Errorf("failed to parse 'deduplicate' option: %v", err)
			}
			return withMergeRows(o, b), nil
		},
	}
	ColumnTypesOption = bulker.ImplementationOption[SQLTypes]{
		Key:          "column_types",
		DefaultValue: SQLTypes{},
		ParseFunc: func(o *bulker.ImplementationOption[SQLTypes], serializedValue any) (bulker.StreamOption, error) {
			switch v := serializedValue.(type) {
			case map[string]any:
				sqlTypes := SQLTypes{}
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
							return nil, fmt.Errorf("failed to parse 'column_types' option: %v incorrect number of elements. expected 1 or 2", v)
						}
					}
				}
				return withColumnTypes(o, sqlTypes), nil
			default:
				return nil, fmt.Errorf("failed to parse 'column_types' option: %v incorrect type: %T expected map[string]any", v, v)
			}
		},
	}
	PartitionIdOption = bulker.ImplementationOption[string]{
		Key: "partition_id",
		ParseFunc: func(o *bulker.ImplementationOption[string], serializedValue any) (bulker.StreamOption, error) {
			switch v := serializedValue.(type) {
			case string:
				return withPartition(o, v), nil
			default:
				return nil, fmt.Errorf("failed to parse 'partition_id' option: %v incorrect type: %T expected string", v, v)
			}
		},
	}
	//BatchSizeOption = bulker.ImplementationOption[int]{
	//	Key:          "batch_size",
	//	DefaultValue: 10_000,
	//	ParseFunc: func(o *bulker.ImplementationOption[int], serializedValue any) (bulker.StreamOption, error) {
	//		switch v := serializedValue.(type) {
	//		case int:
	//			return withBatchSize(o, v), nil
	//		default:
	//			return nil, fmt.Errorf("failed to parse 'partition_id' option: %v incorrect type: %T expected int", v, v)
	//		}
	//	},
	//}

	localBatchFileOption = bulker.ImplementationOption[string]{Key: "BULKER_OPTION_LOCAL_BATCH_FILE"}

	s3BatchFileOption = bulker.ImplementationOption[*S3OptionConfig]{Key: "BULKER_OPTION_S3_BATCH_FILE"}
)

func init() {
	bulker.RegisterOption(&PrimaryKeyOption)
	bulker.RegisterOption(&MergeRowsOption)
	bulker.RegisterOption(&ColumnTypesOption)
	bulker.RegisterOption(&PartitionIdOption)
	//bulker.RegisterOption(&BatchSizeOption)

}

type S3OptionConfig struct {
	AccessKeyID string `mapstructure:"access_key_id,omitempty" json:"access_key_id,omitempty" yaml:"access_key_id,omitempty"`
	SecretKey   string `mapstructure:"secret_access_key,omitempty" json:"secret_access_key,omitempty" yaml:"secret_access_key,omitempty"`
	Bucket      string `mapstructure:"bucket,omitempty" json:"bucket,omitempty" yaml:"bucket,omitempty"`
	Region      string `mapstructure:"region,omitempty" json:"region,omitempty" yaml:"region,omitempty"`
	Folder      string `mapstructure:"folder,omitempty" json:"folder,omitempty" yaml:"folder,omitempty"`
}

func withPrimaryKey(o *bulker.ImplementationOption[utils.Set[string]], pkFields ...string) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		set := o.Get(options)
		if len(set) == 0 {
			o.Set(options, utils.NewSet(pkFields...))
		} else {
			set.PutAll(pkFields)
		}
	}
}

func WithPrimaryKey(pkFields ...string) bulker.StreamOption {
	return withPrimaryKey(&PrimaryKeyOption, pkFields...)
}

// WithMergeRows - when true merge rows on primary keys collision.
func withMergeRows(o *bulker.ImplementationOption[bool], b bool) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		o.Set(options, b)
	}
}

// WithMergeRows - when true merge rows on primary keys collision.
func WithMergeRows() bulker.StreamOption {
	return withMergeRows(&MergeRowsOption, true)
}

func WithoutMergeRows() bulker.StreamOption {
	return withMergeRows(&MergeRowsOption, false)

}

func withPartition(o *bulker.ImplementationOption[string], partitionId string) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		o.Set(options, partitionId)
	}
}

// WithPartition settings for bulker.ReplacePartition mode only
// partitionId - value of `__partition_id`  for current BulkerStream e.g. id of current partition
// TODO: For bigquery require string in special format
func WithPartition(partitionId string) bulker.StreamOption {
	return withPartition(&PartitionIdOption, partitionId)
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

func withColumnTypes(o *bulker.ImplementationOption[SQLTypes], fields SQLTypes) bulker.StreamOption {
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
func WithColumnTypes(fields SQLTypes) bulker.StreamOption {
	return withColumnTypes(&ColumnTypesOption, fields)
}

// WithColumnType provides overrides for column type of single column for current BulkerStream object fields
func WithColumnType(columnName, sqlType string) bulker.StreamOption {
	return withColumnTypes(&ColumnTypesOption, SQLTypes{}.With(columnName, sqlType))
}

// WithColumnTypeDDL provides overrides for column type and DDL type of single column for current BulkerStream object fields
func WithColumnTypeDDL(columnName, sqlType, ddlType string) bulker.StreamOption {
	return withColumnTypes(&ColumnTypesOption, SQLTypes{}.WithDDL(columnName, sqlType, ddlType))
}

// WithLocalBatchFile setting for all modes except bulker.AutoCommit
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
