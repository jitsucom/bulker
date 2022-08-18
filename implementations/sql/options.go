package sql

import (
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
)

var (
	primaryKeyOption  = bulker.ImplementationOption[utils.Set]{Key: "BULKER_OPTION_PK", DefaultValue: utils.Set{}}
	mergeRowsOption   = bulker.ImplementationOption[bool]{Key: "BULKER_OPTION_MERGE_ROWS"}
	columnTypesOption = bulker.ImplementationOption[SQLTypes]{Key: "BULKER_OPTION_COLUMN_TYPES", DefaultValue: SQLTypes{}}
	partitionIdOption = bulker.ImplementationOption[string]{Key: "BULKER_OPTION_PARTITION_ID"}
)

func WithPrimaryKey(pkField string) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		set := primaryKeyOption.Get(options)
		if len(set) == 0 {
			primaryKeyOption.Set(options, utils.NewSet(pkField))
		} else {
			set.Put(pkField)
		}
	}
}

// WithMergeRows - when true merge rows on primary keys collision.
func WithMergeRows() bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		mergeRowsOption.Set(options, true)
	}
}

func WithoutMergeRows() bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		mergeRowsOption.Set(options, false)
	}
}

// WithPartition settings for ReplacePartition mode only
// partitionId - value of `__partition_id`  for current BulkerStream e.g. id of current partition
func WithPartition(partitionId string) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		partitionIdOption.Set(options, partitionId)
	}
}

// WithColumnTypes provides overrides for column types of current BulkerStream object fields
func WithColumnTypes(fields SQLTypes) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		sqlTypes := columnTypesOption.Get(options)
		if len(sqlTypes) == 0 {
			columnTypesOption.Set(options, fields)
		} else {
			utils.MapPutAll(sqlTypes, fields)
		}
	}
}

// WithColumnType provides overrides for column type of single column for current BulkerStream object fields
func WithColumnType(columnName, sqlType string) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		sqlTypes := columnTypesOption.Get(options)
		if len(sqlTypes) == 0 {
			columnTypesOption.Set(options, SQLTypes{}.With(columnName, sqlType))
		} else {
			utils.MapPutAll(sqlTypes, SQLTypes{}.With(columnName, sqlType))
		}
	}
}

// WithColumnTypeDDL provides overrides for column type and DDL type of single column for current BulkerStream object fields
func WithColumnTypeDDL(columnName, sqlType, ddlType string) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		sqlTypes := columnTypesOption.Get(options)
		if len(sqlTypes) == 0 {
			columnTypesOption.Set(options, SQLTypes{}.WithDDL(columnName, sqlType, ddlType))
		} else {
			utils.MapPutAll(sqlTypes, SQLTypes{}.WithDDL(columnName, sqlType, ddlType))
		}
	}
}
