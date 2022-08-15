package bulker

import (
	"github.com/jitsucom/bulker/base/utils"
)

type StreamOption func(*StreamOptions)

func WithPrimaryKey(pkField string) StreamOption {
	return func(options *StreamOptions) {
		if options.PrimaryKeyFields == nil {
			options.PrimaryKeyFields = utils.NewSet(pkField)
		} else {
			options.PrimaryKeyFields.Put(pkField)
		}
	}
}

// WithMergeRows - when true merge rows on primary keys collision.
func WithMergeRows() StreamOption {
	return func(options *StreamOptions) {
		options.MergeRows = true
	}
}

func WithoutMergeRows() StreamOption {
	return func(options *StreamOptions) {
		options.MergeRows = false
	}
}

////WithMultiRowInserts - when true use multi-row inserts for bulk inserts for to achieve better performance in some databases.
//func WithMultiRowInserts() StreamOption {
//	return func(options *StreamOptions) {
//		options.MultiRowInserts = true
//	}
//}
//func WithoutMultiRowInserts() StreamOption {
//	return func(options *StreamOptions) {
//		options.MultiRowInserts = false
//	}
//}

// WithPartition settings for ReplacePartition mode only
// partitionId - value of `__partition_id`  for current BulkerStream e.g. id of current partition
func WithPartition(partitionId string) StreamOption {
	return func(options *StreamOptions) {
		options.PartitionId = partitionId
	}
}

type StreamOptions struct {
	//CustomTypes      sql.Fields
	PrimaryKeyFields utils.Set
	MergeRows        bool
	PartitionId      string
	//MultiRowInserts  bool
}
