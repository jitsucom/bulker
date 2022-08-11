package bulker

import (
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/types"
)

type StreamOption func(*StreamOptions)

//WithCustomTypes provides overrides for types of current BulkerStream object fields
func WithCustomTypes(fields types.Fields) StreamOption {
	return func(options *StreamOptions) {
		options.CustomTypes = fields
	}
}

func WithPrimaryKey(pkField string) StreamOption {
	return func(options *StreamOptions) {
		if options.PrimaryKeyFields == nil {
			options.PrimaryKeyFields = utils.NewSet(pkField)
		} else {
			options.PrimaryKeyFields.Put(pkField)
		}
	}
}

//WithMergeRows - when true merge rows on primary keys collision.
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

//WithMultiRowInserts - when true use multi-row inserts for bulk inserts for to achieve better performance in some databases.
func WithMultiRowInserts() StreamOption {
	return func(options *StreamOptions) {
		options.MultiRowInserts = true
	}
}
func WithoutMultiRowInserts() StreamOption {
	return func(options *StreamOptions) {
		options.MultiRowInserts = false
	}
}

//WithPartition settings for ReplacePartition mode only
//
//partitionProperty - name of object property used as a partition index
//
//partitionValue - value of that property for current BulkerStream e.g. id of current partition
func WithPartition(partitionProperty string, partitionValue interface{}) StreamOption {
	return func(options *StreamOptions) {
		//TODO: WithPartition

		//options.PartitionsSettings = &partitionsSettings{
		//	Name:  partitionProperty,
		//	Value: partitionValue,
		//}
	}
}

type partitionsSettings struct {
	Name  string
	Value interface{}
}

type StreamOptions struct {
	CustomTypes      types.Fields
	PrimaryKeyFields utils.Set
	MergeRows        bool
	MultiRowInserts  bool
}

//TODO: default options depending on destination implementation
var DefaultStreamOptions = StreamOptions{MergeRows: true, MultiRowInserts: false}
