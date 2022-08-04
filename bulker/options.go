package bulker

import "github.com/jitsucom/bulker/types"

type StreamOption func(*streamOptions)

//WithTable provides table schema for current Stream. Overrides table schema derived from object structure
func WithTable(table *types.Table) StreamOption {
	return func(options *streamOptions) {
		options.Table = table
	}
}

//func WithIgnoreErrors() StreamOption {
//	return func(options *streamOptions) {
//		options.IgnoreErrors = true
//	}
//}

//WithPartition settings for WholePartition mode only
//
//partitionProperty - name of object property used as a partition index
//
//partitionValue - value of that property for current Stream e.g. id of current partition
func WithPartition(partitionProperty string, partitionValue interface{}) StreamOption {
	return func(options *streamOptions) {
		options.PartitionsSettings = &partitionsSettings{
			Name:  partitionProperty,
			Value: partitionValue,
		}
	}
}

type partitionsSettings struct {
	Name  string
	Value interface{}
}

type streamOptions struct {
	Table              *types.Table
	PartitionsSettings *partitionsSettings
	IgnoreErrors       bool
}
