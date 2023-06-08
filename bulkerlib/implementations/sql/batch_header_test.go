package sql

//
//import (
//	"github.com/jitsucom/bulker/types"
//	"github.com/stretchr/testify/require"
//	"testing"
//)
//
//func TestFieldMerge(t *testing.T) {
//	tests := []struct {
//		name     string
//		current  Fields
//		input    Fields
//		expected Fields
//	}{
//		{
//			"Empty current and input",
//			Fields{},
//			Fields{},
//			Fields{},
//		},
//		{
//			"Empty input",
//			Fields{"col1": NewField(types.STRING)},
//			Fields{},
//			Fields{"col1": NewField(types.STRING)},
//		},
//		{
//			"Merged ok",
//			Fields{"col1": NewField(types.STRING), "col2": NewField(types.FLOAT64), "col3": NewField(types.TIMESTAMP), "col4": NewField(types.INT64)},
//			Fields{"col1": NewField(types.INT64), "col2": NewField(types.STRING), "col3": NewField(types.TIMESTAMP), "col5": NewField(types.TIMESTAMP)},
//			Fields{
//				"col1": Field{
//					dataType: nil,
//				},
//				"col2": Field{
//					dataType: nil,
//				},
//				"col3": NewField(types.TIMESTAMP),
//				"col4": NewField(types.INT64),
//				"col5": NewField(types.TIMESTAMP),
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tt.current.Merge(tt.input)
//			require.Equal(t, tt.expected, tt.current, "Fields aren't equal")
//		})
//	}
//}
//
//func TestColumnGetType(t *testing.T) {
//	ts := types.TIMESTAMP
//	tests := []struct {
//		name     string
//		input    Field
//		expected types.DataType
//	}{
//		{
//			"int64+float64=float64",
//			Field{
//				dataType: nil,
//			},
//			types.FLOAT64,
//		},
//		{
//			"int64+string=string",
//			Field{
//				dataType: nil,
//			},
//			types.STRING,
//		},
//		{
//			"int64+timestamp=string",
//			Field{
//				dataType: nil,
//			},
//			types.STRING,
//		},
//		{
//			"int64+timestamp+float64=string",
//			Field{
//				dataType: nil,
//			},
//			types.STRING,
//		},
//		{
//			"timestamp=timestamp",
//			Field{
//				dataType: nil,
//			},
//			types.TIMESTAMP,
//		},
//		{
//			"non-existent case: dataType:timestamp, map[int64] = timestamp",
//			Field{
//				dataType: &ts,
//			},
//			types.TIMESTAMP,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			actual := tt.input.GetType()
//			require.Equal(t, tt.expected, actual, "Types aren't equal")
//		})
//	}
//}
