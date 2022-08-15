package sql

import (
	"github.com/jitsucom/bulker/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFieldMerge(t *testing.T) {
	tests := []struct {
		name     string
		current  Fields
		input    Fields
		expected Fields
	}{
		{
			"Empty current and input",
			Fields{},
			Fields{},
			Fields{},
		},
		{
			"Empty input",
			Fields{"col1": NewField(types.STRING)},
			Fields{},
			Fields{"col1": NewField(types.STRING)},
		},
		{
			"Merged ok",
			Fields{"col1": NewField(types.STRING), "col2": NewField(types.FLOAT64), "col3": NewField(types.TIMESTAMP), "col4": NewField(types.INT64)},
			Fields{"col1": NewField(types.INT64), "col2": NewField(types.STRING), "col3": NewField(types.TIMESTAMP), "col5": NewField(types.TIMESTAMP)},
			Fields{
				"col1": Field{
					dataType:       nil,
					typeOccurrence: map[types.DataType]bool{types.STRING: true, types.INT64: true},
				},
				"col2": Field{
					dataType:       nil,
					typeOccurrence: map[types.DataType]bool{types.STRING: true, types.FLOAT64: true},
				},
				"col3": NewField(types.TIMESTAMP),
				"col4": NewField(types.INT64),
				"col5": NewField(types.TIMESTAMP),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.current.Merge(tt.input)
			require.Equal(t, tt.expected, tt.current, "Fields aren't equal")
		})
	}
}

func TestColumnGetType(t *testing.T) {
	ts := types.TIMESTAMP
	tests := []struct {
		name     string
		input    Field
		expected types.DataType
	}{
		{
			"int64+float64=float64",
			Field{
				dataType:       nil,
				typeOccurrence: map[types.DataType]bool{types.INT64: true, types.FLOAT64: true},
			},
			types.FLOAT64,
		},
		{
			"int64+string=string",
			Field{
				dataType:       nil,
				typeOccurrence: map[types.DataType]bool{types.INT64: true, types.STRING: true},
			},
			types.STRING,
		},
		{
			"int64+timestamp=string",
			Field{
				dataType:       nil,
				typeOccurrence: map[types.DataType]bool{types.INT64: true, types.TIMESTAMP: true},
			},
			types.STRING,
		},
		{
			"int64+timestamp+float64=string",
			Field{
				dataType:       nil,
				typeOccurrence: map[types.DataType]bool{types.INT64: true, types.TIMESTAMP: true, types.FLOAT64: true},
			},
			types.STRING,
		},
		{
			"timestamp=timestamp",
			Field{
				dataType:       nil,
				typeOccurrence: map[types.DataType]bool{types.TIMESTAMP: true},
			},
			types.TIMESTAMP,
		},
		{
			"non-existent case: dataType:timestamp, map[int64] = timestamp",
			Field{
				dataType:       &ts,
				typeOccurrence: map[types.DataType]bool{types.INT64: true},
			},
			types.TIMESTAMP,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.input.GetType()
			require.Equal(t, tt.expected, actual, "Types aren't equal")
		})
	}
}

func TestOverrideTypes(t *testing.T) {
	i := types.INT64
	s := types.STRING
	ts := types.TIMESTAMP
	tests := []struct {
		name       string
		initial    Fields
		toOverride Fields
		expected   Fields
	}{
		{
			"override ok",
			Fields{
				"field1": Field{
					dataType:       &s,
					typeOccurrence: map[types.DataType]bool{types.STRING: true},
				},
				"field2": Field{
					dataType:       &i,
					typeOccurrence: map[types.DataType]bool{types.INT64: true},
				},
			},
			Fields{
				"field2": Field{
					dataType:       &s,
					typeOccurrence: map[types.DataType]bool{types.STRING: true},
				},
				"field3": Field{
					dataType:       &ts,
					typeOccurrence: map[types.DataType]bool{types.TIMESTAMP: true},
				},
			},
			Fields{
				"field1": Field{
					dataType:       &s,
					typeOccurrence: map[types.DataType]bool{types.STRING: true},
				},
				"field2": Field{
					dataType:       &s,
					typeOccurrence: map[types.DataType]bool{types.STRING: true},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.OverrideTypes(tt.toOverride)

			require.Equal(t, tt.expected, tt.initial, "Field types aren't equal")
		})
	}
}
