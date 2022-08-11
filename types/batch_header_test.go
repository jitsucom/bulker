package types

import (
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
			Fields{"col1": NewField(STRING)},
			Fields{},
			Fields{"col1": NewField(STRING)},
		},
		{
			"Merged ok",
			Fields{"col1": NewField(STRING), "col2": NewField(FLOAT64), "col3": NewField(TIMESTAMP), "col4": NewField(INT64)},
			Fields{"col1": NewField(INT64), "col2": NewField(STRING), "col3": NewField(TIMESTAMP), "col5": NewField(TIMESTAMP)},
			Fields{
				"col1": Field{
					dataType:       nil,
					typeOccurrence: map[DataType]bool{STRING: true, INT64: true},
				},
				"col2": Field{
					dataType:       nil,
					typeOccurrence: map[DataType]bool{STRING: true, FLOAT64: true},
				},
				"col3": NewField(TIMESTAMP),
				"col4": NewField(INT64),
				"col5": NewField(TIMESTAMP),
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
	ts := TIMESTAMP
	tests := []struct {
		name     string
		input    Field
		expected DataType
	}{
		{
			"int64+float64=float64",
			Field{
				dataType:       nil,
				typeOccurrence: map[DataType]bool{INT64: true, FLOAT64: true},
			},
			FLOAT64,
		},
		{
			"int64+string=string",
			Field{
				dataType:       nil,
				typeOccurrence: map[DataType]bool{INT64: true, STRING: true},
			},
			STRING,
		},
		{
			"int64+timestamp=string",
			Field{
				dataType:       nil,
				typeOccurrence: map[DataType]bool{INT64: true, TIMESTAMP: true},
			},
			STRING,
		},
		{
			"int64+timestamp+float64=string",
			Field{
				dataType:       nil,
				typeOccurrence: map[DataType]bool{INT64: true, TIMESTAMP: true, FLOAT64: true},
			},
			STRING,
		},
		{
			"timestamp=timestamp",
			Field{
				dataType:       nil,
				typeOccurrence: map[DataType]bool{TIMESTAMP: true},
			},
			TIMESTAMP,
		},
		{
			"non-existent case: dataType:timestamp, map[int64] = timestamp",
			Field{
				dataType:       &ts,
				typeOccurrence: map[DataType]bool{INT64: true},
			},
			TIMESTAMP,
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
	i := INT64
	s := STRING
	ts := TIMESTAMP
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
					typeOccurrence: map[DataType]bool{STRING: true},
				},
				"field2": Field{
					dataType:       &i,
					typeOccurrence: map[DataType]bool{INT64: true},
				},
			},
			Fields{
				"field2": Field{
					dataType:       &s,
					typeOccurrence: map[DataType]bool{STRING: true},
				},
				"field3": Field{
					dataType:       &ts,
					typeOccurrence: map[DataType]bool{TIMESTAMP: true},
				},
			},
			Fields{
				"field1": Field{
					dataType:       &s,
					typeOccurrence: map[DataType]bool{STRING: true},
				},
				"field2": Field{
					dataType:       &s,
					typeOccurrence: map[DataType]bool{STRING: true},
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
