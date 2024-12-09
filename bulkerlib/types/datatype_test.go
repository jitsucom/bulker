package types

import (
	"testing"
	"time"
)

func MustParseTime(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

func TestReformatTimeValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Time
	}{
		{
			"Shopify (Airbyte)",
			"2024-07-16T12:07:15+05:30",
			MustParseTime(time.RFC3339Nano, "2024-07-16T06:37:15Z"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := ReformatTimeValue(tt.input, false)
			if !ok {
				t.Fatalf("Failed to parse datetime: %s", tt.input)
			}
			if result.UnixMicro() != tt.expected.UnixMicro() {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}
