package utils

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseBool parses value of string, int or bool into a bool.
func ParseBool(value any) (bool, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseBool(v)
	case int:
		return v != 0, nil
	case bool:
		return v, nil
	default:
		return false, fmt.Errorf("ParseBool: invalid value type %T", value)
	}
}

func IsTruish(value any) bool {
	switch v := value.(type) {
	case string:
		return strings.ToLower(v) == "true" || v == "1"
	case int:
		return v != 0
	case bool:
		return v
	default:
		return false
	}
}

func BoolPointer(b bool) *bool {
	return &b
}
