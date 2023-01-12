package utils

import (
	"fmt"
	"math"
	"strconv"
)

// ParseInt parses value of string, int, integer float into int.
func ParseInt(value any) (int, error) {
	switch v := value.(type) {
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return int(i), err
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		if v == math.Trunc(v) {
			return int(v), nil
		} else {
			return 0, fmt.Errorf("can't parse float %f as int", v)
		}
	default:
		return 0, fmt.Errorf("ParseInt: invalid value type %T", value)
	}
}

func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
