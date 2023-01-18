package types

import (
	"fmt"
	"github.com/jitsucom/bulker/base/timestamp"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"strings"
	"time"
)

// Typecast tree
//
//	 STRING(4)
//	/      \
//
// FLOAT64(3)  TIMESTAMP(5)
//
//	  |
//	INT64(2)
//	  |
//	BOOL(1)
var (
	typecastTree = &typeNode{
		t: STRING,
		left: &typeNode{
			t: FLOAT64,
			left: &typeNode{
				t:    INT64,
				left: &typeNode{t: BOOL},
			},
		},
		right: &typeNode{t: TIMESTAMP},
	}

	DefaultTypes = map[string]DataType{
		timestamp.Key:               TIMESTAMP,
		"eventn_ctx_utc_time":       TIMESTAMP,
		"eventn_ctx_interval_start": TIMESTAMP,
		"eventn_ctx_interval_end":   TIMESTAMP,
		"utc_time":                  TIMESTAMP,
		"interval_start":            TIMESTAMP,
		"interval_end":              TIMESTAMP,
	}
	convertRules = map[rule]ConvertFunc{
		rule{from: BOOL, to: STRING}:      boolToString,
		rule{from: INT64, to: STRING}:     numberToString,
		rule{from: FLOAT64, to: STRING}:   numberToString,
		rule{from: TIMESTAMP, to: STRING}: timestampToString,
		rule{from: JSON, to: STRING}:      jsonToString,
		rule{from: UNKNOWN, to: STRING}:   anyToString,

		rule{from: BOOL, to: INT64}:    boolToNumber,
		rule{from: FLOAT64, to: INT64}: floatToNumber,

		rule{from: BOOL, to: FLOAT64}:  boolToFloat,
		rule{from: INT64, to: FLOAT64}: numberToFloat,

		rule{from: STRING, to: BOOL}:  anyToBoolean,
		rule{from: INT64, to: BOOL}:   anyToBoolean,
		rule{from: FLOAT64, to: BOOL}: anyToBoolean,

		rule{from: STRING, to: TIMESTAMP}: stringToTimestamp,

		// Future
		/*rule{from: STRING, to: INT64}:     stringToInt,
		  rule{from: STRING, to: FLOAT64}:   stringToFloat,
		  rule{from: FLOAT64, to: INT64}: floatToInt,*/
	}

	charsInNumberStringReplacer = strings.NewReplacer(",", "", " ", "")
)

type typeNode struct {
	t     DataType
	left  *typeNode
	right *typeNode
}

// ConvertFunc is a function for a certain DataType conversion
type ConvertFunc func(v any) (any, error)

type rule struct {
	from DataType
	to   DataType
}

// IsConvertible returns false if there isn't any rule for converting from DataType into to DataType
func IsConvertible(from DataType, to DataType) bool {
	if from == to {
		return true
	}

	if _, ok := convertRules[rule{from: from, to: to}]; ok {
		return true
	}

	return false
}

// Convert returns converted into toType value
// or error if occurred
func Convert(toType DataType, v any) (any, error) {
	currentType, err := TypeFromValue(v)
	if err != nil {
		return nil, err
	}

	if currentType == toType {
		return v, nil
	}

	f, ok := convertRules[rule{from: currentType, to: toType}]
	if !ok {
		return nil, fmt.Errorf("No rule for converting %s to %s", currentType.String(), toType.String())
	}

	return f(v)
}

// GetCommonAncestorType returns lowest common ancestor type
func GetCommonAncestorType(t1, t2 DataType) DataType {
	return lowestCommonAncestor(typecastTree, t1, t2)
}

func lowestCommonAncestor(root *typeNode, t1, t2 DataType) DataType {
	if t1 == UNKNOWN || t2 == UNKNOWN {
		return UNKNOWN
	}
	if t1 == STRING || t2 == STRING {
		return STRING
	}
	// Start from the root node of the tree
	node := root

	// Traverse the tree
	for node != nil {
		if t1 > node.t && t2 > node.t {
			// If both t1 and t2 are greater than parent
			node = node.right
		} else if t1 < node.t && t2 < node.t {
			// If both t1 and t2 are lesser than parent
			node = node.left
		} else {
			// We have found the split point, i.e. the LCA node.
			return node.t
		}
	}

	return UNKNOWN
}

// assume that input v can't be nil
func numberToString(v any) (any, error) {
	switch v := v.(type) {
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int:
		return strconv.Itoa(v), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case string:
		return v, nil
	default:
		return nil, fmt.Errorf("Error numberToString(): Unknown value type: %t", v)
	}
}

func anyToBoolean(v any) (any, error) {
	switch v := v.(type) {
	case string:
		return strconv.ParseBool(v)
	case bool:
		return v, nil
	default:
		s := fmt.Sprint(v)
		if s != "1" && s != "0" {
			return nil, fmt.Errorf("Error anyToBoolean(): Value of type %T can't be converted to boolean: %v", v, v)
		}
		return s == "1", nil
	}
}

// assume that input v can't be nil
func timestampToString(v any) (any, error) {
	switch t := v.(type) {
	case time.Time:
		return t.Format(timestamp.Layout), nil
	case string:
		return t, nil
	default:
		return nil, fmt.Errorf("Error timestampToString(): Unknown value type: %t", v)
	}
}

func jsonToString(v any) (any, error) {
	switch t := v.(type) {
	case map[string]any, []any:
		b, err := jsoniter.Marshal(t)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case string:
		return t, nil
	default:
		return nil, fmt.Errorf("Error jsonToString(): Unknown value type: %t", v)
	}
}

func anyToString(v any) (any, error) {
	switch t := v.(type) {
	case map[string]any, []any:
		b, err := jsoniter.Marshal(t)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	default:
		return fmt.Sprint(t), nil
	}
}

func numberToFloat(v any) (any, error) {
	switch v := v.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return nil, fmt.Errorf("Value: %v with type: %t isn't int", v, v)
	}
}

func boolToString(v any) (any, error) {
	switch b := v.(type) {
	case bool:
		return strconv.FormatBool(b), nil
	default:
		return nil, fmt.Errorf("Error boolToString(): Unknown value type: %t", v)
	}
}

func boolToNumber(v any) (any, error) {
	switch b := v.(type) {
	case bool:
		if b {
			return int64(1), nil
		}
		return int64(0), nil
	default:
		return nil, fmt.Errorf("Error boolToNumber(): Unknown value type: %t", v)
	}
}

// floatToNumber converts only integral values to int64
func floatToNumber(v any) (any, error) {
	switch f := v.(type) {
	case float64:
		if f == float64(int64(f)) {
			return int64(f), nil
		} else {
			return nil, fmt.Errorf("Error floatToNumber(): cannot convert non integral value: %v", v)
		}
	case float32:
		if f == float32(int64(f)) {
			return int64(f), nil
		} else {
			return nil, fmt.Errorf("Error floatToNumber(): cannot convert non integral value: %v", v)
		}
	default:
		return nil, fmt.Errorf("Error floatToNumber(): Unknown value type: %t", v)
	}
}

func boolToFloat(v any) (any, error) {
	switch b := v.(type) {
	case bool:
		if b {
			return float64(1), nil
		}

		return float64(0), nil
	default:
		return nil, fmt.Errorf("Error boolToFloat(): Unknown value type: %t", v)
	}
}

// StringToInt returns int representation of input string
// or error if unconvertable
func StringToInt(v any) (any, error) {
	intValue, err := strconv.Atoi(v.(string))
	if err != nil {
		return nil, fmt.Errorf("Error stringToInt() for value: %v: %v", v, err)
	}

	return int64(intValue), nil
}

// StringToFloat return float64 value from string
// or error if unconvertable
func StringToFloat(v any) (any, error) {
	floatValue, err := strconv.ParseFloat(v.(string), 64)
	if err != nil {
		return nil, fmt.Errorf("Error stringToFloat() for value: %v: %v", v, err)
	}

	return floatValue, nil
}

func stringToTimestamp(v any) (any, error) {
	t, err := time.Parse(time.RFC3339Nano, v.(string))
	if err != nil {
		return nil, fmt.Errorf("Error stringToTimestamp() for value: %v: %v", v, err)
	}

	return t, nil
}

// StringWithCommasToFloat return float64 value from string (1,200.50)
func StringWithCommasToFloat(v any) (any, error) {
	return StringToFloat(charsInNumberStringReplacer.Replace(v.(string)))
}
