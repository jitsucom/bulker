package utils

import (
	"fmt"
	"regexp"
	"strings"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// NvlString returns first not empty string value from varargs
//
// return "" if all strings are empty
func NvlString(args ...string) string {
	for _, str := range args {
		if str != "" {
			return str
		}
	}
	return ""
}

// ShortenString returns the first N slice of a string.
func ShortenString(str string, n int) string {
	if len([]rune(str)) <= n {
		return str
	}
	return string([]rune(str)[:n])
}

// ShortenStringWithEllipsis returns the first N slice of a string and ends with ellipsis.
func ShortenStringWithEllipsis(str string, n int) string {
	if len([]rune(str)) <= n {
		return str
	}
	return string([]rune(str)[:n]) + "..."
}

// IsLetterOrNumber returns true if input symbol is:
//
//	A - Z: 65-90
//	a - z: 97-122
func IsLetterOrNumber(symbol int32) bool {
	return ('a' <= symbol && symbol <= 'z') ||
		('A' <= symbol && symbol <= 'Z') ||
		('0' <= symbol && symbol <= '9')
}

func IsNumber(symbol int32) bool {
	return '0' <= symbol && symbol <= '9'
}

// SanitizeString returns string with only alphanumeric characters and underscores
func SanitizeString(str string) string {
	return nonAlphanumericRegex.ReplaceAllString(str, "_")
}

// JoinNonEmptyStrings joins strings with separator, but ignoring empty strings
func JoinNonEmptyStrings(sep string, elems ...string) string {
	switch len(elems) {
	case 0:
		return ""
	case 1:
		return elems[0]
	}
	n := len(sep) * (len(elems) - 1)
	for i := 0; i < len(elems); i++ {
		n += len(elems[i])
	}

	var b strings.Builder
	b.Grow(n)
	for _, s := range elems {
		if len(s) > 0 {
			if b.Len() > 0 {
				b.WriteString(sep)
			}
			b.WriteString(s)
		}
	}
	return b.String()
}

// ParseString parses value of string - effectively strict type checking.
func ParseString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("ParseString: invalid value type %T", value)
	}
}
