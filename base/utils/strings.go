package utils

import "regexp"

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
