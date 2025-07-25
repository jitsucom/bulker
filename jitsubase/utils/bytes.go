package utils

import (
	"bytes"
)

func TruncateBytes(data []byte, maxSize int) []byte {
	if len(data) <= maxSize {
		return data
	}
	return data[:maxSize]
}

func SubstringBetweenSeparators(data []byte, sep byte, start int) []byte {
	if start < 0 {
		return nil
	}
	var count = 0
	var pos = 0
	for {
		idx := bytes.IndexByte(data[pos:], sep)
		if idx == -1 {
			if start == 0 && count == 0 {
				return data
			} else if count == start {
				return data[pos:]
			}
			return nil
		}
		if start == 0 {
			return data[0:idx]
		}
		if count == start {
			return data[pos : pos+idx]
		}
		pos = pos + idx + 1 // Move past the found separator
		count++
	}
}
