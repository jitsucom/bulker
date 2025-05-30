package utils

func TruncateBytes(data []byte, maxSize int) []byte {
	if len(data) <= maxSize {
		return data
	}
	return data[:maxSize]
}
