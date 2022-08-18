package utils

func ArrayContains(arr []any, value any) bool {
	for _, a := range arr {
		if a == value {
			return true
		}
	}
	return false
}
