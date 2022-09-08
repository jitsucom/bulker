package utils

func MapPutAll[K comparable, V any](destination map[K]V, source map[K]V) map[K]V {
	for k, v := range source {
		destination[k] = v
	}
	return destination
}

func MapHasSameKeys[K comparable, V any, J any](a map[K]V, b map[K]J) bool {
	if len(a) != len(b) {
		return false
	}

	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}

	return true
}
