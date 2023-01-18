package utils

// MapCopy returns copy of map
func MapCopy[K comparable, V any](mp map[K]V) map[K]V {
	result := make(map[K]V, len(mp))
	for k, v := range mp {
		result[k] = v
	}
	return result
}

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

func MapClear[K comparable, V any](mp map[K]V) {
	for key, _ := range mp {
		delete(mp, key)
	}
}

func MapPutIfAbsent[K comparable, V any](mp map[K]V, key K, value V) {
	if _, ok := mp[key]; !ok {
		mp[key] = value
	}
}

func MapNVL[K comparable, V any](mp map[K]V, key K, defaultValue V) V {
	if value, ok := mp[key]; ok {
		return value
	}
	return defaultValue
}

// MapNVLKeys returns value by first key that exists or empty value of V type if no keys exist
func MapNVLKeys[K comparable, V any](mp map[K]V, keys ...K) V {
	for _, key := range keys {
		if value, ok := mp[key]; ok {
			return value
		}
	}
	var defaultValue V
	return defaultValue
}

func MapToSlice[K comparable, V any, R any](mp map[K]V, mappingFunc func(K, V) R) []R {
	result := make([]R, 0, len(mp))
	for k, v := range mp {
		result = append(result, mappingFunc(k, v))
	}
	return result
}
