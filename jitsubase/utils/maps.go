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
	for key := range mp {
		delete(mp, key)
	}
}

// MapPutIfAbsent puts value into map if key doesn't exist. Returns true if value was put
func MapPutIfAbsent[K comparable, V any](mp map[K]V, key K, value V) bool {
	if _, ok := mp[key]; !ok {
		mp[key] = value
		return true
	}
	return false
}

func MapGetOrCreate[K comparable, V any](mp map[K]V, key K, value V) V {
	var res V
	var ok bool
	if res, ok = mp[key]; !ok {
		mp[key] = value
		return value
	}
	return res
}

func MapNVL[K comparable, V any](mp map[K]V, key K, defaultValue V) V {
	if value, ok := mp[key]; ok {
		return value
	}
	return defaultValue
}

func MapNVLFunc[K comparable, V any](mp map[K]V, key K, defaultFunc func() V) V {
	if value, ok := mp[key]; ok {
		return value
	}
	return defaultFunc()
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
	if mp == nil {
		return nil
	}
	result := make([]R, 0, len(mp))
	for k, v := range mp {
		result = append(result, mappingFunc(k, v))
	}
	return result
}

func MapFilter[K comparable, V any](mp map[K]V, predicate func(K, V) bool) map[K]V {
	if mp == nil {
		return nil
	}
	result := make(map[K]V, len(mp))
	for k, v := range mp {
		if predicate(k, v) {
			result[k] = v
		}
	}
	return result
}

func MapMap[K comparable, V any, R any](mp map[K]V, mappingFunc func(K, V) R) map[K]R {
	if mp == nil {
		return nil
	}
	result := make(map[K]R, len(mp))
	for k, v := range mp {
		result[k] = mappingFunc(k, v)
	}
	return result
}

func Values[V any, K comparable](mp map[K]V) []V {
	if mp == nil {
		return nil
	}
	result := make([]V, 0, len(mp))
	for _, v := range mp {
		result = append(result, v)
	}
	return result
}

func MapKeys[K comparable, V any](mp map[K]V) []K {
	if mp == nil {
		return nil
	}
	result := make([]K, 0, len(mp))
	for k := range mp {
		result = append(result, k)
	}
	return result
}
