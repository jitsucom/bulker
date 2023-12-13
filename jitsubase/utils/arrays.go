package utils

func ArrayContains[T comparable](arr []T, value T) bool {
	for _, a := range arr {
		if a == value {
			return true
		}
	}
	return false
}

func ArrayExcluding[T comparable](arr []T, valueToExclude ...T) []T {
	res := make([]T, 0, len(arr))
	for _, a := range arr {
		exclude := false
		for _, v := range valueToExclude {
			if a == v {
				exclude = true
				break
			}
		}
		if !exclude {
			res = append(res, a)
		}
	}
	return res
}

func ArrayIntersection[T comparable](firstArray []T, secondArray []T) []T {
	if len(secondArray) < len(firstArray) {
		firstArray, secondArray = secondArray, firstArray
	}
	res := make([]T, 0, len(firstArray))
	for _, a := range firstArray {
		if ArrayContains(secondArray, a) {
			res = append(res, a)
		}
	}
	return res
}

func ArrayMap[V any, R any](arr []V, mappingFunc func(V) R) []R {
	result := make([]R, len(arr))
	for i, v := range arr {
		result[i] = mappingFunc(v)
	}
	return result
}

func ArrayFilter[V any](arr []V, filterFunc func(V) bool) []V {
	result := make([]V, 0, len(arr))
	for i, v := range arr {
		if filterFunc(v) {
			result = append(result, arr[i])
		}
	}
	return result
}

func ArrayFilterMap[V any, R any](arr []V, filterFunc func(V) bool, mappingFunc func(V) R) []R {
	result := make([]R, 0, len(arr))
	for i, v := range arr {
		if filterFunc(v) {
			result = append(result, mappingFunc(arr[i]))
		}
	}
	return result
}

func ArrayIndexOf[V any](arr []V, filterFunc func(V) bool) int {
	for i, v := range arr {
		if filterFunc(v) {
			return i
		}
	}
	return -1
}

func ArrayContainsF[V any](arr []V, filterFunc func(V) bool) bool {
	return ArrayIndexOf(arr, filterFunc) >= 0
}
