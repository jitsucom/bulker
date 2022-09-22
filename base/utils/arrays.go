package utils

func ArrayContains[T comparable](arr []T, value T) bool {
	for _, a := range arr {
		if a == value {
			return true
		}
	}
	return false
}

func ArrayExcluding[T comparable](arr []T, valueToExclude T) []T {
	res := make([]T, 0, len(arr))
	for _, a := range arr {
		if a != valueToExclude {
			res = append(res, a)
		}
	}
	return res
}

func ArrayIntersection[T comparable](firstArray []T, secondArray []T) []T {
	res := make([]T, 0, len(firstArray))
	if len(secondArray) < len(firstArray) {
		firstArray, secondArray = secondArray, firstArray
	}
	for _, a := range firstArray {
		if ArrayContains(secondArray, a) {
			res = append(res, a)
		}
	}
	return res
}
