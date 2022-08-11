package utils

func MapPutAll[K comparable, V interface{}](destination map[K]V, source map[K]V) {
	for k, v := range source {
		destination[k] = v
	}
}
