package utils

type Set[K comparable] map[K]struct{}

func NewSet[K comparable](values ...K) Set[K] {
	s := make(Set[K])
	s.PutAll(values)
	return s
}

func (s Set[K]) Contains(key K) bool {
	_, ok := s[key]
	return ok
}

func (s Set[K]) Put(key K) {
	s[key] = struct{}{}
}

func (s Set[K]) PutAll(keys []K) {
	for _, key := range keys {
		s.Put(key)
	}
}

func (s Set[K]) PutSet(keys Set[K]) {
	for key, _ := range keys {
		s.Put(key)
	}
}

func (s Set[K]) Remove(key K) {
	delete(s, key)
}

func (s Set[K]) Clear() {
	for key, _ := range s {
		delete(s, key)
	}
}

func (s Set[K]) Clone() Set[K] {
	newSet := make(Set[K])
	for k := range s {
		newSet.Put(k)
	}
	return newSet
}

func (s Set[K]) ToSlice() []K {
	slice := make([]K, 0, len(s))
	for k := range s {
		slice = append(slice, k)
	}
	return slice
}

func (s Set[K]) Equals(other Set[K]) bool {
	if len(s) != len(other) {
		return false
	}

	for k := range s {
		if !other.Contains(k) {
			return false
		}
	}
	return true
}
