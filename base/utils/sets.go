package utils

type Set map[string]struct{}

func NewSet(values ...string) Set {
	s := make(Set)
	s.PutAll(values)
	return s
}

func (s Set) Contains(key string) bool {
	_, ok := s[key]
	return ok
}

func (s Set) Put(key string) {
	s[key] = struct{}{}
}

func (s Set) PutAll(keys []string) {
	for _, key := range keys {
		s.Put(key)
	}
}

func (s Set) Remove(key string) {
	delete(s, key)
}

func (s Set) Clone() Set {
	newSet := make(Set)
	for k := range s {
		newSet.Put(k)
	}
	return newSet
}

func (s Set) ToSlice() []string {
	slice := make([]string, 0, len(s))
	for k := range s {
		slice = append(slice, k)
	}
	return slice
}

func (s Set) Equals(other Set) bool {
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
