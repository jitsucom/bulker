package types

import (
	"cmp"
	"sort"
	"strconv"

	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/utils"
)

type Set[K cmp.Ordered] map[K]struct{}

func NewSet[K cmp.Ordered](values ...K) Set[K] {
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

func (s Set[K]) PutAllOrderedKeys(m *jsonorder.OrderedMap[K, any]) {
	for el := m.Front(); el != nil; el = el.Next() {
		s.Put(el.Key)
	}
}

func (s Set[K]) PutAllKeys(m map[K]any) {
	for key := range m {
		s.Put(key)
	}
}

func (s Set[K]) PutSet(keys Set[K]) {
	for key := range keys {
		s.Put(key)
	}
}

func (s Set[K]) Remove(key K) {
	delete(s, key)
}

func (s Set[K]) Clear() {
	for key := range s {
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

func (s Set[K]) Size() int {
	return len(s)
}

func (s Set[K]) ToSlice() []K {
	if len(s) == 0 {
		return []K{}
	}
	slice := make([]K, 0, len(s))
	for k := range s {
		slice = append(slice, k)
	}
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] < slice[j]
	})
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

func (s Set[K]) Hash() string {
	h, _ := utils.HashAny(s)
	return strconv.FormatUint(h, 10)
}
