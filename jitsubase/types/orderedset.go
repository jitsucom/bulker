package types

import (
	"cmp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"strconv"
)

type OrderedSet[K cmp.Ordered] struct {
	mp *OrderedMap[K, struct{}]
}

func NewOrderedSet[K cmp.Ordered](values ...K) OrderedSet[K] {
	s := NewOrderedMap[K, struct{}](len(values))
	for _, v := range values {
		s.Set(v, struct{}{})
	}
	return OrderedSet[K]{mp: s}
}

func (s OrderedSet[K]) Contains(key K) bool {
	if s.mp == nil {
		return false
	}
	_, ok := s.mp.kv[key]
	return ok
}

func (s OrderedSet[K]) Put(key K) {
	s.mp.Set(key, struct{}{})
}

func (s OrderedSet[K]) PutAll(keys []K) {
	for _, key := range keys {
		s.Put(key)
	}
}

func (s OrderedSet[K]) PutAllOrderedKeys(m *OrderedMap[K, any]) {
	for el := m.Front(); el != nil; el = el.Next() {
		s.Put(el.Key)
	}
}

func (s OrderedSet[K]) PutOrderedSet(keys OrderedSet[K]) {
	for el := keys.mp.Front(); el != nil; el = el.Next() {
		s.Put(el.Key)
	}
}

func (s OrderedSet[K]) Remove(key K) {
	s.mp.Delete(key)
}

func (s OrderedSet[K]) Clear() {
	s.mp = NewOrderedMap[K, struct{}](0)
}

func (s OrderedSet[K]) Clone() OrderedSet[K] {
	var mp = NewOrderedMap[K, struct{}](s.Size())
	for el := s.mp.Front(); el != nil; el = el.Next() {
		mp.Set(el.Key, struct{}{})
	}
	return OrderedSet[K]{mp: mp}
}

func (s OrderedSet[K]) Map(f func(K) K) OrderedSet[K] {
	var mp = NewOrderedMap[K, struct{}](s.Size())
	for el := s.mp.Front(); el != nil; el = el.Next() {
		mp.Set(f(el.Key), struct{}{})
	}
	return OrderedSet[K]{mp: mp}
}

func (s OrderedSet[K]) Size() int {
	return s.mp.Len()
}

func (s OrderedSet[K]) Empty() bool {
	return s.mp.Len() == 0
}

func (s OrderedSet[K]) ToSlice() []K {
	if s.Size() == 0 {
		return []K{}
	}
	slice := make([]K, 0, s.Size())
	for el := s.mp.Front(); el != nil; el = el.Next() {
		slice = append(slice, el.Key)
	}
	return slice
}

func (s OrderedSet[K]) Equals(other OrderedSet[K]) bool {
	if s.Size() != other.Size() {
		return false
	}
	if s.Empty() {
		return true
	}
	if s.Size() == 1 {
		return s.mp.ll.Front().Key == other.mp.ll.Front().Key
	}
	el2 := other.mp.Front()
	for el := s.mp.Front(); el != nil; {
		if el.Key != el2.Key {
			return false
		}
		el = el.Next()
		el2 = el2.Next()
	}
	return true
}

func (s OrderedSet[K]) ForEach(f func(K)) {
	for el := s.mp.Front(); el != nil; el = el.Next() {
		f(el.Key)
	}
}

func (s OrderedSet[K]) Hash() string {
	h, _ := utils.HashAny(s.ToSlice())
	return strconv.FormatUint(h, 10)
}
