package types

import (
	"cmp"
	"encoding/json"
	"strings"
)

type OrderedMap[K cmp.Ordered, V any] struct {
	kv map[K]*Element[K, V]
	ll list[K, V]
}

type El[K cmp.Ordered, V any] struct {
	Key   K
	Value V
}

func NewOrderedMap[K cmp.Ordered, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		kv: make(map[K]*Element[K, V]),
	}
}

// Get returns the value for a key. If the key does not exist, the second return
// parameter will be false and the value will be nil.
func (m *OrderedMap[K, V]) Get(key K) (value V, ok bool) {
	v, ok := m.kv[key]
	if ok {
		value = v.Value
	}

	return
}

// GetN returns the value for a key. If the key does not exist, it returns the zero value for type V, such as 0 for int or an empty string for string.
func (m *OrderedMap[K, V]) GetN(key K) V {
	var empty V
	v, ok := m.kv[key]
	if ok {
		return v.Value
	}

	return empty
}

// GetS returns the value for a key if it exists and has string type. Otherwise, it returns an empty string.
func (m *OrderedMap[K, V]) GetS(key K) string {
	v, ok := m.Get(key)
	if !ok {
		return ""
	}
	str, ok := any(v).(string)
	return str
}

// GetPathS returns the value at the specified path in the OrderedMap if that value is of string type
// The path is a dot-separated string representing a nested structure of keys.
// If the path does not exist or any intermediate key is not of type *OrderedMap[K, V], an empty string is returned.
func (m *OrderedMap[K, V]) GetPathS(path K) string {
	s, ok := any(path).(string)
	if !ok {
		return ""
	}
	p := strings.Split(s, ".")
	obj := m
	for i, key := range p {
		if i == len(p)-1 {
			v, ok := obj.Get(any(key).(K))
			if !ok {
				return ""
			}
			str, ok := any(v).(string)
			return str
		}
		var ok bool
		o, ok := obj.Get(any(key).(K))
		if !ok {
			return ""
		}
		obj, ok = any(o).(*OrderedMap[K, V])
		if !ok {
			return ""
		}
	}
	return ""
}

func (m *OrderedMap[K, V]) GetPathN(path []K) any {
	obj := m
	for i, key := range path {
		if i == len(path)-1 {
			v, _ := obj.Get(any(key).(K))
			return v
		}
		var ok bool
		o, ok := obj.Get(any(key).(K))
		if !ok {
			return nil
		}
		obj, ok = any(o).(*OrderedMap[K, V])
		if !ok {
			return nil
		}
	}
	return nil
}

// Set will set (or replace) a value for a key. If the key was new, then true
// will be returned. The returned value will be false if the value was replaced
// (even if the value was the same).
func (m *OrderedMap[K, V]) Set(key K, value V) bool {
	_, alreadyExist := m.kv[key]
	if alreadyExist {
		m.kv[key].Value = value
		return false
	}

	element := m.ll.PushBack(key, value)
	m.kv[key] = element
	return true
}

// SetIfAbsent sets a value for a key only if the key does not already exist in the map.
// It returns true if the value was set successfully, or false if the key already exists.
func (m *OrderedMap[K, V]) SetIfAbsent(key K, value V) bool {
	_, alreadyExist := m.kv[key]
	if alreadyExist {
		return false
	}

	element := m.ll.PushBack(key, value)
	m.kv[key] = element
	return true
}

// SetIfAbsentFunc sets a value for a key only if the key does not already exist in the map.
// It returns true if the value was set successfully, or false if the key already exists.
func (m *OrderedMap[K, V]) SetIfAbsentFunc(key K, f func() V) bool {
	if _, alreadyExist := m.kv[key]; alreadyExist {
		return false
	}
	value := f()
	element := m.ll.PushBack(key, value)
	m.kv[key] = element
	return true
}

func (m *OrderedMap[K, V]) Merge(key K, value V, f func(existing V, value V) V) {
	existing, alreadyExist := m.kv[key]
	if !alreadyExist {
		element := m.ll.PushBack(key, value)
		m.kv[key] = element
		return
	}
	existing.Value = f(existing.Value, value)
}

func (m *OrderedMap[K, V]) MergeAll(s *OrderedMap[K, V], f func(existing V, value V) V) {
	for el := s.Front(); el != nil; el = el.Next() {
		m.Merge(el.Key, el.Value, f)
	}
}

// GetOrDefault returns the value for a key. If the key does not exist, returns
// the default value instead.
func (m *OrderedMap[K, V]) GetOrDefault(key K, defaultValue V) V {
	if value, ok := m.kv[key]; ok {
		return value.Value
	}

	return defaultValue
}

func (m *OrderedMap[K, V]) GetOrCreate(key K, value V) V {
	value, ok := m.Get(key)
	if ok {
		return value
	}

	element := m.ll.PushBack(key, value)
	m.kv[key] = element
	return value
}

func (m *OrderedMap[K, V]) GetOrCreateFunc(key K, f func() V) V {
	value, ok := m.Get(key)
	if ok {
		return value
	}
	value = f()
	element := m.ll.PushBack(key, value)
	m.kv[key] = element
	return value
}

// GetElement returns the element for a key. If the key does not exist, the
// pointer will be nil.
func (m *OrderedMap[K, V]) GetElement(key K) *Element[K, V] {
	element, ok := m.kv[key]
	if ok {
		return element
	}

	return nil
}

// Len returns the number of elements in the map.
func (m *OrderedMap[K, V]) Len() int {
	if m == nil {
		return 0
	}
	return len(m.kv)
}

// Keys returns all of the keys in the order they were inserted. If a key was
// replaced it will retain the same position. To ensure most recently set keys
// are always at the end you must always Delete before Set.
func (m *OrderedMap[K, V]) Keys() (keys []K) {
	keys = make([]K, 0, m.Len())
	for el := m.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Key)
	}
	return keys
}

// Delete will remove a key from the map. It will return true if the key was
// removed (the key did exist).
func (m *OrderedMap[K, V]) Delete(key K) (didDelete bool) {
	element, ok := m.kv[key]
	if ok {
		m.ll.Remove(element)
		delete(m.kv, key)
	}

	return ok
}

func (m *OrderedMap[K, V]) Rename(key K, newkey K) {
	el, ok := m.kv[key]
	if !ok {
		return
	}
	el.Key = newkey
	m.kv[newkey] = el
	delete(m.kv, key)
}

// Front will return the element that is the first (oldest Set element). If
// there are no elements this will return nil.
func (m *OrderedMap[K, V]) Front() *Element[K, V] {
	if m == nil {
		return nil
	}
	return m.ll.Front()
}

// Back will return the element that is the last (most recent Set element). If
// there are no elements this will return nil.
func (m *OrderedMap[K, V]) Back() *Element[K, V] {
	return m.ll.Back()
}

// Copy returns a new OrderedMap with the same elements.
// Using Copy while there are concurrent writes may mangle the result.
func (m *OrderedMap[K, V]) Copy() *OrderedMap[K, V] {
	m2 := NewOrderedMap[K, V]()
	for el := m.Front(); el != nil; el = el.Next() {
		m2.Set(el.Key, el.Value)
	}
	return m2
}

func (m *OrderedMap[K, V]) ToMap() map[K]V {
	var result map[K]V
	for k, v := range m.kv {
		result[k] = v.Value
	}
	return result
}

func (m *OrderedMap[K, V]) ToArray() []El[K, V] {
	arr := make([]El[K, V], 0, m.Len())
	for el := m.Front(); el != nil; el = el.Next() {
		arr = append(arr, El[K, V]{el.Key, el.Value})
	}
	return arr
}

func (m *OrderedMap[K, V]) SetAll(s *OrderedMap[K, V]) {
	for el := s.Front(); el != nil; el = el.Next() {
		m.Set(el.Key, el.Value)
	}
}

func (m *OrderedMap[K, V]) SetAllMap(s map[K]V) {
	for key, value := range s {
		m.Set(key, value)
	}
}

func (m *OrderedMap[K, V]) ForEach(f func(k K, v V)) {
	for el := m.Front(); el != nil; el = el.Next() {
		f(el.Key, el.Value)
	}
}

func (m *OrderedMap[K, V]) ForEachIndexed(f func(i int, k K, v V)) {
	i := 0
	for el := m.Front(); el != nil; el = el.Next() {
		f(i, el.Key, el.Value)
		i++
	}
}

func (m *OrderedMap[K, V]) ForEachIndexedE(f func(i int, k K, v V) error) error {
	i := 0
	for el := m.Front(); el != nil; el = el.Next() {
		err := f(i, el.Key, el.Value)
		if err != nil {
			return err
		}
		i++
	}
	return nil
}

func (m *OrderedMap[K, V]) String() string {
	var buf strings.Builder
	buf.WriteString("{")
	first := true
	for el := m.Front(); el != nil; el = el.Next() {
		if !first {
			buf.WriteString(",")
		}
		first = false
		key, _ := json.Marshal(el.Key)
		value, _ := json.Marshal(el.Value)
		buf.Write(key)
		buf.WriteString(":")
		buf.Write(value)
	}
	buf.WriteString("}")
	return buf.String()
}
