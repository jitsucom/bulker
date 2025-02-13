package utils

import (
	"sync"
	"sync/atomic"
	"time"
)

type CacheEntry[T any] struct {
	addedAt int64
	value   T
}

type Cache[T any] struct {
	sync.RWMutex
	ttlSeconds int64
	entries    map[string]*CacheEntry[T]
}

func NewCache[T any](ttlSeconds int64) *Cache[T] {
	return &Cache[T]{
		ttlSeconds: ttlSeconds,
		entries:    make(map[string]*CacheEntry[T]),
	}
}

func (c *Cache[T]) Get(key string) (T, bool) {
	c.RLock()
	defer c.RUnlock()
	var dflt T
	entry, ok := c.entries[key]
	if !ok {
		return dflt, false
	}
	if entry.addedAt+c.ttlSeconds < time.Now().Unix() {
		return dflt, false
	}
	return entry.value, true
}

func (c *Cache[T]) Set(key string, value T) {
	c.Lock()
	defer c.Unlock()
	c.entries[key] = &CacheEntry[T]{addedAt: time.Now().Unix(), value: value}
}

type SyncMapCache[T any] struct {
	maxSize int64
	entries sync.Map
	length  atomic.Int64
}

func NewSyncMapCache[T any](maxSize int) *SyncMapCache[T] {
	return &SyncMapCache[T]{
		maxSize: int64(maxSize),
		entries: sync.Map{},
		length:  atomic.Int64{},
	}
}

func (s *SyncMapCache[T]) Get(key string) (*T, bool) {
	entry, ok := s.entries.Load(key)
	if !ok {
		return nil, false
	}
	return entry.(*T), true
}

func (s *SyncMapCache[T]) Set(key string, value *T) {
	if s.length.Load() >= s.maxSize {
		s.entries.Clear()
	}
	_, ok := s.entries.Swap(key, value)
	if !ok {
		s.length.Add(1)
	}
}
