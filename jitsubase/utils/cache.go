package utils

import (
	"sync"
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
