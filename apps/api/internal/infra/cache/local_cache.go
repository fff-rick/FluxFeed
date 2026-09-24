package infracache

import (
	"sync"
	"time"
)

type localCacheEntry struct {
	value     []byte
	staleAt   time.Time
	expiresAt time.Time
}

// localCache 是进程内有界 L1；只缓存短生命周期的序列化值，避免共享可变对象。
type localCache struct {
	mu         sync.Mutex
	entries    map[string]localCacheEntry
	maxEntries int
}

func newLocalCache(maxEntries int) *localCache {
	return &localCache{entries: make(map[string]localCacheEntry), maxEntries: maxEntries}
}

func (c *localCache) get(key string) ([]byte, bool) {
	if c == nil || c.maxEntries <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	now := time.Now()
	if now.After(entry.expiresAt) {
		delete(c.entries, key)
		return nil, false
	}
	if now.After(entry.staleAt) {
		return nil, false
	}
	return append([]byte(nil), entry.value...), true
}

func (c *localCache) getStale(key string) ([]byte, bool) {
	if c == nil || c.maxEntries <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	now := time.Now()
	if now.After(entry.expiresAt) {
		delete(c.entries, key)
		return nil, false
	}
	if now.Before(entry.staleAt) {
		return nil, false
	}
	return append([]byte(nil), entry.value...), true
}

func (c *localCache) set(key string, value []byte, ttl time.Duration) {
	c.setStale(key, value, ttl, ttl)
}

func (c *localCache) setStale(key string, value []byte, freshTTL time.Duration, hardTTL time.Duration) {
	if c == nil || c.maxEntries <= 0 || freshTTL <= 0 || hardTTL < freshTTL {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if len(c.entries) >= c.maxEntries {
		for cachedKey, entry := range c.entries {
			if now.After(entry.expiresAt) {
				delete(c.entries, cachedKey)
			}
		}
	}
	if len(c.entries) >= c.maxEntries {
		// ponytail: 容量触顶时淘汰任意一项；命中率需要精细优化时再换近似 LRU。
		for cachedKey := range c.entries {
			delete(c.entries, cachedKey)
			break
		}
	}
	c.entries[key] = localCacheEntry{
		value:     append([]byte(nil), value...),
		staleAt:   now.Add(freshTTL),
		expiresAt: now.Add(hardTTL),
	}
}

func minTTL(ttl time.Duration, ceiling time.Duration) time.Duration {
	if ttl <= 0 || ttl > ceiling {
		return ceiling
	}
	return ttl
}
