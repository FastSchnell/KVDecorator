package kvdecorator

import (
	"sync"
	"time"
)

// Item represents a cached value with optional expiration.
type Item struct {
	Value      string
	Expiration int64 // UnixNano timestamp; 0 means no expiration
}

// Expired returns true if the item has expired.
func (item Item) Expired() bool {
	return item.Expiration > 0 && time.Now().UnixNano() > item.Expiration
}

// LocalCache is a concurrent-safe in-memory key-value cache with TTL support.
type LocalCache struct {
	mu    sync.RWMutex
	items map[string]Item
	stop  chan struct{}
}

// NewLocalCache creates a new LocalCache and starts the background cleanup goroutine.
// cleanupInterval controls how often expired items are removed.
func NewLocalCache(cleanupInterval time.Duration) *LocalCache {
	c := &LocalCache{
		items: make(map[string]Item),
		stop:  make(chan struct{}),
	}
	if cleanupInterval > 0 {
		go c.janitor(cleanupInterval)
	}
	return c
}

// Get retrieves a value by key. Returns the value and true if found and not expired.
func (c *LocalCache) Get(key string) (string, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok {
		return "", false
	}
	if item.Expired() {
		c.Delete(key)
		return "", false
	}
	return item.Value, true
}

// Set stores a key-value pair with an optional TTL. Zero TTL means no expiration.
func (c *LocalCache) Set(key, value string, ttl time.Duration) {
	var exp int64
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}
	c.mu.Lock()
	c.items[key] = Item{Value: value, Expiration: exp}
	c.mu.Unlock()
}

// Delete removes one or more keys. Returns the number of keys that were present.
func (c *LocalCache) Delete(keys ...string) int64 {
	var count int64
	c.mu.Lock()
	for _, key := range keys {
		if _, ok := c.items[key]; ok {
			delete(c.items, key)
			count++
		}
	}
	c.mu.Unlock()
	return count
}

// Exists returns the number of specified keys that exist (and are not expired).
func (c *LocalCache) Exists(keys ...string) int64 {
	var count int64
	c.mu.RLock()
	for _, key := range keys {
		if item, ok := c.items[key]; ok && !item.Expired() {
			count++
		}
	}
	c.mu.RUnlock()
	return count
}

// MGet returns the values for the given keys. Missing or expired keys have nil entries.
func (c *LocalCache) MGet(keys ...string) []interface{} {
	result := make([]interface{}, len(keys))
	c.mu.RLock()
	for i, key := range keys {
		if item, ok := c.items[key]; ok && !item.Expired() {
			result[i] = item.Value
		}
	}
	c.mu.RUnlock()
	return result
}

// MSet stores multiple key-value pairs. keys and values must have the same length.
func (c *LocalCache) MSet(pairs map[string]string) {
	c.mu.Lock()
	for k, v := range pairs {
		c.items[k] = Item{Value: v, Expiration: 0}
	}
	c.mu.Unlock()
}

// Expire sets a TTL on an existing key. Returns true if the key exists.
func (c *LocalCache) Expire(key string, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() {
		return false
	}
	item.Expiration = time.Now().Add(ttl).UnixNano()
	c.items[key] = item
	return true
}

// TTL returns the remaining TTL for a key.
// Returns -2 if the key does not exist, -1 if the key has no expiration.
func (c *LocalCache) TTL(key string) time.Duration {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() {
		return -2
	}
	if item.Expiration == 0 {
		return -1
	}
	remaining := time.Until(time.Unix(0, item.Expiration))
	if remaining < 0 {
		return -2
	}
	return remaining
}

// Flush removes all items from the cache.
func (c *LocalCache) Flush() {
	c.mu.Lock()
	c.items = make(map[string]Item)
	c.mu.Unlock()
}

// Close stops the background cleanup goroutine.
func (c *LocalCache) Close() {
	close(c.stop)
}

// janitor periodically removes expired items.
func (c *LocalCache) janitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.deleteExpired()
		}
	}
}

func (c *LocalCache) deleteExpired() {
	now := time.Now().UnixNano()
	c.mu.Lock()
	for k, v := range c.items {
		if v.Expiration > 0 && now > v.Expiration {
			delete(c.items, k)
		}
	}
	c.mu.Unlock()
}
