package kvdecorator

import (
	"sync"
	"time"
)

// Item represents a cached value with optional expiration.
// Only one of Value, HashValue, ListValue, SetValue should be used per key.
type Item struct {
	Value      string
	HashValue  map[string]string   // non-nil when this key holds a hash
	ListValue  []string            // non-nil when this key holds a list
	SetValue   map[string]struct{} // non-nil when this key holds a set
	Expiration int64               // UnixNano timestamp; 0 means no expiration
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

// --- Hash operations ---

// HGet retrieves a field from a hash. Returns ("", false) if key or field is missing.
func (c *LocalCache) HGet(key, field string) (string, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() || item.HashValue == nil {
		return "", false
	}
	val, exists := item.HashValue[field]
	return val, exists
}

// HSet sets field-value pairs in a hash. Returns the number of fields that were added (not updated).
func (c *LocalCache) HSet(key string, fields map[string]string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.HashValue == nil {
		item = Item{HashValue: make(map[string]string), Expiration: 0}
	}
	var added int64
	for f, v := range fields {
		if _, exists := item.HashValue[f]; !exists {
			added++
		}
		item.HashValue[f] = v
	}
	c.items[key] = item
	return added
}

// HDel removes fields from a hash. Returns the number of fields that were removed.
func (c *LocalCache) HDel(key string, fields ...string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.HashValue == nil {
		return 0
	}
	var count int64
	for _, f := range fields {
		if _, exists := item.HashValue[f]; exists {
			delete(item.HashValue, f)
			count++
		}
	}
	if len(item.HashValue) == 0 {
		delete(c.items, key)
	} else {
		c.items[key] = item
	}
	return count
}

// HGetAll returns all field-value pairs in a hash. Returns an empty map if key is missing.
func (c *LocalCache) HGetAll(key string) map[string]string {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() || item.HashValue == nil {
		return map[string]string{}
	}
	result := make(map[string]string, len(item.HashValue))
	for f, v := range item.HashValue {
		result[f] = v
	}
	return result
}

// --- List operations ---

// LPush prepends values to a list. Returns the new length.
func (c *LocalCache) LPush(key string, values ...string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.ListValue == nil {
		item = Item{ListValue: []string{}, Expiration: 0}
	}
	// Redis LPUSH: each value is prepended in order, so "LPUSH key a b c" → [c, b, a, ...]
	for _, v := range values {
		item.ListValue = append([]string{v}, item.ListValue...)
	}
	c.items[key] = item
	return int64(len(item.ListValue))
}

// RPush appends values to a list. Returns the new length.
func (c *LocalCache) RPush(key string, values ...string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.ListValue == nil {
		item = Item{ListValue: []string{}, Expiration: 0}
	}
	item.ListValue = append(item.ListValue, values...)
	c.items[key] = item
	return int64(len(item.ListValue))
}

// LPop removes and returns the first element. Returns ("", false) if empty or missing.
func (c *LocalCache) LPop(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.ListValue == nil || len(item.ListValue) == 0 {
		return "", false
	}
	val := item.ListValue[0]
	item.ListValue = item.ListValue[1:]
	if len(item.ListValue) == 0 {
		delete(c.items, key)
	} else {
		c.items[key] = item
	}
	return val, true
}

// RPop removes and returns the last element. Returns ("", false) if empty or missing.
func (c *LocalCache) RPop(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.ListValue == nil || len(item.ListValue) == 0 {
		return "", false
	}
	last := len(item.ListValue) - 1
	val := item.ListValue[last]
	item.ListValue = item.ListValue[:last]
	if len(item.ListValue) == 0 {
		delete(c.items, key)
	} else {
		c.items[key] = item
	}
	return val, true
}

// LRange returns elements from index start to stop (inclusive). Negative indices count from the end.
func (c *LocalCache) LRange(key string, start, stop int64) []string {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() || item.ListValue == nil {
		return []string{}
	}
	length := int64(len(item.ListValue))
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return []string{}
	}
	result := make([]string, stop-start+1)
	copy(result, item.ListValue[start:stop+1])
	return result
}

// LLen returns the length of the list. Returns 0 if key does not exist.
func (c *LocalCache) LLen(key string) int64 {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() || item.ListValue == nil {
		return 0
	}
	return int64(len(item.ListValue))
}

// --- Set operations ---

// SAdd adds members to a set. Returns the number of members that were added (not already present).
func (c *LocalCache) SAdd(key string, members ...string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.SetValue == nil {
		item = Item{SetValue: make(map[string]struct{}), Expiration: 0}
	}
	var added int64
	for _, m := range members {
		if _, exists := item.SetValue[m]; !exists {
			item.SetValue[m] = struct{}{}
			added++
		}
	}
	c.items[key] = item
	return added
}

// SRem removes members from a set. Returns the number of members that were removed.
func (c *LocalCache) SRem(key string, members ...string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[key]
	if !ok || item.Expired() || item.SetValue == nil {
		return 0
	}
	var count int64
	for _, m := range members {
		if _, exists := item.SetValue[m]; exists {
			delete(item.SetValue, m)
			count++
		}
	}
	if len(item.SetValue) == 0 {
		delete(c.items, key)
	} else {
		c.items[key] = item
	}
	return count
}

// SMembers returns all members of a set.
func (c *LocalCache) SMembers(key string) []string {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() || item.SetValue == nil {
		return []string{}
	}
	result := make([]string, 0, len(item.SetValue))
	for m := range item.SetValue {
		result = append(result, m)
	}
	return result
}

// SIsMember returns true if member is in the set.
func (c *LocalCache) SIsMember(key, member string) bool {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() || item.SetValue == nil {
		return false
	}
	_, exists := item.SetValue[member]
	return exists
}

// SCard returns the cardinality (number of elements) of the set.
func (c *LocalCache) SCard(key string) int64 {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if !ok || item.Expired() || item.SetValue == nil {
		return 0
	}
	return int64(len(item.SetValue))
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
