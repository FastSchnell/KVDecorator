package kvdecorator

import (
	"sync"
	"testing"
	"time"
)

func TestLocalCache_SetGet(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.Set("key1", "val1", 0)
	val, ok := c.Get("key1")
	if !ok || val != "val1" {
		t.Fatalf("expected val1, got %q ok=%v", val, ok)
	}

	_, ok = c.Get("nonexistent")
	if ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestLocalCache_TTLExpiration(t *testing.T) {
	c := NewLocalCache(50 * time.Millisecond)
	defer c.Close()

	c.Set("key1", "val1", 100*time.Millisecond)
	val, ok := c.Get("key1")
	if !ok || val != "val1" {
		t.Fatalf("expected val1 before expiry, got %q ok=%v", val, ok)
	}

	time.Sleep(150 * time.Millisecond)
	_, ok = c.Get("key1")
	if ok {
		t.Fatal("expected key1 to be expired")
	}
}

func TestLocalCache_Delete(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.Set("a", "1", 0)
	c.Set("b", "2", 0)
	count := c.Delete("a", "b", "c")
	if count != 2 {
		t.Fatalf("expected 2 deleted, got %d", count)
	}
	_, ok := c.Get("a")
	if ok {
		t.Fatal("expected a to be deleted")
	}
}

func TestLocalCache_Exists(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.Set("a", "1", 0)
	c.Set("b", "2", 0)
	count := c.Exists("a", "b", "c")
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestLocalCache_MGetMSet(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.MSet(map[string]string{"a": "1", "b": "2"})
	vals := c.MGet("a", "b", "c")
	if vals[0] != "1" || vals[1] != "2" || vals[2] != nil {
		t.Fatalf("unexpected mget result: %v", vals)
	}
}

func TestLocalCache_Expire(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.Set("key1", "val1", 0)
	ok := c.Expire("key1", 100*time.Millisecond)
	if !ok {
		t.Fatal("expected Expire to return true")
	}

	val, found := c.Get("key1")
	if !found || val != "val1" {
		t.Fatal("expected key1 to exist before TTL")
	}

	time.Sleep(150 * time.Millisecond)
	_, found = c.Get("key1")
	if found {
		t.Fatal("expected key1 to be expired after TTL")
	}
}

func TestLocalCache_TTLMethod(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	d := c.TTL("nonexistent")
	if d != -2 {
		t.Fatalf("expected -2 for missing key, got %v", d)
	}

	c.Set("persist", "val", 0)
	d = c.TTL("persist")
	if d != -1 {
		t.Fatalf("expected -1 for no-TTL key, got %v", d)
	}

	c.Set("expiring", "val", time.Second)
	d = c.TTL("expiring")
	if d <= 0 || d > time.Second {
		t.Fatalf("expected positive TTL <= 1s, got %v", d)
	}
}

func TestLocalCache_Flush(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.Set("a", "1", 0)
	c.Set("b", "2", 0)
	c.Flush()
	_, ok := c.Get("a")
	if ok {
		t.Fatal("expected cache to be empty after flush")
	}
}

func TestLocalCache_ConcurrentAccess(t *testing.T) {
	c := NewLocalCache(50 * time.Millisecond)
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		key := "key"
		go func() {
			defer wg.Done()
			c.Set(key, "val", 10*time.Millisecond)
		}()
		go func() {
			defer wg.Done()
			c.Get(key)
		}()
		go func() {
			defer wg.Done()
			c.Delete(key)
		}()
	}
	wg.Wait()
}

// --- Hash tests ---

func TestLocalCache_HSetHGet(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	added := c.HSet("h1", map[string]string{"f1": "v1", "f2": "v2"})
	if added != 2 {
		t.Fatalf("expected 2 added, got %d", added)
	}

	val, ok := c.HGet("h1", "f1")
	if !ok || val != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", val, ok)
	}

	// Update existing field — should return 0 new
	added = c.HSet("h1", map[string]string{"f1": "v1_new"})
	if added != 0 {
		t.Fatalf("expected 0 added on update, got %d", added)
	}
	val, _ = c.HGet("h1", "f1")
	if val != "v1_new" {
		t.Fatalf("expected v1_new, got %q", val)
	}

	// Miss
	_, ok = c.HGet("h1", "nonexistent")
	if ok {
		t.Fatal("expected miss for nonexistent field")
	}
	_, ok = c.HGet("nonexistent", "f1")
	if ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestLocalCache_HDel(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.HSet("h1", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
	count := c.HDel("h1", "f1", "f2", "nonexistent")
	if count != 2 {
		t.Fatalf("expected 2 deleted, got %d", count)
	}

	_, ok := c.HGet("h1", "f1")
	if ok {
		t.Fatal("expected f1 to be deleted")
	}

	// Delete last field — key should be auto-deleted
	c.HDel("h1", "f3")
	if c.Exists("h1") != 0 {
		t.Fatal("expected key to be auto-deleted when hash is empty")
	}
}

func TestLocalCache_HGetAll(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.HSet("h1", map[string]string{"f1": "v1", "f2": "v2"})
	m := c.HGetAll("h1")
	if len(m) != 2 || m["f1"] != "v1" || m["f2"] != "v2" {
		t.Fatalf("unexpected HGetAll result: %v", m)
	}

	// Missing key
	m = c.HGetAll("nonexistent")
	if len(m) != 0 {
		t.Fatalf("expected empty map for missing key, got %v", m)
	}
}

func TestLocalCache_HSetExpiration(t *testing.T) {
	c := NewLocalCache(50 * time.Millisecond)
	defer c.Close()

	c.HSet("h1", map[string]string{"f1": "v1"})
	c.Expire("h1", 100*time.Millisecond)

	val, ok := c.HGet("h1", "f1")
	if !ok || val != "v1" {
		t.Fatalf("expected v1 before expiry, got %q ok=%v", val, ok)
	}

	time.Sleep(150 * time.Millisecond)
	_, ok = c.HGet("h1", "f1")
	if ok {
		t.Fatal("expected hash to be expired")
	}
}

// --- List tests ---

func TestLocalCache_LPushLPop(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	n := c.LPush("l1", "a", "b", "c")
	if n != 3 {
		t.Fatalf("expected length 3, got %d", n)
	}

	// LPUSH a b c → [c, b, a]
	val, ok := c.LPop("l1")
	if !ok || val != "c" {
		t.Fatalf("expected c, got %q ok=%v", val, ok)
	}
	val, ok = c.LPop("l1")
	if !ok || val != "b" {
		t.Fatalf("expected b, got %q ok=%v", val, ok)
	}
	val, ok = c.LPop("l1")
	if !ok || val != "a" {
		t.Fatalf("expected a, got %q ok=%v", val, ok)
	}

	// Now empty — key auto-deleted
	_, ok = c.LPop("l1")
	if ok {
		t.Fatal("expected empty after popping all")
	}
}

func TestLocalCache_RPushRPop(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	n := c.RPush("l1", "a", "b", "c")
	if n != 3 {
		t.Fatalf("expected length 3, got %d", n)
	}

	val, ok := c.RPop("l1")
	if !ok || val != "c" {
		t.Fatalf("expected c, got %q ok=%v", val, ok)
	}
	val, ok = c.RPop("l1")
	if !ok || val != "b" {
		t.Fatalf("expected b, got %q ok=%v", val, ok)
	}
}

func TestLocalCache_LRange(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.RPush("l1", "a", "b", "c", "d", "e")

	// Full range
	vals := c.LRange("l1", 0, -1)
	if len(vals) != 5 || vals[0] != "a" || vals[4] != "e" {
		t.Fatalf("unexpected LRange 0 -1: %v", vals)
	}

	// Partial range
	vals = c.LRange("l1", 1, 3)
	if len(vals) != 3 || vals[0] != "b" || vals[2] != "d" {
		t.Fatalf("unexpected LRange 1 3: %v", vals)
	}

	// Negative indices
	vals = c.LRange("l1", -3, -1)
	if len(vals) != 3 || vals[0] != "c" || vals[2] != "e" {
		t.Fatalf("unexpected LRange -3 -1: %v", vals)
	}

	// Out of range
	vals = c.LRange("l1", 10, 20)
	if len(vals) != 0 {
		t.Fatalf("expected empty for out of range, got %v", vals)
	}

	// Missing key
	vals = c.LRange("nonexistent", 0, -1)
	if len(vals) != 0 {
		t.Fatalf("expected empty for missing key, got %v", vals)
	}
}

func TestLocalCache_LLen(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	if c.LLen("l1") != 0 {
		t.Fatal("expected 0 for missing key")
	}
	c.RPush("l1", "a", "b", "c")
	if c.LLen("l1") != 3 {
		t.Fatalf("expected 3, got %d", c.LLen("l1"))
	}
}

func TestLocalCache_LPopEmpty(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	_, ok := c.LPop("nonexistent")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestLocalCache_RPopEmpty(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	_, ok := c.RPop("nonexistent")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestLocalCache_ListExpiration(t *testing.T) {
	c := NewLocalCache(50 * time.Millisecond)
	defer c.Close()

	c.RPush("l1", "a", "b")
	c.Expire("l1", 100*time.Millisecond)

	if c.LLen("l1") != 2 {
		t.Fatal("expected length 2 before expiry")
	}

	time.Sleep(150 * time.Millisecond)
	if c.LLen("l1") != 0 {
		t.Fatal("expected length 0 after expiry")
	}
}

// --- Set tests ---

func TestLocalCache_SAddSMembers(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	added := c.SAdd("s1", "a", "b", "c")
	if added != 3 {
		t.Fatalf("expected 3 added, got %d", added)
	}

	members := c.SMembers("s1")
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	memberSet := make(map[string]bool)
	for _, m := range members {
		memberSet[m] = true
	}
	if !memberSet["a"] || !memberSet["b"] || !memberSet["c"] {
		t.Fatalf("missing expected members: %v", members)
	}
}

func TestLocalCache_SRem(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.SAdd("s1", "a", "b", "c")
	count := c.SRem("s1", "a", "nonexistent")
	if count != 1 {
		t.Fatalf("expected 1 removed, got %d", count)
	}

	// Remove remaining — key auto-deleted
	c.SRem("s1", "b", "c")
	if c.Exists("s1") != 0 {
		t.Fatal("expected key to be auto-deleted when set is empty")
	}
}

func TestLocalCache_SIsMember(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.SAdd("s1", "a", "b")
	if !c.SIsMember("s1", "a") {
		t.Fatal("expected a to be a member")
	}
	if c.SIsMember("s1", "z") {
		t.Fatal("expected z to not be a member")
	}
	if c.SIsMember("nonexistent", "a") {
		t.Fatal("expected false for missing key")
	}
}

func TestLocalCache_SCard(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	if c.SCard("s1") != 0 {
		t.Fatal("expected 0 for missing key")
	}
	c.SAdd("s1", "a", "b", "c")
	if c.SCard("s1") != 3 {
		t.Fatalf("expected 3, got %d", c.SCard("s1"))
	}
}

func TestLocalCache_SAddDuplicate(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.SAdd("s1", "a", "b")
	added := c.SAdd("s1", "b", "c")
	if added != 1 {
		t.Fatalf("expected 1 new member, got %d", added)
	}
	if c.SCard("s1") != 3 {
		t.Fatalf("expected 3 total members, got %d", c.SCard("s1"))
	}
}

func TestLocalCache_SetExpiration(t *testing.T) {
	c := NewLocalCache(50 * time.Millisecond)
	defer c.Close()

	c.SAdd("s1", "a", "b")
	c.Expire("s1", 100*time.Millisecond)

	if c.SCard("s1") != 2 {
		t.Fatal("expected 2 before expiry")
	}

	time.Sleep(150 * time.Millisecond)
	if c.SCard("s1") != 0 {
		t.Fatal("expected 0 after expiry")
	}
}

// --- Cross-type tests ---

func TestLocalCache_DeleteRemovesAllTypes(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.HSet("h", map[string]string{"f": "v"})
	c.RPush("l", "a")
	c.SAdd("s", "a")

	count := c.Delete("h", "l", "s")
	if count != 3 {
		t.Fatalf("expected 3 deleted, got %d", count)
	}
}

func TestLocalCache_ExistsAllTypes(t *testing.T) {
	c := NewLocalCache(0)
	defer c.Close()

	c.HSet("h", map[string]string{"f": "v"})
	c.RPush("l", "a")
	c.SAdd("s", "a")

	count := c.Exists("h", "l", "s", "nonexistent")
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}
