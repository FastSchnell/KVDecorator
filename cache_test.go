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
