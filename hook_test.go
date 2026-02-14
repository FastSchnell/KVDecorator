package kvdecorator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// mockHook creates a FallbackHook with breaker forced to down state (local-only mode).
func mockHook() *FallbackHook {
	cache := NewLocalCache(time.Second)
	breaker := NewCircuitBreaker("127.0.0.1:1",
		WithProbeInterval(time.Hour), // don't actually probe
		WithThreshold(1),
	)
	breaker.isDown.Store(true) // force local mode

	return &FallbackHook{
		breaker: breaker,
		cache:   cache,
	}
}

func TestHook_GetSet(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	// SET
	setCmd := redis.NewStatusCmd(ctx, "set", "mykey", "myval", "ex", "60")
	err := h.handleLocally(setCmd)
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if setCmd.Val() != "OK" {
		t.Fatalf("expected OK, got %q", setCmd.Val())
	}

	// GET
	getCmd := redis.NewStringCmd(ctx, "get", "mykey")
	err = h.handleLocally(getCmd)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if getCmd.Val() != "myval" {
		t.Fatalf("expected myval, got %q", getCmd.Val())
	}
}

func TestHook_GetMiss(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	getCmd := redis.NewStringCmd(ctx, "get", "nonexistent")
	err := h.handleLocally(getCmd)
	if err != redis.Nil {
		t.Fatalf("expected redis.Nil, got %v", err)
	}
}

func TestHook_Del(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.Set("a", "1", 0)
	h.cache.Set("b", "2", 0)

	delCmd := redis.NewIntCmd(ctx, "del", "a", "b", "c")
	err := h.handleLocally(delCmd)
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	if delCmd.Val() != 2 {
		t.Fatalf("expected 2, got %d", delCmd.Val())
	}
}

func TestHook_Exists(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.Set("a", "1", 0)

	existsCmd := redis.NewIntCmd(ctx, "exists", "a", "b")
	err := h.handleLocally(existsCmd)
	if err != nil {
		t.Fatalf("EXISTS failed: %v", err)
	}
	if existsCmd.Val() != 1 {
		t.Fatalf("expected 1, got %d", existsCmd.Val())
	}
}

func TestHook_MGetMSet(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	// MSET
	msetCmd := redis.NewStatusCmd(ctx, "mset", "a", "1", "b", "2")
	err := h.handleLocally(msetCmd)
	if err != nil {
		t.Fatalf("MSET failed: %v", err)
	}

	// MGET
	mgetCmd := redis.NewSliceCmd(ctx, "mget", "a", "b", "c")
	err = h.handleLocally(mgetCmd)
	if err != nil {
		t.Fatalf("MGET failed: %v", err)
	}
	vals := mgetCmd.Val()
	if vals[0] != "1" || vals[1] != "2" || vals[2] != nil {
		t.Fatalf("unexpected mget result: %v", vals)
	}
}

func TestHook_Expire(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.Set("key1", "val1", 0)

	expireCmd := redis.NewBoolCmd(ctx, "expire", "key1", 1)
	err := h.handleLocally(expireCmd)
	if err != nil {
		t.Fatalf("EXPIRE failed: %v", err)
	}
	if !expireCmd.Val() {
		t.Fatal("expected EXPIRE to return true")
	}
}

func TestHook_TTL(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.Set("key1", "val1", 10*time.Second)

	ttlCmd := redis.NewDurationCmd(ctx, time.Second, "ttl", "key1")
	err := h.handleLocally(ttlCmd)
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	if ttlCmd.Val() <= 0 || ttlCmd.Val() > 10*time.Second {
		t.Fatalf("unexpected TTL: %v", ttlCmd.Val())
	}
}

func TestHook_Ping(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	pingCmd := redis.NewStatusCmd(ctx, "ping")
	err := h.handleLocally(pingCmd)
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if pingCmd.Val() != "PONG" {
		t.Fatalf("expected PONG, got %q", pingCmd.Val())
	}
}

func TestHook_UnsupportedCommand(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	cmd := redis.NewStatusCmd(ctx, "lpush", "list", "val")
	err := h.handleLocally(cmd)
	if err != ErrDegraded {
		t.Fatalf("expected ErrDegraded, got %v", err)
	}
}

func TestHook_BackupLocally(t *testing.T) {
	h := mockHook()
	defer h.Close()
	h.breaker.isDown.Store(false) // simulate healthy
	ctx := context.Background()

	// Simulate a SET command that succeeded on remote
	setCmd := redis.NewStatusCmd(ctx, "set", "backup_key", "backup_val", "ex", "60")
	setCmd.SetVal("OK")
	h.backupLocally(setCmd)

	// Verify it was backed up
	val, ok := h.cache.Get("backup_key")
	if !ok || val != "backup_val" {
		t.Fatalf("expected backup_val, got %q ok=%v", val, ok)
	}
}

func TestHook_BackupDel(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.Set("del_key", "val", 0)

	delCmd := redis.NewIntCmd(ctx, "del", "del_key")
	h.backupLocally(delCmd)

	_, ok := h.cache.Get("del_key")
	if ok {
		t.Fatal("expected del_key to be removed from local cache after backup")
	}
}

func TestHook_SetWithPX(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	setCmd := redis.NewStatusCmd(ctx, "set", "pxkey", "pxval", "px", "5000")
	err := h.handleLocally(setCmd)
	if err != nil {
		t.Fatalf("SET PX failed: %v", err)
	}

	d := h.cache.TTL("pxkey")
	if d <= 0 || d > 5*time.Second {
		t.Fatalf("expected TTL around 5s, got %v", d)
	}
}

func TestHook_ProcessHookDegraded(t *testing.T) {
	h := mockHook()
	defer h.Close()

	// Pre-populate local cache
	h.cache.Set("fallback_key", "fallback_val", 0)

	// The ProcessHook should route to local when breaker is down
	hook := h.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
		return fmt.Errorf("should not reach remote")
	})

	ctx := context.Background()
	getCmd := redis.NewStringCmd(ctx, "get", "fallback_key")
	err := hook(ctx, getCmd)
	if err != nil {
		t.Fatalf("ProcessHook failed: %v", err)
	}
	if getCmd.Val() != "fallback_val" {
		t.Fatalf("expected fallback_val, got %q", getCmd.Val())
	}
}
