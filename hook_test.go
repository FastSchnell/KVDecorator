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

	cmd := redis.NewStatusCmd(ctx, "zadd", "zset", "1", "member")
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

// --- Hash hook tests ---

func TestHook_HSetHGet(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	// HSET
	hsetCmd := redis.NewIntCmd(ctx, "hset", "myhash", "f1", "v1", "f2", "v2")
	err := h.handleLocally(hsetCmd)
	if err != nil {
		t.Fatalf("HSET failed: %v", err)
	}
	if hsetCmd.Val() != 2 {
		t.Fatalf("expected 2, got %d", hsetCmd.Val())
	}

	// HGET
	hgetCmd := redis.NewStringCmd(ctx, "hget", "myhash", "f1")
	err = h.handleLocally(hgetCmd)
	if err != nil {
		t.Fatalf("HGET failed: %v", err)
	}
	if hgetCmd.Val() != "v1" {
		t.Fatalf("expected v1, got %q", hgetCmd.Val())
	}
}

func TestHook_HGetMiss(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	hgetCmd := redis.NewStringCmd(ctx, "hget", "nonexistent", "f1")
	err := h.handleLocally(hgetCmd)
	if err != redis.Nil {
		t.Fatalf("expected redis.Nil, got %v", err)
	}
}

func TestHook_HDel(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.HSet("myhash", map[string]string{"f1": "v1", "f2": "v2"})

	hdelCmd := redis.NewIntCmd(ctx, "hdel", "myhash", "f1", "nonexistent")
	err := h.handleLocally(hdelCmd)
	if err != nil {
		t.Fatalf("HDEL failed: %v", err)
	}
	if hdelCmd.Val() != 1 {
		t.Fatalf("expected 1, got %d", hdelCmd.Val())
	}
}

func TestHook_HGetAll(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.HSet("myhash", map[string]string{"f1": "v1", "f2": "v2"})

	hgetallCmd := redis.NewMapStringStringCmd(ctx, "hgetall", "myhash")
	err := h.handleLocally(hgetallCmd)
	if err != nil {
		t.Fatalf("HGETALL failed: %v", err)
	}
	m := hgetallCmd.Val()
	if len(m) != 2 || m["f1"] != "v1" || m["f2"] != "v2" {
		t.Fatalf("unexpected HGETALL result: %v", m)
	}
}

func TestHook_BackupHSet(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	hsetCmd := redis.NewIntCmd(ctx, "hset", "bh", "f1", "v1")
	hsetCmd.SetVal(1)
	h.backupLocally(hsetCmd)

	val, ok := h.cache.HGet("bh", "f1")
	if !ok || val != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", val, ok)
	}
}

func TestHook_BackupHDel(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.HSet("bh", map[string]string{"f1": "v1", "f2": "v2"})

	hdelCmd := redis.NewIntCmd(ctx, "hdel", "bh", "f1")
	h.backupLocally(hdelCmd)

	_, ok := h.cache.HGet("bh", "f1")
	if ok {
		t.Fatal("expected f1 to be removed after backup hdel")
	}
}

// --- List hook tests ---

func TestHook_LPushLPop(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	lpushCmd := redis.NewIntCmd(ctx, "lpush", "mylist", "a", "b", "c")
	err := h.handleLocally(lpushCmd)
	if err != nil {
		t.Fatalf("LPUSH failed: %v", err)
	}
	if lpushCmd.Val() != 3 {
		t.Fatalf("expected 3, got %d", lpushCmd.Val())
	}

	lpopCmd := redis.NewStringCmd(ctx, "lpop", "mylist")
	err = h.handleLocally(lpopCmd)
	if err != nil {
		t.Fatalf("LPOP failed: %v", err)
	}
	if lpopCmd.Val() != "c" {
		t.Fatalf("expected c, got %q", lpopCmd.Val())
	}
}

func TestHook_RPushRPop(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	rpushCmd := redis.NewIntCmd(ctx, "rpush", "mylist", "a", "b", "c")
	err := h.handleLocally(rpushCmd)
	if err != nil {
		t.Fatalf("RPUSH failed: %v", err)
	}
	if rpushCmd.Val() != 3 {
		t.Fatalf("expected 3, got %d", rpushCmd.Val())
	}

	rpopCmd := redis.NewStringCmd(ctx, "rpop", "mylist")
	err = h.handleLocally(rpopCmd)
	if err != nil {
		t.Fatalf("RPOP failed: %v", err)
	}
	if rpopCmd.Val() != "c" {
		t.Fatalf("expected c, got %q", rpopCmd.Val())
	}
}

func TestHook_LRange(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.RPush("mylist", "a", "b", "c", "d")

	lrangeCmd := redis.NewStringSliceCmd(ctx, "lrange", "mylist", "0", "-1")
	err := h.handleLocally(lrangeCmd)
	if err != nil {
		t.Fatalf("LRANGE failed: %v", err)
	}
	vals := lrangeCmd.Val()
	if len(vals) != 4 || vals[0] != "a" || vals[3] != "d" {
		t.Fatalf("unexpected LRANGE result: %v", vals)
	}
}

func TestHook_LLen(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.RPush("mylist", "a", "b", "c")

	llenCmd := redis.NewIntCmd(ctx, "llen", "mylist")
	err := h.handleLocally(llenCmd)
	if err != nil {
		t.Fatalf("LLEN failed: %v", err)
	}
	if llenCmd.Val() != 3 {
		t.Fatalf("expected 3, got %d", llenCmd.Val())
	}
}

func TestHook_LPopEmpty(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	lpopCmd := redis.NewStringCmd(ctx, "lpop", "nonexistent")
	err := h.handleLocally(lpopCmd)
	if err != redis.Nil {
		t.Fatalf("expected redis.Nil, got %v", err)
	}
}

func TestHook_RPopEmpty(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	rpopCmd := redis.NewStringCmd(ctx, "rpop", "nonexistent")
	err := h.handleLocally(rpopCmd)
	if err != redis.Nil {
		t.Fatalf("expected redis.Nil, got %v", err)
	}
}

func TestHook_BackupLPush(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	lpushCmd := redis.NewIntCmd(ctx, "lpush", "bl", "a", "b")
	lpushCmd.SetVal(2)
	h.backupLocally(lpushCmd)

	if h.cache.LLen("bl") != 2 {
		t.Fatalf("expected length 2, got %d", h.cache.LLen("bl"))
	}
}

func TestHook_BackupRPush(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	rpushCmd := redis.NewIntCmd(ctx, "rpush", "bl", "a", "b")
	rpushCmd.SetVal(2)
	h.backupLocally(rpushCmd)

	if h.cache.LLen("bl") != 2 {
		t.Fatalf("expected length 2, got %d", h.cache.LLen("bl"))
	}
}

func TestHook_BackupLPop(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.RPush("bl", "a", "b", "c")

	lpopCmd := redis.NewStringCmd(ctx, "lpop", "bl")
	lpopCmd.SetVal("a")
	h.backupLocally(lpopCmd)

	if h.cache.LLen("bl") != 2 {
		t.Fatalf("expected length 2 after backup lpop, got %d", h.cache.LLen("bl"))
	}
}

func TestHook_BackupRPop(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.RPush("bl", "a", "b", "c")

	rpopCmd := redis.NewStringCmd(ctx, "rpop", "bl")
	rpopCmd.SetVal("c")
	h.backupLocally(rpopCmd)

	if h.cache.LLen("bl") != 2 {
		t.Fatalf("expected length 2 after backup rpop, got %d", h.cache.LLen("bl"))
	}
}

// --- Set hook tests ---

func TestHook_SAddSMembers(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	saddCmd := redis.NewIntCmd(ctx, "sadd", "myset", "a", "b", "c")
	err := h.handleLocally(saddCmd)
	if err != nil {
		t.Fatalf("SADD failed: %v", err)
	}
	if saddCmd.Val() != 3 {
		t.Fatalf("expected 3, got %d", saddCmd.Val())
	}

	smembersCmd := redis.NewStringSliceCmd(ctx, "smembers", "myset")
	err = h.handleLocally(smembersCmd)
	if err != nil {
		t.Fatalf("SMEMBERS failed: %v", err)
	}
	if len(smembersCmd.Val()) != 3 {
		t.Fatalf("expected 3 members, got %d", len(smembersCmd.Val()))
	}
}

func TestHook_SRem(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.SAdd("myset", "a", "b", "c")

	sremCmd := redis.NewIntCmd(ctx, "srem", "myset", "a", "nonexistent")
	err := h.handleLocally(sremCmd)
	if err != nil {
		t.Fatalf("SREM failed: %v", err)
	}
	if sremCmd.Val() != 1 {
		t.Fatalf("expected 1, got %d", sremCmd.Val())
	}
}

func TestHook_SIsMember(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.SAdd("myset", "a", "b")

	sismemberCmd := redis.NewBoolCmd(ctx, "sismember", "myset", "a")
	err := h.handleLocally(sismemberCmd)
	if err != nil {
		t.Fatalf("SISMEMBER failed: %v", err)
	}
	if !sismemberCmd.Val() {
		t.Fatal("expected true")
	}

	sismemberCmd2 := redis.NewBoolCmd(ctx, "sismember", "myset", "z")
	err = h.handleLocally(sismemberCmd2)
	if err != nil {
		t.Fatalf("SISMEMBER failed: %v", err)
	}
	if sismemberCmd2.Val() {
		t.Fatal("expected false")
	}
}

func TestHook_SCard(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.SAdd("myset", "a", "b", "c")

	scardCmd := redis.NewIntCmd(ctx, "scard", "myset")
	err := h.handleLocally(scardCmd)
	if err != nil {
		t.Fatalf("SCARD failed: %v", err)
	}
	if scardCmd.Val() != 3 {
		t.Fatalf("expected 3, got %d", scardCmd.Val())
	}
}

func TestHook_BackupSAdd(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	saddCmd := redis.NewIntCmd(ctx, "sadd", "bs", "a", "b")
	saddCmd.SetVal(2)
	h.backupLocally(saddCmd)

	if h.cache.SCard("bs") != 2 {
		t.Fatalf("expected 2 members, got %d", h.cache.SCard("bs"))
	}
}

func TestHook_BackupSRem(t *testing.T) {
	h := mockHook()
	defer h.Close()
	ctx := context.Background()

	h.cache.SAdd("bs", "a", "b", "c")

	sremCmd := redis.NewIntCmd(ctx, "srem", "bs", "a")
	h.backupLocally(sremCmd)

	if h.cache.SCard("bs") != 2 {
		t.Fatalf("expected 2 members after backup srem, got %d", h.cache.SCard("bs"))
	}
}
