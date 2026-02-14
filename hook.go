package kvdecorator

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrDegraded is returned for commands not supported in local fallback mode.
var ErrDegraded = fmt.Errorf("kvdecorator: service degraded, command not supported locally")

// FallbackHook implements redis.Hook. It intercepts all commands and routes them
// to a local in-memory cache when the remote Redis is unreachable.
type FallbackHook struct {
	breaker *CircuitBreaker
	cache   *LocalCache
}

// Compile-time check that FallbackHook implements redis.Hook.
var _ redis.Hook = (*FallbackHook)(nil)

// HookOption configures the FallbackHook.
type HookOption func(*hookConfig)

type hookConfig struct {
	probeInterval   time.Duration
	dialTimeout     time.Duration
	threshold       int64
	cleanupInterval time.Duration
}

// WithHookProbeInterval sets the TCP probe interval. Default: 1s.
func WithHookProbeInterval(d time.Duration) HookOption {
	return func(c *hookConfig) { c.probeInterval = d }
}

// WithHookDialTimeout sets the TCP dial timeout. Default: 500ms.
func WithHookDialTimeout(d time.Duration) HookOption {
	return func(c *hookConfig) { c.dialTimeout = d }
}

// WithHookThreshold sets the consecutive failure threshold. Default: 3.
func WithHookThreshold(n int64) HookOption {
	return func(c *hookConfig) { c.threshold = n }
}

// WithHookCleanupInterval sets the local cache cleanup interval. Default: 10s.
func WithHookCleanupInterval(d time.Duration) HookOption {
	return func(c *hookConfig) { c.cleanupInterval = d }
}

// NewFallbackHook creates a FallbackHook for the given Redis address.
// The addr should match the Redis server address (e.g. "localhost:6379").
// Use AddHook on your redis.Client to install it:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	hook := kvdecorator.NewFallbackHook("localhost:6379")
//	defer hook.Close()
//	rdb.AddHook(hook)
func NewFallbackHook(addr string, opts ...HookOption) *FallbackHook {
	cfg := &hookConfig{
		probeInterval:   time.Second,
		dialTimeout:     500 * time.Millisecond,
		threshold:       3,
		cleanupInterval: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	breaker := NewCircuitBreaker(addr,
		WithProbeInterval(cfg.probeInterval),
		WithDialTimeout(cfg.dialTimeout),
		WithThreshold(cfg.threshold),
	)
	breaker.Start()

	cache := NewLocalCache(cfg.cleanupInterval)

	return &FallbackHook{
		breaker: breaker,
		cache:   cache,
	}
}

// Close stops the circuit breaker probe and the local cache cleanup goroutine.
func (h *FallbackHook) Close() {
	h.breaker.Stop()
	h.cache.Close()
}

// IsDown reports whether the remote Redis is currently considered unreachable.
func (h *FallbackHook) IsDown() bool {
	return h.breaker.IsDown()
}

// DialHook passes through to the next hook.
func (h *FallbackHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

// ProcessHook intercepts each command. If the breaker is open, the command
// is handled locally. Otherwise, it is forwarded to Redis and the result
// is backed up in the local cache.
func (h *FallbackHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.breaker.IsDown() {
			return h.handleLocally(cmd)
		}
		err := next(ctx, cmd)
		if err == nil {
			h.backupLocally(cmd)
		}
		return err
	}
}

// ProcessPipelineHook intercepts pipeline commands with the same logic.
func (h *FallbackHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.breaker.IsDown() {
			for _, cmd := range cmds {
				h.handleLocally(cmd)
			}
			return nil
		}
		err := next(ctx, cmds)
		if err == nil {
			for _, cmd := range cmds {
				h.backupLocally(cmd)
			}
		}
		return err
	}
}

// handleLocally executes a Redis command against the local cache.
func (h *FallbackHook) handleLocally(cmd redis.Cmder) error {
	args := cmd.Args()
	if len(args) == 0 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}

	name := strings.ToLower(fmt.Sprintf("%v", args[0]))
	switch name {
	case "get":
		return h.handleGet(cmd, args)
	case "set":
		return h.handleSet(cmd, args)
	case "del":
		return h.handleDel(cmd, args)
	case "exists":
		return h.handleExists(cmd, args)
	case "mget":
		return h.handleMGet(cmd, args)
	case "mset":
		return h.handleMSet(cmd, args)
	case "expire":
		return h.handleExpire(cmd, args)
	case "ttl":
		return h.handleTTL(cmd, args)
	case "ping":
		return h.handlePing(cmd)
	case "hget":
		return h.handleHGet(cmd, args)
	case "hset":
		return h.handleHSet(cmd, args)
	case "hdel":
		return h.handleHDel(cmd, args)
	case "hgetall":
		return h.handleHGetAll(cmd, args)
	case "lpush":
		return h.handleLPush(cmd, args)
	case "rpush":
		return h.handleRPush(cmd, args)
	case "lpop":
		return h.handleLPop(cmd, args)
	case "rpop":
		return h.handleRPop(cmd, args)
	case "lrange":
		return h.handleLRange(cmd, args)
	case "llen":
		return h.handleLLen(cmd, args)
	case "sadd":
		return h.handleSAdd(cmd, args)
	case "srem":
		return h.handleSRem(cmd, args)
	case "smembers":
		return h.handleSMembers(cmd, args)
	case "sismember":
		return h.handleSIsMember(cmd, args)
	case "scard":
		return h.handleSCard(cmd, args)
	default:
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
}

func (h *FallbackHook) handleGet(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	val, ok := h.cache.Get(key)
	if !ok {
		cmd.SetErr(redis.Nil)
		return redis.Nil
	}
	if c, ok := cmd.(*redis.StringCmd); ok {
		c.SetVal(val)
	}
	return nil
}

func (h *FallbackHook) handleSet(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	value := fmt.Sprintf("%v", args[2])
	ttl := parseTTLFromArgs(args[3:])
	h.cache.Set(key, value, ttl)
	if c, ok := cmd.(*redis.StatusCmd); ok {
		c.SetVal("OK")
	}
	return nil
}

func (h *FallbackHook) handleDel(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, fmt.Sprintf("%v", a))
	}
	count := h.cache.Delete(keys...)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(count)
	}
	return nil
}

func (h *FallbackHook) handleExists(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, fmt.Sprintf("%v", a))
	}
	count := h.cache.Exists(keys...)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(count)
	}
	return nil
}

func (h *FallbackHook) handleMGet(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, fmt.Sprintf("%v", a))
	}
	vals := h.cache.MGet(keys...)
	if c, ok := cmd.(*redis.SliceCmd); ok {
		c.SetVal(vals)
	}
	return nil
}

func (h *FallbackHook) handleMSet(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 || len(args)%2 == 0 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	pairs := make(map[string]string)
	for i := 1; i < len(args)-1; i += 2 {
		k := fmt.Sprintf("%v", args[i])
		v := fmt.Sprintf("%v", args[i+1])
		pairs[k] = v
	}
	h.cache.MSet(pairs)
	if c, ok := cmd.(*redis.StatusCmd); ok {
		c.SetVal("OK")
	}
	return nil
}

func (h *FallbackHook) handleExpire(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	seconds, err := toInt64(args[2])
	if err != nil {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	existed := h.cache.Expire(key, time.Duration(seconds)*time.Second)
	if c, match := cmd.(*redis.BoolCmd); match {
		c.SetVal(existed)
	}
	return nil
}

func (h *FallbackHook) handleTTL(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	d := h.cache.TTL(key)
	if c, ok := cmd.(*redis.DurationCmd); ok {
		c.SetVal(d)
	}
	return nil
}

func (h *FallbackHook) handlePing(cmd redis.Cmder) error {
	if c, ok := cmd.(*redis.StatusCmd); ok {
		c.SetVal("PONG")
	}
	return nil
}

// --- Hash handlers ---

func (h *FallbackHook) handleHGet(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	field := fmt.Sprintf("%v", args[2])
	val, ok := h.cache.HGet(key, field)
	if !ok {
		cmd.SetErr(redis.Nil)
		return redis.Nil
	}
	if c, ok := cmd.(*redis.StringCmd); ok {
		c.SetVal(val)
	}
	return nil
}

func (h *FallbackHook) handleHSet(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 4 || len(args)%2 != 0 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	fields := make(map[string]string)
	for i := 2; i < len(args)-1; i += 2 {
		f := fmt.Sprintf("%v", args[i])
		v := fmt.Sprintf("%v", args[i+1])
		fields[f] = v
	}
	count := h.cache.HSet(key, fields)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(count)
	}
	return nil
}

func (h *FallbackHook) handleHDel(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	fields := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		fields = append(fields, fmt.Sprintf("%v", a))
	}
	count := h.cache.HDel(key, fields...)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(count)
	}
	return nil
}

func (h *FallbackHook) handleHGetAll(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	m := h.cache.HGetAll(key)
	if c, ok := cmd.(*redis.MapStringStringCmd); ok {
		c.SetVal(m)
	}
	return nil
}

// --- List handlers ---

func (h *FallbackHook) handleLPush(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	values := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		values = append(values, fmt.Sprintf("%v", a))
	}
	length := h.cache.LPush(key, values...)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(length)
	}
	return nil
}

func (h *FallbackHook) handleRPush(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	values := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		values = append(values, fmt.Sprintf("%v", a))
	}
	length := h.cache.RPush(key, values...)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(length)
	}
	return nil
}

func (h *FallbackHook) handleLPop(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	val, ok := h.cache.LPop(key)
	if !ok {
		cmd.SetErr(redis.Nil)
		return redis.Nil
	}
	if c, ok := cmd.(*redis.StringCmd); ok {
		c.SetVal(val)
	}
	return nil
}

func (h *FallbackHook) handleRPop(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	val, ok := h.cache.RPop(key)
	if !ok {
		cmd.SetErr(redis.Nil)
		return redis.Nil
	}
	if c, ok := cmd.(*redis.StringCmd); ok {
		c.SetVal(val)
	}
	return nil
}

func (h *FallbackHook) handleLRange(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 4 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	start, err := toInt64(args[2])
	if err != nil {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	stop, err := toInt64(args[3])
	if err != nil {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	vals := h.cache.LRange(key, start, stop)
	if c, ok := cmd.(*redis.StringSliceCmd); ok {
		c.SetVal(vals)
	}
	return nil
}

func (h *FallbackHook) handleLLen(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	length := h.cache.LLen(key)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(length)
	}
	return nil
}

// --- Set handlers ---

func (h *FallbackHook) handleSAdd(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	members := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		members = append(members, fmt.Sprintf("%v", a))
	}
	count := h.cache.SAdd(key, members...)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(count)
	}
	return nil
}

func (h *FallbackHook) handleSRem(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	members := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		members = append(members, fmt.Sprintf("%v", a))
	}
	count := h.cache.SRem(key, members...)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(count)
	}
	return nil
}

func (h *FallbackHook) handleSMembers(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	members := h.cache.SMembers(key)
	if c, ok := cmd.(*redis.StringSliceCmd); ok {
		c.SetVal(members)
	}
	return nil
}

func (h *FallbackHook) handleSIsMember(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 3 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	member := fmt.Sprintf("%v", args[2])
	isMember := h.cache.SIsMember(key, member)
	if c, ok := cmd.(*redis.BoolCmd); ok {
		c.SetVal(isMember)
	}
	return nil
}

func (h *FallbackHook) handleSCard(cmd redis.Cmder, args []interface{}) error {
	if len(args) < 2 {
		cmd.SetErr(ErrDegraded)
		return ErrDegraded
	}
	key := fmt.Sprintf("%v", args[1])
	count := h.cache.SCard(key)
	if c, ok := cmd.(*redis.IntCmd); ok {
		c.SetVal(count)
	}
	return nil
}

// backupLocally saves the result of a successful remote command into the local cache.
func (h *FallbackHook) backupLocally(cmd redis.Cmder) {
	args := cmd.Args()
	if len(args) == 0 {
		return
	}

	name := strings.ToLower(fmt.Sprintf("%v", args[0]))
	switch name {
	case "set":
		if len(args) >= 3 {
			key := fmt.Sprintf("%v", args[1])
			value := fmt.Sprintf("%v", args[2])
			ttl := parseTTLFromArgs(args[3:])
			h.cache.Set(key, value, ttl)
		}
	case "del":
		keys := make([]string, 0, len(args)-1)
		for _, a := range args[1:] {
			keys = append(keys, fmt.Sprintf("%v", a))
		}
		h.cache.Delete(keys...)
	case "mset":
		for i := 1; i < len(args)-1; i += 2 {
			k := fmt.Sprintf("%v", args[i])
			v := fmt.Sprintf("%v", args[i+1])
			h.cache.Set(k, v, 0)
		}
	case "hset":
		if len(args) >= 4 {
			key := fmt.Sprintf("%v", args[1])
			fields := make(map[string]string)
			for i := 2; i < len(args)-1; i += 2 {
				f := fmt.Sprintf("%v", args[i])
				v := fmt.Sprintf("%v", args[i+1])
				fields[f] = v
			}
			h.cache.HSet(key, fields)
		}
	case "hdel":
		if len(args) >= 3 {
			key := fmt.Sprintf("%v", args[1])
			fields := make([]string, 0, len(args)-2)
			for _, a := range args[2:] {
				fields = append(fields, fmt.Sprintf("%v", a))
			}
			h.cache.HDel(key, fields...)
		}
	case "lpush":
		if len(args) >= 3 {
			key := fmt.Sprintf("%v", args[1])
			values := make([]string, 0, len(args)-2)
			for _, a := range args[2:] {
				values = append(values, fmt.Sprintf("%v", a))
			}
			h.cache.LPush(key, values...)
		}
	case "rpush":
		if len(args) >= 3 {
			key := fmt.Sprintf("%v", args[1])
			values := make([]string, 0, len(args)-2)
			for _, a := range args[2:] {
				values = append(values, fmt.Sprintf("%v", a))
			}
			h.cache.RPush(key, values...)
		}
	case "lpop":
		if len(args) >= 2 {
			key := fmt.Sprintf("%v", args[1])
			h.cache.LPop(key)
		}
	case "rpop":
		if len(args) >= 2 {
			key := fmt.Sprintf("%v", args[1])
			h.cache.RPop(key)
		}
	case "sadd":
		if len(args) >= 3 {
			key := fmt.Sprintf("%v", args[1])
			members := make([]string, 0, len(args)-2)
			for _, a := range args[2:] {
				members = append(members, fmt.Sprintf("%v", a))
			}
			h.cache.SAdd(key, members...)
		}
	case "srem":
		if len(args) >= 3 {
			key := fmt.Sprintf("%v", args[1])
			members := make([]string, 0, len(args)-2)
			for _, a := range args[2:] {
				members = append(members, fmt.Sprintf("%v", a))
			}
			h.cache.SRem(key, members...)
		}
	}
}

// parseTTLFromArgs extracts TTL from SET command optional args like EX, PX, EXAT, PXAT.
func parseTTLFromArgs(args []interface{}) time.Duration {
	for i := 0; i < len(args)-1; i++ {
		flag := strings.ToUpper(fmt.Sprintf("%v", args[i]))
		val, err := toInt64(args[i+1])
		if err != nil {
			continue
		}
		switch flag {
		case "EX":
			return time.Duration(val) * time.Second
		case "PX":
			return time.Duration(val) * time.Millisecond
		case "EXAT":
			return time.Until(time.Unix(val, 0))
		case "PXAT":
			return time.Until(time.Unix(0, val*int64(time.Millisecond)))
		}
	}
	return 0
}

func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	case string:
		var i int64
		_, err := fmt.Sscanf(n, "%d", &i)
		return i, err
	default:
		var i int64
		_, err := fmt.Sscanf(fmt.Sprintf("%v", v), "%d", &i)
		return i, err
	}
}
