# KVDecorator

[![Go Reference](https://pkg.go.dev/badge/github.com/FastSchnell/KVDecorator.svg)](https://pkg.go.dev/github.com/FastSchnell/KVDecorator)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A transparent fallback cache for [go-redis](https://github.com/redis/go-redis). When your Redis goes down, KVDecorator automatically routes supported commands to an in-memory cache — **zero code changes required** in your business logic.

## How It Works

```
                          ┌─────────────────────────────────────────────────┐
                          │              FallbackHook (redis.Hook)         │
                          │                                                 │
                          │  ┌──────────────┐       ┌──────────────────┐   │
  rdb.Get / rdb.Set       │  │  ProcessHook │       │  CircuitBreaker  │   │
 ─────────────────────▶   │  │              │       │                  │   │
   (any redis.Cmder)      │  │  breaker.Is  │◀──────│  TCP probe loop  │   │
                          │  │  Down()?     │       │  (background)    │   │
                          │  └──┬───────┬───┘       └────────┬─────────┘   │
                          │     │       │                     │             │
                          │  breaker    │ breaker           TCP dial        │
                          │  = open     │ = closed         every Ns        │
                          │     │       │                     │             │
                          │     ▼       ▼                     ▼             │
                          │ ┌───────┐ ┌──────────┐    ┌─────────────┐      │
                          │ │ Local │ │  Remote  │    │    Redis    │      │
                          │ │ Cache │ │  Redis   │───▶│   Server    │      │
                          │ │  (map)│ │          │    │  :6379      │      │
                          │ └───────┘ └─────┬────┘    └─────────────┘      │
                          │     ▲           │                              │
                          │     │  dual-write on success                   │
                          │     └───────────┘                              │
                          └─────────────────────────────────────────────────┘
```

**Normal** — Commands go to Redis. Write operations (`SET`/`DEL`/`MSET`/`HSET`/`HDEL`/`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`SADD`/`SREM`) are dual-written to the local cache as backup.

**Degraded** — TCP probe detects Redis is unreachable. The breaker opens, and all supported commands are served from the local cache. No request ever blocks on a dead connection.

**Recovery** — TCP probe detects Redis is back. The breaker closes, and traffic automatically routes back to Redis.

### Components

1. **CircuitBreaker** — A background goroutine probes the Redis address via `net.DialTimeout("tcp", ...)` at a configurable interval. After N consecutive failures, the breaker opens. No request-path latency is added.
2. **FallbackHook** — Implements `redis.Hook`. Inspects the breaker state on every command and routes accordingly.
3. **LocalCache** — A `sync.RWMutex`-protected `map[string]Item` with lazy expiration on read and periodic background cleanup.

## Use Cases

### Database + Redis Cache Layer

A common architecture places Redis as a cache in front of the database. When Redis goes down, every request becomes a cache miss and hits the database directly — potentially causing cascading failures or even taking down the DB.

```
                Without KVDecorator              With KVDecorator
               ┌──────────────────┐            ┌──────────────────┐
               │   Application    │            │   Application    │
               └────────┬─────────┘            └────────┬─────────┘
                        │                               │
                        ▼                               ▼
                ┌───────────────┐              ┌───────────────┐
                │  Redis (down) │              │  Redis (down) │
                │      ✗        │              │      ✗        │
                └───────────────┘              └───────┬───────┘
                        │                              │ breaker open
                  all cache misses                     ▼
                        │                      ┌───────────────┐
                        ▼                      │  Local Cache   │
                ┌───────────────┐              │   (in-memory) │
                │   Database    │              └───────────────┘
                │  (overloaded) │                 serves cached data,
                └───────────────┘                 DB stays safe
```

KVDecorator absorbs the traffic with its in-memory cache, giving you time to restore Redis without risking the database.

### Session Storage

Web applications storing sessions in Redis risk logging out all users when Redis becomes unavailable. With KVDecorator, sessions survive in memory — users stay logged in and experience no interruption.

```go
// Sessions keep working even if Redis goes down
rdb.Set(ctx, "session:abc123", sessionJSON, 30*time.Minute)
rdb.Get(ctx, "session:abc123") // served from local cache during outage
```

### API Rate Limiting

Rate limiters backed by Redis fail open (no protection) or fail closed (block all traffic) when Redis is unavailable. KVDecorator keeps rate limit counters in local memory, so rate limiting continues to function during an outage.

### Feature Flags & Configuration

Services that read feature flags or configuration from Redis lose access to their config when Redis goes down, potentially causing unexpected behavior. KVDecorator ensures the last-known configuration remains accessible from the local cache.

### Microservice Caching

In microservice architectures, each service often caches upstream responses in Redis to reduce inter-service calls. A Redis outage would trigger a storm of requests to upstream services. KVDecorator acts as a safety net, serving cached responses locally and preventing cascading load.

## Installation

```bash
go get github.com/FastSchnell/KVDecorator
```

Requires Go 1.21+ and go-redis v9.

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/FastSchnell/KVDecorator"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	hook := kvdecorator.NewFallbackHook("localhost:6379")
	defer hook.Close()
	rdb.AddHook(hook)

	ctx := context.Background()

	// Business code is unchanged — fallback is transparent
	rdb.Set(ctx, "user:1:name", "Alice", 10*time.Minute)
	val, err := rdb.Get(ctx, "user:1:name").Result()
	fmt.Println(val, err) // "Alice" <nil>  — from Redis or local cache
}
```

## Configuration

```go
hook := kvdecorator.NewFallbackHook("localhost:6379",
	kvdecorator.WithHookProbeInterval(2*time.Second),   // TCP probe interval (default: 1s)
	kvdecorator.WithHookDialTimeout(time.Second),        // TCP dial timeout  (default: 500ms)
	kvdecorator.WithHookThreshold(5),                    // consecutive failures to trip (default: 3)
	kvdecorator.WithHookCleanupInterval(30*time.Second), // expired-key cleanup interval (default: 10s)
)
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithHookProbeInterval` | 1s | How often the background goroutine probes Redis via TCP |
| `WithHookDialTimeout` | 500ms | Timeout for each TCP probe dial |
| `WithHookThreshold` | 3 | Consecutive probe failures required to open the breaker |
| `WithHookCleanupInterval` | 10s | Interval for the background goroutine that purges expired keys |

## Supported Commands in Degraded Mode

| Command | Local Behavior |
|---------|---------------|
| `GET` | Returns cached value or `redis.Nil` |
| `SET` | Stores in local cache, supports `EX` / `PX` / `EXAT` / `PXAT` |
| `DEL` | Removes from local cache, returns delete count |
| `MGET` | Returns multiple cached values |
| `MSET` | Stores multiple key-value pairs |
| `EXISTS` | Returns count of existing keys |
| `EXPIRE` | Updates TTL on an existing key |
| `TTL` | Returns remaining TTL |
| `PING` | Returns `PONG` |
| `HGET` | Returns cached hash field value or `redis.Nil` |
| `HSET` | Stores field-value pairs in local hash |
| `HDEL` | Removes fields from local hash, returns count |
| `HGETALL` | Returns all field-value pairs from local hash |
| `LPUSH` | Prepends to local list, returns new length |
| `RPUSH` | Appends to local list, returns new length |
| `LPOP` | Removes and returns first element or `redis.Nil` |
| `RPOP` | Removes and returns last element or `redis.Nil` |
| `LRANGE` | Returns elements in range from local list |
| `LLEN` | Returns length of local list |
| `SADD` | Adds members to local set, returns count of new |
| `SREM` | Removes members from local set, returns count |
| `SMEMBERS` | Returns all members of local set |
| `SISMEMBER` | Returns whether member is in local set |
| `SCARD` | Returns cardinality of local set |

Unsupported commands (e.g. `ZADD`, `SUBSCRIBE`, `EVAL`) return `kvdecorator.ErrDegraded`.

## Observability

```go
// Check breaker state programmatically
if hook.IsDown() {
	log.Warn("Redis is unreachable, serving from local cache")
}
```

## Dev Without Redis

The same code works whether Redis is running or not — no `if` branches, no mock clients:

```go
// Exact same code in dev and production
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
hook := kvdecorator.NewFallbackHook("localhost:6379")
defer hook.Close()
rdb.AddHook(hook)

rdb.Set(ctx, "session:abc", token, 30*time.Minute)
rdb.Get(ctx, "session:abc")
```

| Environment | Redis | What happens |
|-------------|-------|-------------|
| Dev laptop  | not installed | `NewFallbackHook` probes on init, breaker trips immediately, all commands run against in-memory cache |
| CI / test   | not running   | same — tests pass without a Redis dependency |
| Production  | running       | commands go to Redis, local cache stays warm as backup |

No code changes between environments. Just start (or don't start) Redis.

## Design Decisions

- **TCP probe, not request-path detection** — The circuit breaker runs independently. It never adds latency to real commands, and it detects recovery even when there is no traffic.
- **Dual-write on success** — During normal operation, all write commands (`SET`/`DEL`/`MSET`/`HSET`/`HDEL`/`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`SADD`/`SREM`) are mirrored to the local cache. This ensures the local cache is warm when a failover happens.
- **`redis.Hook` integration** — No wrapper client, no custom interface. Just `AddHook()` on any existing `redis.Client`, `redis.ClusterClient`, or `redis.Ring`.
- **No external dependencies** — Only depends on `go-redis/v9` and the Go standard library.

## Testing

```bash
go test -race -v ./...
```

66 tests covering:
- LocalCache: string ops (set/get, TTL, delete, exists, mget/mset, expire, flush), hash ops (hset/hget, hdel, hgetall), list ops (lpush/lpop, rpush/rpop, lrange, llen), set ops (sadd/smembers, srem, sismember, scard), cross-type ops, concurrent access
- CircuitBreaker: healthy server, dead server, recovery cycle
- FallbackHook: all 24 supported commands, backup logic for all write operations, degraded routing, pipeline handling

## License

KVDecorator is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
