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

**Normal** — Commands go to Redis. Write operations (`SET`/`DEL`/`MSET`) are dual-written to the local cache as backup.

**Degraded** — TCP probe detects Redis is unreachable. The breaker opens, and all supported commands are served from the local cache. No request ever blocks on a dead connection.

**Recovery** — TCP probe detects Redis is back. The breaker closes, and traffic automatically routes back to Redis.

### Components

1. **CircuitBreaker** — A background goroutine probes the Redis address via `net.DialTimeout("tcp", ...)` at a configurable interval. After N consecutive failures, the breaker opens. No request-path latency is added.
2. **FallbackHook** — Implements `redis.Hook`. Inspects the breaker state on every command and routes accordingly.
3. **LocalCache** — A `sync.RWMutex`-protected `map[string]Item` with lazy expiration on read and periodic background cleanup.

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

Unsupported commands (e.g. `LPUSH`, `ZADD`, `HSET`) return `kvdecorator.ErrDegraded`.

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
- **Dual-write on success** — During normal operation, `SET`/`DEL`/`MSET` results are mirrored to the local cache. This ensures the local cache is warm when a failover happens.
- **`redis.Hook` integration** — No wrapper client, no custom interface. Just `AddHook()` on any existing `redis.Client`, `redis.ClusterClient`, or `redis.Ring`.
- **No external dependencies** — Only depends on `go-redis/v9` and the Go standard library.

## Testing

```bash
go test -race -v ./...
```

25 tests covering:
- LocalCache: set/get, TTL expiration, delete, exists, mget/mset, expire, flush, concurrent access
- CircuitBreaker: healthy server, dead server, recovery cycle
- FallbackHook: all supported commands, backup logic, degraded routing, pipeline handling

## License

KVDecorator is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
