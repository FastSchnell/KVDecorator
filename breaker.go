package kvdecorator

import (
	"net"
	"sync/atomic"
	"time"
)

// CircuitBreaker monitors a remote service via TCP probes
// and switches to a degraded (open) state when the service is unreachable.
type CircuitBreaker struct {
	isDown           atomic.Bool
	addr             string
	probeInterval    time.Duration
	dialTimeout      time.Duration
	threshold        int64 // consecutive failures before tripping
	consecutiveFails atomic.Int64
	stop             chan struct{}
}

// BreakerOption configures the CircuitBreaker.
type BreakerOption func(*CircuitBreaker)

// WithProbeInterval sets the interval between TCP probes. Default: 1s.
func WithProbeInterval(d time.Duration) BreakerOption {
	return func(cb *CircuitBreaker) { cb.probeInterval = d }
}

// WithDialTimeout sets the TCP dial timeout. Default: 500ms.
func WithDialTimeout(d time.Duration) BreakerOption {
	return func(cb *CircuitBreaker) { cb.dialTimeout = d }
}

// WithThreshold sets the number of consecutive failures to trip. Default: 3.
func WithThreshold(n int64) BreakerOption {
	return func(cb *CircuitBreaker) { cb.threshold = n }
}

// NewCircuitBreaker creates a new CircuitBreaker for the given address.
func NewCircuitBreaker(addr string, opts ...BreakerOption) *CircuitBreaker {
	cb := &CircuitBreaker{
		addr:          addr,
		probeInterval: time.Second,
		dialTimeout:   500 * time.Millisecond,
		threshold:     3,
		stop:          make(chan struct{}),
	}
	for _, opt := range opts {
		opt(cb)
	}
	return cb
}

// Start runs an initial synchronous probe to determine the current state of
// the remote service, then launches the background probe goroutine.
// If Redis is unreachable, the breaker trips before Start returns.
func (cb *CircuitBreaker) Start() {
	for range cb.threshold {
		cb.probe()
	}
	go cb.probeLoop()
}

// Stop terminates the background TCP probe goroutine.
func (cb *CircuitBreaker) Stop() {
	close(cb.stop)
}

// IsDown returns true if the remote service is considered unreachable.
func (cb *CircuitBreaker) IsDown() bool {
	return cb.isDown.Load()
}

func (cb *CircuitBreaker) probeLoop() {
	ticker := time.NewTicker(cb.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-cb.stop:
			return
		case <-ticker.C:
			cb.probe()
		}
	}
}

func (cb *CircuitBreaker) probe() {
	conn, err := net.DialTimeout("tcp", cb.addr, cb.dialTimeout)
	if err != nil {
		if cb.consecutiveFails.Add(1) >= cb.threshold {
			cb.isDown.Store(true)
		}
		return
	}
	conn.Close()
	cb.consecutiveFails.Store(0)
	cb.isDown.Store(false)
}
