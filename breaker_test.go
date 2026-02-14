package kvdecorator

import (
	"net"
	"testing"
	"time"
)

func TestCircuitBreaker_HealthyServer(t *testing.T) {
	// Start a TCP listener to simulate a healthy Redis
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	cb := NewCircuitBreaker(ln.Addr().String(),
		WithProbeInterval(50*time.Millisecond),
		WithDialTimeout(100*time.Millisecond),
		WithThreshold(2),
	)
	cb.Start()
	defer cb.Stop()

	// Breaker should be healthy right after Start returns
	if cb.IsDown() {
		t.Fatal("expected circuit breaker to be up with healthy server")
	}
}

func TestCircuitBreaker_DeadServer(t *testing.T) {
	// Use a port that is not listening
	cb := NewCircuitBreaker("127.0.0.1:1",
		WithProbeInterval(50*time.Millisecond),
		WithDialTimeout(50*time.Millisecond),
		WithThreshold(2),
	)
	cb.Start()
	defer cb.Stop()

	// Breaker should already be tripped right after Start returns
	if !cb.IsDown() {
		t.Fatal("expected circuit breaker to be down immediately after Start with dead server")
	}
}

func TestCircuitBreaker_Recovery(t *testing.T) {
	// Start a listener, get its address, then close it to simulate failure
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	// Accept connections initially
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	cb := NewCircuitBreaker(addr,
		WithProbeInterval(50*time.Millisecond),
		WithDialTimeout(50*time.Millisecond),
		WithThreshold(2),
	)
	cb.Start()
	defer cb.Stop()

	// Verify initially healthy
	time.Sleep(200 * time.Millisecond)
	if cb.IsDown() {
		t.Fatal("expected circuit breaker to be up initially")
	}

	// Close the listener to simulate failure
	ln.Close()
	time.Sleep(300 * time.Millisecond)
	if !cb.IsDown() {
		t.Fatal("expected circuit breaker to be down after server closed")
	}

	// Restart listener on the same address to simulate recovery
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()
	go func() {
		for {
			conn, err := ln2.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	time.Sleep(300 * time.Millisecond)
	if cb.IsDown() {
		t.Fatal("expected circuit breaker to recover after server comes back")
	}
}
