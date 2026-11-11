package sim

import (
	"sync"
	"time"
)

// Clock abstracts time for deterministic testing and fast-forward simulations (Spec B5).
type Clock interface {
	Now() time.Time
}

// RealClock returns wall-clock UTC time.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

// FakeClock is a thread-safe controllable clock for tests and simulations.
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewFakeClock creates a FakeClock initialized to t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t.UTC()}
}

func (c *FakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Set sets the current clock time to t.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}
