package health

import (
	"sync"
	"time"
)

// FakeClock is a manual clock for tests of the ejection time.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a clock stopped at an arbitrary fixed instant.
func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
}

// Now returns the clock's time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
