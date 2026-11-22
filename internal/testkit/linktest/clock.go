package linktest

import (
	"sort"
	"sync"
	"testing"
	"time"
)

// eventTimeout is the package-wide fallback for every real-time wait in this
// package's tests, including FakeClock.waitUntil and the proxy forwarding
// checks. It only decides how long a wait that will never complete takes to
// fail: the success path selects on the event itself and never spends this
// time, so a larger value costs nothing. Thirty seconds is far above the worst
// normal latency under a loaded runner; it is not a claim that an event should
// take that long.
const eventTimeout = 30 * time.Second

// FakeClock is a manually advanced time source. Tests call Advance to make
// timers fire, so they never have to sleep to reach a deadline.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	seq     int
	timers  []*fakeTimer
	changed chan struct{}
}

type fakeTimer struct {
	deadline time.Time
	seq      int
	f        func()
	fired    bool
	stopped  bool
}

// NewFakeClock returns a clock whose current time is start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start, changed: make(chan struct{})}
}

// Now returns the clock's current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc registers f to run at Now()+d and returns a stop function.
//
// A non-positive d runs f immediately in a new goroutine; the returned stop
// function then reports false. For a positive d the stop function cancels the
// timer and reports true when the timer had not fired yet, and reports false
// once it has fired or was already stopped.
//
// Advance runs due callbacks one at a time in deadline order, each in its own
// goroutine: a callback that blocks delays later callbacks but never Advance
// itself.
func (c *FakeClock) AfterFunc(d time.Duration, f func()) (stop func() bool) {
	c.mu.Lock()
	if d <= 0 {
		c.mu.Unlock()
		go f()
		return func() bool { return false }
	}
	tm := &fakeTimer{deadline: c.now.Add(d), seq: c.seq, f: f}
	c.seq++
	c.timers = append(c.timers, tm)
	c.notifyLocked()
	c.mu.Unlock()
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if tm.fired || tm.stopped {
			return false
		}
		tm.stopped = true
		c.notifyLocked()
		return true
	}
}

// Advance moves the clock forward by d and runs every timer that becomes due.
// Callbacks run in deadline order, ties in registration order, and in a
// separate goroutine each. Advance returns without waiting for them.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	for _, tm := range c.timers {
		if !tm.fired && !tm.stopped && !tm.deadline.After(c.now) {
			tm.fired = true
			due = append(due, tm)
		}
	}
	c.notifyLocked()
	c.mu.Unlock()

	sort.SliceStable(due, func(i, j int) bool {
		if due[i].deadline.Equal(due[j].deadline) {
			return due[i].seq < due[j].seq
		}
		return due[i].deadline.Before(due[j].deadline)
	})

	// Chain the goroutines: each callback waits for the previous one to return.
	// The callbacks therefore write in deadline order even though Advance does
	// not wait for any of them.
	var previous chan struct{}
	for _, tm := range due {
		done := make(chan struct{})
		go func(tm *fakeTimer, wait, done chan struct{}) {
			if wait != nil {
				<-wait
			}
			tm.f()
			close(done)
		}(tm, previous, done)
		previous = done
	}
}

// Pending reports how many timers are registered and have neither fired nor
// been stopped.
func (c *FakeClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pendingLocked()
}

// Earliest returns the deadline of the timer that will fire first among the
// pending ones, and reports whether any timer is pending. It is read-only and
// lets a test compare the scheduled instant with the link's NextDial.
func (c *FakeClock) Earliest() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var earliest time.Time
	found := false
	for _, tm := range c.timers {
		if tm.fired || tm.stopped {
			continue
		}
		if !found || tm.deadline.Before(earliest) {
			earliest = tm.deadline
			found = true
		}
	}
	return earliest, found
}

func (c *FakeClock) pendingLocked() int {
	n := 0
	for _, tm := range c.timers {
		if !tm.fired && !tm.stopped {
			n++
		}
	}
	return n
}

// WaitPending blocks until at least n timers are pending, failing the test
// after three seconds. It lets a test observe that code under test has reached
// a backoff or timeout without sleeping.
func (c *FakeClock) WaitPending(t testing.TB, n int) {
	t.Helper()
	waitUntil(t, "pending fake timers", c.changedCh, func() bool {
		return c.Pending() >= n
	})
}

func (c *FakeClock) changedCh() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.changed
}

// notifyLocked wakes every waiter. The caller must hold c.mu.
func (c *FakeClock) notifyLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// waitUntil evaluates cond every time the channel returned by changed fires,
// and fails the test when cond is still false after eventTimeout. cond and
// changed must not be called with any lock held by the caller; each fakes type
// evaluates cond without its own lock so cond may call the fake's methods.
func waitUntil(t testing.TB, what string, changed func() <-chan struct{}, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		ch := changed()
		if cond() {
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("linktest: timed out waiting for %s", what)
		}
		select {
		case <-ch:
		case <-time.After(remaining):
		}
	}
}
