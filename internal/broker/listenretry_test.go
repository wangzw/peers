package broker

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// manualClock is a Clock the test fires by hand, so backoff is deterministic.
type manualClock struct {
	mu     sync.Mutex
	timers []*manualTimer
	signal chan struct{}
}

type manualTimer struct {
	d time.Duration
	f func()

	mu      sync.Mutex
	stopped bool
	fired   bool
}

func newManualClock() *manualClock {
	return &manualClock{signal: make(chan struct{}, 1)}
}

func (c *manualClock) Now() time.Time { return time.Time{} }

func (c *manualClock) AfterFunc(d time.Duration, f func()) Timer {
	t := &manualTimer{d: d, f: f}
	c.mu.Lock()
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	select {
	case c.signal <- struct{}{}:
	default:
	}
	return t
}

// nextTimer waits for the timer at index n and returns it.
func (c *manualClock) nextTimer(t *testing.T, n int) *manualTimer {
	t.Helper()
	deadline := time.After(eventTimeout)
	for {
		c.mu.Lock()
		if len(c.timers) > n {
			timer := c.timers[n]
			c.mu.Unlock()
			return timer
		}
		c.mu.Unlock()
		select {
		case <-c.signal:
		case <-deadline:
			t.Fatalf("the retrier registered %d timers, want at least %d", len(c.timers), n+1)
		}
	}
}

func (t *manualTimer) fire() {
	t.mu.Lock()
	if t.stopped || t.fired {
		t.mu.Unlock()
		return
	}
	t.fired = true
	f := t.f
	t.mu.Unlock()
	f()
}

func (t *manualTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestListenRetrierBackoffSequenceAndCap(t *testing.T) {
	clock := newManualClock()
	var mu sync.Mutex
	attempts := 0
	r := newListenRetrier(
		func(string) error { mu.Lock(); attempts++; mu.Unlock(); return errors.New("address already in use") },
		func() (string, error) { return "127.0.0.1:17443", nil },
		clock, nil, nil, nil,
	)
	r.start()
	defer r.stop()

	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, time.Minute, time.Minute,
	}
	for i, d := range want {
		timer := clock.nextTimer(t, i)
		if timer.d != d {
			t.Fatalf("wait %d = %s, want %s", i+1, timer.d, d)
		}
		timer.fire()
		eventually(t, "an attempt", func() bool {
			mu.Lock()
			defer mu.Unlock()
			return attempts == i+1
		})
	}
}

func TestListenRetrierAddressChangeResetsBackoff(t *testing.T) {
	clock := newManualClock()
	var mu sync.Mutex
	addr := "127.0.0.1:17443"
	var seen []string
	r := newListenRetrier(
		func(a string) error {
			mu.Lock()
			seen = append(seen, a)
			mu.Unlock()
			return errors.New("address already in use")
		},
		func() (string, error) { mu.Lock(); defer mu.Unlock(); return addr, nil },
		clock, nil, nil, nil,
	)
	r.start()
	defer r.stop()

	first := clock.nextTimer(t, 0)
	_ = first
	mu.Lock()
	addr = "127.0.0.1:18000"
	mu.Unlock()
	r.retryNow()

	eventually(t, "the immediate retry", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 1
	})
	mu.Lock()
	got := seen[0]
	mu.Unlock()
	if got != "127.0.0.1:18000" {
		t.Fatalf("immediate retry bound %q, want the new address", got)
	}
	// After the immediate attempt fails, the backoff starts again at 1s.
	second := clock.nextTimer(t, 1)
	if second.d != time.Second {
		t.Fatalf("next wait = %s, want 1s after an address change", second.d)
	}
}

func TestListenRetrierStopsAfterSuccess(t *testing.T) {
	clock := newManualClock()
	var mu sync.Mutex
	attempts := 0
	var succeeded string
	r := newListenRetrier(
		func(string) error {
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if attempts >= 3 {
				return nil
			}
			return errors.New("address already in use")
		},
		func() (string, error) { return "127.0.0.1:17443", nil },
		clock, nil,
		func(addr string) (string, error) { mu.Lock(); succeeded = addr; mu.Unlock(); return addr, nil },
		nil,
	)
	r.start()
	defer r.stop()

	clock.nextTimer(t, 0).fire()
	clock.nextTimer(t, 1).fire()
	clock.nextTimer(t, 2).fire()
	eventually(t, "the success callback", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return succeeded != ""
	})
	if succeeded != "127.0.0.1:17443" {
		t.Fatalf("success address = %q", succeeded)
	}
}

func TestListenRetrierReportsOnlyChangedReasons(t *testing.T) {
	clock := newManualClock()
	var mu sync.Mutex
	addr := "127.0.0.1:17443"
	bindErr := errors.New("address already in use")
	failures := 0
	r := newListenRetrier(
		func(string) error { mu.Lock(); defer mu.Unlock(); return bindErr },
		func() (string, error) { mu.Lock(); defer mu.Unlock(); return addr, nil },
		clock, nil, nil,
		func(string, string, error) { mu.Lock(); failures++; mu.Unlock() },
	)
	r.start()
	defer r.stop()

	clock.nextTimer(t, 0).fire()
	eventually(t, "the first failure", func() bool { mu.Lock(); defer mu.Unlock(); return failures == 1 })
	clock.nextTimer(t, 1).fire()
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if failures != 1 {
		t.Fatalf("the same reason was reported %d times, want 1", failures)
	}
	mu.Unlock()

	mu.Lock()
	bindErr = errors.New("permission denied")
	mu.Unlock()
	clock.nextTimer(t, 2).fire()
	eventually(t, "the changed reason", func() bool { mu.Lock(); defer mu.Unlock(); return failures == 2 })
}

func TestListenRetrierStopsCleanly(t *testing.T) {
	clock := newManualClock()
	var mu sync.Mutex
	attempts := 0
	r := newListenRetrier(
		func(string) error { mu.Lock(); attempts++; mu.Unlock(); return errors.New("in use") },
		func() (string, error) { return "127.0.0.1:17443", nil },
		clock, nil, nil, nil,
	)
	r.start()
	timer := clock.nextTimer(t, 0)
	r.stop()
	timer.fire()
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if attempts != 0 {
		t.Fatalf("the retrier made %d attempts after Stop, want 0", attempts)
	}
}
