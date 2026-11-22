package broker

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func testWatchLogger() *slog.Logger {
	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	return NewLogger(io.Discard, level)
}

// fakeConfigDirSource delivers the watcher's events from the test.
type fakeConfigDirSource struct {
	events chan fsnotify.Event
	errors chan error
}

func (s *fakeConfigDirSource) Events() <-chan fsnotify.Event { return s.events }
func (s *fakeConfigDirSource) Errors() <-chan error          { return s.errors }
func (s *fakeConfigDirSource) Close() error                  { return nil }

// TestConfigDirWatchReportsChanges checks that a write to the configuration
// file wakes the diagnostics-only retry.
func TestConfigDirWatchReportsChanges(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configFile, []byte("broker: {}\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	w := startConfigDirWatch(configFile, testWatchLogger())
	t.Cleanup(w.stopNow)

	if err := os.WriteFile(configFile, []byte("broker:\n  listen: ''\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	select {
	case <-w.events:
	case <-time.After(eventTimeout):
		t.Fatal("the configuration write did not wake the retry")
	}
}

// TestConfigDirWatchDebouncesBurst checks that changes inside one debounce
// window produce one wake, and that a later change starts a new window. The
// events and the clock are driven by the test, so the result does not depend on
// when the watcher goroutine is scheduled: with a real clock, a burst whose
// events were processed on both sides of the window woke the retry twice.
func TestConfigDirWatchDebouncesBurst(t *testing.T) {
	const name = "config.yaml"
	clock := newFakeClock(time.Now())
	source := &fakeConfigDirSource{
		events: make(chan fsnotify.Event),
		errors: make(chan error),
	}
	w := newConfigDirWatch(source, testWatchLogger(), clock)
	go w.run(name)
	t.Cleanup(w.stopNow)

	// noWake fails on a wake left in the buffer. Call it after a send to the
	// unbuffered source: that send completes only when the watcher is back at
	// select, so any wake produced earlier has already been sent.
	noWake := func() {
		t.Helper()
		select {
		case <-w.events:
			t.Fatal("an unexpected wake reached the retry")
		default:
		}
	}

	// change delivers one event. The send returning proves the watcher read
	// it; waiting for the new timer proves the debounce was armed.
	change := func() {
		t.Helper()
		want := clock.createdTimers() + 1
		source.events <- fsnotify.Event{Name: name, Op: fsnotify.Write}
		waitForTimers(t, clock, want)
		noWake()
	}

	// waitWake consumes one wake, bounded only so a broken watcher fails.
	waitWake := func() {
		t.Helper()
		select {
		case <-w.events:
		case <-time.After(eventTimeout):
			t.Fatal("a change did not wake the retry")
		}
	}

	// An event for another file, and one of a kind the watcher ignores, do not
	// arm the debounce.
	before := clock.createdTimers()
	source.events <- fsnotify.Event{Name: "other.yaml", Op: fsnotify.Write}
	source.events <- fsnotify.Event{Name: name, Op: fsnotify.Chmod}
	change()
	if got := clock.createdTimers(); got != before+1 {
		t.Fatalf("ignored events armed the debounce: timers registered = %d, want %d", got, before+1)
	}

	// The window ends one debounce after the change, not earlier.
	clock.Advance(configDirWatchDebounce - 1)
	if got := clock.pendingTimers(); got != 1 {
		t.Fatalf("timers before the window ended = %d, want 1", got)
	}
	noWake()
	clock.Advance(1)
	waitWake()

	// A later change restarts the window: half a window after the first change
	// and another half plus a nanosecond later, the last change's window is
	// still open. A deadline counted from the first change would have fired.
	change()
	clock.Advance(configDirWatchDebounce / 2)
	change()
	clock.Advance(configDirWatchDebounce/2 + 1)
	if got := clock.pendingTimers(); got != 1 {
		t.Fatalf("timers a nanosecond past the first change's window = %d, want 1", got)
	}
	noWake()
	clock.Advance(configDirWatchDebounce/2 - 1)
	waitWake()

	// Changes inside one window wake the retry once. The send after the window
	// is the barrier for the assertion: it completes only after the watcher is
	// back at select, so a second wake produced by the burst would be visible.
	change()
	change()
	change()
	if got := clock.pendingTimers(); got != 1 {
		t.Fatalf("timers while the burst is open = %d, want 1", got)
	}
	clock.Advance(configDirWatchDebounce)
	waitWake()
	if got := clock.pendingTimers(); got != 0 {
		t.Fatalf("the burst left %d timer(s) pending, want 0", got)
	}
	change()

	// A change after the window starts a new one.
	clock.Advance(configDirWatchDebounce)
	waitWake()
	noWake()
}

// waitForTimers waits until the clock has registered want timers.
func waitForTimers(t *testing.T, clock *fakeClock, want int) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for clock.createdTimers() < want {
		if !time.Now().Before(deadline) {
			t.Fatalf("timers registered = %d, want %d", clock.createdTimers(), want)
		}
		runtime.Gosched()
	}
}

// TestConfigDirWatchStopClosesCleanly checks that stopping closes the watcher
// and that a write afterwards neither wakes the retry nor blocks shutdown.
func TestConfigDirWatchStopClosesCleanly(t *testing.T) {
	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configFile, []byte("broker: {}\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	w := startConfigDirWatch(configFile, testWatchLogger())
	w.stopNow()
	select {
	case <-w.done:
	default:
		t.Fatal("stopNow returned before the watcher finished")
	}
	if err := os.WriteFile(configFile, []byte("broker:\n  listen: ''\n"), 0o600); err != nil {
		t.Fatalf("WriteFile after stop: %v", err)
	}
	// The window is the assertion: a wake after stop would be a violation. A
	// window can only miss one under load, never fail a correct run, so it stays.
	select {
	case <-w.events:
		t.Fatal("a write after stop woke the retry")
	case <-time.After(100 * time.Millisecond):
	}
	w.stopNow() // idempotent
}

// TestConfigDirWatchWithoutConfigFileIsInert covers a Load failure that left no
// configuration path: the watcher is inert and the minute tick still retries.
func TestConfigDirWatchWithoutConfigFileIsInert(t *testing.T) {
	w := startConfigDirWatch("", testWatchLogger())
	select {
	case <-w.done:
	default:
		t.Fatal("the inert watcher did not finish")
	}
	w.stopNow()
}
