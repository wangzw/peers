package inbox

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWatch_UnresolvableConfigDir reports a missing home from both entry points
// and starts a watcher once a home exists.
func TestWatch_UnresolvableConfigDir(t *testing.T) {
	t.Setenv("HOME", "")
	if w, entry, err := makeWatcher("", func(int) {}); err == nil || w != nil || entry != nil ||
		!strings.Contains(err.Error(), "cannot resolve the home directory") {
		t.Fatalf("makeWatcher with an unresolvable home = (%v, %v, %v), want the home directory error", w, entry, err)
	}
	if stop, err := Watch("", func(int) {}); err == nil || stop != nil ||
		!strings.Contains(err.Error(), "cannot resolve the home directory") {
		t.Fatalf("Watch with an unresolvable home error = %v (stop returned: %t), want the home directory error", err, stop != nil)
	}

	// Positive control: with a home the same call starts a watcher.
	t.Setenv("HOME", t.TempDir())
	stop, err := Watch("", func(int) {})
	if err != nil {
		t.Fatalf("Watch with a resolvable home: %v", err)
	}
	stop()
}

// aWatchResult is one Watch call's outcome.
type aWatchResult struct {
	stop func()
	err  error
}

// TestWatch_WaiterUsesCreationInFlight proves that a second Watch joins the
// creation the first Watch has in flight instead of building a second watcher.
func TestWatch_WaiterUsesCreationInFlight(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCreation := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseCreation)

	var installed atomic.Int32
	var enterOnce sync.Once
	testWatchAfterInstall = func() {
		installed.Add(1)
		enterOnce.Do(func() { close(entered) })
		<-release
	}
	t.Cleanup(func() { testWatchAfterInstall = nil })

	firstPIDs := make(chan int, 16)
	firstResults := make(chan aWatchResult, 1)
	go func() {
		stop, err := Watch(dir, func(pid int) { firstPIDs <- pid })
		firstResults <- aWatchResult{stop: stop, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(aWaitTimeout):
		t.Fatal("the first Watch did not reach the blocked creation")
	}

	// The hook lets the test release the creator only after the waiter has
	// found the in-flight creation.
	waiting := make(chan struct{})
	var waitOnce sync.Once
	testWatchBeforePendingWait = func() { waitOnce.Do(func() { close(waiting) }) }
	t.Cleanup(func() { testWatchBeforePendingWait = nil })

	secondPIDs := make(chan int, 16)
	secondResults := make(chan aWatchResult, 1)
	go func() {
		stop, err := Watch(dir, func(pid int) { secondPIDs <- pid })
		secondResults <- aWatchResult{stop: stop, err: err}
	}()
	select {
	case <-waiting:
	case <-time.After(aWaitTimeout):
		t.Fatal("the second Watch did not wait for the creation in flight")
	}

	releaseCreation()
	first := aWaitWatchResult(t, firstResults, "the first Watch")
	second := aWaitWatchResult(t, secondResults, "the second Watch")
	t.Cleanup(func() {
		if first.stop != nil {
			first.stop()
		}
	})
	t.Cleanup(func() {
		if second.stop != nil {
			second.stop()
		}
	})
	if first.err != nil {
		t.Fatalf("first Watch: %v", first.err)
	}
	if second.err != nil {
		t.Fatalf("second Watch: %v", second.err)
	}
	if got := installed.Load(); got != 1 {
		t.Fatalf("watcher creations = %d, want 1: the second Watch built its own watcher", got)
	}

	// Both registrations are on one watcher: the record reaches both of them.
	const pid = 7701
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-creation.sock",
	})
	aWaitPID(t, firstPIDs, pid)
	aWaitPID(t, secondPIDs, pid)

	second.stop()
	first.stop()
	if got := sharedWatcherCount(dir); got != 0 {
		t.Fatalf("sharedWatcherCount = %d after both stops, want 0", got)
	}
}

// aWaitWatchResult returns one Watch result, failing the test if it does not
// arrive within aWaitTimeout.
func aWaitWatchResult(t *testing.T, results <-chan aWatchResult, what string) aWatchResult {
	t.Helper()
	select {
	case r := <-results:
		return r
	case <-time.After(aWaitTimeout):
		t.Fatalf("%s did not return", what)
		return aWatchResult{}
	}
}

// TestWatch_UnwatchableRegistryFallsBack reports a record written after an
// unwatchable registry directory becomes readable.
func TestWatch_UnwatchableRegistryFallsBack(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	dir := t.TempDir()
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}
	if err := os.Chmod(sessions, 0o000); err != nil {
		t.Fatalf("removing permissions from the registry directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(sessions, 0o700) })

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch on an unwatchable registry directory: %v", err)
	}
	defer stop()

	// Making the registry readable changes what the watcher can observe; the
	// event on the configuration directory makes it choose its target again.
	if err := os.Chmod(sessions, 0o700); err != nil {
		t.Fatalf("restoring registry directory permissions: %v", err)
	}
	const pid = 7801
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-fallback.sock",
	})
	aWriteFile(t, filepath.Join(dir, "trigger"), []byte("x"))

	aWaitPID(t, pids, pid)
}

// TestWatch_SwitchedReconcileNotifiesZero reports 0 for an event that arrives
// while the watch moves down to the configuration directory.
func TestWatch_SwitchedReconcileNotifiesZero(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")

	pids := make(chan int, 16)
	w, err := newWatcher(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("newWatcher on a missing configuration directory: %v", err)
	}
	t.Cleanup(w.shutdown)

	// The watcher started above the configuration directory and reported once
	// with 0 for the caller to re-read.
	aWaitPID(t, pids, 0)
	aDrain(pids)

	// The configuration directory appears while the sibling's event is handled,
	// so that event belongs to the old watch.
	var created sync.Once
	createConfig := func() {
		created.Do(func() {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Errorf("creating the configuration directory: %v", err)
			}
		})
	}
	testWatchBeforeHandleEvent.Store(&createConfig)
	t.Cleanup(func() { testWatchBeforeHandleEvent.Store(nil) })

	aWriteFile(t, filepath.Join(root, "sibling"), []byte("x"))
	aWaitPID(t, pids, 0)

	// The watch moved to the configuration directory, so a record written there
	// is reported.
	const pid = 7901
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-switched.sock",
	})
	aWaitPID(t, pids, pid)
}

// TestPathWithin pins the strict inside test: a path is inside its parent but
// not itself, not above it, and not a sibling that shares its name prefix.
func TestPathWithin(t *testing.T) {
	cases := []struct {
		name   string
		parent string
		child  string
		want   bool
	}{
		{"child", "/a/b", "/a/b/c", true},
		{"itself", "/a/b", "/a/b", false},
		{"ancestor", "/a/b", "/a", false},
		{"sibling with shared prefix", "/a", "/ab", false},
		{"unrelated", "/a/b", "/c/d", false},
		// A relative parent and an absolute child cannot be made relative, so
		// the helper reports false instead of guessing.
		{"mixed spelling", "relative", "/absolute", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pathWithin(tc.parent, tc.child); got != tc.want {
				t.Fatalf("pathWithin(%q, %q) = %v, want %v", tc.parent, tc.child, got, tc.want)
			}
		})
	}
}
