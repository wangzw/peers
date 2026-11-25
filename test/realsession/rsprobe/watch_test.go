package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// matchEventGap separates the two poll events in the matchEvents test so one
	// notify event can cover only the earlier of them.
	matchEventGap = 10 * time.Second
)

// watchLog collects events from the watcher and wakes waiters when one arrives,
// so the test never depends on a fixed sleep.
type watchLog struct {
	mu     sync.Mutex
	events []event
	wake   chan struct{}
}

func newWatchLog() *watchLog {
	return &watchLog{wake: make(chan struct{}, 1)}
}

func (l *watchLog) add(e event) error {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return nil
}

func (l *watchLog) snapshot() []event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]event(nil), l.events...)
}

func (l *watchLog) count(pred func(event) bool) int {
	n := 0
	for _, e := range l.snapshot() {
		if pred(e) {
			n++
		}
	}
	return n
}

// waitUntil waits for pred to hold, up to eventTimeout.
func (l *watchLog) waitUntil(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		if pred() {
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for %s; events: %+v", what, l.snapshot())
		}
		select {
		case <-l.wake:
		case <-time.After(remaining):
		}
	}
}

func srcIs(src, op, file string) func(event) bool {
	return func(e event) bool { return e.Src == src && e.Op == op && e.File == file }
}

func notifyIs(file string, opPart string) func(event) bool {
	return func(e event) bool {
		return e.Src == "notify" && e.File == file && strings.Contains(e.Op, opPart)
	}
}

// TestWatchNoMissesOnInPlaceRewrites checks that in-place rewrites, creates and
// removes produce no unmatched poll observations.
func TestWatchNoMissesOnInPlaceRewrites(t *testing.T) {
	dir := t.TempDir()
	log := newWatchLog()
	w, err := newWatcher(dir, 50*time.Millisecond, time.Now, log.add)
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.run(ctx) }()
	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		return <-done
	}
	defer func() {
		_ = stop()
		w.close()
	}()

	path := filepath.Join(dir, "s.json")
	if err := os.WriteFile(path, []byte("0"), 0o600); err != nil {
		t.Fatalf("create: %v", err)
	}
	log.waitUntil(t, "poll create of s.json", func() bool { return log.count(srcIs("poll", "create", "s.json")) > 0 })
	log.waitUntil(t, "notify create of s.json", func() bool { return log.count(notifyIs("s.json", "CREATE")) > 0 })

	for i := 1; i <= 20; i++ {
		beforePoll := log.count(srcIs("poll", "modify", "s.json"))
		beforeNotify := log.count(notifyIs("s.json", "WRITE"))
		// Truncate and rewrite in place, the way Claude Code rewrites its
		// session records.
		if err := os.WriteFile(path, []byte(fmt.Sprintf("%d", i)), 0o600); err != nil {
			t.Fatalf("rewrite %d: %v", i, err)
		}
		log.waitUntil(t, fmt.Sprintf("poll modify %d", i), func() bool {
			return log.count(srcIs("poll", "modify", "s.json")) > beforePoll
		})
		log.waitUntil(t, fmt.Sprintf("notify write %d", i), func() bool {
			return log.count(notifyIs("s.json", "WRITE")) > beforeNotify
		})
	}

	drop := filepath.Join(dir, "drop.json")
	if err := os.WriteFile(drop, []byte("gone"), 0o600); err != nil {
		t.Fatalf("create drop: %v", err)
	}
	log.waitUntil(t, "poll create of drop.json", func() bool { return log.count(srcIs("poll", "create", "drop.json")) > 0 })
	log.waitUntil(t, "notify create of drop.json", func() bool { return log.count(notifyIs("drop.json", "CREATE")) > 0 })

	beforeDropNotify := log.count(func(e event) bool { return e.Src == "notify" && e.File == "drop.json" })
	if err := os.Remove(drop); err != nil {
		t.Fatalf("remove drop: %v", err)
	}
	log.waitUntil(t, "poll remove of drop.json", func() bool { return log.count(srcIs("poll", "remove", "drop.json")) > 0 })
	log.waitUntil(t, "notify event for removal of drop.json", func() bool {
		return log.count(func(e event) bool { return e.Src == "notify" && e.File == "drop.json" }) > beforeDropNotify
	})

	if err := stop(); err != nil {
		t.Fatalf("watcher run: %v", err)
	}
	polls, notifies := w.collected()
	if misses := matchEvents(polls, notifies, watchMatchWindow); len(misses) != 0 {
		t.Fatalf("misses = %+v, want none (polls=%d notifies=%d)", misses, len(polls), len(notifies))
	}
}

// TestMatchEventsReportsMiss checks that a poll event outside the window of any
// notify event is reported as a miss.
func TestMatchEventsReportsMiss(t *testing.T) {
	base := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	polls := []event{
		{T: base, Src: "poll", Op: "modify", File: "a.json"},
		{T: base.Add(matchEventGap), Src: "poll", Op: "modify", File: "a.json"},
	}
	notifies := []event{
		{T: base, Src: "notify", Op: "WRITE", File: "a.json"},
	}
	misses := matchEvents(polls, notifies, watchMatchWindow)
	if len(misses) != 1 {
		t.Fatalf("misses = %+v, want exactly the later poll event", misses)
	}
	if !misses[0].T.Equal(polls[1].T) {
		t.Fatalf("missed event at %s, want %s", misses[0].T, polls[1].T)
	}
}
