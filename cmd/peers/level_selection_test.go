package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/mcpserver/termcmd"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// levelHits counts the RPCs each fake Broker received, keyed by level and
// method; the fake serves on its own goroutine, so a mutex guards the counters.
type levelHits struct {
	mu      sync.Mutex
	methods map[string]map[string]int
}

func (h *levelHits) add(level, method string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.methods[level] == nil {
		h.methods[level] = make(map[string]int)
	}
	h.methods[level][method]++
}

func (h *levelHits) count(level, method string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.methods[level][method]
}

func levelHandlers(hits *levelHits, level string) fakelocal.Handlers {
	return fakelocal.RecordingHandlers(func(method string) { hits.add(level, method) })
}

func startLevelSelection(t *testing.T) (*levelHits, *app, *bytes.Buffer) {
	t.Helper()

	hits := &levelHits{methods: make(map[string]map[string]int)}
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, levelHandlers(hits, "user"))
	systemPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, systemPath, levelHandlers(hits, "system"))

	original := findBrokerForLevel
	t.Cleanup(func() { findBrokerForLevel = original })
	findBrokerForLevel = func(ctx context.Context, system, _ bool) (string, termcmd.Dialer, error) {
		path := userPath
		if system {
			path = systemPath
		}
		return fakelocal.Selector(path)(ctx)
	}

	stderr := new(bytes.Buffer)
	a := &app{
		stdin:   strings.NewReader(""),
		stdout:  new(bytes.Buffer),
		stderr:  stderr,
		version: "v0.0.0-test",
		deps:    realDeps("v0.0.0-test"),
	}
	return hits, a, stderr
}

func assertLevelRouting(t *testing.T, a *app, hits *levelHits, args []string, method string, stderr *bytes.Buffer) {
	t.Helper()

	cases := []struct{ flag, want, other string }{
		{"--system", "system", "user"},
		{"--user", "user", "system"},
	}
	for _, tc := range cases {
		beforeWant := hits.count(tc.want, method)
		beforeOther := hits.count(tc.other, method)

		run := append(append([]string(nil), args...), tc.flag)
		if got := a.run(context.Background(), run); got != exitOK {
			t.Fatalf("%v = %d, want %d (stderr %q)", run, got, exitOK, stderr.String())
		}
		if got := hits.count(tc.want, method) - beforeWant; got != 1 {
			t.Fatalf("%v reached the %s-level Broker %d times, want 1", run, tc.want, got)
		}
		if got := hits.count(tc.other, method) - beforeOther; got != 0 {
			t.Fatalf("%v also reached the %s-level Broker %d times, want 0", run, tc.other, got)
		}
	}
}

// TestStatusSelectsTheLevelSocket checks that service status reaches the Broker
// of the selected level.
func TestStatusSelectsTheLevelSocket(t *testing.T) {
	hits, a, stderr := startLevelSelection(t)
	assertLevelRouting(t, a, hits, []string{"service", "status"}, "Status", stderr)
}

// TestAuditSelectsTheLevelSocket checks that service audit reaches the Broker of
// the selected level.
func TestAuditSelectsTheLevelSocket(t *testing.T) {
	hits, a, stderr := startLevelSelection(t)
	assertLevelRouting(t, a, hits, []string{"service", "audit"}, "Audit", stderr)
}

// TestOnlineSelectsTheLevelSocket checks that service online reaches the Broker
// of the selected level.
func TestOnlineSelectsTheLevelSocket(t *testing.T) {
	hits, a, stderr := startLevelSelection(t)
	assertLevelRouting(t, a, hits, []string{"service", "online"}, "SetConfig", stderr)
}

// TestOfflineSelectsTheLevelSocket checks that service offline reaches the Broker
// of the selected level.
func TestOfflineSelectsTheLevelSocket(t *testing.T) {
	hits, a, stderr := startLevelSelection(t)
	assertLevelRouting(t, a, hits, []string{"service", "offline"}, "SetConfig", stderr)
}
