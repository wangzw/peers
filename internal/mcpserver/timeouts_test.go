package mcpserver

import (
	"strings"
	"testing"
	"time"
)

// eventTimeout is the package-wide fallback for a wait on an event that is
// expected to arrive but that the test cannot force, whether the event is a
// child MCP process answering on stdio, a Register stream opened on a real
// Unix socket, or a registration returning. It only decides how long a wait
// that will never complete takes to fail: the success path selects on the
// event itself and never spends this time, so a larger value costs nothing.
// Thirty seconds is far above the worst normal latency under load; it is not a
// claim that an event should take that long.
const eventTimeout = 30 * time.Second

// handlerTimeoutSeam is the short Bound the handler timeout tests set on the
// handler field, standing in for the package constant. handlerTimeoutBackstop
// is the test's own bound: a handler that does not bound its Broker call never
// returns, and the backstop fails the test instead of hanging the package.
const (
	handlerTimeoutSeam     = 300 * time.Millisecond
	handlerTimeoutBackstop = 5 * time.Second
)

// waitBoundedCall runs call in a goroutine and fails the test when it has not
// returned within handlerTimeoutBackstop. It returns how long the call took and
// its error.
func waitBoundedCall(t *testing.T, name string, call func() error) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		return time.Since(start), err
	case <-time.After(handlerTimeoutBackstop):
		t.Fatalf("%s did not return within %s; the handler does not bound its Broker call", name, handlerTimeoutBackstop)
		return 0, nil
	}
}

// checkBoundedError asserts that a call made against a Broker that never
// answers returned an error, no earlier than roughly the seam, containing every
// text in wants.
func checkBoundedError(t *testing.T, name string, err error, elapsed time.Duration, wants ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded although the Broker never answered", name)
	}
	if elapsed < handlerTimeoutSeam/2 {
		t.Errorf("%s returned after %s, want about %s", name, elapsed, handlerTimeoutSeam)
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s error = %q, want it to contain %q", name, err, want)
		}
	}
}
