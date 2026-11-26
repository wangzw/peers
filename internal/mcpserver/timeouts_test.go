package mcpserver

import (
	"strings"
	"testing"
	"time"
)

// eventTimeout is the package-wide fallback for a wait on an event the test
// cannot force.
const eventTimeout = 30 * time.Second

// handlerTimeoutSeam stands in for the package constant, and
// handlerTimeoutBackstop fails a handler that does not bound its Broker call.
const (
	handlerTimeoutSeam     = 300 * time.Millisecond
	handlerTimeoutBackstop = 5 * time.Second
)

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
