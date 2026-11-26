// Package node provides the Broker, MCP server and simulated session processes
// the end-to-end suite drives, together with the probes and helpers that
// inspect them.
package node

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

// TB satisfies testing.TB for fakesession outside a test binary. Only the
// methods fakesession calls are implemented; any other method panics.
type TB struct {
	// The embedded interface is always nil; it exists only to promote the
	// unexported methods of testing.TB, so that *TB satisfies the interface at
	// compile time.
	testing.TB

	mu       sync.Mutex
	name     string
	tempRoot string
	cleanups []func()
	errs     []string
	closed   bool
}

// NewTB returns a TB named name. Temporary directories are created under
// tempRoot, or under the default temporary directory when it is empty.
func NewTB(name, tempRoot string) *TB {
	return &TB{name: name, tempRoot: tempRoot}
}

// Helper does nothing. It exists so fakesession can call it.
func (t *TB) Helper() {}

// Name returns the name given to NewTB.
func (t *TB) Name() string { return t.name }

// TempDir creates a temporary directory under the root given to NewTB and
// registers its removal with Cleanup. A failure is fatal: it is recorded and
// raised as a FatalError panic, like every other failure of this TB.
func (t *TB) TempDir() string {
	dir, err := os.MkdirTemp(t.tempRoot, "tb-")
	if err != nil {
		t.Fatalf("node: creating temporary directory: %v", err)
		return ""
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// Cleanup registers f to run when Close is called. After Close it runs f
// immediately.
func (t *TB) Cleanup(f func()) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		f()
		return
	}
	t.cleanups = append(t.cleanups, f)
	t.mu.Unlock()
}

// Errorf records a formatted error. It never panics.
func (t *TB) Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	t.mu.Lock()
	t.errs = append(t.errs, msg)
	t.mu.Unlock()
}

// Fatalf records a formatted error and panics with FatalError, so Guard can
// turn it into an ordinary error in the node agent's request handler.
func (t *TB) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	t.mu.Lock()
	t.errs = append(t.errs, msg)
	t.mu.Unlock()
	panic(FatalError{Msg: msg})
}

// Logf writes a formatted line to standard error.
func (t *TB) Logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", t.name, msg)
}

// Close runs the registered cleanups in reverse order, each exactly once. It
// is idempotent.
func (t *TB) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	cleanups := t.cleanups
	t.cleanups = nil
	t.mu.Unlock()

	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

// TakeErrors returns the recorded errors and forgets them.
func (t *TB) TakeErrors() []string {
	t.mu.Lock()
	errs := t.errs
	t.errs = nil
	t.mu.Unlock()
	return errs
}

// FatalError is raised by TB.Fatalf. Guard recovers it and returns it as an
// error.
type FatalError struct {
	// Msg is the formatted message the failing check passed to Fatalf.
	Msg string
}

// Error returns the message given to Fatalf.
func (e FatalError) Error() string { return e.Msg }

// Guard runs fn and recovers a FatalError panic into an error. Any other panic
// is re-raised unchanged, so a programming error is not reported as a request
// failure.
func Guard(fn func()) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if fatal, ok := r.(FatalError); ok {
			err = fatal
			return
		}
		panic(r)
	}()
	fn()
	return nil
}
