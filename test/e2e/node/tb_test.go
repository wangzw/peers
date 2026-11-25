package node

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

var _ testing.TB = (*TB)(nil)

// TestTBDrivesEveryFakeMethod calls every method of fakesession.Fake through
// the adapter. A method that falls through to the nil embedded testing.TB
// panics with a nil pointer dereference instead of a FatalError, which Guard
// re-raises, so the test fails.
func TestTBDrivesEveryFakeMethod(t *testing.T) {
	tb := NewTB("x", t.TempDir())

	var (
		fake       *fakesession.Fake
		recordPath string
		socketPath string
	)
	err := Guard(func() {
		configDir := tb.TempDir()
		fake = fakesession.New(tb, fakesession.Options{Name: "a", ConfigDir: configDir})
		fake.SetName("b")
		fake.Rename("c")
		fake.SetStatus("idle")
		fake.SetBusy(true)
		fake.SetBusy(false)
		fake.SetSessionID("session-1")
		fake.RemoveRecord()
		fake.RestoreRecord()
		fake.SetMode(fakesession.HoldOpen)
		fake.SetBusyDelay(time.Millisecond)
		fake.Release()
		fake.Frames()
		fake.WaitFrame(10 * time.Millisecond)
		fake.WaitFrameN(1, 10*time.Millisecond)
		recordPath = fake.RecordPath()
		socketPath = fake.SocketPath()
		fake.RemoveSocket()
		fake.Close()
	})
	if err != nil {
		t.Fatalf("driving fakesession through TB: %v", err)
	}
	if errs := tb.TakeErrors(); len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if socketPath == "" {
		t.Fatal("fake did not bind a socket")
	}

	tb.Close()

	if _, err := os.Stat(recordPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("record %s still present after Close: %v", recordPath, err)
	}
	if _, err := os.Stat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket %s still present after Close: %v", socketPath, err)
	}
}

// TestTBFatalBecomesError checks that a fatal failure inside fakesession is
// returned as a FatalError instead of ending the process or hanging the
// caller, and that the same message is available through TakeErrors.
func TestTBFatalBecomesError(t *testing.T) {
	tb := NewTB("x", t.TempDir())
	dir := filepath.Join(socktest.Dir(t), "readonly")
	if err := os.Mkdir(dir, 0o500); err != nil {
		t.Fatalf("creating read-only directory: %v", err)
	}
	socketPath := filepath.Join(dir, "sock")

	err := Guard(func() {
		fakesession.New(tb, fakesession.Options{Name: "a", ConfigDir: t.TempDir(), SocketPath: socketPath})
	})
	if err == nil {
		t.Fatal("fakesession.New succeeded on a read-only directory")
	}
	var fatal FatalError
	if !errors.As(err, &fatal) {
		t.Fatalf("error is %T, want FatalError: %v", err, err)
	}
	if !strings.Contains(fatal.Error(), "fakesession: binding") {
		t.Fatalf("FatalError %q does not mention binding", fatal.Error())
	}
	// FatalError carries the formatted message rather than the wrapped error,
	// so EACCES is matched by the text it produces.
	if msg := fatal.Error(); !strings.Contains(msg, "permission denied") {
		t.Fatalf("FatalError %q is not a permission error (EACCES)", msg)
	}
	errs := tb.TakeErrors()
	if len(errs) != 1 || !strings.Contains(errs[0], "fakesession: binding") {
		t.Fatalf("TakeErrors() = %q, want one binding error", errs)
	}
	tb.Close()
}

// TestTBCleanupOrderAndIdempotence checks that Close runs the registered
// cleanups once each, in reverse order, even when called twice.
func TestTBCleanupOrderAndIdempotence(t *testing.T) {
	tb := NewTB("x", t.TempDir())
	var order []int
	for _, n := range []int{1, 2, 3} {
		tb.Cleanup(func() { order = append(order, n) })
	}

	tb.Close()
	tb.Close()

	if want := []int{3, 2, 1}; !slices.Equal(order, want) {
		t.Fatalf("cleanup order = %v, want %v", order, want)
	}
}

// TestGuardRepanicsOthers checks that Guard does not swallow a panic that is
// not a FatalError.
func TestGuardRepanicsOthers(t *testing.T) {
	repanicked := false
	func() {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			repanicked = true
			if r != "x" {
				t.Errorf("re-raised %v, want \"x\"", r)
			}
		}()
		_ = Guard(func() { panic("x") })
	}()
	if !repanicked {
		t.Fatal("Guard swallowed a non-FatalError panic")
	}
}
