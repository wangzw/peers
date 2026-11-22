package mcpserver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// resetFallbackBaseCache clears the cached getconf result so each test starts
// from an unresolvable state.
func resetFallbackBaseCache() {
	fallbackBaseCache.mu.Lock()
	fallbackBaseCache.dirs = make(map[int]string)
	fallbackBaseCache.mu.Unlock()
}

// TestDefaultFallbackRuntimeBasePerPlatform covers the platform branch: the
// logind directory on Linux, and the per-user temporary directory getconf
// reports on macOS. The macOS branch also runs in the macOS probe job.
func TestDefaultFallbackRuntimeBasePerPlatform(t *testing.T) {
	if runtime.GOOS != "darwin" {
		got, err := defaultFallbackRuntimeBase(1234)
		if err != nil {
			t.Fatalf("defaultFallbackRuntimeBase: %v", err)
		}
		if want := "/run/user/1234"; got != want {
			t.Errorf("defaultFallbackRuntimeBase(1234) = %q, want %q", got, want)
		}
		return
	}

	resetFallbackBaseCache()
	got, err := defaultFallbackRuntimeBase(os.Geteuid())
	if err != nil {
		t.Fatalf("defaultFallbackRuntimeBase: %v", err)
	}
	if !filepath.IsAbs(got) {
		t.Errorf("defaultFallbackRuntimeBase = %q, want an absolute path", got)
	}
	info, err := os.Lstat(got)
	if err != nil {
		t.Fatalf("Lstat %s: %v", got, err)
	}
	if !info.IsDir() {
		t.Errorf("%s is not a directory", got)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || int(st.Uid) != os.Geteuid() {
		t.Errorf("%s is not owned by uid %d", got, os.Geteuid())
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("%s mode = %04o, want no group or other access", got, perm)
	}
}

// TestDarwinFallbackBaseCachesGetconf covers the in-process cache: the second
// lookup reuses the first result instead of starting getconf again.
func TestDarwinFallbackBaseCachesGetconf(t *testing.T) {
	resetFallbackBaseCache()
	oldOutput := getconfOutput
	t.Cleanup(func() {
		getconfOutput = oldOutput
		resetFallbackBaseCache()
	})

	calls := 0
	getconfOutput = func(context.Context) ([]byte, error) {
		calls++
		return []byte("/var/folders/xy/T\n"), nil
	}

	first, err := darwinFallbackBase(7001)
	if err != nil {
		t.Fatalf("darwinFallbackBase: %v", err)
	}
	second, err := darwinFallbackBase(7001)
	if err != nil {
		t.Fatalf("darwinFallbackBase (second): %v", err)
	}
	if first != "/var/folders/xy/T" || second != first {
		t.Errorf("results = %q and %q, want %q", first, second, "/var/folders/xy/T")
	}
	if calls != 1 {
		t.Errorf("getconf ran %d times for two lookups, want 1", calls)
	}
}

// TestDarwinFallbackBaseTimesOut covers the context deadline: a stalled
// getconf returns a deadline error instead of blocking the lookup.
func TestDarwinFallbackBaseTimesOut(t *testing.T) {
	resetFallbackBaseCache()
	oldOutput, oldTimeout := getconfOutput, fallbackBaseProbeTimeout
	t.Cleanup(func() {
		getconfOutput = oldOutput
		fallbackBaseProbeTimeout = oldTimeout
		resetFallbackBaseCache()
	})
	fallbackBaseProbeTimeout = 25 * time.Millisecond
	// The probe deadline is the product's bound on getconf. Checking the value
	// the product passed keeps the assertion independent of scheduling: a probe
	// that received no deadline, or the caller's much later one, returns at once
	// so the test reports it instead of blocking.
	const deadlineBound = time.Second
	deadlines := make(chan time.Duration, 1)
	getconfOutput = func(ctx context.Context) ([]byte, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			deadlines <- -1
			return nil, errors.New("the getconf context has no deadline")
		}
		remaining := time.Until(deadline)
		deadlines <- remaining
		if remaining > deadlineBound {
			return nil, errors.New("the getconf probe timeout was not applied")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, err := darwinFallbackBase(7002)
	if err == nil {
		t.Fatal("darwinFallbackBase succeeded with a stalled getconf")
	}
	var got time.Duration
	select {
	case got = <-deadlines:
	default:
		t.Fatal("getconf was not called")
	}
	if got < 0 || got > deadlineBound {
		t.Fatalf("getconf context deadline was %v away, want about the %s probe timeout", got, fallbackBaseProbeTimeout)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
}
