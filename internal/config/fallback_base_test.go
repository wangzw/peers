package config

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

// TestDefaultRuntimeFallbackBasePerPlatform covers the platform branch: the
// logind directory on Linux, and the per-user temporary directory getconf
// reports on macOS. The macOS branch also runs in the macOS probe job.
func TestDefaultRuntimeFallbackBasePerPlatform(t *testing.T) {
	if runtime.GOOS != "darwin" {
		got, err := defaultRuntimeFallbackBase(1234)
		if err != nil {
			t.Fatalf("defaultRuntimeFallbackBase: %v", err)
		}
		if want := "/run/user/1234"; got != want {
			t.Errorf("defaultRuntimeFallbackBase(1234) = %q, want %q", got, want)
		}
		return
	}

	resetFallbackBaseCache()
	got, err := defaultRuntimeFallbackBase(os.Geteuid())
	if err != nil {
		t.Fatalf("defaultRuntimeFallbackBase: %v", err)
	}
	if !filepath.IsAbs(got) {
		t.Errorf("defaultRuntimeFallbackBase = %q, want an absolute path", got)
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
	getconfOutput = func(ctx context.Context) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	started := time.Now()
	_, err := darwinFallbackBase(7002)
	if err == nil {
		t.Fatal("darwinFallbackBase succeeded with a stalled getconf")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("darwinFallbackBase took %v, want the bounded wait", elapsed)
	}
}
