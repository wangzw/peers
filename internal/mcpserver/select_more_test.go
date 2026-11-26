package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestLiveUserSocketFallsBackToFirst returns the first socket unless exactly one
// runtime directory holds the lock.
func TestLiveUserSocketFallsBackToFirst(t *testing.T) {
	t.Run("no held lock", func(t *testing.T) {
		s := &selector{lockHeld: func(string) bool { return false }}
		if got := s.liveUserSocket([]string{"a", "b"}); got != "a" {
			t.Fatalf("liveUserSocket = %q, want the first path a", got)
		}
	})
	t.Run("two held locks", func(t *testing.T) {
		s := &selector{lockHeld: func(string) bool { return true }}
		if got := s.liveUserSocket([]string{"a", "b"}); got != "a" {
			t.Fatalf("liveUserSocket = %q, want the first path a", got)
		}
	})
	t.Run("one held lock", func(t *testing.T) {
		s := &selector{lockHeld: func(dir string) bool { return strings.HasSuffix(dir, "b") }}
		if got := s.liveUserSocket([]string{"/x/a/broker.sock", "/x/b/broker.sock"}); got != "/x/b/broker.sock" {
			t.Fatalf("liveUserSocket = %q, want the locked path", got)
		}
	})
}

// TestUserLockHeldWithoutSeam reports the account lock as held while this
// process holds it and free after unlocking.
func TestUserLockHeldWithoutSeam(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("creating the lock file: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("locking the file: %v", err)
	}

	s := &selector{}
	if !s.userLockHeld(dir) {
		t.Fatal("userLockHeld reported a held lock as free")
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_UN); err != nil {
		t.Fatalf("unlocking the file: %v", err)
	}
	if s.userLockHeld(dir) {
		t.Fatal("userLockHeld reported an unlocked file as held")
	}
}

// TestFindAtLevelSharedSocketPath selects a usable shared socket for root and
// reports one served by another account as a mismatch.
func TestFindAtLevelSharedSocketPath(t *testing.T) {
	shared := filepath.Join(shortTempDir(t), "broker.sock")

	t.Run("usable", func(t *testing.T) {
		s := &selector{
			userSocket:   shared,
			systemSocket: shared,
			probe: func(context.Context, string) probeInfo {
				return probeInfo{Outcome: probeOK, PeerUID: 0, PeerKnown: true}
			},
		}
		got, found, err := s.findAtLevel(context.Background(), true)
		if err != nil || !found {
			t.Fatalf("findAtLevel = (%+v, %v, %v), want the shared socket", got, found, err)
		}
		if got.Path != shared || got.Kind != kindSystem {
			t.Fatalf("findAtLevel = %+v, want {%s kindSystem}", got, shared)
		}
	})

	t.Run("wrong peer", func(t *testing.T) {
		const foreign = uint32(7)
		s := &selector{
			userSocket:   shared,
			systemSocket: shared,
			probe: func(context.Context, string) probeInfo {
				return probeInfo{Outcome: probeWrongPeer, PeerUID: foreign, PeerKnown: true}
			},
		}
		_, found, err := s.findAtLevel(context.Background(), true)
		if found || err == nil {
			t.Fatalf("findAtLevel = (found %v, err %v), want the peer mismatch", found, err)
		}
		want := fmt.Sprintf("the system Broker socket %s is served by uid %d, not root", shared, foreign)
		if err.Error() != want {
			t.Fatalf("error = %q, want %q", err, want)
		}
	})
}

// TestFindAtLevelSystemWrongPeerDifferingPaths reports a system socket served by
// another account as a mismatch rather than a missing Broker.
func TestFindAtLevelSystemWrongPeerDifferingPaths(t *testing.T) {
	const foreign = uint32(11)
	userPath := filepath.Join(shortTempDir(t), "user.sock")
	systemPath := filepath.Join(shortTempDir(t), "system.sock")
	s := &selector{
		userSocket:   userPath,
		systemSocket: systemPath,
		probe: func(_ context.Context, path string) probeInfo {
			if path == systemPath {
				return probeInfo{Outcome: probeWrongPeer, PeerUID: foreign, PeerKnown: true}
			}
			return probeInfo{Outcome: probeMissing}
		},
	}
	_, found, err := s.findAtLevel(context.Background(), true)
	if found || err == nil {
		t.Fatalf("findAtLevel = (found %v, err %v), want the peer mismatch", found, err)
	}
	want := fmt.Sprintf("the system Broker socket %s is served by uid %d, not root", systemPath, foreign)
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err, want)
	}
}

// TestFindAtLevelSystemRefusedThenUp retries a refused system socket and selects
// it after the first wait.
func TestFindAtLevelSystemRefusedThenUp(t *testing.T) {
	userPath := filepath.Join(shortTempDir(t), "user.sock")
	systemPath := filepath.Join(shortTempDir(t), "system.sock")
	after, waits := recordingAfter()

	s := &selector{
		userSocket:   userPath,
		systemSocket: systemPath,
		after:        after,
		probe: func(_ context.Context, path string) probeInfo {
			if path != systemPath {
				return probeInfo{Outcome: probeMissing}
			}
			if len(waits()) == 0 {
				return probeInfo{Outcome: probeRefused}
			}
			return probeInfo{Outcome: probeOK, PeerUID: 0, PeerKnown: true}
		},
	}

	got, found, err := s.findAtLevel(context.Background(), true)
	if err != nil || !found {
		t.Fatalf("findAtLevel = (%+v, %v, %v), want the system socket", got, found, err)
	}
	if got.Path != systemPath || got.Kind != kindSystem {
		t.Fatalf("findAtLevel = %+v, want {%s kindSystem}", got, systemPath)
	}
	if len(waits()) != 1 {
		t.Fatalf("waits = %v, want one retry wait", waits())
	}
}

func neverAfter(time.Duration) <-chan time.Time { return make(chan time.Time) }

// TestProbeSystemWithRetryReturnsContextError returns the context's error when it
// ends before the first wait.
func TestProbeSystemWithRetryReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &selector{
		systemSocket: filepath.Join(shortTempDir(t), "system.sock"),
		after:        neverAfter,
		probe: func(context.Context, string) probeInfo {
			return probeInfo{Outcome: probeRefused}
		},
	}
	ok, err := s.probeSystemWithRetry(ctx)
	if ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("probeSystemWithRetry = (%v, %v), want (false, context.Canceled)", ok, err)
	}
}

// TestProbeSystemWithRetryFinalWaitContextError returns the context's error when
// it ends at the final wait.
func TestProbeSystemWithRetryFinalWaitContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	after := func(time.Duration) <-chan time.Time {
		calls++
		if calls == len(systemRetryWaits)+1 {
			cancel()
			return make(chan time.Time)
		}
		ch := make(chan time.Time)
		close(ch)
		return ch
	}

	s := &selector{
		systemSocket: filepath.Join(shortTempDir(t), "system.sock"),
		after:        after,
		probe: func(context.Context, string) probeInfo {
			return probeInfo{Outcome: probeRefused}
		},
	}
	ok, err := s.probeSystemWithRetry(ctx)
	if ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("probeSystemWithRetry = (%v, %v), want (false, context.Canceled)", ok, err)
	}
}

// TestProbeSystemWithRetryFindsBrokerOnFinalWait finds the Broker after the final
// wait.
func TestProbeSystemWithRetryFindsBrokerOnFinalWait(t *testing.T) {
	after, wait := recordingAfter()

	s := &selector{
		systemSocket: filepath.Join(shortTempDir(t), "system.sock"),
		after:        after,
		probe: func(context.Context, string) probeInfo {
			if len(wait()) <= len(systemRetryWaits) {
				return probeInfo{Outcome: probeRefused}
			}
			return probeInfo{Outcome: probeOK, PeerUID: 0, PeerKnown: true}
		},
	}
	ok, err := s.probeSystemWithRetry(context.Background())
	if err != nil || !ok {
		t.Fatalf("probeSystemWithRetry = (%v, %v), want (true, nil)", ok, err)
	}
	if len(wait()) != len(systemRetryWaits)+1 {
		t.Fatalf("waits = %v, want the five backoffs and the final rest", wait())
	}
}

// TestAfterFuncDefaultsToTimeAfter checks that a nil after field schedules on
// the real timer: a long request does not deliver at once and a short one fires.
func TestAfterFuncDefaultsToTimeAfter(t *testing.T) {
	t.Parallel()

	after := (&selector{}).afterFunc()
	select {
	case <-after(time.Hour):
		t.Fatal("the default after function fired before its duration")
	default:
	}
	select {
	case <-after(5 * time.Millisecond):
	case <-time.After(5 * time.Second):
		t.Fatal("the default after function did not fire")
	}
}

// TestUserSocketPathIsABrokerSocket names a broker.sock inside a runtime
// directory.
func TestUserSocketPathIsABrokerSocket(t *testing.T) {
	got := UserSocketPath()
	if filepath.Base(got) != "broker.sock" || filepath.Dir(got) == "." {
		t.Fatalf("UserSocketPath = %q, want a broker.sock inside a runtime directory", got)
	}
}
