package broker

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestPeriodicCheckRefreshesRuntimeTimestamps checks that a periodic check
// refreshes the timestamps of the account lock, the runtime directory and the
// local socket.
func TestPeriodicCheckRefreshesRuntimeTimestamps(t *testing.T) {
	rt := newRunTest(t)
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	targets := []string{
		rt.cfg.lockPath,
		rt.dir,
		rt.socketPath(),
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	for _, path := range targets {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("Chtimes %s: %v", path, err)
		}
	}
	before := make(map[string]time.Time, len(targets))
	for _, path := range targets {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		before[path] = info.ModTime()
	}

	rt.ticks <- time.Now()
	select {
	case <-rt.checks:
	case <-time.After(runWaitBound):
		t.Fatal("the periodic check did not run")
	}

	for _, path := range targets {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s after the check: %v", path, err)
		}
		if !info.ModTime().After(before[path]) {
			t.Errorf("%s modification time = %v after the check, want later than %v", path, info.ModTime(), before[path])
		}
	}
}

// TestAccountLockTouchSetsModificationTime checks that Touch sets the lock
// file's modification time.
func TestAccountLockTouchSetsModificationTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.lock")
	lock, err := AcquireAccountLock(path)
	if err != nil {
		t.Fatalf("AcquireAccountLock: %v", err)
	}
	defer lock.Release()

	want := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	if err := lock.Touch(want); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(want) {
		t.Errorf("modification time = %v after Touch, want %v", info.ModTime(), want)
	}
}

// TestAccountLockTouchAfterReleaseFails checks that Touch on a released lock
// returns an error.
func TestAccountLockTouchAfterReleaseFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.lock")
	lock, err := AcquireAccountLock(path)
	if err != nil {
		t.Fatalf("AcquireAccountLock: %v", err)
	}
	lock.Release()

	if err := lock.Touch(time.Now()); err == nil {
		t.Fatal("Touch succeeded after Release, want an error")
	}
}

// TestPeriodicCheckTouchFailureDoesNotExit checks that a failed refresh is
// logged once and the Broker keeps serving.
func TestPeriodicCheckTouchFailureDoesNotExit(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.touchLock = func(*AccountLock, time.Time) error {
			return errors.New("injected touch failure")
		}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	rt.ticks <- time.Now()
	select {
	case <-rt.checks:
	case <-time.After(runWaitBound):
		t.Fatal("the periodic check did not run")
	}

	rt.ticks <- time.Now()
	select {
	case <-rt.checks:
	case err := <-rt.done:
		rt.waited = true
		t.Fatalf("Run returned after a failed timestamp refresh: %v", err)
	case <-time.After(runWaitBound):
		t.Fatal("the periodic check did not run after a failed timestamp refresh")
	}
	if logs := rt.logs.String(); strings.Count(logs, EvAccountLockTouchFailed) != 1 {
		t.Errorf("logs = %q, want exactly one %s record", logs, EvAccountLockTouchFailed)
	}
}

// TestRuntimeTimestampRefresherLogsEachTargetOnceAndRecovery checks that each
// target's failure is logged once and a recovery is logged when it clears.
func TestRuntimeTimestampRefresherLogsEachTargetOnceAndRecovery(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "broker.lock")
	lock, err := AcquireAccountLock(lockPath)
	if err != nil {
		t.Fatalf("AcquireAccountLock: %v", err)
	}
	defer lock.Release()

	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	now := time.Now()
	missingDir := filepath.Join(dir, "missing-dir")
	missingSock := filepath.Join(dir, "missing.sock")
	touch := func(*AccountLock, time.Time) error { return nil }
	r := &runtimeTimestampRefresher{}

	r.refresh(lock, lockPath, missingDir, missingSock, now, touch, logger)
	r.refresh(lock, lockPath, missingDir, missingSock, now.Add(time.Second), touch, logger)

	if got := strings.Count(logs.String(), EvRuntimeDirTouchFailed); got != 1 {
		t.Errorf("%s records = %d, want 1; logs = %q", EvRuntimeDirTouchFailed, got, logs.String())
	}
	if got := strings.Count(logs.String(), EvLocalSocketTouchFailed); got != 1 {
		t.Errorf("%s records = %d, want 1; logs = %q", EvLocalSocketTouchFailed, got, logs.String())
	}
	if strings.Contains(logs.String(), EvAccountLockTouchFailed) {
		t.Errorf("logs = %q, want no account lock failure when its refresh succeeds", logs.String())
	}

	if err := os.Mkdir(missingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	r.refresh(lock, lockPath, missingDir, missingSock, now.Add(2*time.Second), touch, logger)
	if got := strings.Count(logs.String(), EvRuntimeDirTouchRecovered); got != 1 {
		t.Errorf("%s records = %d, want 1; logs = %q", EvRuntimeDirTouchRecovered, got, logs.String())
	}
	if got := strings.Count(logs.String(), EvRuntimeDirTouchFailed); got != 1 {
		t.Errorf("%s records = %d after recovery, want 1; logs = %q", EvRuntimeDirTouchFailed, got, logs.String())
	}
	if got := strings.Count(logs.String(), EvLocalSocketTouchFailed); got != 1 {
		t.Errorf("%s records = %d, want 1; logs = %q", EvLocalSocketTouchFailed, got, logs.String())
	}
}

// TestRuntimeTimestampRefresherLockFailureDeduplicatesAndRecovers checks that a
// repeated account lock failure is logged once and its recovery once.
func TestRuntimeTimestampRefresherLockFailureDeduplicatesAndRecovers(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	now := time.Now()
	fail := true
	touch := func(*AccountLock, time.Time) error {
		if fail {
			return errors.New("injected touch failure")
		}
		return nil
	}
	r := &runtimeTimestampRefresher{}

	r.refresh(nil, "/lock", "", "", now, touch, logger)
	r.refresh(nil, "/lock", "", "", now.Add(time.Second), touch, logger)
	if got := strings.Count(logs.String(), EvAccountLockTouchFailed); got != 1 {
		t.Errorf("%s records = %d, want 1; logs = %q", EvAccountLockTouchFailed, got, logs.String())
	}

	fail = false
	r.refresh(nil, "/lock", "", "", now.Add(2*time.Second), touch, logger)
	if got := strings.Count(logs.String(), EvAccountLockTouchRecovered); got != 1 {
		t.Errorf("%s records = %d, want 1; logs = %q", EvAccountLockTouchRecovered, got, logs.String())
	}
	if got := strings.Count(logs.String(), EvAccountLockTouchFailed); got != 1 {
		t.Errorf("%s records = %d after recovery, want 1; logs = %q", EvAccountLockTouchFailed, got, logs.String())
	}
}
