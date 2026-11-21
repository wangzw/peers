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

// TestPeriodicCheckRefreshesRuntimeTimestamps covers the minute-long review:
// it must refresh the access and modification times of the account lock, the
// runtime directory and the local socket. Without the refresh, a /tmp cleanup
// rule that deletes old files removes the running Broker's lock and the Broker
// then exits on its own.
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

// TestAccountLockTouchSetsModificationTime covers the exported method on its
// own: it sets the lock file's modification time through the open descriptor.
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

// TestAccountLockTouchAfterReleaseFails covers a released lock: there is no
// descriptor left to set the timestamps on.
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

// TestPeriodicCheckTouchFailureDoesNotExit covers a failed refresh: the Broker
// logs the failure and keeps serving, because the lock and the socket are still
// there and the next check tries again.
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

	// Drive a second check: completing it proves Run kept serving after the
	// first failure, without waiting a fixed duration. The failure repeating
	// must not add a second record.
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

// TestRuntimeTimestampRefresherLogsEachTargetOnceAndRecovery covers the failed
// refresh events: the directory and the socket have distinct event names, and a
// failure that repeats is logged once, with a recovery record when it clears.
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

	// Creating the directory clears its failure; the socket is still missing.
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

// TestRuntimeTimestampRefresherLockFailureDeduplicatesAndRecovers covers the
// same rule for the account lock: a repeated failure is logged once, and the
// first success after it logs the recovery.
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
