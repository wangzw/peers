package inbox

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// TestWatch_SharesOneWatcher checks that every Watch call for one configuration
// directory reuses the same watcher.
func TestWatch_SharesOneWatcher(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	stopFirst, err := Watch(dir, func(int) {})
	if err != nil {
		t.Fatalf("first Watch: %v", err)
	}
	defer stopFirst()
	shared := sharedWatcherForTest(dir)
	if shared == nil {
		t.Fatal("no watcher is registered after the first Watch")
	}

	stopSecond, err := Watch(dir, func(int) {})
	if err != nil {
		t.Fatalf("second Watch: %v", err)
	}
	defer stopSecond()
	if got := sharedWatcherForTest(dir); got != shared {
		t.Fatal("the second Watch call built another watcher instead of sharing the first")
	}

	stopThird, err := Watch(dir, func(int) {})
	if err != nil {
		t.Fatalf("third Watch: %v", err)
	}
	if got := sharedWatcherForTest(dir); got != shared {
		t.Fatal("the third Watch call built another watcher instead of sharing the first")
	}

	stopSecond()
	if got := sharedWatcherForTest(dir); got != shared {
		t.Fatal("removing one registration replaced the shared watcher")
	}
	stopThird()
	stopFirst()
	if got := sharedWatcherForTest(dir); got != nil {
		t.Fatal("the watcher is still registered after the last registration was removed")
	}
}

// TestScanSessions_NoticesSameSizeSameMtimeRewrite checks that the snapshot
// notices a rewrite with the same size and modification time.
func TestScanSessions_NoticesSameSizeSameMtimeRewrite(t *testing.T) {
	dir := t.TempDir()
	fixed := time.Unix(1_700_000_000, 0)
	path := aWriteRecord(t, dir, 6401, map[string]any{
		"name":                "agent",
		"status":              "idle",
		"messagingSocketPath": "",
	})
	aSetModTime(t, path, fixed)

	before := scanSessions(aSessionsDir(dir))

	// "idle" and "busy" encode to the same JSON length; do not change one to a
	// value of a different length.
	aWriteRecord(t, dir, 6401, map[string]any{
		"name":                "agent",
		"status":              "busy",
		"messagingSocketPath": "",
	})
	aSetModTime(t, path, fixed)

	after := scanSessions(aSessionsDir(dir))

	changed := changedPIDs(before, after)
	if len(changed) != 1 || changed[0] != 6401 {
		t.Fatalf("changedPIDs = %v, want [6401]; size and modification time are unchanged, so only the content digest can report this rewrite", changed)
	}
}

// TestWatch_RegistryDirectoryAppearsThenRewrite checks that a record written
// and then rewritten after the registry directory appears is reported both
// times.
func TestWatch_RegistryDirectoryAppearsThenRewrite(t *testing.T) {
	dir := t.TempDir()

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}
	aWaitPID(t, pids, 0)

	aWriteRecord(t, dir, 6751, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-appears.sock",
	})
	aWaitPID(t, pids, 6751)

	aWriteRecord(t, dir, 6751, map[string]any{
		"name":                "renamed",
		"messagingSocketPath": "/tmp/watch-appears.sock",
	})
	aWaitPID(t, pids, 6751)
}

// TestWatch_FileWatcherErrorCallsBackWithZero checks that a file watcher error
// calls back with 0.
func TestWatch_FileWatcherErrorCallsBackWithZero(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	pids := make(chan int, 16)
	w, err := newWatcher(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	t.Cleanup(w.shutdown)

	w.handleWatchError()

	aWaitPID(t, pids, 0)
}

// TestWatch_RegistryDirectoryRemovedAndRecreated checks that a record written
// after the registry directory is removed and recreated is reported.
func TestWatch_RegistryDirectoryRemovedAndRecreated(t *testing.T) {
	dir := t.TempDir()
	const pid = 6901
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "before",
		"messagingSocketPath": "/tmp/watch-recreate.sock",
	})

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(p int) { pids <- p })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	var recreated sync.Once
	// Recreate only when the directory is really absent, and consume the Once
	// only after that check, or RemoveAll may delete it again.
	beforeHandle := func() {
		if info, err := os.Stat(aSessionsDir(dir)); err == nil && info.IsDir() {
			return
		}
		recreated.Do(func() {
			if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
				t.Errorf("recreating sessions directory: %v", err)
			}
		})
	}
	testWatchBeforeHandleEvent.Store(&beforeHandle)
	t.Cleanup(func() { testWatchBeforeHandleEvent.Store(nil) })

	removalHandled := make(chan struct{})
	signalHandled := func() {
		select {
		case <-removalHandled:
		default:
			close(removalHandled)
		}
	}
	afterHandle := func(event fsnotify.Event) {
		if filepath.Clean(event.Name) == aSessionsDir(dir) &&
			event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
			signalHandled()
			return
		}
		// A missing directory also counts as handled because the directory's own
		// removal notification may never arrive.
		if _, err := os.Stat(aSessionsDir(dir)); err != nil {
			signalHandled()
		}
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	if err := os.RemoveAll(aSessionsDir(dir)); err != nil {
		t.Fatalf("removing sessions directory: %v", err)
	}

	select {
	case <-removalHandled:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not handle the registry directory's removal")
	}

	aDrain(pids)

	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "after",
		"messagingSocketPath": "/tmp/watch-recreate.sock",
	})
	aWaitPID(t, pids, pid)
}

// TestWatch_RegistryDirectoryCreatedBetweenStatAndAdd checks that a registry
// directory created between the existence check and Add is still watched.
func TestWatch_RegistryDirectoryCreatedBetweenStatAndAdd(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	var created sync.Once
	statAddWindow := func() {
		created.Do(func() {
			if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
				t.Errorf("creating sessions directory in the Stat/Add window: %v", err)
			}
		})
	}
	testWatchAfterStatBeforeAdd.Store(&statAddWindow)
	t.Cleanup(func() { testWatchAfterStatBeforeAdd.Store(nil) })

	removalHandled := make(chan struct{})
	afterHandle := func(event fsnotify.Event) {
		if filepath.Clean(event.Name) != aSessionsDir(dir) {
			return
		}
		if event.Op&(fsnotify.Remove|fsnotify.Rename) == 0 {
			return
		}
		select {
		case <-removalHandled:
		default:
			close(removalHandled)
		}
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	if err := os.RemoveAll(aSessionsDir(dir)); err != nil {
		t.Fatalf("removing sessions directory: %v", err)
	}

	select {
	case <-removalHandled:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not handle the registry directory's removal")
	}
	aDrain(pids)

	const pid = 6851
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-stat-add.sock",
	})
	aWaitPID(t, pids, pid)
}

// TestWatch_ConfigDirectoryRemovedAndRecreated checks that losing the
// configuration directory calls back with 0 and that the watch recovers when it
// reappears.
func TestWatch_ConfigDirectoryRemovedAndRecreated(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removing configuration directory: %v", err)
	}
	aWaitPID(t, pids, 0)
	aDrain(pids)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("recreating configuration directory: %v", err)
	}

	const pid = 7001
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-config-recreate.sock",
	})
	aWaitPID(t, pids, pid)
}
