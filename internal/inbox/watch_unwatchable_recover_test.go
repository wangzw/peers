package inbox

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// TestWatch_UnwatchableRegistryMovesBackDownAfterEvent checks that a registry
// directory whose watch could not be installed at startup gets that watch
// installed when the configuration directory reports a change, so a record
// written afterwards is delivered.
func TestWatch_UnwatchableRegistryMovesBackDownAfterEvent(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	dir := t.TempDir()
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}
	if err := os.Chmod(sessions, 0o000); err != nil {
		t.Fatalf("removing permissions from the registry directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(sessions, 0o700) })

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch on an unwatchable registry directory: %v", err)
	}
	defer stop()

	// A file created in the configuration directory produces an event on that
	// directory's watch in both backends: inotify raises IN_CREATE for it, and
	// kqueue rescans the watched directory on NOTE_WRITE and reports the new
	// entry as a Create event. That event is held inside handleEvent until the
	// registry directory's permissions are restored, so the reconciliation
	// handling it finds the registry directory watchable. The name sorts before
	// "sessions" because the kqueue rescan stops at the first entry it cannot
	// watch, and the registry directory is still unreadable when the rescan runs.
	trigger := filepath.Join(dir, "a-watch-trigger")
	t.Cleanup(func() { _ = os.Remove(trigger) })
	arrived := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandling := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHandling)
	var pauseOnce sync.Once
	beforeHandle := func() {
		pauseOnce.Do(func() {
			close(arrived)
			<-release
		})
	}
	testWatchBeforeHandleEvent.Store(&beforeHandle)
	t.Cleanup(func() { testWatchBeforeHandleEvent.Store(nil) })

	// The hook runs on the run goroutine after the event has been handled,
	// including the reconciliation, so the watcher's recorded watch path is
	// read there rather than from this goroutine.
	handled := make(chan struct{})
	var handledOnce sync.Once
	var watchedPath string
	afterHandle := func(event fsnotify.Event) {
		if filepath.Clean(event.Name) != trigger {
			return
		}
		handledOnce.Do(func() {
			if w := sharedWatcherForTest(dir); w != nil {
				watchedPath = w.watchPath
			}
			close(handled)
		})
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	aWriteFile(t, trigger, []byte("trigger"))
	select {
	case <-arrived:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not receive the configuration directory's create event")
	}

	if err := os.Chmod(sessions, 0o700); err != nil {
		t.Fatalf("restoring registry directory permissions: %v", err)
	}
	releaseHandling()
	select {
	case <-handled:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not handle the configuration directory's create event")
	}
	if watchedPath != sessions {
		t.Fatalf("watch path after the configuration directory's create event = %q, want %q", watchedPath, sessions)
	}

	// No configuration-directory event follows this write, so only a watch on
	// the registry directory reports it.
	const pid = 7851
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-moved-down.sock",
	})
	aWaitPID(t, pids, pid)
}
