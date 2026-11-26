package inbox

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// fallbackHandled is one event the watcher handled, with the watch path and the
// number of callbacks reporting process id 0 observed by the end of that event.
type fallbackHandled struct {
	name      string
	watchPath string
	zeros     int
}

// TestWatch_FallbackBelowTwoUnwatchableLevels is the regression test for a
// watcher whose registry directory and configuration directory both exist but
// cannot be watched. One reconciliation must skip each failing directory and
// fall back to the nearest watchable ancestor, so Watch succeeds, reports the
// fallback once, and moves the watch back down when the directories become
// watchable again.
func TestWatch_FallbackBelowTwoUnwatchableLevels(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	if runtime.GOOS == "darwin" {
		t.Skip("kqueue opens directories with O_EVTONLY, which needs no read permission, so a 0o100 configuration directory stays watchable")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}
	if err := os.Chmod(sessions, 0o000); err != nil {
		t.Fatalf("removing permissions from the registry directory: %v", err)
	}
	if err := os.Chmod(dir, 0o100); err != nil {
		t.Fatalf("removing permissions from the configuration directory: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700)
		_ = os.Chmod(sessions, 0o700)
	})

	// zeros is touched only on the watcher's run goroutine: by the callback and
	// by the afterHandle hook that reports it here.
	var zeros int
	pids := make(chan int, 256)
	stop, err := Watch(dir, func(pid int) {
		if pid == 0 {
			zeros++
		}
		pids <- pid
	})
	if err != nil {
		t.Fatalf("Watch above unwatchable configuration and registry directories: %v", err)
	}
	defer stop()

	// The fallback to the ancestor is reported once, before any event arrives.
	aWaitPID(t, pids, 0)

	handled := make(chan fallbackHandled, 512)
	moved := make(chan struct{})
	var movedOnce sync.Once
	afterHandle := func(event fsnotify.Event) {
		var path string
		if w := sharedWatcherForTest(dir); w != nil {
			path = w.watchPath
		}
		got := fallbackHandled{name: filepath.Clean(event.Name), watchPath: path, zeros: zeros}
		handled <- got
		if got.watchPath == sessions {
			movedOnce.Do(func() { close(moved) })
		}
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	// Events in the ancestor are unrelated to every candidate directory. The
	// watcher must stay on the ancestor and must not report the fallback again.
	const unrelated = 5
	for i := 0; i <= unrelated; i++ {
		name := filepath.Join(root, "aaa-"+strconv.Itoa(i))
		aWriteFile(t, name, []byte("x"))
		got := aWaitFallbackHandled(t, handled, name)
		if got.watchPath != root {
			t.Fatalf("event %d: watch path = %q, want the configuration directory parent %q", i+1, got.watchPath, root)
		}
		if got.zeros != 1 {
			t.Fatalf("event %d: %d callbacks reported process id 0, want the single fallback report", i+1, got.zeros)
		}
	}

	// Block the event loop inside handleEvent, restore both modes while it is
	// blocked, then release it: the reconciliation handling that event sees the
	// restored directories.
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

	aWriteFile(t, filepath.Join(root, "zzz-restore"), []byte("x"))
	select {
	case <-arrived:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not receive an event on the ancestor watch")
	}

	if err := os.Chmod(sessions, 0o700); err != nil {
		t.Fatalf("restoring registry directory permissions: %v", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("restoring configuration directory permissions: %v", err)
	}
	releaseHandling()
	select {
	case <-moved:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not move its watch down to the registry directory after the directories became watchable")
	}

	// No ancestor event follows this write, so only a watch on the registry
	// directory reports it.
	aDrain(pids)
	const pid = 7852
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-fallback-both.sock",
	})
	aWaitPID(t, pids, pid)
}

// TestWatch_FallbackSkipsFailedAdds asserts the same fallback through the Add
// seam, so it runs where the platform cannot be made to refuse a directory:
// kqueue opens with O_EVTONLY, which needs no read permission. Both deeper
// levels fail in one reconciliation, which must skip them and fall back to the
// ancestor, then move down once Add succeeds again.
func TestWatch_FallbackSkipsFailedAdds(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}

	fail := func(fsw *fsnotify.Watcher, path string) error {
		if path == sessions || path == dir {
			return errors.New("injected watch failure")
		}
		return fsw.Add(path)
	}
	testWatchAdd.Store(&fail)
	t.Cleanup(func() { testWatchAdd.Store(nil) })

	var zeros int
	pids := make(chan int, 256)
	stop, err := Watch(dir, func(pid int) {
		if pid == 0 {
			zeros++
		}
		pids <- pid
	})
	if err != nil {
		t.Fatalf("Watch with two failing Add attempts: %v", err)
	}
	defer stop()

	aWaitPID(t, pids, 0)

	handled := make(chan fallbackHandled, 512)
	moved := make(chan struct{})
	var movedOnce sync.Once
	afterHandle := func(event fsnotify.Event) {
		var path string
		if w := sharedWatcherForTest(dir); w != nil {
			path = w.watchPath
		}
		got := fallbackHandled{name: filepath.Clean(event.Name), watchPath: path, zeros: zeros}
		handled <- got
		if got.watchPath == sessions {
			movedOnce.Do(func() { close(moved) })
		}
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	// These names are outside the configuration directory, so an event about
	// one adds no 0 of its own.
	const unrelated = 5
	for i := 0; i <= unrelated; i++ {
		name := filepath.Join(root, "aaa-"+strconv.Itoa(i))
		aWriteFile(t, name, []byte("x"))
		got := aWaitFallbackHandled(t, handled, name)
		if got.watchPath != root {
			t.Fatalf("event %d: watch path = %q, want the configuration directory parent %q", i+1, got.watchPath, root)
		}
		if got.zeros != 1 {
			t.Fatalf("event %d: %d callbacks reported process id 0, want the single fallback report", i+1, got.zeros)
		}
	}

	// Block the event loop inside handleEvent, let Add succeed again while it is
	// blocked, then release it so that event's reconciliation moves the watch
	// down.
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

	aWriteFile(t, filepath.Join(root, "zzz-restore"), []byte("x"))
	select {
	case <-arrived:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not receive an event on the ancestor watch")
	}
	testWatchAdd.Store(nil)
	releaseHandling()
	select {
	case <-moved:
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not move its watch down to the registry directory after Add succeeded again")
	}

	aDrain(pids)
	const pid = 7853
	aWriteRecord(t, dir, pid, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-fallback-failed-adds.sock",
	})
	aWaitPID(t, pids, pid)
}

// aWaitFallbackHandled waits until the watcher has handled the event for want.
// Events for other names are ignored.
func aWaitFallbackHandled(t *testing.T, handled <-chan fallbackHandled, want string) fallbackHandled {
	t.Helper()
	timer := time.NewTimer(aWaitTimeout)
	defer timer.Stop()
	for {
		select {
		case got := <-handled:
			if got.name == want {
				return got
			}
		case <-timer.C:
			t.Fatalf("the watcher did not handle the event for %s", want)
			return fallbackHandled{}
		}
	}
}
