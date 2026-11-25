package inbox

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// TestWatch_UnwatchableTargetNotifiesZeroOnce checks that retrying an
// unwatchable configuration directory on later notifications does not report
// the fallback to the callbacks a second time.
func TestWatch_UnwatchableTargetNotifiesZeroOnce(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	if err := os.Mkdir(dir, 0o000); err != nil {
		t.Fatalf("creating the unwatchable configuration directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	pids := make(chan int, 128)
	w, err := newWatcher(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("newWatcher above an unwatchable configuration directory: %v", err)
	}
	t.Cleanup(w.shutdown)

	// No directory the watcher could follow is watchable, so it started on the
	// nearest existing ancestor and reported the fallback once.
	aWaitPID(t, pids, 0)
	aDrain(pids)

	// Each later event retries the configuration directory's Add and fails
	// again. The events are unrelated to the failing path, so anything else a
	// callback receives comes from the fallback report. The names sort before
	// "config" because the kqueue backend's directory scan stops at the first
	// entry it cannot watch, and the configuration directory is that entry.
	handled := make(chan string, 16)
	afterHandle := func(event fsnotify.Event) { handled <- filepath.Clean(event.Name) }
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	const events = 6
	for i := 0; i < events; i++ {
		name := filepath.Join(root, "aaa-"+strconv.Itoa(i))
		aWriteFile(t, name, []byte("x"))
		aWaitHandledEvent(t, handled, name)

		// The callback runs inside handleEvent, and the hook above runs after
		// it returned, so every report this event caused is already queued.
		for draining := true; draining; {
			select {
			case pid := <-pids:
				if pid != 0 {
					t.Fatalf("event %d reported process id %d, want only the fallback's 0", i+1, pid)
				}
				t.Fatalf("event %d reported the fallback again, want the single report sent before the events", i+1)
			default:
				draining = false
			}
		}
	}
}

// aWaitHandledEvent waits until the watcher has handled the event for want.
// Events for other names are ignored.
func aWaitHandledEvent(t *testing.T, handled <-chan string, want string) {
	t.Helper()
	timer := time.NewTimer(aWaitTimeout)
	defer timer.Stop()
	for {
		select {
		case got := <-handled:
			if got == want {
				return
			}
		case <-timer.C:
			t.Fatalf("the watcher did not handle the event for %s", want)
		}
	}
}
