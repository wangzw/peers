package inbox

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestREQAgentMessaging001_WatchReportsNewRecord checks that Watch reports a
// newly written registry record by its process id.
func TestREQAgentMessaging001_WatchReportsNewRecord(t *testing.T) {
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

	aWriteRecord(t, dir, 6101, map[string]any{
		"name":                "api-worker",
		"messagingSocketPath": "/tmp/watch-new.sock",
	})
	aWaitPID(t, pids, 6101)
}

// TestREQAgentMessaging005_WatchReportsRename checks that rewriting a record
// under the same file name is reported by its process id.
func TestREQAgentMessaging005_WatchReportsRename(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 6201, map[string]any{
		"name":                "api-worker",
		"messagingSocketPath": socket,
	})
	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	aWriteRecord(t, dir, 6201, map[string]any{
		"name":                "api-v2",
		"messagingSocketPath": socket,
	})
	aWaitPID(t, pids, 6201)
}

// TestWatch_ReportsDeletion checks that deleting a record is reported by its
// process id.
func TestWatch_ReportsDeletion(t *testing.T) {
	dir := t.TempDir()
	path := aWriteRecord(t, dir, 6301, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-delete.sock",
	})
	pids := make(chan int, 16)
	w, err := newWatcher(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	t.Cleanup(w.shutdown)

	if err := os.Remove(path); err != nil {
		t.Fatalf("removing record: %v", err)
	}
	aWaitPID(t, pids, 6301)
}

// TestWatch_ReportsRecordFileRename checks that renaming a record file reports
// both the old and the new process id.
func TestWatch_ReportsRecordFileRename(t *testing.T) {
	dir := t.TempDir()
	path := aWriteRecord(t, dir, 6801, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-file-rename.sock",
	})
	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	if err := os.Rename(path, filepath.Join(aSessionsDir(dir), "6802.json")); err != nil {
		t.Fatalf("renaming record: %v", err)
	}
	aWaitPIDs(t, pids, 6801, 6802)
}

// TestWatch_SameSizeSameMtimeRewrite checks that a rewrite with the same size
// and modification time as the snapshot is still reported.
func TestWatch_SameSizeSameMtimeRewrite(t *testing.T) {
	dir := t.TempDir()
	fixed := time.Unix(1_700_000_000, 0)
	path := aWriteRecord(t, dir, 6501, map[string]any{
		"name":                "agent",
		"status":              "idle",
		"messagingSocketPath": "",
	})
	aSetModTime(t, path, fixed)

	pids := make(chan int, 16)
	w, err := newWatcher(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	t.Cleanup(w.shutdown)

	// "idle" and "busy" encode to the same JSON length; do not change one to a
	// value of a different length.
	aWriteRecord(t, dir, 6501, map[string]any{
		"name":                "agent",
		"status":              "busy",
		"messagingSocketPath": "",
	})
	aSetModTime(t, path, fixed)

	aWaitPID(t, pids, 6501)
}

// TestWatch_StopSkipsPendingCallback checks that a registration stopped while
// another callback is running is not invoked afterwards.
func TestWatch_StopSkipsPendingCallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	invoked := make(chan int, 16)
	var firstStarted atomic.Bool
	var firstID atomic.Int64
	firstID.Store(-1)

	blocker := func(id int) func(int) {
		return func(int) {
			if firstStarted.CompareAndSwap(false, true) {
				firstID.Store(int64(id))
				close(entered)
				<-release
			}
			invoked <- id
		}
	}

	stopOne, err := Watch(dir, blocker(1))
	if err != nil {
		t.Fatalf("first Watch: %v", err)
	}
	defer stopOne()
	stopTwo, err := Watch(dir, blocker(2))
	if err != nil {
		t.Fatalf("second Watch: %v", err)
	}
	defer stopTwo()
	stopThree, err := Watch(dir, blocker(3))
	if err != nil {
		t.Fatalf("third Watch: %v", err)
	}
	defer stopThree()

	// The sentinel is registered last, so callbacks run serially before it.
	sentinel := make(chan int, 16)
	stopSentinel, err := Watch(dir, func(pid int) { sentinel <- pid })
	if err != nil {
		t.Fatalf("sentinel Watch: %v", err)
	}
	defer stopSentinel()

	aWriteRecord(t, dir, 7101, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-stop-race.sock",
	})

	select {
	case <-entered:
	case <-time.After(aWaitTimeout):
		t.Fatal("no callback started")
	}

	oneStopped := make(chan struct{})
	go func() {
		stopOne()
		close(oneStopped)
	}()
	stopTwo()
	close(release)
	select {
	case <-oneStopped:
	case <-time.After(aWaitTimeout):
		t.Fatal("stopOne did not return after its callback finished")
	}

	select {
	case pid := <-sentinel:
		if pid != 7101 {
			t.Fatalf("sentinel reported pid %d, want 7101", pid)
		}
	case <-time.After(aWaitTimeout):
		t.Fatal("sentinel did not report the record")
	}
	startedID := int(firstID.Load())
	for {
		select {
		case id := <-invoked:
			if (id == 1 || id == 2) && id != startedID {
				t.Fatalf("callback %d was invoked after its stop returned", id)
			}
		default:
			return
		}
	}
}

// TestWatch_StopAndShared checks that Watch shares one watcher per directory
// and that a stopped registration stops receiving while the others continue.
func TestWatch_StopAndShared(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}
	first := make(chan int, 16)
	second := make(chan int, 16)
	stopFirst, err := Watch(dir, func(pid int) { first <- pid })
	if err != nil {
		t.Fatalf("first Watch: %v", err)
	}
	stopSecond, err := Watch(dir, func(pid int) { second <- pid })
	if err != nil {
		t.Fatalf("second Watch: %v", err)
	}
	if got := sharedWatcherCount(dir); got != 1 {
		t.Fatalf("sharedWatcherCount = %d after two Watch calls, want 1", got)
	}

	stopFirst()
	stopFirst() // idempotent
	if got := sharedWatcherCount(dir); got != 1 {
		t.Fatalf("sharedWatcherCount = %d after removing one callback, want 1", got)
	}

	sentinel := make(chan int, 16)
	stopSentinel, err := Watch(dir, func(pid int) { sentinel <- pid })
	if err != nil {
		t.Fatalf("sentinel Watch: %v", err)
	}

	aWriteRecord(t, dir, 6601, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-shared.sock",
	})
	aWaitPID(t, sentinel, 6601)
	select {
	case got := <-first:
		t.Fatalf("stopped callback reported pid %d", got)
	default:
	}
	select {
	case got := <-second:
		if got != 6601 {
			t.Fatalf("live callback reported pid %d, want 6601", got)
		}
	default:
		t.Fatal("live callback did not report the record before the sentinel")
	}

	stopSentinel()
	if got := sharedWatcherCount(dir); got != 1 {
		t.Fatalf("sharedWatcherCount = %d after removing the sentinel, want 1", got)
	}
	stopSecond()
	stopSecond() // idempotent
	if got := sharedWatcherCount(dir); got != 0 {
		t.Fatalf("sharedWatcherCount = %d after the last stop, want 0", got)
	}
	aDrain(second)

	sentinel = make(chan int, 16)
	stopSentinel, err = Watch(dir, func(pid int) { sentinel <- pid })
	if err != nil {
		t.Fatalf("second sentinel Watch: %v", err)
	}
	defer stopSentinel()
	aWriteRecord(t, dir, 6602, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-shared-2.sock",
	})
	aWaitPID(t, sentinel, 6602)
	select {
	case got := <-second:
		t.Fatalf("stopped callback reported pid %d after its Stop returned", got)
	default:
	}
}

// TestWatch_CleanupStopsBeforeTempDirRemoval checks that the registered stop
// runs before t.TempDir removes the directory.
func TestWatch_CleanupStopsBeforeTempDirRemoval(t *testing.T) {
	dir := t.TempDir()
	stop, err := Watch(dir, func(int) {})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if got := sharedWatcherCount(dir); got != 1 {
		t.Fatalf("sharedWatcherCount = %d after Watch, want 1", got)
	}

	// This cleanup is registered before stop, so it runs after stop and while
	// the temporary directory still exists; the two registrations must not be swapped.
	t.Cleanup(func() {
		if got := sharedWatcherCount(dir); got != 0 {
			t.Errorf("sharedWatcherCount = %d in cleanup after stop, want 0: the watcher outlived the stop that should precede the directory removal", got)
		}
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("temporary directory %s is gone in cleanup after stop: %v", dir, err)
		}
	})
	t.Cleanup(stop)
}

// TestWatch_UnknownEventCallsZero checks that an event on a file that is not a
// registry record is reported as 0.
func TestWatch_UnknownEventCallsZero(t *testing.T) {
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

	aWriteFile(t, filepath.Join(aSessionsDir(dir), "notes.txt"), []byte("not a record"))
	aWaitPID(t, pids, 0)
}

// TestWatch_RegistryDirectoryAppears checks that a registry directory created
// after Watch starts is watched and a record written there is reported.
func TestWatch_RegistryDirectoryAppears(t *testing.T) {
	dir := t.TempDir()
	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	aWriteRecord(t, dir, 6701, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-late.sock",
	})
	aWaitPID(t, pids, 6701)
}

// TestWatch_MissingConfigDir checks that a configuration directory absent at
// startup is not an error and is reported as 0.
func TestWatch_MissingConfigDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	pids := make(chan int, 16)
	stop, err := Watch(missing, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch on a missing configuration directory: %v", err)
	}
	defer stop()

	aWaitPID(t, pids, 0)
}

// TestWatch_StopWaitsForRunningCallback checks that Stop waits for a running
// callback and that no callback of that registration starts after it returns.
func TestWatch_StopWaitsForRunningCallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	invoked := make(chan int, 16)
	var starts atomic.Int64

	stop, err := Watch(dir, func(pid int) {
		invoked <- pid
		if starts.Add(1) == 1 {
			close(entered)
			<-release
		}
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	aWriteRecord(t, dir, 7201, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-stop-wait.sock",
	})
	select {
	case <-entered:
	case <-time.After(aWaitTimeout):
		t.Fatal("callback did not start")
	}

	stopCommitted := make(chan struct{})
	var commitOnce sync.Once
	testWatchBeforeStopWait = func() { commitOnce.Do(func() { close(stopCommitted) }) }
	t.Cleanup(func() { testWatchBeforeStopWait = nil })

	stopReturned := make(chan struct{})
	go func() {
		stop()
		close(stopReturned)
	}()

	select {
	case <-stopCommitted:
	case <-time.After(aWaitTimeout):
		t.Fatal("Stop did not reach its wait for the running callback")
	}
	select {
	case <-stopReturned:
		t.Fatal("Stop returned while its callback was still running")
	default:
	}

	aDrain(invoked)
	close(release)
	select {
	case <-stopReturned:
	case <-time.After(aWaitTimeout):
		t.Fatal("Stop did not return after the callback finished")
	}

	sentinel := make(chan int, 16)
	stopSentinel, err := Watch(dir, func(pid int) { sentinel <- pid })
	if err != nil {
		t.Fatalf("sentinel Watch: %v", err)
	}
	defer stopSentinel()
	aWriteRecord(t, dir, 7202, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-stop-wait-2.sock",
	})
	aWaitPID(t, sentinel, 7202)
	select {
	case pid := <-invoked:
		t.Fatalf("callback reporting pid %d started after Stop returned", pid)
	default:
	}
}

// TestWatch_ConcurrentStopsDoNotDeadlock checks that a stop called from inside
// a callback does not deadlock against a concurrent external stop.
func TestWatch_ConcurrentStopsDoNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	entered := make(chan struct{})
	callbackMayStop := make(chan struct{})
	callbackStopReturned := make(chan struct{})
	externalStopCommitted := make(chan struct{})
	externalStopReturned := make(chan struct{})

	var enterOnce, commitOnce sync.Once
	var stop func()
	var err error
	stop, err = Watch(dir, func(int) {
		enterOnce.Do(func() {
			close(entered)
			<-callbackMayStop
			stop()
			close(callbackStopReturned)
		})
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	testWatchBeforeStopWait = func() {
		commitOnce.Do(func() { close(externalStopCommitted) })
	}
	t.Cleanup(func() { testWatchBeforeStopWait = nil })

	aWriteRecord(t, dir, 7501, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-concurrent-stop.sock",
	})

	select {
	case <-entered:
	case <-time.After(aWaitTimeout):
		t.Fatal("callback did not start")
	}

	go func() {
		stop()
		close(externalStopReturned)
	}()

	select {
	case <-externalStopCommitted:
	case <-time.After(aWaitTimeout):
		t.Fatal("external Stop did not commit to releasing the callback")
	}

	// The callback's own Stop is released only after the external Stop has
	// entered its wait.
	close(callbackMayStop)

	select {
	case <-callbackStopReturned:
	case <-time.After(aWaitTimeout):
		t.Fatal("Stop called from inside its callback deadlocked against the external Stop")
	}
	select {
	case <-externalStopReturned:
	case <-time.After(aWaitTimeout):
		t.Fatal("external Stop did not return after the callback finished")
	}
}

// TestWatch_StopFromCallbackDoesNotDeadlock checks that a callback may stop its
// own registration without waiting for itself.
func TestWatch_StopFromCallbackDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	ready := make(chan struct{})
	finished := make(chan struct{}, 1)
	invoked := make(chan int, 16)
	var stop func()

	var err error
	stop, err = Watch(dir, func(pid int) {
		invoked <- pid
		<-ready
		stop()
		finished <- struct{}{}
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	close(ready)

	aWriteRecord(t, dir, 7301, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-reentrant-stop.sock",
	})
	select {
	case <-finished:
	case <-time.After(aWaitTimeout):
		t.Fatal("Stop called from its own callback deadlocked")
	}

	aDrain(invoked)

	sentinel := make(chan int, 16)
	stopSentinel, err := Watch(dir, func(pid int) { sentinel <- pid })
	if err != nil {
		t.Fatalf("sentinel Watch: %v", err)
	}
	defer stopSentinel()
	aWriteRecord(t, dir, 7302, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-reentrant-stop-2.sock",
	})
	aWaitPID(t, sentinel, 7302)
	select {
	case pid := <-invoked:
		t.Fatalf("callback reporting pid %d started after Stop returned", pid)
	default:
	}
}

// TestWatch_StartupWindowReportsRecord checks that a record created after the
// watch is installed and before the post-install snapshot is reported.
func TestWatch_StartupWindowReportsRecord(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	const pid = 7401
	testWatchAfterInstall = func() {
		aWriteRecord(t, dir, pid, map[string]any{
			"name":                "agent",
			"messagingSocketPath": "/tmp/watch-startup-window.sock",
		})
	}
	t.Cleanup(func() { testWatchAfterInstall = nil })

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(p int) { pids <- p })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	aWaitPID(t, pids, pid)
}

// TestWatch_ErrorsChannelReachesCallback checks that an error from the file
// watcher is reported to callbacks as 0.
func TestWatch_ErrorsChannelReachesCallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
		t.Fatalf("creating sessions directory: %v", err)
	}

	errs := make(chan error, 1)
	testWatchErrorSource = func() <-chan error { return errs }
	t.Cleanup(func() { testWatchErrorSource = nil })

	pids := make(chan int, 16)
	stop, err := Watch(dir, func(pid int) { pids <- pid })
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stop()

	errs <- errors.New("watch error")
	aWaitPID(t, pids, 0)
}
