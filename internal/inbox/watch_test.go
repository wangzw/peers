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

func TestREQAgentMessaging001_WatchReportsNewRecord(t *testing.T) {
	// REQ-agent-messaging-001
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

func TestREQAgentMessaging005_WatchReportsRename(t *testing.T) {
	// REQ-agent-messaging-005
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

	// "idle" and "busy" have the same JSON length; the rewrite lands on the
	// same modification time as the snapshot. Only the content digest can
	// notice this change, and it is the file system's write notification that
	// makes the watcher look.
	aWriteRecord(t, dir, 6501, map[string]any{
		"name":                "agent",
		"status":              "busy",
		"messagingSocketPath": "",
	})
	aSetModTime(t, path, fixed)

	aWaitPID(t, pids, 6501)
}

// TestWatch_StopSkipsPendingCallback covers a stop race. notify collects the
// callback list, the first callback blocks, and stop removes the other two
// registrations while it runs. When the first callback returns and notify
// walks the list it collected earlier, the removed registrations must not be
// invoked: a stopped callback is not called after its stop returned.
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

	// Every registration runs the same function. The first invocation records
	// which registration it belongs to and blocks; later invocations only
	// report themselves. Reaching a registration whose stop has returned is
	// the race this test detects.
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
	// When it receives the record, every earlier registration for that event
	// has already run or been skipped.
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

	// Registration 3 stays; one and two are stopped while the event loop is
	// still inside the first callback. Exactly one registration survives, so
	// the watcher itself is not shut down and the collected list is walked.
	// stopOne waits for the callback that is running, so it has to run beside
	// the release below.
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

	// The sentinel receiving the record is the event that closes the window:
	// callbacks run serially, so the stopped registrations have already been
	// offered this notification and must not have been invoked.
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

	// A sentinel on the same directory witnesses that the notification for
	// 6601 was delivered, so the stopped first callback's silence is a fact
	// about that event rather than the length of a quiet period.
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

	// No watcher remains for the stopped registration. A new watcher on the
	// same directory makes the later record an observable event: it receives
	// the record, and the stopped callback must not.
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

// TestWatch_CleanupStopsBeforeTempDirRemoval pins the cleanup order this
// package relies on: the temporary directory comes from t.TempDir, the shared
// watcher is started after it, and the stop function is registered after the
// cleanup that observes the watcher. t.Cleanup runs last-in-first-out, so the
// observation runs after stop has unregistered the watcher and before
// t.TempDir removes the directory. Nothing here waits on time; the order alone
// decides the outcome.
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
	// the temporary directory still exists.
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

// TestWatch_MissingConfigDir pins the startup fallback: a configuration
// directory that does not exist yet is not an error. The watcher follows the
// nearest existing ancestor and reports once with 0 so the caller re-reads what
// it cares about.
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

// TestWatch_StopWaitsForRunningCallback pins the Stop contract: Stop must not
// return while a callback it registered is still running, and no callback of
// that registration may begin after Stop returns. The first callback blocks on
// a channel while Stop runs beside it.
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

	// (a) Stop must wait for the callback that is blocked on release. The seam
	// reports that Stop has released the registration and is about to wait; it
	// must not return until the callback does.
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

	// (b) A record written after Stop returned must not start a callback. The
	// sentinel on the same directory is the event that shows the notification
	// was delivered to the registrations still present.
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

// TestWatch_ConcurrentStopsDoNotDeadlock covers two calls of the same stop:
// one from inside the callback, one from an external goroutine. The external
// call commits to releasing the registration first and then waits for the
// running callback; the callback calls the same stop while that wait is in
// progress. A stop serialized through sync.Once makes the callback block on the
// Once held by the external call whose wait depends on that callback, so the
// two wait for each other forever.
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

	// The external Stop is committed once it reaches the point where it is
	// about to wait for running callbacks. The callback's own Stop is released
	// only after that, so the external call certainly holds whatever
	// serialization the implementation uses before the callback asks for it.
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

	// Both Stop calls are now in flight: the external one waits for the
	// callback, and the callback calls Stop from inside that invocation.
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

// TestWatch_StopFromCallbackDoesNotDeadlock records the rule for a callback
// that stops its own registration: Stop detects that it is running inside that
// callback and does not wait for it, because waiting would wait for the caller.
// No further invocation starts once Stop returns.
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

	// The callback's own Stop shut the watcher down. A fresh watcher on the
	// same directory makes the later record an observable event: it receives
	// the record, and the stopped callback must not.
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

// TestWatch_StartupWindowReportsRecord creates a record after the file watch is
// installed and before the post-install snapshot is taken. The difference
// between the two snapshots must report it: a single snapshot taken after the
// watch would fold the record into the baseline and never report the creation.
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

// TestWatch_ErrorsChannelReachesCallback pins the run loop's error input: an
// error arriving on the file watcher's Errors channel must reach
// handleWatchError and report pid 0, so that every caller re-reads what it
// cares about. The fsnotify backend owns fsw.Errors and writes to it from its
// own goroutine, so the test substitutes the channel the loop reads before the
// loop starts.
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

	// No record exists and none is written, so a callback can only come from
	// the error this test sends.
	errs <- errors.New("watch error")
	aWaitPID(t, pids, 0)
}
