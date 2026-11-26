package inbox

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// failureSetEvent is one event the watcher handled, with the watch path and the
// number of callbacks reporting process id 0 that handling that event added.
type failureSetEvent struct {
	name      string
	watchPath string
	zeros     int
}

// TestWatch_StartupReportsUnwatchableRegistry checks that a startup pass whose
// registry directory cannot be watched reports the fallback with exactly one 0
// even though the configuration directory itself is watchable.
func TestWatch_StartupReportsUnwatchableRegistry(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}
	if runtime.GOOS == "darwin" {
		// kqueue opens directories with O_EVTONLY, which needs no read
		// permission, so the failure is injected through the Add seam instead.
		fail := func(fsw *fsnotify.Watcher, path string) error {
			if path == sessions {
				return errors.New("injected watch failure")
			}
			return fsw.Add(path)
		}
		testWatchAdd.Store(&fail)
		t.Cleanup(func() { testWatchAdd.Store(nil) })
	} else {
		if err := os.Chmod(sessions, 0o000); err != nil {
			t.Fatalf("removing permissions from the registry directory: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(sessions, 0o700) })
	}

	var zeros atomic.Int64
	pids := make(chan int, 256)
	stop, err := Watch(dir, func(pid int) {
		if pid == 0 {
			zeros.Add(1)
		}
		pids <- pid
	})
	if err != nil {
		t.Fatalf("Watch with an unwatchable registry directory and a watchable configuration directory: %v", err)
	}
	defer stop()

	aWaitPID(t, pids, 0)
	aDrain(pids)
	if got := zeros.Load(); got != 1 {
		t.Fatalf("%d callbacks reported process id 0 after Watch, want exactly one", got)
	}

	// An unchanged failure set adds only the event's own 0; a second 0 means the
	// startup failure was reported again.
	handled := make(chan failureSetEvent, 16)
	afterHandle := func(event fsnotify.Event) {
		if filepath.Clean(event.Name) != filepath.Join(dir, "probe") {
			return
		}
		var path string
		if w := sharedWatcherForTest(dir); w != nil {
			path = w.watchPath
		}
		handled <- failureSetEvent{name: filepath.Clean(event.Name), watchPath: path, zeros: int(zeros.Load())}
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	if err := os.Mkdir(filepath.Join(dir, "probe"), 0o700); err != nil {
		t.Fatalf("creating a probe directory: %v", err)
	}
	got := aWaitFailureSetEvent(t, handled, filepath.Join(dir, "probe"))
	if got.watchPath != dir {
		t.Fatalf("watch path = %q, want the configuration directory %q", got.watchPath, dir)
	}
	if got.zeros != 2 {
		t.Fatalf("%d callbacks reported process id 0 after one more event, want the startup report plus the event's own", got.zeros)
	}
}

// TestWatch_FallbackReportsFailureSetChanges checks that a changed failure set
// is reported once, an unchanged one is not, and a failure-free pass clears the
// record so the same failure is reported again after it recurs.
func TestWatch_FallbackReportsFailureSetChanges(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	// The registry directory exists before the watcher starts; created
	// afterwards, the watch would stay on whichever directory the first
	// notification happened to observe.
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}

	// The seam fails the two deepest candidates with a message the test
	// controls; an empty message lets Add through.
	var (
		seamMu  sync.Mutex
		failMsg string
	)
	fail := func(fsw *fsnotify.Watcher, path string) error {
		seamMu.Lock()
		msg := failMsg
		seamMu.Unlock()
		if msg != "" && (path == sessions || path == dir) {
			return errors.New(msg)
		}
		return fsw.Add(path)
	}
	setFail := func(msg string) {
		seamMu.Lock()
		failMsg = msg
		seamMu.Unlock()
	}
	testWatchAdd.Store(&fail)
	t.Cleanup(func() { testWatchAdd.Store(nil) })

	var zeros atomic.Int64
	stop, err := Watch(dir, func(pid int) {
		if pid == 0 {
			zeros.Add(1)
		}
	})
	if err != nil {
		t.Fatalf("Watch on a watchable registry directory: %v", err)
	}
	defer stop()

	// An armed loss drops the installed watch once, leaving the state a watch
	// removal leaves, so the next event must re-add the candidates.
	var (
		dropMu    sync.Mutex
		dropArmed bool
	)
	armLoss := func() {
		dropMu.Lock()
		dropArmed = true
		dropMu.Unlock()
	}
	var zerosAtStart int64
	beforeHandle := func() {
		dropMu.Lock()
		armed := dropArmed
		dropArmed = false
		dropMu.Unlock()
		if armed {
			if w := sharedWatcherForTest(dir); w != nil {
				_ = w.fsw.Remove(w.watchPath)
				w.watchPath = ""
			}
		}
		zerosAtStart = zeros.Load()
	}
	testWatchBeforeHandleEvent.Store(&beforeHandle)
	t.Cleanup(func() { testWatchBeforeHandleEvent.Store(nil) })

	handled := make(chan failureSetEvent, 512)
	afterHandle := func(event fsnotify.Event) {
		var path string
		if w := sharedWatcherForTest(dir); w != nil {
			path = w.watchPath
		}
		handled <- failureSetEvent{
			name:      filepath.Clean(event.Name),
			watchPath: path,
			zeros:     int(zeros.Load() - zerosAtStart),
		}
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	setFail("injected watch failure one")
	armLoss()
	const firstPID = 7881
	record := filepath.Join(sessions, strconv.Itoa(firstPID)+".json")
	aWriteRecord(t, dir, firstPID, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-failure-set.sock",
	})
	got := aWaitFailureSetEvent(t, handled, record)
	if got.watchPath != root {
		t.Fatalf("watch path after both directories failed = %q, want the parent %q", got.watchPath, root)
	}
	if got.zeros != 1 {
		t.Fatalf("the changed failure set added %d callbacks with 0, want exactly one", got.zeros)
	}

	// The same failure set on later events is not reported again.
	for i := 0; i < 3; i++ {
		name := filepath.Join(root, "aaa-"+strconv.Itoa(i))
		if err := os.Mkdir(name, 0o700); err != nil {
			t.Fatalf("creating unrelated directory: %v", err)
		}
		got := aWaitFailureSetEvent(t, handled, name)
		if got.watchPath != root {
			t.Fatalf("event %d: watch path = %q, want %q", i+1, got.watchPath, root)
		}
		if got.zeros != 0 {
			t.Fatalf("event %d: an unchanged failure set added %d callbacks with 0, want none", i+1, got.zeros)
		}
	}

	// A changed error text is a changed failure set and is reported once.
	setFail("injected watch failure two")
	changed := filepath.Join(root, "bbb-changed")
	if err := os.Mkdir(changed, 0o700); err != nil {
		t.Fatalf("creating the changed-failure directory: %v", err)
	}
	got = aWaitFailureSetEvent(t, handled, changed)
	if got.zeros != 1 {
		t.Fatalf("a changed error text added %d callbacks with 0, want exactly one", got.zeros)
	}

	// A failure-free pass moves the watch down and clears the record.
	setFail("")
	moved := filepath.Join(root, "ccc-moved")
	if err := os.Mkdir(moved, 0o700); err != nil {
		t.Fatalf("creating the recovered directory: %v", err)
	}
	got = aWaitFailureSetEvent(t, handled, moved)
	if got.watchPath != sessions {
		t.Fatalf("watch path after Add succeeded again = %q, want %q", got.watchPath, sessions)
	}

	// The same failure as before is reported again because the failure-free
	// pass cleared the record.
	setFail("injected watch failure two")
	armLoss()
	const secondPID = 7882
	record2 := filepath.Join(sessions, strconv.Itoa(secondPID)+".json")
	aWriteRecord(t, dir, secondPID, map[string]any{
		"name":                "agent",
		"messagingSocketPath": "/tmp/watch-failure-set.sock",
	})
	got = aWaitFailureSetEvent(t, handled, record2)
	if got.watchPath != root {
		t.Fatalf("watch path after the failure recurred = %q, want the parent %q", got.watchPath, root)
	}
	if got.zeros != 1 {
		t.Fatalf("the recurring failure added %d callbacks with 0, want exactly one", got.zeros)
	}
}

// TestWatch_FallbackReportsPathOnlyChange checks that a failure set whose paths
// changed while its error text and failure count did not is reported again, so
// the dedupe keys each failure by its path as well as its error.
func TestWatch_FallbackReportsPathOnlyChange(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	superRoot := filepath.Dir(root)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}

	// The seam refuses the paths in failPaths with one fixed error text, so a
	// pass can fail a different pair of candidates with the same text.
	var (
		seamMu    sync.Mutex
		failPaths = make(map[string]bool)
	)
	const msg = "injected watch failure"
	fail := func(fsw *fsnotify.Watcher, path string) error {
		seamMu.Lock()
		failing := failPaths[path]
		seamMu.Unlock()
		if failing {
			return errors.New(msg)
		}
		return fsw.Add(path)
	}
	setFailPaths := func(paths ...string) {
		seamMu.Lock()
		failPaths = make(map[string]bool, len(paths))
		for _, path := range paths {
			failPaths[path] = true
		}
		seamMu.Unlock()
	}
	testWatchAdd.Store(&fail)
	t.Cleanup(func() { testWatchAdd.Store(nil) })

	var zeros atomic.Int64
	stop, err := Watch(dir, func(pid int) {
		if pid == 0 {
			zeros.Add(1)
		}
	})
	if err != nil {
		t.Fatalf("Watch on a watchable registry directory: %v", err)
	}
	defer stop()

	// The watcher goroutine runs beforeAction at the start of the next event, so
	// the test moves the watch and changes the failure set without racing the
	// event loop.
	var (
		actionMu     sync.Mutex
		beforeAction func()
		zerosAtStart int64
	)
	setAction := func(action func()) {
		actionMu.Lock()
		beforeAction = action
		actionMu.Unlock()
	}
	beforeHandle := func() {
		actionMu.Lock()
		action := beforeAction
		actionMu.Unlock()
		if action != nil {
			action()
		}
		zerosAtStart = zeros.Load()
	}
	testWatchBeforeHandleEvent.Store(&beforeHandle)
	t.Cleanup(func() { testWatchBeforeHandleEvent.Store(nil) })

	handled := make(chan failureSetEvent, 128)
	afterHandle := func(event fsnotify.Event) {
		var path string
		if w := sharedWatcherForTest(dir); w != nil {
			path = w.watchPath
		}
		handled <- failureSetEvent{
			name:      filepath.Clean(event.Name),
			watchPath: path,
			zeros:     int(zeros.Load() - zerosAtStart),
		}
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	moveWatch := func(from, to string) func() {
		var once sync.Once
		return func() {
			once.Do(func() {
				w := sharedWatcherForTest(dir)
				if w == nil {
					return
				}
				_ = w.fsw.Remove(from)
				if to != "" {
					_ = w.fsw.Add(to)
				}
				w.watchPath = to
			})
		}
	}

	// The first pass fails the registry and configuration directories and moves
	// the watch to the parent: one 0 for the report and one for the event's own
	// non-record name.
	setFailPaths(sessions, dir)
	setAction(moveWatch(sessions, root))
	first := filepath.Join(sessions, "aaa-first")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatalf("creating the trigger in the registry directory: %v", err)
	}
	got := aWaitFailureSetEvent(t, handled, first)
	if got.watchPath != root {
		t.Fatalf("watch path after the registry and configuration directories failed = %q, want %q", got.watchPath, root)
	}
	if got.zeros != 2 {
		t.Fatalf("the first failure set added %d callbacks with 0, want the report and the event's own", got.zeros)
	}

	// The same paths and text add no 0.
	setAction(nil)
	same := filepath.Join(root, "aaa-same")
	if err := os.Mkdir(same, 0o700); err != nil {
		t.Fatalf("creating the unchanged-set trigger: %v", err)
	}
	got = aWaitFailureSetEvent(t, handled, same)
	if got.zeros != 0 {
		t.Fatalf("the unchanged failure set added %d callbacks with 0, want none", got.zeros)
	}

	// The registry directory disappears, so the same text and failure count now
	// name the configuration directory and the parent. Only the changed path can
	// make this a new set, and the event is outside the configuration directory,
	// so the set change contributes exactly one 0.
	if err := os.RemoveAll(sessions); err != nil {
		t.Fatalf("removing the registry directory: %v", err)
	}
	setFailPaths(dir, root)
	setAction(moveWatch(root, superRoot))
	changed := filepath.Join(root, "bbb-changed")
	if err := os.Mkdir(changed, 0o700); err != nil {
		t.Fatalf("creating the changed-set trigger: %v", err)
	}
	got = aWaitFailureSetEvent(t, handled, changed)
	if got.watchPath != superRoot {
		t.Fatalf("watch path after the parent pair failed = %q, want %q", got.watchPath, superRoot)
	}
	if got.zeros != 1 {
		t.Fatalf("a failure set whose paths changed but whose text and count did not added %d callbacks with 0, want exactly one", got.zeros)
	}
}

// TestWatch_FallbackReinstallDoesNotRereport checks that rebuilding a watch at
// the same level after it was lost does not report a failure set that was
// already reported: a successful Add does not clear the recorded set.
func TestWatch_FallbackReinstallDoesNotRereport(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}

	fail := func(fsw *fsnotify.Watcher, path string) error {
		if path == sessions {
			return errors.New("injected watch failure")
		}
		return fsw.Add(path)
	}
	testWatchAdd.Store(&fail)
	t.Cleanup(func() { testWatchAdd.Store(nil) })

	var zeros atomic.Int64
	pids := make(chan int, 128)
	stop, err := Watch(dir, func(pid int) {
		if pid == 0 {
			zeros.Add(1)
		}
		pids <- pid
	})
	if err != nil {
		t.Fatalf("Watch with an unwatchable registry directory: %v", err)
	}
	defer stop()

	aWaitPID(t, pids, 0)
	aDrain(pids)

	// The drop leaves the watch as a removal does; a successful re-add must not
	// clear the reported failure set, so the only 0 the probe adds is the
	// configuration directory's own.
	probe := filepath.Join(dir, "probe")
	var zerosAtStart int64
	var dropOnce sync.Once
	beforeHandle := func() {
		dropOnce.Do(func() {
			if w := sharedWatcherForTest(dir); w != nil {
				_ = w.fsw.Remove(w.watchPath)
				w.watchPath = ""
			}
		})
		zerosAtStart = zeros.Load()
	}
	testWatchBeforeHandleEvent.Store(&beforeHandle)
	t.Cleanup(func() { testWatchBeforeHandleEvent.Store(nil) })

	handled := make(chan int, 16)
	afterHandle := func(event fsnotify.Event) {
		if filepath.Clean(event.Name) != probe {
			return
		}
		handled <- int(zeros.Load() - zerosAtStart)
	}
	testWatchAfterHandleEvent.Store(&afterHandle)
	t.Cleanup(func() { testWatchAfterHandleEvent.Store(nil) })

	if err := os.Mkdir(probe, 0o700); err != nil {
		t.Fatalf("creating a probe directory: %v", err)
	}
	select {
	case delta := <-handled:
		if delta != 1 {
			t.Fatalf("the probe event added %d callbacks with 0, want only the configuration directory's own; the successful Add re-reported the fallback", delta)
		}
	case <-time.After(aWaitTimeout):
		t.Fatal("the watcher did not handle the probe event")
	}
}

// TestWatch_FallbackTriesEachCandidateOnce checks that one reconciliation
// attempts each candidate level at most once, so the search is bounded by the
// number of levels rather than by the attempt bound.
func TestWatch_FallbackTriesEachCandidateOnce(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}

	var (
		attemptsMu sync.Mutex
		attempts   = make(map[string]int)
	)
	fail := func(fsw *fsnotify.Watcher, path string) error {
		attemptsMu.Lock()
		attempts[path]++
		attemptsMu.Unlock()
		if path == sessions || path == dir {
			return errors.New("injected watch failure")
		}
		return fsw.Add(path)
	}
	testWatchAdd.Store(&fail)
	t.Cleanup(func() { testWatchAdd.Store(nil) })

	stop, err := Watch(dir, func(int) {})
	if err != nil {
		t.Fatalf("Watch with two failing Add attempts: %v", err)
	}
	defer stop()

	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	total := 0
	for path, n := range attempts {
		total += n
		if n > 1 {
			t.Fatalf("Add on %q was attempted %d times in one reconciliation, want at most once", path, n)
		}
	}
	if levels := aCandidateLevels(dir); total > levels {
		t.Fatalf("%d Add attempts in one reconciliation, more than the %d candidate levels", total, levels)
	}
}

// TestWatch_AllAddsFailNamesEveryFailure checks that Watch reports each failing
// candidate with its Add error instead of an empty "no directory to watch"
// message.
func TestWatch_AllAddsFailNamesEveryFailure(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "config")
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}

	const msg = "injected watch failure"
	fail := func(*fsnotify.Watcher, string) error { return errors.New(msg) }
	testWatchAdd.Store(&fail)
	t.Cleanup(func() { testWatchAdd.Store(nil) })

	if _, err := Watch(dir, func(int) {}); err == nil {
		t.Fatal("Watch with every candidate failing returned no error")
	} else {
		got := err.Error()
		for _, path := range []string{sessions, dir, root} {
			if !strings.Contains(got, path) {
				t.Fatalf("Watch error %q does not name the failing path %q", got, path)
			}
		}
		if n := strings.Count(got, msg); n != aCandidateLevels(dir) {
			t.Fatalf("Watch error names %d failures, want one per candidate level (%d): %q", n, aCandidateLevels(dir), got)
		}
	}
}

// aCandidateLevels counts the directories one reconciliation considers: the
// registry directory, the configuration directory and every ancestor of the
// configuration directory.
func aCandidateLevels(configDir string) int {
	n := 2
	for p := filepath.Dir(configDir); ; p = filepath.Dir(p) {
		n++
		if filepath.Dir(p) == p {
			return n
		}
	}
}

// aWaitFailureSetEvent waits until the watcher has handled the event for want.
// Events for other names are ignored.
func aWaitFailureSetEvent(t *testing.T, handled <-chan failureSetEvent, want string) failureSetEvent {
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
			return failureSetEvent{}
		}
	}
}
