package inbox

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

// watcher observes one configuration directory. A configuration directory has
// at most one watcher; Watch shares it among all callbacks and closes it when
// the last callback is removed.
//
// Changes are learned from the operating system's file change notifications
// alone. There is no periodic scan: a notification the platform loses is not
// repaired here, because callers reread a record before using it. The one
// exception is the watcher's own bookkeeping: when the operating system drops
// a watch because its directory was removed or renamed, the watcher notices
// from the notification and installs the watch again.
type watcher struct {
	configDir    string
	sessionsPath string
	fsw          *fsnotify.Watcher

	stopCh   chan struct{}
	stopOnce sync.Once
	stopped  atomic.Bool

	// watchErrors is the channel the run goroutine reads error notifications
	// from. It is fsw.Errors in production; a test replaces it before the loop
	// starts so that an error can be delivered through the loop itself.
	watchErrors <-chan error

	// callbackGID is the goroutine executing a callback, or 0 when no callback
	// is running. Stop reads it to recognize a callback that stops its own
	// registration, which must not wait for itself.
	callbackGID atomic.Uint64

	mu        sync.Mutex
	callbacks []*callbackEntry

	// The fields below are owned by the run goroutine. They are set before it
	// starts and are not touched elsewhere.
	snapshot map[int]fileStamp
	// watchPath is the path currently registered with fsw. It is empty when no
	// watch is installed, which can happen only when every directory the
	// watcher could follow has disappeared.
	watchPath string
	// failedWatch is the path whose Add failed most recently. The next
	// reconciliation skips it and falls back to its nearest existing ancestor
	// instead of retrying the same path forever.
	failedWatch string
}

// watchReconcileAttempts bounds the loop that reconciles the installed watch
// with the file system. Every iteration either installs a watch or falls back
// one directory towards the root, so a stable file system settles long before
// the bound; exceeding it means the tree kept changing underneath the watcher.
const watchReconcileAttempts = 64

// testWatchAfterInstall, when set by a test, runs after the file watch is
// installed and before the post-install snapshot is taken. It makes the
// otherwise unobservable startup window deterministic: a record created there
// must be reported by the initial difference. Production leaves it nil.
var testWatchAfterInstall func()

// testWatchErrorSource, when set by a test, returns the channel the run
// goroutine reads error notifications from instead of fsw.Errors. It runs
// after the watcher is built and before the event loop starts, because the
// fsnotify backend owns fsw.Errors and writes to it from its own goroutine.
// Production leaves it nil.
var testWatchErrorSource func() <-chan error

// testWatchBeforeHandleEvent, when set by a test, runs at the start of
// handleEvent, after the event has been read and before the watcher reconciles
// its target. It makes the race between a directory being removed and being
// recreated deterministic: a test recreates the directory there, so the
// removal event is examined while the path already exists again. Production
// leaves it nil.
var testWatchBeforeHandleEvent atomic.Pointer[func()]

// testWatchAfterHandleEvent, when set by a test, runs at the end of
// handleEvent with the event that was handled. It lets a test wait until a
// directory event has been fully processed before acting on the directory.
// Production leaves it nil.
var testWatchAfterHandleEvent atomic.Pointer[func(fsnotify.Event)]

// testWatchAfterStatBeforeAdd, when set by a test, runs after the watcher has
// chosen a target and before it adds that target. It makes the race between
// the existence check and Add deterministic: a test creates the directory in
// that window, whose creation notification is lost because the parent is not
// watched at that moment. Production leaves it nil.
var testWatchAfterStatBeforeAdd atomic.Pointer[func()]

// testWatchBeforeStopWait, when set by a test, runs after a Stop call has
// taken responsibility for releasing its registration and before that call
// waits for callbacks still running. It makes the ordering between two
// concurrent Stop calls deterministic. Production leaves it nil.
var testWatchBeforeStopWait func()

// testWatchBeforePendingWait, when set by a test, runs in Watch after it has
// found a watcher creation for its directory still in flight and before it
// waits for that creation. It makes the wait deterministic: a test starts the
// waiter only while the creator is blocked, so the waiter certainly takes the
// in-flight path instead of building a second watcher. Production leaves it
// nil.
var testWatchBeforePendingWait func()

// callbackEntry is one registration. stopped is guarded by watcher.mu: notify
// tests it and records an in-flight invocation under that same lock, so once
// stop has set stopped no further invocation can begin. running counts the
// invocations that have begun and not returned; stop waits on it so that no
// callback outlives the stop.
type callbackEntry struct {
	fn      func(int)
	stopped bool // guarded by watcher.mu
	running sync.WaitGroup
}

// newWatcher creates an independent watcher with one callback registered. Its
// main purpose is to let tests drive a watcher without the package-level
// registry; production code goes through Watch so that watchers are shared.
func newWatcher(configDir string, onChange func(int)) (*watcher, error) {
	w, _, err := makeWatcher(configDir, onChange)
	return w, err
}

// makeWatcher creates a watcher, registers onChange and starts its event loop.
// The callback is registered before the initial snapshot and before the loop
// starts, so no notification can be consumed while the watcher has no
// callback. It returns the registration so that Watch can build a stop function
// that waits on it. It must be called without sharedWatchersMu held: scanning
// the registry directory and hashing its records can take as long as the files
// are large.
func makeWatcher(configDir string, onChange func(int)) (*watcher, *callbackEntry, error) {
	dir, err := resolveConfigDir(configDir)
	if err != nil {
		return nil, nil, err
	}
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create the file watcher: %w", err)
	}
	w := &watcher{
		configDir:    dir,
		sessionsPath: sessionsDir(dir),
		fsw:          fsw,
		stopCh:       make(chan struct{}),
		watchErrors:  fsw.Errors,
	}

	// Take one snapshot before the watch is installed and one after. A record
	// that appears between them is absent from the first and present in the
	// second, so the difference reports it. A single snapshot taken after Add
	// would fold such a record into the baseline: the notification that
	// follows finds no difference and the creation is never reported.
	baseline := scanSessions(w.sessionsPath)

	climbed, err := w.installWatch()
	if err != nil {
		_ = fsw.Close()
		return nil, nil, err
	}

	if testWatchAfterInstall != nil {
		testWatchAfterInstall()
	}
	entry := w.addCallback(onChange)
	w.snapshot = scanSessions(w.sessionsPath)
	initial := changedPIDs(baseline, w.snapshot)
	if climbed {
		// The configuration directory was not there when the watcher started,
		// so no watch covers anything inside it yet. Report once with 0 so the
		// caller re-reads what it cares about; the ancestor watch reports the
		// directory when it appears and the watcher moves its watch down.
		initial = append([]int{0}, initial...)
	}

	if testWatchErrorSource != nil {
		w.watchErrors = testWatchErrorSource()
	}
	go w.run(initial)
	return w, entry, nil
}

// installWatch installs the first watch with the same target selection the
// event loop uses: the registry directory, else the configuration directory,
// else the nearest existing ancestor. It reports climbed when the
// configuration directory did not exist and the watch had to move above it.
// The watcher then follows that ancestor and moves the watch down again when
// the directory appears.
//
// It fails only when no candidate directory can be watched at all, because no
// notification could then restore the watch.
func (w *watcher) installWatch() (climbed bool, err error) {
	w.ensureWatchTarget()
	switch w.watchPath {
	case "":
		return false, fmt.Errorf("cannot watch %q: there is no directory to watch", w.configDir)
	case w.sessionsPath, w.configDir:
		return false, nil
	default:
		return true, nil
	}
}

// desiredWatchPath returns the deepest directory the watcher can observe: the
// registry directory when it exists, else the configuration directory, else
// the nearest existing ancestor of the configuration directory. It skips
// failedWatch so that a failed Add falls back one level instead of being
// retried forever.
func (w *watcher) desiredWatchPath() string {
	candidates := []string{w.sessionsPath, w.configDir}
	for p := filepath.Dir(w.configDir); ; p = filepath.Dir(p) {
		candidates = append(candidates, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	for _, path := range candidates {
		if path == w.failedWatch {
			continue
		}
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			return path
		}
	}
	return ""
}

// Watch reports changes to the session registry under configDir. Every record
// that appears, changes or disappears is reported as its process id; an event
// whose record cannot be identified is reported as 0 so that the caller can
// re-read what it cares about.
//
// Watchers are shared per configuration directory. Calling stop more than once
// is safe: only one call removes the registration, and the others still wait
// for an invocation of it that had already begun.
//
// When stop returns, onChange is not called again, and an invocation that had
// already begun has finished. A callback may call its own stop: that call does
// not wait for the callback making it (waiting would deadlock on the calling
// goroutine); the invocation continues after stop returns and no new one
// starts. Two concurrent stop calls never wait for each other, so a callback
// stopping itself cannot deadlock against an external stop that is waiting for
// that callback.
//
// A callback must not stop a registration on a different configuration
// directory while a callback of that other watcher can stop this one. Each
// stop waits for the callbacks already running on its own watcher, so two
// callbacks that stop each other's watchers wait on each other forever. The
// same registration may be stopped from its own callback, as described above.
func Watch(configDir string, onChange func(int)) (stop func(), err error) {
	dir, err := resolveConfigDir(configDir)
	if err != nil {
		return nil, err
	}

	for {
		sharedWatchersMu.Lock()
		if w := sharedWatchers[dir]; w != nil {
			entry := w.addCallback(onChange)
			sharedWatchersMu.Unlock()
			return func() { releaseWatcher(dir, w, entry) }, nil
		}
		if pending := sharedWatcherCreations[dir]; pending != nil {
			sharedWatchersMu.Unlock()
			if testWatchBeforePendingWait != nil {
				testWatchBeforePendingWait()
			}
			<-pending.done
			if pending.err != nil {
				return nil, pending.err
			}
			// The watcher the creator installed may have been stopped again
			// before this call resumed; look it up once more instead of
			// keeping a registration on a watcher that is shutting down.
			continue
		}
		pending := &watcherCreation{done: make(chan struct{})}
		sharedWatcherCreations[dir] = pending
		sharedWatchersMu.Unlock()

		w, entry, buildErr := makeWatcher(dir, onChange)

		sharedWatchersMu.Lock()
		delete(sharedWatcherCreations, dir)
		pending.err = buildErr
		if buildErr == nil {
			sharedWatchers[dir] = w
		}
		close(pending.done)
		sharedWatchersMu.Unlock()

		if buildErr != nil {
			return nil, buildErr
		}
		return func() { releaseWatcher(dir, w, entry) }, nil
	}
}

var (
	sharedWatchersMu       sync.Mutex
	sharedWatchers         = make(map[string]*watcher)
	sharedWatcherCreations = make(map[string]*watcherCreation)
)

// watcherCreation is a watcher being built. Watch calls for the same directory
// wait for it instead of building a second watcher, while the existence checks
// and the content digests of the initial scan run outside the shared lock.
type watcherCreation struct {
	done chan struct{}
	err  error
}

// sharedWatcherCount reports how many watchers the package keeps for dir. It
// exists for the tests that verify sharing.
func sharedWatcherCount(dir string) int {
	sharedWatchersMu.Lock()
	defer sharedWatchersMu.Unlock()
	if _, ok := sharedWatchers[dir]; ok {
		return 1
	}
	return 0
}

// sharedWatcherForTest returns the watcher the package keeps for dir, or nil.
// Comparing the pointer across two Watch calls is how a test proves that the
// second call reused the first watcher instead of building another one.
func sharedWatcherForTest(dir string) *watcher {
	sharedWatchersMu.Lock()
	defer sharedWatchersMu.Unlock()
	return sharedWatchers[dir]
}

// releaseWatcher unregisters entry and waits for an invocation of it that had
// already begun. Every Stop call runs this, not only the first: the removal is
// idempotent, and every caller keeps the promise that a callback it registered
// has finished. No lock is held while waiting, and callers do not wait for each
// other, so a callback that calls its own stop can make progress even while an
// external call waits for that same callback.
func releaseWatcher(dir string, w *watcher, entry *callbackEntry) {
	sharedWatchersMu.Lock()
	_, last := w.removeCallback(entry)
	if last && sharedWatchers[dir] == w {
		delete(sharedWatchers, dir)
	}
	sharedWatchersMu.Unlock()

	if testWatchBeforeStopWait != nil {
		testWatchBeforeStopWait()
	}

	// Waiting happens outside sharedWatchersMu so that a callback that touches
	// the registry is never blocked behind its own stop.
	w.awaitCallback(entry)
	if last {
		w.shutdown()
	}
}

func (w *watcher) addCallback(onChange func(int)) *callbackEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry := &callbackEntry{fn: onChange}
	w.callbacks = append(w.callbacks, entry)
	return entry
}

// removeCallback marks the entry stopped and unregisters it. The mark and the
// removal share the mutex notify uses, so a notification already in flight
// either registers its invocation before the mark and is waited for, or sees
// the mark and skips the entry; no callback can begin after this returns. The
// call is idempotent: a second stop call marks the entry again, finds it
// already removed and reports removed as false.
func (w *watcher) removeCallback(entry *callbackEntry) (removed, last bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry.stopped = true
	for i, e := range w.callbacks {
		if e != entry {
			continue
		}
		w.callbacks = append(w.callbacks[:i], w.callbacks[i+1:]...)
		return true, len(w.callbacks) == 0
	}
	return false, false
}

// awaitCallback waits for an invocation that had already begun when its
// registration was stopped. A callback that calls its own stop is not waited
// for: it runs on the same goroutine as Stop, so waiting would deadlock. That
// invocation is already running, and the rule that stopped callbacks never
// start again means no new invocation can follow it. Each Stop call does this
// independently and no lock is held across the wait, so an external Stop that
// is waiting for a callback does not block the callback's own Stop.
func (w *watcher) awaitCallback(entry *callbackEntry) {
	if entry == nil || w.inCallback() {
		return
	}
	entry.running.Wait()
}

// inCallback reports whether the calling goroutine is executing a callback of
// this watcher.
func (w *watcher) inCallback() bool {
	id := w.callbackGID.Load()
	return id != 0 && id == currentGoroutineID()
}

// invoke runs one callback and records the invocation's goroutine so that a
// Stop called from inside the callback can recognize it.
func (w *watcher) invoke(entry *callbackEntry, pid int) {
	defer entry.running.Done()
	w.callbackGID.Store(currentGoroutineID())
	defer w.callbackGID.Store(0)
	entry.fn(pid)
}

// currentGoroutineID returns the calling goroutine's id. The runtime does not
// expose one, so it is read from the stack header ("goroutine <id> [...]").
// It is used only to keep a callback from waiting for itself.
func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n <= len(prefix) {
		return 0
	}
	rest := string(buf[len(prefix):n])
	if i := strings.IndexByte(rest, ' '); i > 0 {
		id, err := strconv.ParseUint(rest[:i], 10, 64)
		if err == nil {
			return id
		}
	}
	return 0
}

func (w *watcher) shutdown() {
	w.stopOnce.Do(func() {
		w.stopped.Store(true)
		close(w.stopCh)
		_ = w.fsw.Close()
	})
}

// run delivers the records that appeared while the watcher was starting and
// then follows file notifications until stop closes the watcher.
func (w *watcher) run(initial []int) {
	for _, pid := range initial {
		w.notify(pid)
	}
	for {
		select {
		case <-w.stopCh:
			return
		case event, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			w.handleEvent(event)
		case _, ok := <-w.watchErrors:
			if !ok {
				return
			}
			w.handleWatchError()
		}
	}
}

// handleWatchError handles one error notification from the file watcher. An
// error means notifications may have been lost, so every callback runs with 0
// and the caller re-reads what it cares about. In production it runs on the run
// goroutine when an error arrives on fsw.Errors. Tests call it directly to
// exercise the path without writing to that channel, which the kqueue backend
// reads from its own goroutine.
func (w *watcher) handleWatchError() {
	w.notify(0)
}

func (w *watcher) handleEvent(event fsnotify.Event) {
	if w.stopped.Load() {
		return
	}
	if hook := testWatchBeforeHandleEvent.Load(); hook != nil {
		(*hook)()
	}
	if w.watchRemoved(event) {
		// The operating system drops the watch when the watched directory is
		// removed or renamed. The path may exist again by the time this event
		// is examined, so the recorded target cannot be trusted: force the
		// reconciliation to install the watch again.
		w.watchPath = ""
	}
	switched := w.ensureWatchTarget()

	name := filepath.Clean(event.Name)
	switch {
	case name == w.sessionsPath || filepath.Dir(name) == w.sessionsPath:
		// A change in the registry directory, including the directory
		// itself. Report the records that differ and, for anything that is
		// not a record, a 0 so the caller re-reads what it cares about.
		w.scanAndNotify()
		if _, isRecord := parseRecordName(filepath.Base(name)); name == w.sessionsPath || !isRecord {
			w.notify(0)
		}
	case name == w.configDir || pathWithin(w.configDir, name):
		w.scanAndNotify()
		w.notify(0)
	case switched:
		// An event on the way to the configuration directory, for example its
		// parent while the configuration directory is missing. The watch
		// moved closer, so report once for the caller to re-read.
		w.scanAndNotify()
		w.notify(0)
	}
	if hook := testWatchAfterHandleEvent.Load(); hook != nil {
		(*hook)(event)
	}
}

// watchRemoved reports whether event says that the directory currently watched
// was removed or renamed. The operating system drops that watch, so the caller
// must rebuild it even when the path exists again.
func (w *watcher) watchRemoved(event fsnotify.Event) bool {
	if w.watchPath == "" {
		return false
	}
	if filepath.Clean(event.Name) != filepath.Clean(w.watchPath) {
		return false
	}
	return event.Op&(fsnotify.Remove|fsnotify.Rename) != 0
}

// pathWithin reports whether child is strictly inside parent.
func pathWithin(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// ensureWatchTarget makes the installed watch match the file system. It runs
// on the construction goroutine before the event loop starts and afterwards
// only on the run goroutine. It reports whether the watched path changed.
//
// The file system is checked again after every Add, because a directory
// created between the existence check and Add produces no notification for
// the new watch. When no watch can be installed, the watcher calls back with 0
// once per reconciliation instead of failing silently; later events from the
// nearest existing ancestor let it pick the watch up again.
func (w *watcher) ensureWatchTarget() (switched bool) {
	if w.stopped.Load() {
		return false
	}
	notified := false
	notifyLost := func() {
		if !notified {
			notified = true
			w.notify(0)
		}
	}
	for attempt := 0; attempt < watchReconcileAttempts; attempt++ {
		desired := w.desiredWatchPath()
		if desired == w.watchPath {
			return switched
		}
		if desired == "" {
			if w.watchPath != "" {
				_ = w.fsw.Remove(w.watchPath)
				w.watchPath = ""
				switched = true
			}
			notifyLost()
			return switched
		}
		if hook := testWatchAfterStatBeforeAdd.Load(); hook != nil {
			(*hook)()
		}
		if err := w.fsw.Add(desired); err != nil {
			w.failedWatch = desired
			notifyLost()
			continue
		}
		if w.watchPath != "" {
			_ = w.fsw.Remove(w.watchPath)
		}
		w.watchPath = desired
		w.failedWatch = ""
		switched = true
	}
	notifyLost()
	return switched
}

// scanAndNotify compares the registry directory with the snapshot and reports
// every record that appeared, changed or disappeared. It runs only in
// response to a file notification or a watch change, to identify the records
// the notification is about and to collapse repeated notifications about one
// file; it is not a periodic check.
func (w *watcher) scanAndNotify() {
	if w.stopped.Load() {
		return
	}
	current := scanSessions(w.sessionsPath)
	changed := changedPIDs(w.snapshot, current)
	w.snapshot = current
	for _, pid := range changed {
		w.notify(pid)
	}
}

// notify calls every registered callback with pid. Callbacks run serially on
// the watcher goroutine. A registration that stop has removed is skipped even
// when the callback list was collected before the removal. The stopped test and
// the in-flight registration happen under one lock, so a callback can never
// begin after stop has marked the entry stopped.
func (w *watcher) notify(pid int) {
	if w.stopped.Load() {
		return
	}
	w.mu.Lock()
	entries := make([]*callbackEntry, len(w.callbacks))
	copy(entries, w.callbacks)
	w.mu.Unlock()
	for _, entry := range entries {
		w.mu.Lock()
		if w.stopped.Load() {
			w.mu.Unlock()
			return
		}
		if entry.stopped {
			w.mu.Unlock()
			continue
		}
		entry.running.Add(1)
		w.mu.Unlock()
		w.invoke(entry, pid)
	}
}
