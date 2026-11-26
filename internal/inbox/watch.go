package inbox

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

type watcher struct {
	configDir    string
	sessionsPath string
	fsw          *fsnotify.Watcher

	stopCh   chan struct{}
	stopOnce sync.Once
	stopped  atomic.Bool

	// fsw.Errors in production; a test replaces it before the loop starts.
	watchErrors <-chan error

	callbackGID atomic.Uint64

	mu        sync.Mutex
	callbacks []*callbackEntry

	// The fields below are owned by the run goroutine; they are set before it
	// starts.
	snapshot  map[int]fileStamp
	watchPath string
	// watchFailure is the failure set the callbacks were last told about; empty
	// when that pass had no failing Add.
	watchFailure string
	// watchFailureDetail is the readable form of watchFailure, one "path: error"
	// per failing Add.
	watchFailureDetail []string
}

const watchReconcileAttempts = 64 // bounds the reconcile loop; a stable tree settles well before it

// testWatchAfterInstall, when set by a test, runs after the file watch is
// installed and before the post-install snapshot is taken.
var testWatchAfterInstall func()

// testWatchErrorSource, when set by a test, returns the channel the run
// goroutine reads error notifications from instead of fsw.Errors; it is read
// before the loop starts. Production leaves it nil.
var testWatchErrorSource func() <-chan error

// testWatchBeforeHandleEvent, when set by a test, runs at the start of
// handleEvent, after the event has been read.
var testWatchBeforeHandleEvent atomic.Pointer[func()]

// testWatchAfterHandleEvent, when set by a test, runs at the end of
// handleEvent with the event that was handled.
var testWatchAfterHandleEvent atomic.Pointer[func(fsnotify.Event)]

// testWatchAfterStatBeforeAdd, when set by a test, runs after the watcher has
// chosen a target and before it adds that target.
var testWatchAfterStatBeforeAdd atomic.Pointer[func()]

// testWatchAdd, when set by a test, installs the watch instead of fsw.Add.
var testWatchAdd atomic.Pointer[func(*fsnotify.Watcher, string) error]

// testWatchBeforeStopWait, when set by a test, runs after a Stop call has
// taken responsibility for releasing its registration and before it waits for
// callbacks still running.
var testWatchBeforeStopWait func()

// testWatchBeforePendingWait, when set by a test, runs in Watch after it has
// found a watcher creation for its directory still in flight and before it
// waits for that creation.
var testWatchBeforePendingWait func()

type callbackEntry struct {
	fn      func(int)
	stopped bool // guarded by watcher.mu
	running sync.WaitGroup
}

func newWatcher(configDir string, onChange func(int)) (*watcher, error) {
	w, _, err := makeWatcher(configDir, onChange)
	return w, err
}

// makeWatcher creates a watcher, registers onChange and starts its event loop.
// The callback is registered before the initial snapshot and before the loop
// starts, so no notification can be consumed while the watcher has no
// callback. It must be called without sharedWatchersMu held.
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

	// Take one snapshot before the watch is installed and one after: a record
	// that appears between them is absent from the first and present in the
	// second, so the difference reports it.
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
	if climbed || w.watchFailure != "" {
		initial = append([]int{0}, initial...)
	}

	if testWatchErrorSource != nil {
		w.watchErrors = testWatchErrorSource()
	}
	go w.run(initial)
	return w, entry, nil
}

func (w *watcher) installWatch() (climbed bool, err error) {
	w.ensureWatchTarget()
	switch w.watchPath {
	case "":
		if len(w.watchFailureDetail) > 0 {
			return false, fmt.Errorf("cannot watch %q: no directory could be watched (%s)", w.configDir, strings.Join(w.watchFailureDetail, "; "))
		}
		return false, fmt.Errorf("cannot watch %q: there is no directory to watch", w.configDir)
	case w.sessionsPath, w.configDir:
		return false, nil
	default:
		return true, nil
	}
}

func (w *watcher) desiredWatchPath(skipped map[string]bool) string {
	candidates := []string{w.sessionsPath, w.configDir}
	for p := filepath.Dir(w.configDir); ; p = filepath.Dir(p) {
		candidates = append(candidates, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	for _, path := range candidates {
		if skipped[path] {
			continue
		}
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			return path
		}
	}
	return ""
}

func (w *watcher) addWatch(path string) error {
	if hook := testWatchAdd.Load(); hook != nil {
		return (*hook)(w.fsw, path)
	}
	return w.fsw.Add(path)
}

// watchFailureKey quotes the path and the error text so the two stay apart.
func watchFailureKey(path string, err error) string {
	return fmt.Sprintf("%q\x00%q", path, err.Error())
}

// watchFailureSet is empty exactly when the pass had no failing Add.
func watchFailureSet(failures []string) string {
	sorted := append([]string(nil), failures...)
	sort.Strings(sorted)
	return strings.Join(sorted, "\n")
}

// Watch reports changes to the session registry under configDir, where an empty
// configDir means this account's default, $HOME/.claude. Every record that
// appears, changes or disappears is reported as its process id; an event whose
// record cannot be identified is reported as 0. When reconciliation finds no
// directory it can watch, every callback is called with 0. onChange runs
// serially on the watcher's goroutine. It returns an error when the home
// directory cannot be resolved, the file watcher cannot be created, or there is
// no directory to watch.
//
// Watchers are shared per configuration directory, and calling stop more than
// once is safe. When stop returns, onChange is not called again and an
// invocation that had already begun has finished; a callback may call its own
// stop, which does not wait for the callback making it. A callback must not
// stop a registration on a different configuration directory while a callback
// of that other watcher can stop this one: each stop waits for the callbacks
// already running on its own watcher, so the two wait on each other forever.
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
			// before this call resumed; look it up once more.
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

// watcherCreation is a watcher being built; Watch calls for the same directory
// wait for it, while the existence checks and content digests of the initial
// scan run outside the shared lock.
type watcherCreation struct {
	done chan struct{}
	err  error
}

func sharedWatcherCount(dir string) int {
	sharedWatchersMu.Lock()
	defer sharedWatchersMu.Unlock()
	if _, ok := sharedWatchers[dir]; ok {
		return 1
	}
	return 0
}

func sharedWatcherForTest(dir string) *watcher {
	sharedWatchersMu.Lock()
	defer sharedWatchersMu.Unlock()
	return sharedWatchers[dir]
}

// releaseWatcher unregisters entry and waits for an invocation of it that had
// already begun; every stop call runs this, not only the first. No lock is held
// while waiting, and callers do not wait for each other.
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

	// Wait outside sharedWatchersMu so a callback never blocks behind its own
	// stop.
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

// removeCallback marks the entry stopped and unregisters it under the mutex
// notify uses, so no callback can begin after this returns. A second call finds
// it already removed.
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

// awaitCallback waits for an invocation that had already begun, except when
// the calling goroutine is inside that callback, where waiting would deadlock.
func (w *watcher) awaitCallback(entry *callbackEntry) {
	if entry == nil || w.inCallback() {
		return
	}
	entry.running.Wait()
}

func (w *watcher) inCallback() bool {
	id := w.callbackGID.Load()
	return id != 0 && id == currentGoroutineID()
}

func (w *watcher) invoke(entry *callbackEntry, pid int) {
	defer entry.running.Done()
	w.callbackGID.Store(currentGoroutineID())
	defer w.callbackGID.Store(0)
	entry.fn(pid)
}

// currentGoroutineID returns the calling goroutine's id, parsed from the
// runtime stack header because the runtime exposes none; it is used only to
// keep a callback from waiting for itself.
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
// error means notifications may have been lost, so every callback runs with 0;
// tests call it directly instead of writing to fsw.Errors, which the kqueue
// backend reads itself.
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
		// removed or renamed, and the path may exist again already, so the
		// recorded target cannot be trusted.
		w.watchPath = ""
	}
	switched := w.ensureWatchTarget()

	name := filepath.Clean(event.Name)
	switch {
	case name == w.sessionsPath || filepath.Dir(name) == w.sessionsPath:
		w.scanAndNotify()
		if _, isRecord := parseRecordName(filepath.Base(name)); name == w.sessionsPath || !isRecord {
			w.notify(0)
		}
	case name == w.configDir || pathWithin(w.configDir, name):
		w.scanAndNotify()
		w.notify(0)
	case switched:
		w.scanAndNotify()
		w.notify(0)
	}
	if hook := testWatchAfterHandleEvent.Load(); hook != nil {
		(*hook)(event)
	}
}

func (w *watcher) watchRemoved(event fsnotify.Event) bool {
	if w.watchPath == "" {
		return false
	}
	if filepath.Clean(event.Name) != filepath.Clean(w.watchPath) {
		return false
	}
	return event.Op&(fsnotify.Remove|fsnotify.Rename) != 0
}

func pathWithin(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// ensureWatchTarget makes the installed watch match the file system and reports
// whether the watched path changed. It runs on the construction goroutine
// before the event loop starts and afterwards only on the run goroutine.
//
// The file system is checked again after every Add, because a directory created
// between the existence check and Add produces no notification for the new
// watch. When no watch can be installed, the watcher calls back with 0 instead
// of failing silently.
func (w *watcher) ensureWatchTarget() (switched bool) {
	if w.stopped.Load() {
		return false
	}
	skipped := make(map[string]bool)
	var failureKeys []string
	var failureDetail []string
	notified := false
	notifyLost := func() {
		if !notified {
			notified = true
			w.notify(0)
		}
	}
	commit := func() {
		set := watchFailureSet(failureKeys)
		if set == w.watchFailure {
			return
		}
		w.watchFailure = set
		w.watchFailureDetail = append([]string(nil), failureDetail...)
		sort.Strings(w.watchFailureDetail)
		if set != "" {
			notifyLost()
		}
	}

	for attempt := 0; attempt < watchReconcileAttempts; attempt++ {
		desired := w.desiredWatchPath(skipped)
		if desired == w.watchPath {
			commit()
			return switched
		}
		if desired == "" {
			if w.watchPath != "" {
				_ = w.fsw.Remove(w.watchPath)
				w.watchPath = ""
				switched = true
			}
			notifyLost()
			commit()
			return switched
		}
		if hook := testWatchAfterStatBeforeAdd.Load(); hook != nil {
			(*hook)()
		}
		if err := w.addWatch(desired); err != nil {
			skipped[desired] = true
			failureKeys = append(failureKeys, watchFailureKey(desired, err))
			failureDetail = append(failureDetail, fmt.Sprintf("%s: %v", desired, err))
			continue
		}
		if w.watchPath != "" {
			_ = w.fsw.Remove(w.watchPath)
		}
		w.watchPath = desired
		switched = true
	}
	commit()
	// The bound was exhausted; with no failing Add, commit reported nothing.
	if len(failureKeys) == 0 {
		notifyLost()
	}
	return switched
}

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

// notify calls every registered callback with pid, serially, skipping a
// registration that removeCallback has marked stopped; the stopped test and the
// in-flight registration happen under one lock.
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
