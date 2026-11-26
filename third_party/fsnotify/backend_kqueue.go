//go:build freebsd || openbsd || netbsd || dragonfly || darwin

// This vendored copy of fsnotify v1.10.1 carries local patches to the kqueue
// backend: Close no longer closes a watch descriptor whose EV_DELETE failed,
// because a concurrent remove may already have closed it and released the
// number for reuse (the defect from upstream #740, still present in v1.10.1);
// addWatch no longer closes the descriptor of an existing watch when a
// re-registration fails; addWatch unions a re-registration's flags with those
// already recorded, so an internal watch cannot drop NOTE_WRITE from a user
// watch, and reports an error when a user Add loses every retry to a
// concurrent Remove instead of returning success for an unwatched path; and
// sendCreateIfNew installs the watch for a new file before sending its Create
// event, so a write made after a consumer sees Create is reported instead of
// lost. Remove the replace directive and these patches once a release fixes
// them upstream.

package fsnotify

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify/internal"
	"golang.org/x/sys/unix"
)

type kqueue struct {
	*shared
	Events chan Event
	Errors chan error

	kq        int    // File descriptor (as returned by the kqueue() syscall).
	closepipe [2]int // Pipe used for closing kq.
	watches   *watches

	// addMu serializes addWatch's merge-register-record step so a re-registration
	// unions flags instead of overwriting them; watchDirectoryFiles re-enters
	// addWatch, so it is never held across that call. Remove and Close do not take
	// it, so a call already inside can still register a descriptor a concurrent
	// Remove closed, and a retried internal watch can re-add a path removed in
	// that window.
	addMu sync.Mutex
}

type (
	watches struct {
		mu     sync.RWMutex
		wd     map[int]watch               // wd → watch
		path   map[string]int              // pathname → wd
		byDir  map[string]map[int]struct{} // dirname(path) → wd
		seen   map[string]struct{}         // Keep track of if we know this file exists.
		byUser map[string]struct{}         // Watches added with Watcher.Add()
	}
	watch struct {
		wd       int
		name     string
		linkName string // In case of links; name is the target, and this is the link.
		isDir    bool
		dirFlags uint32
	}
)

func newWatches() *watches {
	return &watches{
		wd:     make(map[int]watch),
		path:   make(map[string]int),
		byDir:  make(map[string]map[int]struct{}),
		seen:   make(map[string]struct{}),
		byUser: make(map[string]struct{}),
	}
}

func (w *watches) listPaths(userOnly bool) []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if userOnly {
		l := make([]string, 0, len(w.byUser))
		for p := range w.byUser {
			l = append(l, p)
		}
		return l
	}

	l := make([]string, 0, len(w.path))
	for p := range w.path {
		l = append(l, p)
	}
	return l
}

func (w *watches) watchesInDir(path string) []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	l := make([]string, 0, 4)
	for fd := range w.byDir[path] {
		info := w.wd[fd]
		if _, ok := w.byUser[info.name]; !ok {
			l = append(l, info.name)
		}
	}
	return l
}

// Mark path as added by the user.
func (w *watches) addUserWatch(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.byUser[path] = struct{}{}
}

func (w *watches) addLink(path string, fd int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.path[path] = fd
	w.seen[path] = struct{}{}
}

func (w *watches) add(path, linkPath string, fd int, isDir bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.path[path] = fd
	w.wd[fd] = watch{wd: fd, name: path, linkName: linkPath, isDir: isDir}

	parent := filepath.Dir(path)
	byDir, ok := w.byDir[parent]
	if !ok {
		byDir = make(map[int]struct{}, 1)
		w.byDir[parent] = byDir
	}
	byDir[fd] = struct{}{}
}

func (w *watches) byWd(fd int) (watch, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	info, ok := w.wd[fd]
	return info, ok
}

func (w *watches) byPath(path string) (watch, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	info, ok := w.wd[w.path[path]]
	return info, ok
}

func (w *watches) updateDirFlags(path string, flags uint32) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	fd, ok := w.path[path]
	if !ok { // Already deleted: don't re-set it here.
		return false
	}
	info := w.wd[fd]
	info.dirFlags = flags
	w.wd[fd] = info
	return true
}

func (w *watches) remove(fd int, path string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	isDir := w.wd[fd].isDir
	delete(w.path, path)
	delete(w.byUser, path)

	parent := filepath.Dir(path)
	delete(w.byDir[parent], fd)

	if len(w.byDir[parent]) == 0 {
		delete(w.byDir, parent)
	}

	delete(w.wd, fd)
	delete(w.seen, path)
	return isDir
}

func (w *watches) markSeen(path string, exists bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if exists {
		w.seen[path] = struct{}{}
	} else {
		delete(w.seen, path)
	}
}

func (w *watches) seenBefore(path string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.seen[path]
	return ok
}

var defaultBufferSize = 0

func newBackend(ev chan Event, errs chan error) (backend, error) {
	kq, closepipe, err := newKqueue()
	if err != nil {
		return nil, err
	}

	w := &kqueue{
		shared:    newShared(ev, errs),
		Events:    ev,
		Errors:    errs,
		kq:        kq,
		closepipe: closepipe,
		watches:   newWatches(),
	}

	go w.readEvents()
	return w, nil
}

// newKqueue creates a new kernel event queue and returns a descriptor.
//
// This registers a new event on closepipe, which will trigger an event when
// it's closed. This way we can use kevent() without timeout/polling; without
// the closepipe, it would block forever and we wouldn't be able to stop it at
// all.
func newKqueue() (kq int, closepipe [2]int, err error) {
	kq, err = unix.Kqueue()
	if err != nil {
		return kq, closepipe, err
	}

	// Register the close pipe.
	err = unix.Pipe(closepipe[:])
	if err != nil {
		unix.Close(kq)
		return kq, closepipe, err
	}
	unix.CloseOnExec(closepipe[0])
	unix.CloseOnExec(closepipe[1])

	// Register changes to listen on the closepipe.
	changes := make([]unix.Kevent_t, 1)
	// SetKevent converts int to the platform-specific types.
	unix.SetKevent(&changes[0], closepipe[0], unix.EVFILT_READ,
		unix.EV_ADD|unix.EV_ENABLE|unix.EV_ONESHOT)

	ok, err := unix.Kevent(kq, changes, nil, nil)
	if ok == -1 {
		unix.Close(kq)
		unix.Close(closepipe[0])
		unix.Close(closepipe[1])
		return kq, closepipe, err
	}
	return kq, closepipe, nil
}

func (w *kqueue) Close() error {
	if w.shared.close() {
		return nil
	}

	// Snapshot and drop all watches directly. w.Remove -> w.remove
	// short-circuits on isClosed() (which is already true after
	// w.shared.close() above), so calling Remove here in the happy path
	// leaked every watched directory + file descriptor. On macOS a
	// single directory watch opens an fd for every file in the dir, so
	// long-running processes that recreate watchers (hot-reload dev
	// servers, etc.) ran out of fds with EMFILE (#732).
	pathsToRemove := w.watches.listPaths(false)
	for _, name := range pathsToRemove {
		info, ok := w.watches.byPath(name)
		if !ok {
			// w.path has an entry for name but w.wd doesn't --
			// drop the stale lookup entry so the map state is
			// consistent after Close.
			w.watches.remove(0, name)
			continue
		}
		// EV_DELETE claims the knote: only one of Close and a concurrent
		// remove can delete it. A failed register means the other side
		// already closed the descriptor and the number may be in use
		// elsewhere, so it must not be closed again.
		if err := w.register([]int{info.wd}, unix.EV_DELETE, 0); err == nil {
			unix.Close(info.wd)
		}
		w.watches.remove(info.wd, name)
	}

	unix.Close(w.closepipe[1]) // Send "quit" message to readEvents
	return nil
}

func (w *kqueue) Add(name string) error { return w.AddWith(name) }

func (w *kqueue) AddWith(name string, opts ...addOpt) error {
	if debug {
		fmt.Fprintf(os.Stderr, "FSNOTIFY_DEBUG: %s  AddWith(%q)\n",
			time.Now().Format("15:04:05.000000000"), name)
	}

	with := getOptions(opts...)
	if !w.xSupports(with.op) {
		return fmt.Errorf("%w: %s", xErrUnsupported, with.op)
	}

	_, err := w.addWatch(name, noteAllEvents, false)
	if err != nil {
		return err
	}
	w.watches.addUserWatch(name)
	return nil
}

func (w *kqueue) Remove(name string) error {
	if debug {
		fmt.Fprintf(os.Stderr, "FSNOTIFY_DEBUG: %s  Remove(%q)\n",
			time.Now().Format("15:04:05.000000000"), name)
	}
	return w.remove(name, true)
}

func (w *kqueue) remove(name string, unwatchFiles bool) error {
	if w.isClosed() {
		return nil
	}

	name = filepath.Clean(name)
	info, ok := w.watches.byPath(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNonExistentWatch, name)
	}

	err := w.register([]int{info.wd}, unix.EV_DELETE, 0)
	if err != nil {
		return err
	}

	unix.Close(info.wd)

	isDir := w.watches.remove(info.wd, name)

	// Find all watched paths that are in this directory that are not external.
	if unwatchFiles && isDir {
		pathsToRemove := w.watches.watchesInDir(name)
		for _, name := range pathsToRemove {
			// Since these are internal, not much sense in propagating error to
			// the user, as that will just confuse them with an error about a
			// path they did not explicitly watch themselves.
			w.Remove(name)
		}
	}
	return nil
}

func (w *kqueue) WatchList() []string {
	if w.isClosed() {
		return nil
	}
	return w.watches.listPaths(true)
}

// Watch all events (except NOTE_EXTEND, NOTE_LINK, NOTE_REVOKE)
const noteAllEvents = unix.NOTE_DELETE | unix.NOTE_WRITE | unix.NOTE_ATTRIB | unix.NOTE_RENAME

// addPlan is the out-of-lock result of addWatch's filesystem work.
type addPlan struct {
	info     watch // descriptor and metadata for a new watch
	opened   bool  // info.wd was opened here and must be closed if it goes unused
	linkOnly bool  // name is a symlink whose target is already watched
}

// addWatchAttempts bounds how often addWatch rebuilds a plan whose watch a
// concurrent Remove closed before commit. That needs a Remove to land exactly
// between prepare and commit, so a second loss is already pathological.
const addWatchAttempts = 3

// addWatch adds name to the watched file set; the flags are interpreted as
// described in kevent(2).
//
// Returns the real path to the file which was added, with symlinks resolved.
func (w *kqueue) addWatch(name string, flags uint32, listDir bool) (string, error) {
	name = filepath.Clean(name)

	// Resolve and open before taking addMu: Lstat, Readlink and Open can block on
	// a network, autofs or FUSE mount, and holding the lock there would stall
	// every other Add and the event loop's internal watches.
	attempts := addWatchAttempts
	if n := testAddWatchAttempts.Load(); n > 0 {
		attempts = int(n)
	}
	resolved := name
	for attempt := 0; attempt < attempts; attempt++ {
		// A retry that lands after Close must not open a descriptor nobody will
		// close: Close has already taken its snapshot of the watches.
		if w.isClosed() {
			return "", ErrClosed
		}
		plan, resolvedNow, done, err := w.prepareWatch(name, listDir)
		if err != nil || done {
			return resolvedNow, err
		}
		resolved = resolvedNow

		// Serialize only the merge-register-record step. A re-registration that
		// read dirFlags before a user Add completed must not overwrite it, and
		// two Adds of one path must not leave two descriptors or entries.
		w.addMu.Lock()
		resolved, scanDir, retry, err := w.commitWatch(name, resolved, flags, plan)
		w.addMu.Unlock()
		if retry {
			continue
		}
		if err != nil {
			return resolved, err
		}
		if scanDir == "" {
			return resolved, nil
		}
		if err := w.watchDirectoryFiles(scanDir); err != nil {
			return "", err
		}
		return resolved, nil
	}

	// Every attempt lost its watch to a concurrent Remove. A user Add must not
	// report success for a path it does not watch: AddWith would record it and
	// WatchList would list a path that receives no events. An internal watch
	// keeps returning without a watch, as it does when it loses the same race
	// upstream.
	if listDir {
		return resolved, nil
	}
	return resolved, fmt.Errorf("fsnotify: add %q: a concurrent Remove took the watch on every attempt", name)
}

// prepareWatch resolves name and opens a descriptor without holding addMu. done
// is true for a path that needs no registration (a socket, a pipe, or a symlink
// whose target is already watched).
func (w *kqueue) prepareWatch(name string, listDir bool) (plan addPlan, resolved string, done bool, err error) {
	if cur, ok := w.watches.byPath(name); ok {
		return addPlan{info: cur}, name, false, nil
	}

	fi, err := os.Lstat(name)
	if err != nil {
		return plan, "", true, err
	}

	// Don't watch sockets or named pipes.
	if (fi.Mode()&os.ModeSocket == os.ModeSocket) || (fi.Mode()&os.ModeNamedPipe == os.ModeNamedPipe) {
		return plan, "", true, nil
	}

	// Follow symlinks, but only for paths added with Add(), and not paths
	// we're adding from internalWatch from a listdir.
	if !listDir && fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		link, err := os.Readlink(name)
		if err != nil {
			return plan, "", true, err
		}
		if !filepath.IsAbs(link) {
			link = filepath.Join(filepath.Dir(name), link)
		}

		if _, ok := w.watches.byPath(link); ok {
			// Record the link so a later diff does not report a spurious Create.
			return addPlan{linkOnly: true}, link, false, nil
		}

		plan.info.linkName = name
		name = link
		fi, err = os.Lstat(name)
		if err != nil {
			return plan, "", true, err
		}
	}

	plan.info.wd, err = internal.IgnoringEINTR(func() (int, error) {
		return unix.Open(name, openMode, 0)
	})
	if err != nil {
		return plan, "", true, err
	}
	plan.info.isDir = fi.IsDir()
	plan.opened = true
	return plan, name, false, nil
}

// commitWatch re-checks what is recorded, merges the requested flags with the
// recorded ones, then registers and records, all under addMu. It returns the
// path of a directory addWatch must scan after unlocking. retry is true when
// prepareWatch's descriptor came from a watch that a concurrent Remove has
// since closed; the caller must rebuild the plan and must not register, because
// the number may already belong to another file.
func (w *kqueue) commitWatch(name, resolved string, flags uint32, plan addPlan) (resolvedOut, scanDir string, retry bool, err error) {
	if plan.linkOnly {
		w.watches.addLink(name, 0)
		return resolved, "", false, nil
	}

	cur, alreadyWatching := w.watches.byPath(resolved)
	if !alreadyWatching && !plan.opened {
		// prepareWatch borrowed an existing watch's descriptor without opening
		// it, and that watch is gone from the map by the time this call re-reads
		// it. Registering the number could add a filter to, and recording it
		// would hand ownership of, whatever file now holds it, so rebuild the
		// plan from a descriptor this call opens itself.
		return resolved, "", true, nil
	}
	if alreadyWatching {
		// Another caller registered this path while prepareWatch opened our
		// descriptor: its watch wins, so close ours rather than leak or
		// duplicate it. A symlink followed on the way still needs recording,
		// or a later diff reports a spurious Create for it.
		if plan.opened {
			unix.Close(plan.info.wd)
			plan.opened = false
			if plan.info.linkName != "" {
				w.watches.addLink(plan.info.linkName, 0)
			}
		}
		plan.info = cur
		flags |= plan.info.dirFlags
	}

	if hook := testBeforeRegister.Load(); hook != nil {
		(*hook)(resolved)
	}

	err = w.register([]int{plan.info.wd}, unix.EV_ADD|unix.EV_CLEAR|unix.EV_ENABLE, flags)
	if err != nil {
		// Only a descriptor this call opened may be closed; an existing watch
		// keeps its descriptor and its mapping entry.
		if plan.opened && !alreadyWatching {
			unix.Close(plan.info.wd)
		}
		return "", "", false, err
	}

	if !alreadyWatching {
		w.watches.add(resolved, plan.info.linkName, plan.info.wd, plan.info.isDir)
	}

	// Watch the directory if it has not been watched before, or if it was
	// watched before, but perhaps only a NOTE_DELETE (watchDirectoryFiles)
	if plan.info.isDir {
		watchDir := (flags&unix.NOTE_WRITE) == unix.NOTE_WRITE &&
			(!alreadyWatching || (cur.dirFlags&unix.NOTE_WRITE) != unix.NOTE_WRITE)
		if !w.watches.updateDirFlags(resolved, flags) {
			return "", "", false, nil
		}

		if watchDir {
			d := resolved
			if plan.info.linkName != "" {
				d = plan.info.linkName
			}
			return resolved, d, false, nil
		}
	}
	return resolved, "", false, nil
}

// readEvents reads from kqueue and converts the received kevents into
// Event values that it sends down the Events channel.
func (w *kqueue) readEvents() {
	defer func() {
		close(w.Events)
		close(w.Errors)
		_ = unix.Close(w.kq)
		unix.Close(w.closepipe[0])
	}()

	eventBuffer := make([]unix.Kevent_t, 10)
	for {
		kevents, err := internal.IgnoringEINTR(func() ([]unix.Kevent_t, error) {
			return w.read(eventBuffer)
		})
		if err != nil {
			if !w.sendError(fmt.Errorf("fsnotify.readEvents: %w", err)) {
				return
			}
		}

		for _, kevent := range kevents {
			var (
				wd   = int(kevent.Ident)
				mask = uint32(kevent.Fflags)
			)

			// Shut down the loop when the pipe is closed, but only after all
			// other events have been processed.
			if wd == w.closepipe[0] {
				return
			}

			path, ok := w.watches.byWd(wd)
			if debug {
				internal.Debug(path.name, &kevent)
			}

			// On macOS it seems that sometimes an event with Ident=0 is
			// delivered, and no other flags/information beyond that, even
			// though we never saw such a file descriptor. For example in
			// TestWatchSymlink/277 (usually at the end, but sometimes sooner):
			//
			// fmt.Printf("READ: %2d  %#v\n", kevent.Ident, kevent)
			// unix.Kevent_t{Ident:0x2a, Filter:-4, Flags:0x25, Fflags:0x2, Data:0, Udata:(*uint8)(nil)}
			// unix.Kevent_t{Ident:0x0,  Filter:-4, Flags:0x25, Fflags:0x2, Data:0, Udata:(*uint8)(nil)}
			//
			// The first is a normal event, the second with Ident 0. No error
			// flag, no data, no ... nothing.
			//
			// I read a bit through bsd/kern_event.c from the xnu source, but I
			// don't really see an obvious location where this is triggered –
			// this doesn't seem intentional, but idk...
			//
			// Technically fd 0 is a valid descriptor, so only skip it if
			// there's no path, and if we're on macOS.
			if !ok && kevent.Ident == 0 && runtime.GOOS == "darwin" {
				continue
			}

			event := w.newEvent(path.name, path.linkName, mask)

			if event.Has(Rename) || event.Has(Remove) {
				w.remove(event.Name, false)
				w.watches.markSeen(event.Name, false)
			}

			if path.isDir && event.Has(Write) && !event.Has(Remove) {
				w.dirChange(event.Name)
			} else if !w.sendEvent(event) {
				return
			}

			if event.Has(Remove) {
				// Look for a file that may have overwritten this; for example,
				// mv f1 f2 will delete f2, then create f2.
				if path.isDir {
					fileDir := filepath.Clean(event.Name)
					_, found := w.watches.byPath(fileDir)
					if found {
						// TODO: this branch is never triggered in any test.
						// Added in d6220df (2012).
						// isDir check added in 8611c35 (2016): https://github.com/fsnotify/fsnotify/pull/111
						//
						// I don't really get how this can be triggered either.
						// And it wasn't triggered in the patch that added it,
						// either.
						//
						// Original also had a comment:
						//   make sure the directory exists before we watch for
						//   changes. When we do a recursive watch and perform
						//   rm -rf, the parent directory might have gone
						//   missing, ignore the missing directory and let the
						//   upcoming delete event remove the watch from the
						//   parent directory.
						err := w.dirChange(fileDir)
						if !w.sendError(err) {
							return
						}
					}
				} else {
					path := filepath.Clean(event.Name)
					if fi, err := os.Lstat(path); err == nil {
						err := w.sendCreateIfNew(path, fi)
						if !w.sendError(err) {
							return
						}
					}
				}
			}
		}
	}
}

// newEvent returns an platform-independent Event based on kqueue Fflags.
func (w *kqueue) newEvent(name, linkName string, mask uint32) Event {
	e := Event{Name: name}
	if linkName != "" {
		// If the user watched "/path/link" then emit events as "/path/link"
		// rather than "/path/target".
		e.Name = linkName
	}

	if mask&unix.NOTE_DELETE == unix.NOTE_DELETE {
		e.Op |= Remove
	}
	if mask&unix.NOTE_WRITE == unix.NOTE_WRITE {
		e.Op |= Write
	}
	if mask&unix.NOTE_RENAME == unix.NOTE_RENAME {
		e.Op |= Rename
	}
	if mask&unix.NOTE_ATTRIB == unix.NOTE_ATTRIB {
		e.Op |= Chmod
	}
	// No point sending a write and delete event at the same time: if it's gone,
	// then it's gone.
	if e.Op.Has(Write) && e.Op.Has(Remove) {
		e.Op &^= Write
	}
	return e
}

// watchDirectoryFiles to mimic inotify when adding a watch on a directory
func (w *kqueue) watchDirectoryFiles(dirPath string) error {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, f := range files {
		path := filepath.Join(dirPath, f.Name())

		fi, err := f.Info()
		if err != nil {
			return fmt.Errorf("%q: %w", path, err)
		}

		cleanPath, err := w.internalWatch(path, fi)
		if err != nil {
			// No permission, or the entry resolved to a missing target
			// (e.g. a dangling symlink): not a problem, just skip. But
			// do mark it as seen to prevent it from being picked up as
			// a "new" file later (it still shows up in the directory
			// listing).
			switch {
			case errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) ||
				errors.Is(err, os.ErrNotExist):
				cleanPath = filepath.Clean(path)
			default:
				return fmt.Errorf("%q: %w", path, err)
			}
		}

		w.watches.markSeen(cleanPath, true)
	}

	return nil
}

// Search the directory for new files and send an event for them.
//
// This functionality is to have the BSD watcher match the inotify, which sends
// a create event for files created in a watched directory.
func (w *kqueue) dirChange(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		// Directory no longer exists: we can ignore this safely. kqueue will
		// still give us the correct events.
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("fsnotify.dirChange %q: %w", dir, err)
	}

	for _, f := range files {
		fi, err := f.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("fsnotify.dirChange: %w", err)
		}

		err = w.sendCreateIfNew(filepath.Join(dir, fi.Name()), fi)
		if err != nil {
			// Don't need to send an error if this file isn't readable.
			if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) || errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("fsnotify.dirChange: %w", err)
		}
	}
	return nil
}

// testBeforeCreateEvent, when set by a test, runs after the watch for a new
// file found by the directory scan is installed and before its Create event
// is sent. It makes that window observable: a test can write the file there
// and require the write to be reported. It is atomic because the test sets it
// while the backend goroutine is already running. Production leaves it nil.
var testBeforeCreateEvent atomic.Pointer[func()]

// testBeforeInternalAddWatch, when set by a test, runs after internalWatch
// computes a directory's re-registration flags and before addWatch registers
// them, so a test can interleave a user Add on the same path; production
// leaves it nil.
var testBeforeInternalAddWatch atomic.Pointer[func(name string, flags uint32)]

// testBeforeRegister, when set by a test, runs inside addWatch's locked section
// after a path's flags are decided and before the kernel registration, so a
// test can hold the registering call while a user Add of the same path runs;
// production leaves it nil.
var testBeforeRegister atomic.Pointer[func(name string)]

// testAddWatchAttempts, when positive, replaces addWatchAttempts so a test can
// exhaust the retry loop without racing several real losses. Production leaves
// it zero.
var testAddWatchAttempts atomic.Int32

// Send a create event if the file isn't already being tracked, and start
// watching this file.
func (w *kqueue) sendCreateIfNew(path string, fi os.FileInfo) error {
	newFile := !w.watches.seenBefore(path)

	// Like watchDirectoryFiles, but without doing another ReadDir. The watch is
	// installed before the Create event is sent so that a consumer reacting to
	// Create by reading the file has every later write reported: kqueue
	// reports only changes made after the watch exists.
	cleanPath, err := w.internalWatch(path, fi)
	if err != nil {
		// The Create event still goes out for a new file whose watch could not
		// be installed, matching the order the caller used to observe.
		if newFile {
			w.sendEvent(Event{Name: path, Op: Create})
		}
		return err
	}
	w.watches.markSeen(cleanPath, true)

	if newFile {
		if hook := testBeforeCreateEvent.Load(); hook != nil {
			(*hook)()
		}
		w.sendEvent(Event{Name: path, Op: Create})
	}
	return nil
}

func (w *kqueue) internalWatch(name string, fi os.FileInfo) (string, error) {
	if fi.IsDir() {
		// mimic Linux providing delete events for subdirectories, but preserve
		// the flags used if currently watching subdirectory
		info, _ := w.watches.byPath(name)
		flags := info.dirFlags | unix.NOTE_DELETE | unix.NOTE_RENAME
		if hook := testBeforeInternalAddWatch.Load(); hook != nil {
			(*hook)(name, flags)
		}
		return w.addWatch(name, flags, true)
	}

	// Watch file to mimic Linux inotify.
	return w.addWatch(name, noteAllEvents, true)
}

// Register events with the queue.
func (w *kqueue) register(fds []int, flags int, fflags uint32) error {
	changes := make([]unix.Kevent_t, len(fds))
	for i, fd := range fds {
		// SetKevent converts int to the platform-specific types.
		unix.SetKevent(&changes[i], fd, unix.EVFILT_VNODE, flags)
		changes[i].Fflags = fflags
	}

	// Register the events.
	success, err := unix.Kevent(w.kq, changes, nil, nil)
	if success == -1 {
		return err
	}
	return nil
}

// read retrieves pending events, or waits until an event occurs.
func (w *kqueue) read(events []unix.Kevent_t) ([]unix.Kevent_t, error) {
	n, err := unix.Kevent(w.kq, nil, events, nil)
	if err != nil {
		return nil, err
	}
	return events[0:n], nil
}

func (w *kqueue) xSupports(op Op) bool {
	//if runtime.GOOS == "freebsd" {
	//	return true // Supports everything.
	//}
	if op.Has(xUnportableOpen) || op.Has(xUnportableRead) ||
		op.Has(xUnportableCloseWrite) || op.Has(xUnportableCloseRead) {
		return false
	}
	return true
}

func (w *kqueue) state() {
	w.watches.mu.Lock()
	defer w.watches.mu.Unlock()

	all := make([]int, 0, len(w.watches.wd))
	for wd := range w.watches.wd {
		all = append(all, wd)
	}
	sort.Ints(all)

	for _, wd := range all {
		ww := w.watches.wd[wd]
		fmt.Fprintf(os.Stderr, "%4d  %q  linkname=%q\n", wd, ww.name, ww.linkName)
	}
}
