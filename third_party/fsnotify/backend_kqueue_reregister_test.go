//go:build freebsd || openbsd || netbsd || dragonfly || darwin

package fsnotify

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// reregisterWait bounds every wait, so a lost event fails the test instead of
// hanging it.
const reregisterWait = 5 * time.Second

// eventStream carries the backend's events to a test without blocking
// readEvents.
type eventStream struct{ ch chan Event }

func drainEvents(w *Watcher) *eventStream {
	s := &eventStream{ch: make(chan Event, 64)}
	go func() {
		defer close(s.ch)
		for e := range w.Events {
			s.ch <- e
		}
	}()
	return s
}

// waitFor consumes events until it sees op on name, and fails the test if the
// event does not arrive within reregisterWait or the stream closes first.
func (s *eventStream) waitFor(t *testing.T, name string, op Op) {
	t.Helper()
	timer := time.NewTimer(reregisterWait)
	defer timer.Stop()
	for {
		select {
		case e, ok := <-s.ch:
			if !ok {
				t.Fatalf("event stream closed while waiting for %s on %q", op, name)
			}
			if filepath.Clean(e.Name) == name && e.Op.Has(op) {
				return
			}
		case <-timer.C:
			t.Fatalf("no %s event for %q within %s", op, name, reregisterWait)
		}
	}
}

// holdInternalWatch blocks the next internal watch of name before it registers
// and reports the flags that watch computed; calling the returned release
// unblocks it and retires the hook.
func holdInternalWatch(t *testing.T, name string) (<-chan uint32, func()) {
	t.Helper()
	flags := make(chan uint32, 1)
	release := make(chan struct{})
	var held, released sync.Once
	hook := func(got string, f uint32) {
		if got != name {
			return
		}
		held.Do(func() {
			flags <- f
			<-release
		})
	}
	testBeforeInternalAddWatch.Store(&hook)
	return flags, func() {
		testBeforeInternalAddWatch.Store(nil)
		released.Do(func() { close(release) })
	}
}

// requireDirFlags fails unless the recorded dirFlags of name still include
// NOTE_WRITE.
func requireDirFlags(t *testing.T, kq *kqueue, name string) {
	t.Helper()
	info, ok := kq.watches.byPath(name)
	if !ok {
		t.Fatalf("%q has no watch after re-registration", name)
	}
	if info.dirFlags&unix.NOTE_WRITE != unix.NOTE_WRITE {
		t.Errorf("dirFlags for %q are %#x after re-registration: NOTE_WRITE was dropped", name, info.dirFlags)
	}
}

// TestKqueueInternalReregisterKeepsUserFlags covers the interleaving where the
// internal watch computes its flags before the user Add completes and
// registers after it, which drops NOTE_WRITE from both the filter and
// dirFlags.
func TestKqueueInternalReregisterKeepsUserFlags(t *testing.T) {
	tmp := t.TempDir()
	w := newWatcher(t, tmp)
	defer w.Close()
	kq := w.b.(*kqueue)
	events := drainEvents(w)

	child := filepath.Join(tmp, "child")
	flags, release := holdInternalWatch(t, child)
	t.Cleanup(release)

	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatalf("mkdir %q: %s", child, err)
	}

	select {
	case f := <-flags:
		if f&unix.NOTE_WRITE == unix.NOTE_WRITE {
			t.Fatalf("internal watch of %q already saw NOTE_WRITE before the user Add; the interleaving was not reached (flags %#x)", child, f)
		}
	case <-time.After(reregisterWait):
		t.Fatalf("internalWatch(%q) was not reached within %s", child, reregisterWait)
	}

	if err := w.Add(child); err != nil {
		t.Fatalf("Add(%q): %s", child, err)
	}
	release()

	// The child's Create is sent only after internalWatch returns, so waiting
	// for it orders the recorded-flags check after the internal registration.
	events.waitFor(t, child, Create)
	requireDirFlags(t, kq, child)

	file := filepath.Join(child, "record.json")
	if err := os.WriteFile(file, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write %q: %s", file, err)
	}
	events.waitFor(t, file, Create)
}

// TestKqueueInternalReregisterAfterUserAddKeepsFlags covers the reverse
// interleaving, where the user Add completes before the internal watch reads
// the directory's flags and the internal registration must therefore keep
// them.
func TestKqueueInternalReregisterAfterUserAddKeepsFlags(t *testing.T) {
	tmp := t.TempDir()
	w := newWatcher(t, tmp)
	defer w.Close()
	kq := w.b.(*kqueue)
	events := drainEvents(w)

	child := filepath.Join(tmp, "child")
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatalf("mkdir %q: %s", child, err)
	}
	// The internal watch of a new child completes before its Create event.
	events.waitFor(t, child, Create)

	if err := w.Add(child); err != nil {
		t.Fatalf("Add(%q): %s", child, err)
	}

	flags, release := holdInternalWatch(t, child)
	t.Cleanup(release)

	sibling := filepath.Join(tmp, "zzz-new")
	if err := os.WriteFile(sibling, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write %q: %s", sibling, err)
	}

	select {
	case f := <-flags:
		if f&unix.NOTE_WRITE != unix.NOTE_WRITE {
			t.Fatalf("internal watch of %q did not read the user's NOTE_WRITE from dirFlags (flags %#x)", child, f)
		}
	case <-time.After(reregisterWait):
		t.Fatalf("internalWatch(%q) was not reached within %s", child, reregisterWait)
	}
	release()

	// The sibling's Create follows the child's internal registration in the
	// same sorted directory scan.
	events.waitFor(t, sibling, Create)
	requireDirFlags(t, kq, child)

	file := filepath.Join(child, "record.json")
	if err := os.WriteFile(file, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write %q: %s", file, err)
	}
	events.waitFor(t, file, Create)
}

// probeBlocked bounds the wait for a user Add that addMu should have held out;
// it elapses only when the Add is blocked, so a passing run pays it once.
const probeBlocked = 2 * time.Second

// TestKqueueUserAddSerializedAgainstInternalRegister forces the window addMu
// closes: an internal watch has decided its flags and is about to register when
// a user Add of the same path runs. With addMu the Add is held out until the
// internal call records; it then re-checks and takes the union. Without addMu
// the Add records first and the internal call overwrites it with stale flags,
// leaving two descriptors and dropping NOTE_WRITE.
func TestKqueueUserAddSerializedAgainstInternalRegister(t *testing.T) {
	tmp := t.TempDir()
	w := newWatcher(t, tmp)
	defer w.Close()
	kq := w.b.(*kqueue)
	events := drainEvents(w)

	child := filepath.Join(tmp, "child")

	paused := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	var armed atomic.Bool
	hook := func(name string) {
		if name != child || !armed.CompareAndSwap(true, false) {
			return
		}
		paused <- struct{}{}
		<-release
	}
	testBeforeRegister.Store(&hook)
	defer func() {
		testBeforeRegister.Store(nil)
		releaseFn()
	}()

	armed.Store(true)
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatalf("mkdir %q: %s", child, err)
	}

	select {
	case <-paused:
	case <-time.After(reregisterWait):
		t.Fatalf("the internal watch of %q never reached its register step", child)
	}

	done := make(chan struct{})
	var addErr error
	go func() {
		addErr = w.Add(child)
		close(done)
	}()

	select {
	case <-done:
		// addMu did not hold the Add out: it ran while the internal call was
		// blocked, so the internal registration now records over it.
		t.Errorf("Add(%q) returned while the internal registration held addMu; addMu does not serialize them", child)
	case <-time.After(probeBlocked):
		// addMu held the Add out, as it must.
	}
	releaseFn()
	<-done
	if addErr != nil {
		t.Fatalf("Add(%q): %s", child, addErr)
	}

	// The child's Create is sent only after its internal watch returns, so this
	// orders the checks after the internal registration.
	events.waitFor(t, child, Create)
	requireDirFlags(t, kq, child)

	kq.watches.mu.RLock()
	n := 0
	for _, info := range kq.watches.wd {
		if info.name == child {
			n++
		}
	}
	kq.watches.mu.RUnlock()
	if n != 1 {
		t.Errorf("%q has %d watch entries after the internal registration, want 1", child, n)
	}

	file := filepath.Join(child, "record.json")
	if err := os.WriteFile(file, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write %q: %s", file, err)
	}
	events.waitFor(t, file, Create)
}

// TestKqueueRemoveDuringCommitDoesNotRecordStaleDescriptor pins that a
// descriptor prepareWatch borrowed from an existing watch is neither registered
// nor recorded after a concurrent Remove has closed it. Registering the number
// could add a filter to, and recording it would hand ownership of, whatever file
// now holds it, so a later Remove or Close would close a descriptor the watcher
// does not own.
//
// An internal re-watch is frozen at testBeforeRegister so it keeps addMu while a
// user Add of the same path finishes prepare and queues on the lock. Remove then
// frees the descriptor, a sentinel takes that number, and releasing the internal
// call lets the queued Add commit against the now-unwatched path.
func TestKqueueRemoveDuringCommitDoesNotRecordStaleDescriptor(t *testing.T) {
	watchedPath := filepath.Join(t.TempDir(), "watched")
	if err := os.WriteFile(watchedPath, []byte("watched"), 0o600); err != nil {
		t.Fatalf("writing %q: %s", watchedPath, err)
	}

	w := newWatcher(t)
	drainEvents(w)
	kq := w.b.(*kqueue)
	defer func() { _ = kq.Close() }()
	if err := w.Add(watchedPath); err != nil {
		t.Fatalf("Add(%q): %s", watchedPath, err)
	}
	info, ok := kq.watches.byPath(watchedPath)
	if !ok {
		t.Fatalf("no watch recorded for %q after Add", watchedPath)
	}
	fd := info.wd

	fi, err := os.Lstat(watchedPath)
	if err != nil {
		t.Fatalf("stat %q: %s", watchedPath, err)
	}

	paused := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	var armed atomic.Bool
	hook := func(name string) {
		if name != watchedPath || !armed.CompareAndSwap(true, false) {
			return
		}
		paused <- struct{}{}
		<-release
	}
	testBeforeRegister.Store(&hook)
	t.Cleanup(func() {
		testBeforeRegister.Store(nil)
		releaseFn()
	})

	internalDone := make(chan struct{})
	armed.Store(true)
	go func() {
		defer close(internalDone)
		_, _ = kq.internalWatch(watchedPath, fi)
	}()
	select {
	case <-paused:
	case <-time.After(reregisterWait):
		t.Fatalf("the internal re-watch of %q never reached its register step", watchedPath)
	}

	// The Add sees the watch in prepare, opens nothing, and queues on the addMu
	// the internal call holds. probeBlocked elapses only while it is blocked, and
	// prepare runs before that lock, so this places the Remove in the window.
	addDone := make(chan struct{})
	started := make(chan struct{})
	var addErr error
	go func() {
		close(started)
		addErr = w.Add(watchedPath)
		close(addDone)
	}()
	<-started
	select {
	case <-addDone:
		t.Fatalf("Add(%q) returned while addMu was held; prepare did not separate from commit", watchedPath)
	case <-time.After(probeBlocked):
	}

	if err := w.Remove(watchedPath); err != nil {
		t.Fatalf("Remove(%q): %s", watchedPath, err)
	}

	// Occupy the descriptor number Remove freed. Earlier opens are held so the
	// kernel walks up to fd instead of handing the same lower number back. The
	// descriptors are raw so no finalizer can close one behind the test's back.
	sentinelPath := filepath.Join(t.TempDir(), "sentinel")
	sentinel := -1
	var sentinelIno uint64
	var holders []int
	t.Cleanup(func() {
		for _, held := range holders {
			_ = unix.Close(held)
		}
		if sentinel < 0 {
			return
		}
		// The watcher, or a failed assertion above, may already have closed the
		// number and the kernel handed it to another file. Close it only while it
		// still names the sentinel.
		var st unix.Stat_t
		if err := unix.Fstat(sentinel, &st); err == nil && st.Ino == sentinelIno {
			_ = unix.Close(sentinel)
		}
	})
	for attempt := 0; attempt < 32; attempt++ {
		s, err := unix.Open(sentinelPath, unix.O_RDWR|unix.O_CREAT, 0o600)
		if err != nil {
			t.Fatalf("opening sentinel (attempt %d): %s", attempt+1, err)
		}
		if s == fd {
			var st unix.Stat_t
			if err := unix.Fstat(s, &st); err != nil {
				_ = unix.Close(s)
				t.Fatalf("fstat sentinel descriptor %d: %s", s, err)
			}
			sentinel = s
			sentinelIno = st.Ino
			break
		}
		holders = append(holders, s)
	}
	if sentinel < 0 {
		t.Skipf("descriptor %d was not handed out within 32 opens; this test needs descriptor reuse", fd)
	}

	releaseFn()
	select {
	case <-internalDone:
	case <-time.After(reregisterWait):
		t.Fatalf("the internal re-watch of %q did not return after release", watchedPath)
	}
	select {
	case <-addDone:
	case <-time.After(reregisterWait):
		t.Fatalf("Add(%q) did not finish after the internal call released addMu", watchedPath)
	}
	if addErr != nil {
		t.Fatalf("Add(%q): %s", watchedPath, addErr)
	}

	if got, ok := kq.watches.byWd(fd); ok {
		t.Errorf("watch table records descriptor %d as %q after the path was removed and the number was reused; the watcher does not own it", fd, got.name)
	}

	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		t.Fatalf("sentinel descriptor %d: fstat before Close: %s", fd, err)
	}
	if err := kq.Close(); err != nil {
		t.Fatalf("closing watcher: %s", err)
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		t.Fatalf("sentinel descriptor %d: fstat after Close: %s; the watcher closed a descriptor it did not own", fd, err)
	}
	if after.Ino != before.Ino {
		t.Fatalf("sentinel descriptor %d: inode changed after Close: was %d, now %d", fd, before.Ino, after.Ino)
	}
}

// TestKqueueUserAddFailsWhenRetriesAreExhausted pins that a user Add returns an
// error when every attempt loses its watch to a concurrent Remove. Returning
// success would make AddWith record the path as a user watch, so WatchList would
// list a path that receives no events.
//
// The attempt bound is lowered to one: the retry loop cannot lose a second time
// deterministically, because after the first loss prepareWatch opens its own
// descriptor and the commit cannot be invalidated. One attempt losing is the
// whole loop, so this reaches the exhausted branch.
func TestKqueueUserAddFailsWhenRetriesAreExhausted(t *testing.T) {
	watchedPath := filepath.Join(t.TempDir(), "watched")
	if err := os.WriteFile(watchedPath, []byte("watched"), 0o600); err != nil {
		t.Fatalf("writing %q: %s", watchedPath, err)
	}

	w := newWatcher(t)
	drainEvents(w)
	kq := w.b.(*kqueue)
	defer func() { _ = kq.Close() }()
	if err := w.Add(watchedPath); err != nil {
		t.Fatalf("Add(%q): %s", watchedPath, err)
	}
	fi, err := os.Lstat(watchedPath)
	if err != nil {
		t.Fatalf("stat %q: %s", watchedPath, err)
	}

	testAddWatchAttempts.Store(1)
	t.Cleanup(func() { testAddWatchAttempts.Store(0) })

	paused := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	var armed atomic.Bool
	hook := func(name string) {
		if name != watchedPath || !armed.CompareAndSwap(true, false) {
			return
		}
		paused <- struct{}{}
		<-release
	}
	testBeforeRegister.Store(&hook)
	t.Cleanup(func() {
		testBeforeRegister.Store(nil)
		releaseFn()
	})

	internalDone := make(chan struct{})
	armed.Store(true)
	go func() {
		defer close(internalDone)
		_, _ = kq.internalWatch(watchedPath, fi)
	}()
	select {
	case <-paused:
	case <-time.After(reregisterWait):
		t.Fatalf("the internal re-watch of %q never reached its register step", watchedPath)
	}

	// The Add sees the watch in prepare, opens nothing, and queues on the addMu
	// the internal call holds. probeBlocked elapses only while it is blocked, and
	// prepare runs before that lock, so this places the Remove in the window.
	addDone := make(chan struct{})
	started := make(chan struct{})
	var addErr error
	go func() {
		close(started)
		addErr = w.Add(watchedPath)
		close(addDone)
	}()
	<-started
	select {
	case <-addDone:
		t.Fatalf("Add(%q) returned while addMu was held; prepare did not separate from commit", watchedPath)
	case <-time.After(probeBlocked):
	}

	if err := w.Remove(watchedPath); err != nil {
		t.Fatalf("Remove(%q): %s", watchedPath, err)
	}

	releaseFn()
	select {
	case <-internalDone:
	case <-time.After(reregisterWait):
		t.Fatalf("the internal re-watch of %q did not return after release", watchedPath)
	}
	select {
	case <-addDone:
	case <-time.After(reregisterWait):
		t.Fatalf("Add(%q) did not finish after the internal call released addMu", watchedPath)
	}

	if addErr == nil {
		t.Fatalf("Add(%q) returned nil with no watch; an exhausted retry loop must report the path", watchedPath)
	}
	if !strings.Contains(addErr.Error(), watchedPath) {
		t.Errorf("Add(%q) error %q does not name the path", watchedPath, addErr)
	}
	for _, p := range w.WatchList() {
		if p == watchedPath {
			t.Errorf("WatchList lists %q, which receives no events; a failed Add must not record a user watch", watchedPath)
		}
	}
}

// TestKqueueRetryAfterCloseReturnsErrClosed pins that a retry which lands after
// Close returns ErrClosed instead of opening and recording a descriptor that
// Close's snapshot has already passed, so nobody would ever close it.
//
// The bound is two so the first lost attempt retries. An internal re-watch
// holds addMu while the user Add prepares and queues; Close then removes the
// watch, and releasing the internal call lets the Add retry into a closed
// watcher.
func TestKqueueRetryAfterCloseReturnsErrClosed(t *testing.T) {
	watchedPath := filepath.Join(t.TempDir(), "watched")
	if err := os.WriteFile(watchedPath, []byte("watched"), 0o600); err != nil {
		t.Fatalf("writing %q: %s", watchedPath, err)
	}

	w := newWatcher(t)
	drainEvents(w)
	kq := w.b.(*kqueue)
	if err := w.Add(watchedPath); err != nil {
		t.Fatalf("Add(%q): %s", watchedPath, err)
	}
	fi, err := os.Lstat(watchedPath)
	if err != nil {
		t.Fatalf("stat %q: %s", watchedPath, err)
	}

	testAddWatchAttempts.Store(2)
	t.Cleanup(func() { testAddWatchAttempts.Store(0) })

	paused := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	var armed atomic.Bool
	hook := func(name string) {
		if name != watchedPath || !armed.CompareAndSwap(true, false) {
			return
		}
		paused <- struct{}{}
		<-release
	}
	testBeforeRegister.Store(&hook)
	t.Cleanup(func() {
		testBeforeRegister.Store(nil)
		releaseFn()
	})

	internalDone := make(chan struct{})
	armed.Store(true)
	go func() {
		defer close(internalDone)
		_, _ = kq.internalWatch(watchedPath, fi)
	}()
	select {
	case <-paused:
	case <-time.After(reregisterWait):
		t.Fatalf("the internal re-watch of %q never reached its register step", watchedPath)
	}

	addDone := make(chan struct{})
	started := make(chan struct{})
	var addErr error
	go func() {
		close(started)
		addErr = w.Add(watchedPath)
		close(addDone)
	}()
	<-started
	select {
	case <-addDone:
		t.Fatalf("Add(%q) returned while addMu was held; prepare did not separate from commit", watchedPath)
	case <-time.After(probeBlocked):
	}

	if err := w.Close(); err != nil {
		t.Fatalf("closing watcher: %s", err)
	}

	releaseFn()
	select {
	case <-internalDone:
	case <-time.After(reregisterWait):
		t.Fatalf("the internal re-watch of %q did not return after release", watchedPath)
	}
	select {
	case <-addDone:
	case <-time.After(reregisterWait):
		t.Fatalf("Add(%q) did not finish after the internal call released addMu", watchedPath)
	}

	if !errors.Is(addErr, ErrClosed) {
		t.Fatalf("Add(%q) after Close returned %v, want ErrClosed", watchedPath, addErr)
	}
}
