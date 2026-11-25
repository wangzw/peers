package config

import (
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// defaultWatchDebounce is the quiet period after the last filesystem event
// before the file is reread, so a burst of writes causes one reload;
// startWatchWithDebounce lets tests shorten it.
const defaultWatchDebounce = 100 * time.Millisecond

// watchTimer is the part of time.Timer the debounce uses; a test can supply a
// fake timer and observe the quiet period without real time.
type watchTimer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type realWatchTimer struct{ timer *time.Timer }

func (t realWatchTimer) C() <-chan time.Time        { return t.timer.C }
func (t realWatchTimer) Stop() bool                 { return t.timer.Stop() }
func (t realWatchTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }

func newRealWatchTimer(d time.Duration) watchTimer { return realWatchTimer{time.NewTimer(d)} }

// newWatchTimer builds the debounce timer; tests replace it to fire the
// debounce on command.
var newWatchTimer = newRealWatchTimer

// newFSWatcher builds the filesystem watcher; tests replace it to force the
// creation failure.
var newFSWatcher = fsnotify.NewWatcher

type configWatcher struct {
	c        *core
	fsw      *fsnotify.Watcher
	debounce time.Duration
	stopCh   chan struct{}
	done     chan struct{}
}

func startWatch(c *core) (*configWatcher, error) {
	return startWatchWithDebounce(c, defaultWatchDebounce)
}

func startWatchWithDebounce(c *core, debounce time.Duration) (*configWatcher, error) {
	if c.noBackground {
		return nil, nil
	}
	if debounce <= 0 {
		debounce = defaultWatchDebounce
	}
	w := &configWatcher{
		c:        c,
		debounce: debounce,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	fsw, err := newFSWatcher()
	if err != nil {
		c.logger.Error("config: cannot create the file watcher; configuration reloads will not be noticed", "error", err)
	} else {
		dir := filepath.Dir(c.paths.ConfigFile)
		if addErr := fsw.Add(dir); addErr != nil {
			c.logger.Error("config: cannot watch the config directory; configuration reloads will not be noticed", "error", addErr)
			_ = fsw.Close()
		} else {
			w.fsw = fsw
		}
	}
	go w.run()
	return w, nil
}

func (w *configWatcher) run() {
	defer close(w.done)
	var events <-chan fsnotify.Event
	var errs <-chan error
	if w.fsw != nil {
		events = w.fsw.Events
		errs = w.fsw.Errors
	}
	var debounce watchTimer
	var debounceC <-chan time.Time
	base := filepath.Base(w.c.paths.ConfigFile)
	for {
		select {
		case <-w.stopCh:
			if debounce != nil {
				debounce.Stop()
			}
			if w.fsw != nil {
				_ = w.fsw.Close()
			}
			return
		case ev, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if filepath.Base(ev.Name) != base {
				continue
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
				continue
			}
			if debounce == nil {
				debounce = newWatchTimer(w.debounce)
			} else {
				if !debounce.Stop() {
					select {
					case <-debounce.C():
					default:
					}
				}
				debounce.Reset(w.debounce)
			}
			debounceC = debounce.C()
		case <-debounceC:
			debounceC = nil
			w.c.reloadFromDisk()
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			w.c.logger.Error("config: file watcher error", "error", err)
		}
	}
}

func (w *configWatcher) stop() {
	if w == nil {
		return
	}
	close(w.stopCh)
	<-w.done
}

// handleFSEvent is the seam tests use to drive the watcher without real
// filesystem events.
func (w *configWatcher) handleFSEvent(ev fsnotify.Event) {
	if filepath.Base(ev.Name) != filepath.Base(w.c.paths.ConfigFile) {
		return
	}
	if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
		return
	}
	w.c.reloadFromDisk()
}

// rescanOnce is the seam tests and internal callers use to reload immediately.
// It is not a periodic rescan.
func (c *core) rescanOnce() {
	c.reloadFromDisk()
}
