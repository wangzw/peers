package broker

import (
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// configDirWatchDebounce is how long the watcher waits after the last change
// before waking the retry, so a burst of writes is reported once.
const configDirWatchDebounce = 50 * time.Millisecond

// configDirSource supplies the directory events the watcher consumes.
type configDirSource interface {
	Events() <-chan fsnotify.Event
	Errors() <-chan error
	Close() error
}

type fsnotifySource struct{ w *fsnotify.Watcher }

func (s fsnotifySource) Events() <-chan fsnotify.Event { return s.w.Events }
func (s fsnotifySource) Errors() <-chan error          { return s.w.Errors }
func (s fsnotifySource) Close() error                  { return s.w.Close() }

// configDirWatch reports changes to the configuration file while a degraded
// Broker runs diagnostics only.
type configDirWatch struct {
	events chan struct{}

	source configDirSource
	log    *slog.Logger
	clock  Clock
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
}

// newConfigDirWatch builds a watcher over source; a nil clock uses the system
// clock.
func newConfigDirWatch(source configDirSource, log *slog.Logger, clock Clock) *configDirWatch {
	if log == nil {
		log = slog.Default()
	}
	if clock == nil {
		clock = SystemClock()
	}
	return &configDirWatch{
		events: make(chan struct{}, 1),
		source: source,
		log:    log,
		clock:  clock,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// startConfigDirWatch watches dir for changes to configFile. A watcher that
// cannot be created is logged and inert; the minute retry still runs.
func startConfigDirWatch(configFile string, log *slog.Logger) *configDirWatch {
	w := newConfigDirWatch(nil, log, nil)
	if configFile == "" {
		close(w.done)
		return w
	}
	fsw, err := fsnotify.NewWatcher()
	if err == nil {
		err = fsw.Add(filepath.Dir(configFile))
	}
	if err != nil {
		if log != nil {
			log.Warn("degraded_config_watch_failed", "error", err)
		}
		if fsw != nil {
			_ = fsw.Close()
		}
		close(w.done)
		return w
	}
	w.source = fsnotifySource{fsw}
	go w.run(baseName(configFile))
	return w
}

func baseName(path string) string { return filepath.Base(path) }

func (w *configDirWatch) run(name string) {
	defer close(w.done)
	var (
		timer  Timer
		timing <-chan struct{}
	)
	arm := func() {
		if timer != nil {
			timer.Stop()
		}
		ch := make(chan struct{})
		timer = w.clock.AfterFunc(configDirWatchDebounce, func() { close(ch) })
		timing = ch
	}
	for {
		select {
		case <-w.stop:
			if timer != nil {
				timer.Stop()
			}
			_ = w.source.Close()
			return
		case ev, ok := <-w.source.Events():
			if !ok {
				return
			}
			if baseName(ev.Name) != name {
				continue
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) == 0 {
				continue
			}
			arm()
		case err, ok := <-w.source.Errors():
			if !ok {
				return
			}
			if w.log != nil {
				w.log.Warn("degraded_config_watch_error", "error", err)
			}
			// A watcher error only costs the immediate retry.
		case <-timing:
			// Keep timing nil until the next change so a stale firing cannot
			// wake the loop twice.
			timing = nil
			select {
			case w.events <- struct{}{}:
			default:
			}
		}
	}
}

func (w *configDirWatch) stopNow() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		if w.source != nil {
			close(w.stop)
		}
	})
	<-w.done
}
