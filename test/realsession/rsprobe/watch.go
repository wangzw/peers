package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const watchMatchWindow = time.Second

func init() {
	register("watch", cmdWatch)
}

type event struct {
	T    time.Time `json:"t"`
	Src  string    `json:"src"`
	Op   string    `json:"op"`
	File string    `json:"file"`
}

// matchEvents returns the poll observations no notify event on the same file
// covers within window, comparing time so an earlier event cannot hide a later
// change.
func matchEvents(polls, notifies []event, window time.Duration) []event {
	var misses []event
	for _, poll := range polls {
		matched := false
		for _, notify := range notifies {
			if notify.File != poll.File {
				continue
			}
			d := notify.T.Sub(poll.T)
			if d < 0 {
				d = -d
			}
			if d <= window {
				matched = true
				break
			}
		}
		if !matched {
			misses = append(misses, poll)
		}
	}
	return misses
}

type fileState struct {
	size    int64
	modTime time.Time
	hash    [sha256.Size]byte
}

func (s fileState) same(other fileState) bool {
	return s.size == other.size && s.hash == other.hash && s.modTime.Equal(other.modTime)
}

// watcher records fsnotify events alongside an independent periodic scan; it
// deliberately does not use the inbox watcher, because it is the reference that
// watcher is measured against.
type watcher struct {
	dir      string
	interval time.Duration
	now      func() time.Time
	onEvent  func(event) error
	fsw      *fsnotify.Watcher
	states   map[string]fileState
	polls    []event
	notifies []event
}

func newWatcher(dir string, interval time.Duration, now func() time.Time, onEvent func(event) error) (*watcher, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("watch directory %s: %v", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("watch directory %s: not a directory", dir)
	}
	if interval <= 0 {
		return nil, fmt.Errorf("poll interval must be positive, got %s", interval)
	}
	if now == nil {
		now = time.Now
	}
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := fsw.Add(dir); err != nil {
		_ = fsw.Close()
		return nil, fmt.Errorf("watch directory %s: %v", dir, err)
	}
	w := &watcher{
		dir:      dir,
		interval: interval,
		now:      now,
		onEvent:  onEvent,
		fsw:      fsw,
		states:   make(map[string]fileState),
	}
	if err := w.scan(false); err != nil {
		_ = fsw.Close()
		return nil, err
	}
	return w, nil
}

func (w *watcher) run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.fsw.Events:
			if !ok {
				return nil
			}
			if err := w.emit(event{
				T:    w.now(),
				Src:  "notify",
				Op:   opNames(ev.Op),
				File: filepath.Base(ev.Name),
			}); err != nil {
				return err
			}
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return nil
			}
			if err != nil {
				return err
			}
		case <-ticker.C:
			if err := w.scan(true); err != nil {
				return err
			}
		}
	}
}

func (w *watcher) scan(report bool) error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("scan %s: %v", w.dir, err)
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(w.dir, name)
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("scan %s: %v", path, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("scan %s: %v", path, err)
		}
		seen[name] = struct{}{}
		state := fileState{size: info.Size(), modTime: info.ModTime(), hash: sha256.Sum256(data)}
		previous, known := w.states[name]
		switch {
		case !known:
			if report {
				if err := w.emit(w.pollEvent("create", name)); err != nil {
					return err
				}
			}
		case !previous.same(state):
			if report {
				if err := w.emit(w.pollEvent("modify", name)); err != nil {
					return err
				}
			}
		}
		w.states[name] = state
	}
	for name := range w.states {
		if _, ok := seen[name]; ok {
			continue
		}
		if report {
			if err := w.emit(w.pollEvent("remove", name)); err != nil {
				return err
			}
		}
		delete(w.states, name)
	}
	return nil
}

func (w *watcher) pollEvent(op, name string) event {
	return event{T: w.now(), Src: "poll", Op: op, File: name}
}

func (w *watcher) emit(e event) error {
	if e.Src == "poll" {
		w.polls = append(w.polls, e)
	} else {
		w.notifies = append(w.notifies, e)
	}
	if w.onEvent == nil {
		return nil
	}
	return w.onEvent(e)
}

func (w *watcher) collected() (polls, notifies []event) {
	return append([]event(nil), w.polls...), append([]event(nil), w.notifies...)
}

func (w *watcher) close() {
	_ = w.fsw.Close()
}

// opNames renders an fsnotify operation set as Create|Write|Remove|Rename|Chmod
// names; fsnotify's String has a leading "|", so the names are built here.
func opNames(op fsnotify.Op) string {
	known := []struct {
		op   fsnotify.Op
		name string
	}{
		{fsnotify.Create, "CREATE"},
		{fsnotify.Write, "WRITE"},
		{fsnotify.Remove, "REMOVE"},
		{fsnotify.Rename, "RENAME"},
		{fsnotify.Chmod, "CHMOD"},
	}
	names := make([]string, 0, len(known))
	for _, item := range known {
		if op.Has(item.op) {
			names = append(names, item.name)
		}
	}
	if len(names) == 0 {
		return "NONE"
	}
	return strings.Join(names, "|")
}

// cmdWatch exits with code 3 on misses, which the shared dispatch cannot express
// through its error report.
func cmdWatch(args []string) error {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)
	code, err := executeWatch(args, os.Stdout, interrupt)
	if err != nil {
		return err
	}
	if code != 0 {
		os.Exit(code)
	}
	return nil
}

func executeWatch(args []string, stdout io.Writer, interrupt <-chan os.Signal) (int, error) {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", "", "directory to watch")
	out := fs.String("out", "", "file to append the observed events to")
	poll := fs.Duration("poll", 50*time.Millisecond, "poll interval")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if fs.NArg() > 0 {
		return 0, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *dir == "" || *out == "" {
		return 0, errors.New("usage: rsprobe watch --dir D --out L [--poll DURATION]")
	}

	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	w, err := newWatcher(*dir, *poll, time.Now, func(e event) error {
		raw, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(f, "%s\n", raw)
		return err
	})
	if err != nil {
		return 0, err
	}
	defer w.close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.run(ctx) }()

	var runErr error
	select {
	case <-interrupt:
		cancel()
		runErr = <-done
	case runErr = <-done:
		cancel()
	}
	if runErr != nil {
		return 0, runErr
	}

	polls, notifies := w.collected()
	misses := matchEvents(polls, notifies, watchMatchWindow)
	if _, err := fmt.Fprintf(stdout, "poll=%d notify=%d misses=%d\n", len(polls), len(notifies), len(misses)); err != nil {
		return 0, err
	}
	for _, miss := range misses {
		raw, err := json.Marshal(miss)
		if err != nil {
			return 0, err
		}
		if _, err := fmt.Fprintf(stdout, "miss %s\n", raw); err != nil {
			return 0, err
		}
	}
	if len(misses) > 0 {
		return 3, nil
	}
	return 0, nil
}
