package config

import (
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// eventTimeout bounds a wait on an event the test cannot force; it only decides
// how long a wait that will never complete takes to fail, since the success
// path selects on the event itself.
const eventTimeout = 30 * time.Second

// watchTestRoutes reports no default-route addresses, so a test not about routes
// never reads the host's routing table.
type watchTestRoutes struct{}

func (watchTestRoutes) DefaultRouteAddresses() ([]netip.Addr, error) { return nil, nil }

func newWatchCore(t *testing.T, body string, background bool, routes addressSource) (*core, string) {
	t.Helper()
	home := t.TempDir()
	c := newCore()
	c.skipDataCheck = true
	c.noBackground = !background
	c.checkRuntime = func() (string, error) { return filepath.Join(home, "run"), nil }
	c.homeDir = func() (string, error) { return home, nil }
	c.hostname = func() (string, error) { return "hostA", nil }
	c.accountName = func() (string, error) { return "tester", nil }
	c.now = func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
	c.routesSrc = watchTestRoutes{}
	if routes != nil {
		c.routesSrc = routes
	}
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.load(ModeUser, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	return c, path
}

func writeConfigBody(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func aliasOf(t *testing.T, c *core) string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values["broker.alias"].String
}

func countEvent(t *testing.T, c *core, kind EventKind, key string) int {
	t.Helper()
	events, err := c.readAudit(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range events {
		if e.Event == kind && (key == "" || e.Key == key) {
			n++
		}
	}
	return n
}

// TestConfigWatchFallbackRescan checks that rescanOnce adopts a changed alias and
// records one config_file_changed event.
func TestConfigWatchFallbackRescan(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	writeConfigBody(t, path, "broker:\n  alias: new\n")
	c.rescanOnce()
	if got := aliasOf(t, c); got != "new" {
		t.Fatalf("alias = %q, want new", got)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "broker.alias"); n != 1 {
		t.Fatalf("config_file_changed records = %d, want 1", n)
	}
}

// TestConfigWatchRenameRecreate checks that a rename over the configuration file
// is adopted.
func TestConfigWatchRenameRecreate(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	tmp := filepath.Join(filepath.Dir(path), "tmp-config")
	writeConfigBody(t, tmp, "broker:\n  alias: renamed\n")
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	c.rescanOnce()
	if got := aliasOf(t, c); got != "renamed" {
		t.Fatalf("alias = %q, want renamed", got)
	}
}

// TestConfigWatchHandleFSEvent checks that handleFSEvent reloads for the
// configuration file and ignores events for another file.
func TestConfigWatchHandleFSEvent(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	writeConfigBody(t, path, "broker:\n  alias: event\n")
	w := &configWatcher{c: c}
	w.handleFSEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	if got := aliasOf(t, c); got != "event" {
		t.Fatalf("alias = %q, want event", got)
	}
	writeConfigBody(t, path, "broker:\n  alias: ignored\n")
	w.handleFSEvent(fsnotify.Event{Name: filepath.Join(filepath.Dir(path), "other.yaml"), Op: fsnotify.Write})
	if got := aliasOf(t, c); got != "event" {
		t.Fatalf("alias = %q after unrelated event, want event", got)
	}
}

// TestConfigWatchSelfWriteIsNotReloaded checks that the file written by Set is
// not adopted again as an external change.
func TestConfigWatchSelfWriteIsNotReloaded(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	c.mu.Lock()
	_, err := c.setLocked(Actor{Account: "ana"}, "broker.alias", stringValue("self"))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	before := countEvent(t, c, EventConfigFileChanged, "")
	if before != 0 {
		t.Fatalf("unexpected config_file_changed records before rescan: %d", before)
	}
	c.rescanOnce()
	if after := countEvent(t, c, EventConfigFileChanged, ""); after != before {
		t.Fatalf("self-written file produced %d config_file_changed records", after-before)
	}
	if got := aliasOf(t, c); got != "self" {
		t.Fatalf("alias = %q, want self", got)
	}
}

// fakeWatchTimer is a debounce timer whose channel fires only when the test calls
// fire.
type fakeWatchTimer struct {
	c chan time.Time
}

func newFakeWatchTimer() *fakeWatchTimer {
	return &fakeWatchTimer{c: make(chan time.Time, 1)}
}

func (t *fakeWatchTimer) C() <-chan time.Time { return t.c }

func (t *fakeWatchTimer) Stop() bool { return true }

func (t *fakeWatchTimer) Reset(time.Duration) bool { return true }

func (t *fakeWatchTimer) fire() { t.c <- time.Now() }

// TestConfigWatchDebounceDelaysReload checks that a file event reloads only after
// the fake debounce timer fires.
func TestConfigWatchDebounceDelaysReload(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	c.mu.Lock()
	c.noBackground = false
	c.mu.Unlock()

	const debounce = 37 * time.Millisecond
	fake := newFakeWatchTimer()
	armed := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var delays []time.Duration
	oldNewTimer := newWatchTimer
	newWatchTimer = func(d time.Duration) watchTimer {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		once.Do(func() { close(armed) })
		return fake
	}
	defer func() { newWatchTimer = oldNewTimer }()

	w, err := startWatchWithDebounce(c, debounce)
	if err != nil {
		t.Fatal(err)
	}
	if w == nil || w.fsw == nil {
		if w != nil {
			w.stop()
		}
		t.Skip("fsnotify is unavailable on this host; there is no periodic rescan to fall back on, but Secret rereads the file before returning a value so the change is still observed")
	}
	defer w.stop()

	reloaded := make(chan struct{}, 1)
	c.mu.Lock()
	c.subs["broker.alias"] = append(c.subs["broker.alias"], func(old, new Value) {
		reloaded <- struct{}{}
	})
	c.mu.Unlock()

	writeConfigBody(t, path, "broker:\n  alias: watched\n")

	select {
	case <-armed:
	case <-time.After(eventTimeout):
		t.Fatal("the watcher did not arm the debounce after the file event")
	}

	mu.Lock()
	gotDelay := delays[0]
	mu.Unlock()
	if gotDelay != debounce {
		t.Fatalf("debounce = %v, want the injected %v", gotDelay, debounce)
	}
	if got := aliasOf(t, c); got != "old" {
		t.Fatalf("alias = %q while the debounce timer is armed, want old", got)
	}

	fake.fire()

	select {
	case <-reloaded:
	case <-time.After(eventTimeout):
		t.Fatal("the reload did not run after the debounce fired")
	}
	if got := aliasOf(t, c); got != "watched" {
		t.Fatalf("alias = %q after the debounce fired, want watched", got)
	}
}

// TestConfigWatchFSNotifySmoke checks that a real fsnotify event reloads the
// configuration.
func TestConfigWatchFSNotifySmoke(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: old\n", true, watchTestRoutes{})
	if c.watch == nil {
		t.Skip("fsnotify is unavailable on this host; there is no periodic rescan to fall back on, but Secret rereads the file before returning a value so the change is still observed")
	}
	writeConfigBody(t, path, "broker:\n  alias: watched\n")
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		if aliasOf(t, c) == "watched" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("alias = %q after waiting, want watched", aliasOf(t, c))
}
