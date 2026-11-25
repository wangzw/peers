package config

import (
	"crypto/sha256"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

type exitRecordingTicker struct {
	ch       chan time.Time
	interval time.Duration

	mu      sync.Mutex
	stopped int
	exited  chan struct{}
	once    sync.Once
}

func newExitRecordingTicker(d time.Duration) *exitRecordingTicker {
	return &exitRecordingTicker{ch: make(chan time.Time), interval: d, exited: make(chan struct{})}
}

func (t *exitRecordingTicker) C() <-chan time.Time { return t.ch }

func (t *exitRecordingTicker) Stop() {
	t.mu.Lock()
	t.stopped++
	t.mu.Unlock()
}

// tickerExited satisfies tickerExitReporter so the test can wait for the
// goroutine to exit.
func (t *exitRecordingTicker) tickerExited() {
	t.once.Do(func() { close(t.exited) })
}

func (t *exitRecordingTicker) stopCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

func startTestAddressTicker(t *testing.T, c *core, d time.Duration) (*exitRecordingTicker, func() []*exitRecordingTicker) {
	t.Helper()
	var mu sync.Mutex
	var created []*exitRecordingTicker
	c.mu.Lock()
	c.noBackground = false
	c.newTicker = func(d time.Duration) ticker {
		tick := newExitRecordingTicker(d)
		mu.Lock()
		created = append(created, tick)
		mu.Unlock()
		return tick
	}
	c.startAddressTickerLocked(d)
	c.mu.Unlock()

	mu.Lock()
	first := created[0]
	mu.Unlock()
	return first, func() []*exitRecordingTicker {
		mu.Lock()
		defer mu.Unlock()
		return append([]*exitRecordingTicker(nil), created...)
	}
}

func assertTickerReplaced(t *testing.T, old *exitRecordingTicker, created func() []*exitRecordingTicker, want time.Duration) {
	t.Helper()
	if got := old.stopCount(); got == 0 {
		t.Fatal("the old address ticker was not stopped")
	}
	select {
	case <-old.exited:
	case <-time.After(eventTimeout):
		t.Fatal("the old address ticker goroutine did not exit")
	}
	list := created()
	last := list[len(list)-1]
	if last.interval != want {
		t.Fatalf("restarted ticker interval = %s, want %s", last.interval, want)
	}
}

// TestConfigReloadEmptyFileKeepsValues checks that a reload of an empty file
// keeps every value and writes no audit record.
func TestConfigReloadEmptyFileKeepsValues(t *testing.T) {
	body := "broker:\n  listen: \"127.0.0.1:7777\"\n  public: true\npairing:\n  static_key: \"0123456789abcdef\"\n  max_failures: 9\n"
	c, path := newWatchCore(t, body, false, nil)

	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	c.rescanOnce()

	c.mu.Lock()
	listen := c.values["broker.listen"].String
	public := c.values["broker.public"].Bool
	secret := c.values["pairing.static_key"].String
	failures := c.values["pairing.max_failures"].Int
	c.mu.Unlock()

	if listen != "127.0.0.1:7777" {
		t.Errorf("broker.listen = %q after an empty reload, want 127.0.0.1:7777", listen)
	}
	if !public {
		t.Error("broker.public = false after an empty reload, want true")
	}
	if secret != "0123456789abcdef" {
		t.Errorf("pairing.static_key = %q after an empty reload, want the previous key", secret)
	}
	if failures != 9 {
		t.Errorf("pairing.max_failures = %d after an empty reload, want 9", failures)
	}
	if n := countEvent(t, c, EventConfigFileChanged, ""); n != 0 {
		t.Errorf("config_file_changed records after an empty reload = %d, want 0", n)
	}
}

// TestConfigWatcherHasNoPeriodicRescan checks that starting the watcher creates
// no periodic rescan ticker.
func TestConfigWatcherHasNoPeriodicRescan(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  alias: old\n", false, nil)

	var mu sync.Mutex
	var durations []time.Duration
	c.mu.Lock()
	c.noBackground = false
	c.newTicker = func(d time.Duration) ticker {
		mu.Lock()
		durations = append(durations, d)
		mu.Unlock()
		return newExitRecordingTicker(d)
	}
	w, err := startWatch(c)
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("startWatch: %v", err)
	}
	if w != nil {
		w.stop()
	}

	mu.Lock()
	got := append([]time.Duration(nil), durations...)
	mu.Unlock()
	if len(got) != 0 {
		t.Fatalf("the watcher created tickers %v; the periodic rescan must not exist", got)
	}
}

// TestConfigSetWriteFailureRestoresOriginalBytes checks that a write that
// truncates and then fails restores the original bytes, keeps memory and the
// audit log, and records the hash of the file now on disk.
func TestConfigSetWriteFailureRestoresOriginalBytes(t *testing.T) {
	originalBody := "broker:\n  public: false\n  alias: old\n"
	c, path := newWatchCore(t, originalBody, false, nil)

	realWrite := c.writeFile
	calls := 0
	c.writeFile = func(p string, data []byte, mode Mode) error {
		calls++
		if calls == 1 {
			if err := os.WriteFile(p, nil, 0o600); err != nil {
				return err
			}
			return ErrFileWrite
		}
		return realWrite(p, data, mode)
	}

	before := countEvent(t, c, EventConfigFileChanged, "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.public", boolValue(true))
	c.mu.Unlock()

	if !errors.Is(err, ErrFileWrite) {
		t.Fatalf("Set error = %v, want ErrFileWrite", err)
	}
	if calls != 2 {
		t.Fatalf("write attempts = %d, want 2 (the failed write and the restore)", calls)
	}
	disk, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(disk) != originalBody {
		t.Fatalf("file after a failed write =\n%s\nwant the original bytes:\n%s", disk, originalBody)
	}
	c.mu.Lock()
	public := c.values["broker.public"].Bool
	hash := c.lastFileHash
	c.mu.Unlock()
	if public {
		t.Error("broker.public = true in memory after a failed write, want false")
	}
	if hash != sha256.Sum256(disk) {
		t.Error("lastFileHash does not describe the file on disk after the failed write")
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records after a failed write = %d, want 0", n)
	}

	c.rescanOnce()
	if after := countEvent(t, c, EventConfigFileChanged, ""); after != before {
		t.Errorf("the reload adopted the file after the failed write: %d new config_file_changed records", after-before)
	}
	c.mu.Lock()
	public = c.values["broker.public"].Bool
	c.mu.Unlock()
	if public {
		t.Error("broker.public changed on the reload after a failed write")
	}
}

// TestConfigSetWriteFailureFailedRestoreDoesNotResetValues checks that a failed
// write whose restore also fails leaves the values in memory and lets no later
// reload adopt the empty file.
func TestConfigSetWriteFailureFailedRestoreDoesNotResetValues(t *testing.T) {
	body := "broker:\n  listen: \"127.0.0.1:7777\"\n  public: true\npairing:\n  max_failures: 9\n"
	c, path := newWatchCore(t, body, false, nil)

	c.writeFile = func(p string, data []byte, mode Mode) error {
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			return err
		}
		return ErrFileWrite
	}

	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.public", boolValue(false))
	c.mu.Unlock()
	if !errors.Is(err, ErrFileWrite) {
		t.Fatalf("Set error = %v, want ErrFileWrite", err)
	}

	disk, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(disk) != 0 {
		t.Fatalf("the injected writer left %d bytes, want an empty file", len(disk))
	}

	c.mu.Lock()
	hash := c.lastFileHash
	listen := c.values["broker.listen"].String
	public := c.values["broker.public"].Bool
	failures := c.values["pairing.max_failures"].Int
	c.mu.Unlock()
	if hash != sha256.Sum256(disk) {
		t.Error("lastFileHash does not describe the empty file on disk")
	}
	if listen != "127.0.0.1:7777" || !public || failures != 9 {
		t.Fatalf("memory changed by the failed write: listen=%q public=%v max_failures=%d", listen, public, failures)
	}

	before := countEvent(t, c, EventConfigFileChanged, "")
	c.rescanOnce()
	if after := countEvent(t, c, EventConfigFileChanged, ""); after != before {
		t.Errorf("the reload adopted the empty file: %d new config_file_changed records", after-before)
	}
	c.mu.Lock()
	listen = c.values["broker.listen"].String
	public = c.values["broker.public"].Bool
	failures = c.values["pairing.max_failures"].Int
	c.mu.Unlock()
	if listen != "127.0.0.1:7777" || !public || failures != 9 {
		t.Fatalf("reload reset values after a failed write: listen=%q public=%v max_failures=%d", listen, public, failures)
	}
}

// TestConfigSetAdoptsPendingExternalChangeFirst checks that Set first adopts a
// pending external change with its audit record, then validates and writes its
// own key.
func TestConfigSetAdoptsPendingExternalChangeFirst(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  public: false\n  alias: old\n", false, nil)
	writeConfigBody(t, path, "broker:\n  public: true\n  alias: old\n")

	c.mu.Lock()
	_, err := c.setLocked(Actor{Account: "ana"}, "broker.alias", stringValue("new"))
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	c.mu.Lock()
	public := c.values["broker.public"].Bool
	alias := c.values["broker.alias"].String
	c.mu.Unlock()
	if !public {
		t.Error("broker.public = false after Set; the pending external change was swallowed")
	}
	if alias != "new" {
		t.Errorf("broker.alias = %q after Set, want new", alias)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "broker.public"); n != 1 {
		t.Errorf("config_file_changed records for the adopted broker.public = %d, want 1", n)
	}

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	back, parseErr := parseConfigFile(data)
	if parseErr != nil {
		t.Fatalf("the written file does not parse: %v", parseErr)
	}
	if !back.values["broker.public"].Bool || back.values["broker.alias"].String != "new" {
		t.Errorf("file holds public=%v alias=%q, want true/new", back.values["broker.public"].Bool, back.values["broker.alias"].String)
	}
}

// TestConfigReloadKeepsKeepalivePairTogether checks that a reload adopts a legal
// keepalive_interval/keepalive_timeout pair together.
func TestConfigReloadKeepsKeepalivePairTogether(t *testing.T) {
	c, path := newWatchCore(t, "link:\n  keepalive_interval: 15s\n  keepalive_timeout: 10s\n", false, nil)
	writeConfigBody(t, path, "link:\n  keepalive_interval: 5s\n  keepalive_timeout: 3s\n")
	c.rescanOnce()

	c.mu.Lock()
	interval := c.values["link.keepalive_interval"].Duration
	timeout := c.values["link.keepalive_timeout"].Duration
	c.mu.Unlock()
	if interval != 5*time.Second || timeout != 3*time.Second {
		t.Fatalf("keepalive pair = %s/%s after reload, want 5s/3s", interval, timeout)
	}
	for _, key := range []string{"link.keepalive_interval", "link.keepalive_timeout"} {
		if n := countEvent(t, c, EventConfigFileChanged, key); n != 1 {
			t.Errorf("config_file_changed records for %s = %d, want 1", key, n)
		}
	}
}

// TestConfigReloadKeepsReconnectPairTogether checks that an illegal reconnect
// pair leaves both members at their old values and writes no audit record.
func TestConfigReloadKeepsReconnectPairTogether(t *testing.T) {
	c, path := newWatchCore(t, "link:\n  reconnect_min: 1s\n  reconnect_max: 60s\n", false, nil)
	writeConfigBody(t, path, "link:\n  reconnect_min: 5s\n  reconnect_max: 3s\n")
	c.rescanOnce()

	c.mu.Lock()
	min := c.values["link.reconnect_min"].Duration
	max := c.values["link.reconnect_max"].Duration
	c.mu.Unlock()
	if min != time.Second || max != time.Minute {
		t.Fatalf("reconnect pair = %s/%s after an invalid reload, want 1s/1m", min, max)
	}
	for _, key := range []string{"link.reconnect_min", "link.reconnect_max"} {
		if n := countEvent(t, c, EventConfigFileChanged, key); n != 0 {
			t.Errorf("config_file_changed records for %s = %d, want 0", key, n)
		}
	}
}

// TestConfigReloadStopsOldAddressTicker checks that reloading a changed
// address_check_interval stops the previous ticker, lets its goroutine exit and
// starts one with the new interval.
func TestConfigReloadStopsOldAddressTicker(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  address_check_interval: 1m\n", false, nil)
	old, created := startTestAddressTicker(t, c, time.Minute)

	writeConfigBody(t, path, "broker:\n  address_check_interval: 2m\n")
	c.rescanOnce()

	c.mu.Lock()
	value := c.values["broker.address_check_interval"].Duration
	c.mu.Unlock()
	if value != 2*time.Minute {
		t.Fatalf("broker.address_check_interval = %s after reload, want 2m", value)
	}
	assertTickerReplaced(t, old, created, 2*time.Minute)
}

// TestConfigSetStopsOldAddressTicker checks that a Set that changes
// address_check_interval stops the previous ticker, lets its goroutine exit and
// starts one with the new interval.
func TestConfigSetStopsOldAddressTicker(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  address_check_interval: 1m\n", false, nil)
	old, created := startTestAddressTicker(t, c, time.Minute)

	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.address_check_interval", durationValue(3*time.Minute))
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	assertTickerReplaced(t, old, created, 3*time.Minute)
}

// TestConfigClosedSetAndSecret checks that Set cannot change a closed core and
// Secret returns the empty string.
func TestConfigClosedSetAndSecret(t *testing.T) {
	c, _ := newWatchCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", false, nil)
	c.close()

	if _, err := c.setLocked(Actor{}, "broker.public", boolValue(true)); !errors.Is(err, ErrNotLoaded) {
		t.Fatalf("Set after Close error = %v, want ErrNotLoaded", err)
	}
	if got := c.secretLocked("pairing.static_key"); got != "" {
		t.Fatalf("Secret after Close = %q, want empty", got)
	}
}
