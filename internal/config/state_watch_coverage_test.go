package config

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/wangzw/peers/internal/accountdb"
)

// covRoutes is an address source whose result and failure the test sets
// between calls. callCount records how often the package enumerated it, so a
// test can prove a branch did or did not enumerate the default routes.
type covRoutes struct {
	mu    sync.Mutex
	addrs []netip.Addr
	err   error
	calls int
}

func (r *covRoutes) DefaultRouteAddresses() ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return append([]netip.Addr(nil), r.addrs...), r.err
}

func (r *covRoutes) set(addrs []netip.Addr, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addrs = append([]netip.Addr(nil), addrs...)
	r.err = err
}

func (r *covRoutes) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// covLogger replaces the core's logger with one writing to a buffer the test
// reads.
func covLogger(c *core) *bytes.Buffer {
	var buf bytes.Buffer
	c.logger = slog.New(slog.NewTextHandler(&buf, nil))
	return &buf
}

// covValue reads one key while holding the core lock.
func covValue(t *testing.T, c *core, key string) Value {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.getLocked(key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return v
}

// covAdvertised returns the advertised addresses in string form.
func covAdvertised(t *testing.T, c *core) []string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.advertised))
	for _, a := range c.advertised {
		out = append(out, a.Addr)
	}
	return out
}

// covSwapLookupAccount replaces the account-database seam for one test.
func covSwapLookupAccount(t *testing.T, fn func(uint32) (accountdb.User, error)) {
	t.Helper()
	old := lookupAccount
	lookupAccount = fn
	t.Cleanup(func() { lookupAccount = old })
}

// TestAccountHomeDirFallsBackToUserHomeDir checks that an account database
// lookup failure falls back to HOME.
func TestAccountHomeDirFallsBackToUserHomeDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	covSwapLookupAccount(t, func(uint32) (accountdb.User, error) {
		return accountdb.User{}, errors.New("no account database entry")
	})

	got, err := accountHomeDir()
	if err != nil {
		t.Fatalf("accountHomeDir() error = %v, want nil", err)
	}
	if got != home {
		t.Fatalf("accountHomeDir() = %q, want the HOME fallback %q", got, home)
	}
}

// TestRunningAccountNameReturnsLookedUpName checks that the running account
// name comes from the account database entry for the effective uid.
func TestRunningAccountNameReturnsLookedUpName(t *testing.T) {
	want := uint32(os.Geteuid())
	covSwapLookupAccount(t, func(uid uint32) (accountdb.User, error) {
		if uid != want {
			t.Errorf("lookupAccount uid = %d, want the effective uid %d", uid, want)
		}
		return accountdb.User{Name: "alice", UID: uid}, nil
	})

	got, err := runningAccountName()
	if err != nil {
		t.Fatalf("runningAccountName() error = %v, want nil", err)
	}
	if got != "alice" {
		t.Fatalf("runningAccountName() = %q, want alice", got)
	}
}

// TestRunningAccountNameReturnsLookupError checks that a failed account
// database lookup is returned as the error and yields no name.
func TestRunningAccountNameReturnsLookupError(t *testing.T) {
	injected := errors.New("account database unavailable")
	covSwapLookupAccount(t, func(uint32) (accountdb.User, error) {
		return accountdb.User{}, injected
	})

	got, err := runningAccountName()
	if !errors.Is(err, injected) {
		t.Fatalf("runningAccountName() error = %v, want %v", err, injected)
	}
	if got != "" {
		t.Fatalf("runningAccountName() = %q after a lookup failure, want empty", got)
	}
}

// TestSecretReloadFailureKeepsValueAndLogs checks that a secret keeps its
// loaded value and logs the failure when the refresh read fails.
func TestSecretReloadFailureKeepsValueAndLogs(t *testing.T) {
	c, _ := newWatchCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", false, nil)
	logs := covLogger(c)
	injected := errors.New("injected read failure")
	c.readFile = func(string) ([]byte, error) { return nil, injected }

	c.mu.Lock()
	got := c.secretLocked("pairing.static_key")
	c.mu.Unlock()

	if got != "0123456789abcdef" {
		t.Fatalf("Secret = %q after a failed refresh, want the loaded value", got)
	}
	if !strings.Contains(logs.String(), "cannot refresh the configuration for a secret") {
		t.Fatalf("the failed secret refresh was not logged; log = %q", logs.String())
	}
	if !strings.Contains(logs.String(), injected.Error()) {
		t.Fatalf("the log does not carry the read failure; log = %q", logs.String())
	}
}

// TestSetRejectsWrongValueKind checks that a Set of the wrong value type is
// refused without changing memory, the file or the audit log.
func TestSetRejectsWrongValueKind(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  public: false\n", false, nil)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	_, setErr := c.setLocked(Actor{}, "broker.public", stringValue("true"))
	public := c.values["broker.public"].Bool
	c.mu.Unlock()

	if !errors.Is(setErr, ErrInvalidValue) {
		t.Fatalf("Set error = %v, want ErrInvalidValue", setErr)
	}
	if !strings.Contains(setErr.Error(), "type does not match the key") {
		t.Fatalf("Set error = %v, want the type mismatch text", setErr)
	}
	if public {
		t.Error("broker.public = true in memory after a rejected Set")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Error("the configuration file changed after a rejected Set")
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records = %d, want 0", n)
	}
}

// TestSetListenRejectsCollidingDerivedAlias checks that changing broker.listen
// is refused when the derived alias collides with a peer.
func TestSetListenRejectsCollidingDerivedAlias(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  public: false\n", false, nil)
	installCoreForTest(t, c)

	// Change the hostname after loading so the derived alias differs from the
	// stored one.
	c.mu.Lock()
	c.hostname = func() (string, error) { return "hostB", nil }
	c.mu.Unlock()

	rejected := errors.New("derived alias collides with a peer")
	Validate("broker.alias", func(old, new Value) error {
		if new.String == "hostB.tester" {
			return rejected
		}
		return nil
	})

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, setErr := Set(Actor{}, "broker.listen", stringValue("127.0.0.1:27443"))
	if !errors.Is(setErr, ErrInvalidValue) {
		t.Fatalf("Set(broker.listen) error = %v, want ErrInvalidValue", setErr)
	}
	if !strings.Contains(setErr.Error(), "broker.alias") {
		t.Fatalf("Set error = %v, want it to name broker.alias", setErr)
	}
	if v := covValue(t, c, "broker.listen"); v.String == "127.0.0.1:27443" {
		t.Error("broker.listen was committed although the derived alias was rejected")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Error("the configuration file changed after a rejected Set")
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records = %d, want 0", n)
	}
}

// TestReloadInvalidListenTypeKeepsGroup checks that an invalidly typed
// broker.listen keeps the address and its whole cross-key group.
func TestReloadInvalidListenTypeKeepsGroup(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  listen: \":17443\"\n", false, nil)
	logs := covLogger(c)

	writeConfigBody(t, path, "broker:\n  listen: 27443\n")
	c.rescanOnce()

	if v := covValue(t, c, "broker.listen"); v.String != ":17443" {
		t.Fatalf("broker.listen = %q after an invalid reload, want the old value", v.String)
	}
	if !strings.Contains(logs.String(), "keeping the current value of an invalid key") {
		t.Fatalf("the invalid group member was not logged; log = %q", logs.String())
	}
	if !strings.Contains(logs.String(), "key=broker.listen") {
		t.Fatalf("the log does not name broker.listen; log = %q", logs.String())
	}
}

// TestReloadGroupValidatorRejectionKeepsListen checks that a validator
// rejecting a reloaded broker.listen keeps the old address.
func TestReloadGroupValidatorRejectionKeepsListen(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  listen: \":17443\"\n", false, nil)
	installCoreForTest(t, c)
	logs := covLogger(c)

	rejected := errors.New("address already taken")
	Validate("broker.listen", func(old, new Value) error { return rejected })

	writeConfigBody(t, path, "broker:\n  listen: \"127.0.0.1:27443\"\n")
	c.rescanOnce()

	if v := covValue(t, c, "broker.listen"); v.String != ":17443" {
		t.Fatalf("broker.listen = %q after a rejected reload, want the old value", v.String)
	}
	if !strings.Contains(logs.String(), "a validator rejected a reloaded value") {
		t.Fatalf("the rejected group member was not logged; log = %q", logs.String())
	}
}

// TestReloadCrossKeyGroupInvalidTypeRollsBack checks that a cross-key group
// with an invalid member type rolls back every member.
func TestReloadCrossKeyGroupInvalidTypeRollsBack(t *testing.T) {
	c, path := newWatchCore(t, "link:\n  reconnect_min: \"1s\"\n  reconnect_max: \"1m\"\n", false, nil)
	logs := covLogger(c)

	writeConfigBody(t, path, "link:\n  reconnect_min: 5\n  reconnect_max: \"2m\"\n")
	c.rescanOnce()

	if v := covValue(t, c, "link.reconnect_min"); v.Duration != time.Second {
		t.Fatalf("link.reconnect_min = %s after a rejected reload, want 1s", v.Duration)
	}
	if v := covValue(t, c, "link.reconnect_max"); v.Duration != time.Minute {
		t.Fatalf("link.reconnect_max = %s after a rejected reload, want the old 1m", v.Duration)
	}
	if !strings.Contains(logs.String(), "keeping a cross-key group; a member is invalid") {
		t.Fatalf("the invalid cross-key member was not logged; log = %q", logs.String())
	}
	if !strings.Contains(logs.String(), "must be a YAML string holding a duration") {
		t.Fatalf("the log does not carry the type error; log = %q", logs.String())
	}
}

// TestReloadCrossKeyGroupInvalidValueRollsBack checks that a cross-key group
// with an out-of-range member rolls back every member.
func TestReloadCrossKeyGroupInvalidValueRollsBack(t *testing.T) {
	c, path := newWatchCore(t, "link:\n  reconnect_min: \"1s\"\n  reconnect_max: \"1m\"\n", false, nil)
	logs := covLogger(c)

	writeConfigBody(t, path, "link:\n  reconnect_min: \"-5s\"\n  reconnect_max: \"2m\"\n")
	c.rescanOnce()

	if v := covValue(t, c, "link.reconnect_min"); v.Duration != time.Second {
		t.Fatalf("link.reconnect_min = %s after a rejected reload, want 1s", v.Duration)
	}
	if v := covValue(t, c, "link.reconnect_max"); v.Duration != time.Minute {
		t.Fatalf("link.reconnect_max = %s after a rejected reload, want the old 1m", v.Duration)
	}
	if !strings.Contains(logs.String(), "must be greater than 0") {
		t.Fatalf("the rejected value error was not logged; log = %q", logs.String())
	}
}

// TestReloadCrossKeyGroupValidatorRejectionRollsBack checks that a validator
// rejecting a cross-key member rolls back the group.
func TestReloadCrossKeyGroupValidatorRejectionRollsBack(t *testing.T) {
	c, path := newWatchCore(t, "link:\n  reconnect_min: \"1s\"\n  reconnect_max: \"1m\"\n", false, nil)
	installCoreForTest(t, c)
	logs := covLogger(c)

	rejected := errors.New("reconnect_min exceeds the peer limit")
	Validate("link.reconnect_min", func(old, new Value) error {
		if new.Duration == 2*time.Second {
			return rejected
		}
		return nil
	})

	writeConfigBody(t, path, "link:\n  reconnect_min: \"2s\"\n  reconnect_max: \"1m\"\n")
	c.rescanOnce()

	if v := covValue(t, c, "link.reconnect_min"); v.Duration != time.Second {
		t.Fatalf("link.reconnect_min = %s after a rejected reload, want 1s", v.Duration)
	}
	if !strings.Contains(logs.String(), "a validator rejected a member") {
		t.Fatalf("the rejected cross-key member was not logged; log = %q", logs.String())
	}
}

// TestReloadInvalidGenericKeyKeepsValue checks that an invalidly typed key
// keeps its current value on reload.
func TestReloadInvalidGenericKeyKeepsValue(t *testing.T) {
	c, path := newWatchCore(t, "pairing:\n  max_failures: 5\n", false, nil)
	logs := covLogger(c)

	writeConfigBody(t, path, "pairing:\n  max_failures: \"five\"\n")
	c.rescanOnce()

	if v := covValue(t, c, "pairing.max_failures"); v.Int != 5 {
		t.Fatalf("pairing.max_failures = %d after an invalid reload, want 5", v.Int)
	}
	if !strings.Contains(logs.String(), "keeping the current value of an invalid key") {
		t.Fatalf("the invalid key was not logged; log = %q", logs.String())
	}
	if !strings.Contains(logs.String(), "key=pairing.max_failures") {
		t.Fatalf("the log does not name the invalid key; log = %q", logs.String())
	}
}

// TestReloadGenericValidatorRejectionKeepsValue checks that a validator
// rejecting a reloaded value keeps the current value.
func TestReloadGenericValidatorRejectionKeepsValue(t *testing.T) {
	c, path := newWatchCore(t, "pairing:\n  max_failures: 5\n", false, nil)
	installCoreForTest(t, c)
	logs := covLogger(c)

	rejected := errors.New("max_failures is fixed for this deployment")
	Validate("pairing.max_failures", func(old, new Value) error {
		if new.Int == 9 {
			return rejected
		}
		return nil
	})

	writeConfigBody(t, path, "pairing:\n  max_failures: 9\n")
	c.rescanOnce()

	if v := covValue(t, c, "pairing.max_failures"); v.Int != 5 {
		t.Fatalf("pairing.max_failures = %d after a rejected reload, want 5", v.Int)
	}
	if !strings.Contains(logs.String(), "a validator rejected a reloaded value") {
		t.Fatalf("the rejected value was not logged; log = %q", logs.String())
	}
	if !strings.Contains(logs.String(), rejected.Error()) {
		t.Fatalf("the log does not carry the validator error; log = %q", logs.String())
	}
}

// TestReloadInvalidAliasKeepsCurrentAlias checks that an invalid broker.alias
// in the file keeps the current alias.
func TestReloadInvalidAliasKeepsCurrentAlias(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: keepme\n", false, nil)
	logs := covLogger(c)

	writeConfigBody(t, path, "broker:\n  alias: 123\n")
	c.rescanOnce()

	if got := aliasOf(t, c); got != "keepme" {
		t.Fatalf("broker.alias = %q after an invalid reload, want the old value", got)
	}
	if !strings.Contains(logs.String(), "keeping the current alias; the file value is invalid") {
		t.Fatalf("the invalid alias was not logged; log = %q", logs.String())
	}
}

// TestReloadIgnoredListenLogsOnce checks that a broker.listen ignored under a
// command-line override is logged once across reloads.
func TestReloadIgnoredListenLogsOnce(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  listen: \":17443\"\n  public: false\n", "127.0.0.1:19999")
	logs := covLogger(c)

	writeConfigBody(t, path, "broker:\n  listen: \":17444\"\n  public: true\n")
	c.rescanOnce()
	writeConfigBody(t, path, "broker:\n  listen: \":17444\"\n  public: false\n")
	c.rescanOnce()

	if v := covValue(t, c, "broker.listen"); v.String != "127.0.0.1:19999" {
		t.Fatalf("broker.listen = %q, want the command-line override", v.String)
	}
	if n := strings.Count(logs.String(), "ignoring broker.listen because the command line fixed the listen address"); n != 1 {
		t.Fatalf("the ignored listen was logged %d times, want once", n)
	}
}

// TestAddressConfigReportsBrokerAdvertiseErrors checks that addressConfig
// reports a portless broker.advertise_extra as an invalid value naming the key.
func TestAddressConfigReportsBrokerAdvertiseErrors(t *testing.T) {
	c, _ := newWatchCore(t, "", false, nil)

	values := map[string]Value{
		"broker.advertise":       stringValue(""),
		"broker.advertise_extra": stringListValue([]string{"example.com"}),
	}
	if _, _, err := c.addressConfig(values, listenAddr{}); !errors.Is(err, ErrInvalidValue) ||
		!strings.Contains(err.Error(), "broker.advertise_extra") {
		t.Fatalf("addressConfig with a portless extra = %v, want an invalid broker.advertise_extra error", err)
	}
}

// TestSetAdvertiseExtraReturnsAddressConfigError checks that Set refuses a
// portless broker.advertise_extra when broker.listen is empty, reporting
// ErrInvalidValue naming broker.advertise_extra.
func TestSetAdvertiseExtraReturnsAddressConfigError(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  listen: \"\"\n", false, nil)
	installCoreForTest(t, c)

	_, err := Set(Actor{}, "broker.advertise_extra", stringListValue([]string{"example.com"}))
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Set error = %v, want ErrInvalidValue", err)
	}
	if !strings.Contains(err.Error(), "broker.advertise_extra") {
		t.Fatalf("Set error = %v, want it to name broker.advertise_extra", err)
	}
}

// TestBuildAdvertisedKeepsPreviousRoutesWhenEnumerationFails checks that an
// enumeration failure keeps the previous route list.
func TestBuildAdvertisedKeepsPreviousRoutesWhenEnumerationFails(t *testing.T) {
	routes := &covRoutes{err: errors.New("no route table")}
	c, _ := newWatchCore(t, "", false, routes)
	logs := covLogger(c)

	preset := netip.MustParseAddr("192.0.2.10")
	c.mu.Lock()
	c.routes = []netip.Addr{preset}
	c.routesValid = false
	values := copyValues(c.values)
	effective := c.effectiveListen
	c.mu.Unlock()

	got, err := c.buildAdvertised(values, effective, true)
	if err != nil {
		t.Fatalf("buildAdvertised error = %v, want nil with the previous routes", err)
	}
	want := []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "192.0.2.10:17443"}}
	if !sameAddresses(got, want) {
		t.Fatalf("buildAdvertised = %v, want the previous route %v", got, want)
	}
	if !strings.Contains(logs.String(), "keeping the previous list") {
		t.Fatalf("the enumeration failure was not logged; log = %q", logs.String())
	}
}

// TestReevaluateAdvertisedDoesNothingAfterClose checks that re-evaluating the
// advertised addresses on a closed core changes nothing and enumerates no
// routes.
func TestReevaluateAdvertisedDoesNothingAfterClose(t *testing.T) {
	routes := &covRoutes{addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")}}
	c, _ := newWatchCore(t, "", false, routes)
	before := covAdvertised(t, c)
	calls := routes.callCount()

	c.close()
	c.reevaluateAdvertised()

	if after := covAdvertised(t, c); !slices.Equal(after, before) {
		t.Fatalf("advertised = %v after close, want the unchanged %v", after, before)
	}
	if got := routes.callCount(); got != calls {
		t.Fatalf("the routes were enumerated %d times after close, want %d", got-calls, 0)
	}
}

// TestReevaluateAdvertisedSkipsRoutesForSpecificListen checks that
// re-evaluating a specific listen address does not enumerate the default
// routes.
func TestReevaluateAdvertisedSkipsRoutesForSpecificListen(t *testing.T) {
	routes := &covRoutes{}
	c, _ := newWatchCore(t, "broker:\n  listen: \"203.0.113.5:17443\"\n", false, routes)
	calls := routes.callCount()

	c.reevaluateAdvertised()

	if got := routes.callCount(); got != calls {
		t.Fatalf("the routes were enumerated %d times, want 0 for a specific listen", got-calls)
	}
	want := []string{"203.0.113.5:17443"}
	if got := covAdvertised(t, c); !slices.Equal(got, want) {
		t.Fatalf("advertised = %v, want %v", got, want)
	}
}

// TestReevaluateAdvertisedKeepsListWhenRoutesFail checks that a route
// enumeration failure during re-evaluation keeps the current list.
func TestReevaluateAdvertisedKeepsListWhenRoutesFail(t *testing.T) {
	routes := &covRoutes{addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")}}
	c, _ := newWatchCore(t, "", false, routes)
	logs := covLogger(c)
	before := covAdvertised(t, c)

	routes.set(nil, errors.New("no route table"))
	c.reevaluateAdvertised()

	if after := covAdvertised(t, c); !slices.Equal(after, before) {
		t.Fatalf("advertised = %v after a route failure, want the unchanged %v", after, before)
	}
	if !strings.Contains(logs.String(), "cannot re-enumerate the default-route addresses") {
		t.Fatalf("the route failure was not logged; log = %q", logs.String())
	}
}

// TestReloadFromDiskDoesNothingAfterClose checks that reloading from disk on a
// closed core changes no value.
func TestReloadFromDiskDoesNothingAfterClose(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  alias: old\n", false, nil)
	c.close()

	writeConfigBody(t, path, "broker:\n  alias: new\n")
	c.reloadFromDisk()

	if got := aliasOf(t, c); got != "old" {
		t.Fatalf("broker.alias = %q after a reload on a closed core, want old", got)
	}
}

// TestStartAddressTickerSkippedWithoutBackground checks that the address
// ticker is started only with background work.
func TestStartAddressTickerSkippedWithoutBackground(t *testing.T) {
	c, _ := newWatchCore(t, "", false, nil)

	c.mu.Lock()
	c.startAddressTickerLocked(time.Minute)
	tick, done := c.addrTicker, c.addrTickerDone
	c.mu.Unlock()

	if tick != nil || done != nil {
		t.Fatalf("addrTicker = %v, addrTickerDone = %v, want both nil without background work", tick, done)
	}

	c.mu.Lock()
	c.noBackground = false
	c.startAddressTickerLocked(time.Minute)
	tick, done = c.addrTicker, c.addrTickerDone
	c.mu.Unlock()

	if tick == nil || done == nil {
		t.Fatalf("addrTicker = %v, addrTickerDone = %v, want both set with background work", tick, done)
	}
}

// TestAddressTickerTickReevaluatesAdvertised checks that an address-ticker
// tick re-evaluates and publishes the advertised addresses.
func TestAddressTickerTickReevaluatesAdvertised(t *testing.T) {
	routes := &covRoutes{addrs: []netip.Addr{netip.MustParseAddr("192.0.2.10")}}
	c, _ := newWatchCore(t, "", false, routes)
	installCoreForTest(t, c)

	tick, _ := startTestAddressTicker(t, c, time.Minute)
	changes := make(chan []AdvertisedAddr, 1)
	SubscribeAdvertised(func(old, new []AdvertisedAddr) { changes <- new })

	routes.set([]netip.Addr{netip.MustParseAddr("192.0.2.20")}, nil)
	select {
	case tick.ch <- time.Now():
	case <-time.After(eventTimeout):
		t.Fatal("the address ticker did not accept a tick")
	}

	select {
	case got := <-changes:
		if len(got) != 1 || got[0].Addr != "192.0.2.20:17443" {
			t.Fatalf("notification new = %v, want [192.0.2.20:17443]", got)
		}
	case <-time.After(eventTimeout):
		t.Fatal("the address ticker did not re-evaluate the advertised addresses")
	}
}

// TestSetWriteFailureRecordsEmptyHashWhenFileVanishes checks that a failed
// write whose file vanished records the hash of the missing file.
func TestSetWriteFailureRecordsEmptyHashWhenFileVanishes(t *testing.T) {
	c, path := newWatchCore(t, "broker:\n  public: false\n", false, nil)
	injected := errors.New("injected write failure")
	c.writeFile = func(name string, data []byte, mode Mode) error {
		_ = os.Remove(name)
		return injected
	}

	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.public", boolValue(true))
	hash := c.lastFileHash
	public := c.values["broker.public"].Bool
	c.mu.Unlock()

	if !errors.Is(err, injected) {
		t.Fatalf("Set error = %v, want the write failure", err)
	}
	if public {
		t.Error("broker.public = true in memory after a failed write")
	}
	if hash != sha256.Sum256(nil) {
		t.Error("lastFileHash does not describe the missing file after a failed write")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("os.Stat(%s) error = %v, want the file to be gone", path, statErr)
	}
}

// TestSetWriteFailureLogsUnreadableFileAfterWrite checks that a failed write
// that left an unreadable file logs the unreadable file and keeps the old
// hash.
func TestSetWriteFailureLogsUnreadableFileAfterWrite(t *testing.T) {
	c, _ := newWatchCore(t, "broker:\n  public: false\n", false, nil)
	logs := covLogger(c)
	c.mu.Lock()
	before := c.lastFileHash
	c.mu.Unlock()

	injected := errors.New("injected write failure")
	c.writeFile = func(name string, data []byte, mode Mode) error {
		_ = os.Remove(name)
		_ = os.Mkdir(name, 0o700)
		return injected
	}

	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.public", boolValue(true))
	hash := c.lastFileHash
	c.mu.Unlock()

	if !errors.Is(err, injected) {
		t.Fatalf("Set error = %v, want the write failure", err)
	}
	if hash != before {
		t.Error("lastFileHash changed after the failed write left an unreadable file")
	}
	if !strings.Contains(logs.String(), "cannot read the configuration file after a failed write") {
		t.Fatalf("the unreadable file after a failed write was not logged; log = %q", logs.String())
	}
}

// TestRealWatchTimerResetAfterStop checks that resetting a stopped timer
// restarts it and that Stop and C delegate to time.Timer.
func TestRealWatchTimerResetAfterStop(t *testing.T) {
	timer := newRealWatchTimer(time.Hour)
	if !timer.Stop() {
		t.Fatal("Stop on a running timer = false, want true")
	}
	if timer.Reset(time.Hour) {
		t.Error("Reset after Stop = true, want false for an inactive timer")
	}
	if !timer.Stop() {
		t.Error("Stop after Reset = false, want true for the restarted timer")
	}
	if timer.C() == nil {
		t.Error("C() = nil, want the timer channel")
	}
}

// TestStartWatchWithDebounceWithoutBackground checks that startWatchWithDebounce
// returns a watcher only with background work.
func TestStartWatchWithDebounceWithoutBackground(t *testing.T) {
	c, _ := newWatchCore(t, "", false, nil)

	w, err := startWatchWithDebounce(c, time.Millisecond)
	if err != nil {
		t.Fatalf("startWatchWithDebounce error = %v, want nil", err)
	}
	if w != nil {
		t.Fatalf("startWatchWithDebounce = %v, want nil without background work", w)
	}

	c.mu.Lock()
	c.noBackground = false
	c.mu.Unlock()
	w, err = startWatchWithDebounce(c, time.Millisecond)
	if err != nil {
		t.Fatalf("startWatchWithDebounce error = %v, want nil", err)
	}
	if w == nil {
		t.Fatal("startWatchWithDebounce returned nil with background work enabled")
	}
	t.Cleanup(w.stop)
}

// TestStartWatchWithDebounceDefaultsNonPositive checks that a non-positive
// debounce falls back to the default.
func TestStartWatchWithDebounceDefaultsNonPositive(t *testing.T) {
	c, _ := newWatchCore(t, "", false, nil)
	c.mu.Lock()
	c.noBackground = false
	c.mu.Unlock()

	w, err := startWatchWithDebounce(c, 0)
	if err != nil {
		t.Fatalf("startWatchWithDebounce error = %v, want nil", err)
	}
	if w == nil {
		t.Fatal("startWatchWithDebounce returned nil with background work enabled")
	}
	defer w.stop()
	if w.debounce != defaultWatchDebounce {
		t.Fatalf("debounce = %v, want the default %v", w.debounce, defaultWatchDebounce)
	}
}

// TestStartWatchLogsUnwatchableDirectory checks that a watch on a missing
// config directory is logged and installs no filesystem watcher.
func TestStartWatchLogsUnwatchableDirectory(t *testing.T) {
	c, _ := newWatchCore(t, "", false, nil)
	logs := covLogger(c)
	missing := filepath.Join(t.TempDir(), "gone")
	c.mu.Lock()
	c.paths.ConfigFile = filepath.Join(missing, "config.yaml")
	c.noBackground = false
	c.mu.Unlock()

	w, err := startWatchWithDebounce(c, time.Millisecond)
	if err != nil {
		t.Fatalf("startWatchWithDebounce error = %v, want nil", err)
	}
	if w == nil {
		t.Fatal("startWatchWithDebounce returned nil with background work enabled")
	}
	defer w.stop()
	if w.fsw != nil {
		t.Fatal("the watcher was installed for a directory that does not exist")
	}
	if !strings.Contains(logs.String(), "cannot watch the config directory") {
		t.Fatalf("the unwatchable directory was not logged; log = %q", logs.String())
	}
}

// TestStartWatchLogsWatcherCreationFailure checks that a filesystem watcher
// creation failure is logged and installs no watcher.
func TestStartWatchLogsWatcherCreationFailure(t *testing.T) {
	c, _ := newWatchCore(t, "", false, nil)
	logs := covLogger(c)
	injected := errors.New("cannot create an inotify instance")
	old := newFSWatcher
	newFSWatcher = func() (*fsnotify.Watcher, error) { return nil, injected }
	t.Cleanup(func() { newFSWatcher = old })
	c.mu.Lock()
	c.noBackground = false
	c.mu.Unlock()

	w, err := startWatchWithDebounce(c, time.Millisecond)
	if err != nil {
		t.Fatalf("startWatchWithDebounce error = %v, want nil", err)
	}
	if w == nil {
		t.Fatal("startWatchWithDebounce returned nil with background work enabled")
	}
	defer w.stop()
	if w.fsw != nil {
		t.Fatal("the watcher was installed although its creation failed")
	}
	if !strings.Contains(logs.String(), "cannot create the file watcher") {
		t.Fatalf("the watcher creation failure was not logged; log = %q", logs.String())
	}
}

// TestConfigWatcherContinuesAfterChannelsClose checks that the watcher keeps
// running after its fsnotify channels close and stops only on request.
func TestConfigWatcherContinuesAfterChannelsClose(t *testing.T) {
	c, _ := newWatchCore(t, "", false, nil)
	c.mu.Lock()
	c.noBackground = false
	c.mu.Unlock()

	w, err := startWatchWithDebounce(c, time.Millisecond)
	if err != nil {
		t.Fatalf("startWatchWithDebounce error = %v, want nil", err)
	}
	stopped := false
	if w != nil {
		t.Cleanup(func() {
			if !stopped {
				w.stop()
			}
		})
	}
	if w == nil || w.fsw == nil {
		t.Skip("fsnotify is unavailable on this host; the closed-channel paths cannot be driven")
	}
	if err := w.fsw.Close(); err != nil {
		t.Fatalf("close the watcher: %v", err)
	}

	select {
	case <-w.done:
		t.Fatal("the watcher exited after its fsnotify channels closed instead of waiting for stop")
	case <-time.After(50 * time.Millisecond):
	}

	stopped = true
	go w.stop()
	select {
	case <-w.done:
	case <-time.After(eventTimeout):
		t.Fatal("the watcher did not close done after stop")
	}
}

// TestConfigWatcherStopOnNilIsNoop checks that stopping a nil watcher does
// nothing.
func TestConfigWatcherStopOnNilIsNoop(t *testing.T) {
	var w *configWatcher
	w.stop()
}
