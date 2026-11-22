package config

import (
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file fills the requirement-test gaps reported by the verification of
// the config unit. It owns its helpers and prefixes them with "gap" so the
// tests written by the other areas keep their own names.

// gapNoRoutes is an addressSource with no default-route addresses.
type gapNoRoutes struct{}

func (gapNoRoutes) DefaultRouteAddresses() ([]netip.Addr, error) { return nil, nil }

// gapRoutes is a mutable addressSource that drives re-enumeration tests.
type gapRoutes struct {
	mu    sync.Mutex
	addrs []netip.Addr
}

func (r *gapRoutes) DefaultRouteAddresses() ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]netip.Addr(nil), r.addrs...), nil
}

func (r *gapRoutes) set(addrs ...netip.Addr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addrs = append([]netip.Addr(nil), addrs...)
}

func gapAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return a
}

// newGapCore builds a core with the same test dependencies as newValueCore but
// returns the Load error instead of failing, so startup rejection can be
// asserted. A successful load registers the close cleanup.
func newGapCore(t *testing.T, body, override string) (*core, string, error) {
	t.Helper()
	home := t.TempDir()
	c := newCore()
	c.skipDataCheck = true
	c.noBackground = true
	c.checkRuntime = func() (string, error) { return filepath.Join(home, "run"), nil }
	c.homeDir = func() (string, error) { return home, nil }
	c.hostname = func() (string, error) { return "hostA", nil }
	c.accountName = func() (string, error) { return "tester", nil }
	c.now = func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
	c.routesSrc = gapNoRoutes{}
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	err := c.load(ModeUser, override)
	if err == nil {
		t.Cleanup(c.close)
	}
	return c, path, err
}

// gapValue reads one key while holding the core lock.
func gapValue(t *testing.T, c *core, key string) Value {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	v, err := c.getLocked(key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return v
}

// gapEvent returns the first audit record of kind for key.
func gapEvent(t *testing.T, c *core, kind EventKind, key string) (Event, bool) {
	t.Helper()
	events, err := c.readAudit(time.Time{})
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	for _, e := range events {
		if e.Event == kind && e.Key == key {
			return e, true
		}
	}
	return Event{}, false
}

func gapExtraItems(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, "extra"+string(rune('1'+i))+".example")
	}
	return out
}

// REQ-broker-config-006: Set must record the actor, the account and both the
// old and the new value in the config_set audit record.
func TestREQBrokerConfig006_SetWritesActorAccountOldNew(t *testing.T) {
	c, _ := newValueCore(t, "broker:\n  public: false\n", "")
	installCoreForTest(t, c)

	old, err := Set(Actor{Account: "ana", Agent: "hostA/ana"}, "broker.public", boolValue(true))
	if err != nil {
		t.Fatal(err)
	}
	if old.Kind != KindBool || old.Bool {
		t.Fatalf("Set returned old = %+v, want the previous false value", old)
	}

	e, ok := gapEvent(t, c, EventConfigSet, "broker.public")
	if !ok {
		t.Fatal("no config_set audit record for broker.public")
	}
	if e.Actor != "hostA/ana" {
		t.Errorf("audit actor = %q, want hostA/ana", e.Actor)
	}
	if e.Account != "ana" {
		t.Errorf("audit account = %q, want ana", e.Account)
	}
	if e.Old == nil || e.Old.Kind != KindBool || e.Old.Bool {
		t.Errorf("audit old = %+v, want a bool false value", e.Old)
	}
	if e.New == nil || e.New.Kind != KindBool || !e.New.Bool {
		t.Errorf("audit new = %+v, want a bool true value", e.New)
	}
}

// REQ-broker-config-011: log.level defaults to info and only accepts the four
// documented levels.
func TestREQBrokerConfig011_LogLevelDefaultAndValidation(t *testing.T) {
	c, _ := newValueCore(t, "", "")
	installCoreForTest(t, c)

	v, err := Get("log.level")
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != KindString || v.String != "info" {
		t.Fatalf("default log.level = %+v, want info", v)
	}

	current := "info"
	for _, level := range []string{"debug", "info", "warn", "error"} {
		if _, err := Set(Actor{}, "log.level", stringValue(level)); err != nil {
			t.Fatalf("Set(log.level=%q): %v", level, err)
		}
		current = level
	}
	for _, bad := range []string{"trace", "", "INFO", "warning"} {
		if _, err := Set(Actor{}, "log.level", stringValue(bad)); !errors.Is(err, ErrInvalidValue) {
			t.Errorf("Set(log.level=%q) error = %v, want ErrInvalidValue", bad, err)
		}
		got, err := Get("log.level")
		if err != nil {
			t.Fatal(err)
		}
		if got.String != current {
			t.Errorf("log.level = %q after rejecting %q, want %q", got.String, bad, current)
		}
	}
}

// REQ-broker-config-017: reload adopts a legal new value and audits it with the
// old and new values.
func TestREQBrokerConfig017_ReloadAuditsEachAdoptedKey(t *testing.T) {
	c, path := newWatchCore(t, "log:\n  level: info\nsend:\n  link_retries: 5\n", false, nil)
	writeConfigBody(t, path, "log:\n  level: debug\nsend:\n  link_retries: 3\n")
	c.rescanOnce()

	level := gapValue(t, c, "log.level")
	if level.String != "debug" {
		t.Errorf("log.level = %q after reload, want debug", level.String)
	}
	retries := gapValue(t, c, "send.link_retries")
	if retries.Int != 3 {
		t.Errorf("send.link_retries = %d after reload, want 3", retries.Int)
	}

	levelEvent, ok := gapEvent(t, c, EventConfigFileChanged, "log.level")
	if !ok {
		t.Fatal("no config_file_changed record for log.level")
	}
	if levelEvent.Old == nil || levelEvent.Old.String != "info" {
		t.Errorf("log.level audit old = %+v, want info", levelEvent.Old)
	}
	if levelEvent.New == nil || levelEvent.New.String != "debug" {
		t.Errorf("log.level audit new = %+v, want debug", levelEvent.New)
	}
	retryEvent, ok := gapEvent(t, c, EventConfigFileChanged, "send.link_retries")
	if !ok {
		t.Fatal("no config_file_changed record for send.link_retries")
	}
	if retryEvent.Old == nil || retryEvent.Old.Int != 5 {
		t.Errorf("send.link_retries audit old = %+v, want 5", retryEvent.Old)
	}
	if retryEvent.New == nil || retryEvent.New.Int != 3 {
		t.Errorf("send.link_retries audit new = %+v, want 3", retryEvent.New)
	}
}

// REQ-broker-config-017: reload keeps an illegal value and does not audit it.
func TestREQBrokerConfig017_ReloadKeepsIllegalLogLevel(t *testing.T) {
	c, path := newWatchCore(t, "log:\n  level: info\n", false, nil)
	writeConfigBody(t, path, "log:\n  level: verbose\n")
	c.rescanOnce()

	if got := gapValue(t, c, "log.level"); got.String != "info" {
		t.Fatalf("log.level = %q after an invalid reload, want info", got.String)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "log.level"); n != 0 {
		t.Fatalf("config_file_changed records for log.level = %d, want 0", n)
	}
}

// REQ-broker-config-017: a command-line listen override excludes broker.listen
// from reload while the other keys keep reloading.
func TestREQBrokerConfig017_OverrideExcludesListenOnReload(t *testing.T) {
	c, path := newValueCore(t, "log:\n  level: info\n", ":17444")
	writeConfigBody(t, path, "broker:\n  listen: \":17445\"\nlog:\n  level: debug\n")
	c.rescanOnce()

	c.mu.Lock()
	port := c.effectiveListen.port
	storedListen := c.values["broker.listen"].String
	level := c.values["log.level"].String
	c.mu.Unlock()

	if port != 17444 {
		t.Errorf("effective listen port = %d, want the 17444 override", port)
	}
	if storedListen != ":17444" {
		t.Errorf("broker.listen = %q, want the override value", storedListen)
	}
	if level != "debug" {
		t.Errorf("log.level = %q, want debug", level)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "broker.listen"); n != 0 {
		t.Errorf("config_file_changed records for broker.listen = %d, want 0 under the override", n)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "log.level"); n != 1 {
		t.Errorf("config_file_changed records for log.level = %d, want 1", n)
	}
}

// REQ-broker-config-017: when one key of the listen/advertise group is
// invalid, the whole group keeps its previous values.
func TestREQBrokerConfig017_ReloadKeepsTheListenGroupTogether(t *testing.T) {
	c, path := newWatchCore(t, "", false, nil)
	writeConfigBody(t, path, "broker:\n  listen: \":17444\"\n  advertise_extra:\n    - \":17445\"\n")
	c.rescanOnce()

	c.mu.Lock()
	port := c.effectiveListen.port
	stored := c.values["broker.listen"].String
	extra := copyValue(c.values["broker.advertise_extra"]).Strings
	c.mu.Unlock()

	if port != 17443 || stored != ":17443" {
		t.Errorf("listen = %q, effective port = %d, want both unchanged after an invalid sibling", stored, port)
	}
	if len(extra) != 0 {
		t.Errorf("broker.advertise_extra = %v, want unchanged (empty)", extra)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "broker.listen"); n != 0 {
		t.Errorf("config_file_changed records for broker.listen = %d, want 0", n)
	}
}

// REQ-broker-config-017: reload adopts a legal broker.listen change, updating
// the effective listener and the advertised addresses; the default alias does
// not depend on the listener and stays the same.
func TestREQBrokerConfig017_ReloadAdoptsLegalListen(t *testing.T) {
	routes := &gapRoutes{}
	routes.set(gapAddr(t, "192.0.2.10"))
	c, path := newWatchCore(t, "", false, routes)

	writeConfigBody(t, path, "broker:\n  listen: \":17444\"\n")
	c.rescanOnce()

	c.mu.Lock()
	port := c.effectiveListen.port
	stored := c.values["broker.listen"].String
	alias := c.values["broker.alias"].String
	advertised := copyAddresses(c.advertised)
	c.mu.Unlock()

	if stored != ":17444" {
		t.Errorf("broker.listen = %q after reload, want :17444", stored)
	}
	if port != 17444 {
		t.Errorf("effective listen port = %d after adopting %q, want 17444", port, stored)
	}
	if alias != "hostA.tester" {
		t.Errorf("broker.alias = %q after the listen change, want the unchanged hostA.tester", alias)
	}
	if len(advertised) != 1 || advertised[0].Addr != "192.0.2.10:17444" {
		t.Errorf("advertised addresses = %v after the listen change, want [192.0.2.10:17444]", advertised)
	}
}

// REQ-broker-config-021: the config file is created 0600 in both deployment
// forms. A system Broker cannot know the administration group the installation
// gives the file, so it writes the conservative mode.
func TestREQBrokerConfig021_ConfigFileCreationModes(t *testing.T) {
	cases := []struct {
		name string
		mode Mode
		want os.FileMode
	}{
		{"user", ModeUser, 0o600},
		{"system", ModeSystem, 0o600},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, path := newValueCore(t, "", "")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			c.mode = tc.mode
			installCoreForTest(t, c)

			if _, err := Set(Actor{}, "broker.public", boolValue(true)); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if perm := info.Mode().Perm(); perm != tc.want {
				t.Errorf("created config file mode = %o, want %o", perm, tc.want)
			}
		})
	}
}

// REQ-broker-config-022: the path set is derived from the deployment form and
// the account, so the config file and audit log are visible to the Broker.
func TestREQBrokerConfig022_PathRulesBothModes(t *testing.T) {
	c := newCore()
	c.homeDir = func() (string, error) { return "/home/ana", nil }
	c.euid = func() int { return 4242 }

	c.mode = ModeUser
	user, err := c.resolvePaths()
	if err != nil {
		t.Fatal(err)
	}
	userData := filepath.Join("/home/ana", ".peers")
	wantUser := PathSet{
		RuntimeDir: "/tmp/peers-4242",
		ConfigFile: filepath.Join(userData, "config.yaml"),
		DataDir:    userData,
		AuditLog:   filepath.Join(userData, "audit.log"),
	}
	if user != wantUser {
		t.Errorf("user paths = %+v, want %+v", user, wantUser)
	}

	c.mode = ModeSystem
	system, err := c.resolvePaths()
	if err != nil {
		t.Fatal(err)
	}
	wantSystem := PathSet{
		RuntimeDir: "/tmp/peers-4242",
		ConfigFile: "/etc/peers/config.yaml",
		DataDir:    "/var/lib/peers",
		AuditLog:   "/var/log/peers/audit.log",
	}
	if system != wantSystem {
		t.Errorf("system paths = %+v, want %+v", system, wantSystem)
	}
}

// REQ-broker-config-027: broker.local_socket_group and
// broker.local_socket_mode validate their values.
func TestREQBrokerConfig027_LocalSocketKeys(t *testing.T) {
	c, _ := newValueCore(t, "", "")
	installCoreForTest(t, c)

	lastGood := ""
	for _, good := range []string{"", "peer", "peer_group-1.example"} {
		if _, err := Set(Actor{}, "broker.local_socket_group", stringValue(good)); err != nil {
			t.Fatalf("Set(local_socket_group=%q): %v", good, err)
		}
		lastGood = good
	}
	for _, bad := range []string{"bad group", "a:b", "a/b", "x\ty"} {
		if _, err := Set(Actor{}, "broker.local_socket_group", stringValue(bad)); !errors.Is(err, ErrInvalidValue) {
			t.Errorf("Set(local_socket_group=%q) error = %v, want ErrInvalidValue", bad, err)
		}
		if got, _ := Get("broker.local_socket_group"); got.String != lastGood {
			t.Errorf("local_socket_group = %q after rejecting %q, want %q", got.String, bad, lastGood)
		}
	}

	lastGood = "0666"
	for _, good := range []string{"666", "0644", "0777"} {
		if _, err := Set(Actor{}, "broker.local_socket_mode", stringValue(good)); err != nil {
			t.Fatalf("Set(local_socket_mode=%q): %v", good, err)
		}
		lastGood = good
	}
	for _, bad := range []string{"0778", "0688", "77", "07777", "0o666", "abc"} {
		if _, err := Set(Actor{}, "broker.local_socket_mode", stringValue(bad)); !errors.Is(err, ErrInvalidValue) {
			t.Errorf("Set(local_socket_mode=%q) error = %v, want ErrInvalidValue", bad, err)
		}
		if got, _ := Get("broker.local_socket_mode"); got.String != lastGood {
			t.Errorf("local_socket_mode = %q after rejecting %q, want %q", got.String, bad, lastGood)
		}
	}
}

// REQ-topics-020: topics.auto_join defaults to true and a reload adopts a new
// value.
func TestREQTopics020_AutoJoinDefault(t *testing.T) {
	c, _ := newValueCore(t, "", "")
	installCoreForTest(t, c)
	v, err := Get("topics.auto_join")
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != KindBool || !v.Bool {
		t.Fatalf("default topics.auto_join = %+v, want true", v)
	}
}

func TestREQTopics020_AutoJoinReload(t *testing.T) {
	c, path := newWatchCore(t, "topics:\n  auto_join: true\n", false, nil)
	installCoreForTest(t, c)

	writeConfigBody(t, path, "topics:\n  auto_join: false\n")
	c.rescanOnce()

	v, err := Get("topics.auto_join")
	if err != nil {
		t.Fatal(err)
	}
	if v.Bool {
		t.Fatalf("topics.auto_join = true after reload, want false")
	}
	if n := countEvent(t, c, EventConfigFileChanged, "topics.auto_join"); n != 1 {
		t.Fatalf("config_file_changed records for topics.auto_join = %d, want 1", n)
	}
}

// REQ-broker-pairing-029: startup rejects a static key whose length is outside
// 16..128 characters, and the error never contains the value.
func TestREQBrokerPairing029_StartupRejectsInvalidKey(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"absent", "", false},
		{"15 characters", "0123456789abcde", true},
		{"16 characters", "0123456789abcdef", false},
		{"128 characters", strings.Repeat("a", 128), false},
		{"129 characters", strings.Repeat("a", 129), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := ""
			if tc.key != "" {
				body = "pairing:\n  static_key: \"" + tc.key + "\"\n"
			}
			_, _, err := newGapCore(t, body, "")
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidValue) {
					t.Fatalf("Load error = %v, want ErrInvalidValue", err)
				}
				if tc.key != "" && strings.Contains(err.Error(), tc.key) {
					t.Errorf("Load error leaks the static key value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load error = %v, want nil", err)
			}
		})
	}
}

// REQ-broker-pairing-029: reload keeps the previous static key when the file
// value is invalid.
func TestREQBrokerPairing029_ReloadKeepsInvalidKey(t *testing.T) {
	c, path := newWatchCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", false, nil)
	installCoreForTest(t, c)

	writeConfigBody(t, path, "pairing:\n  static_key: \"short\"\n")
	if got := Secret("pairing.static_key"); got != "0123456789abcdef" {
		t.Fatalf("Secret = %q after an invalid reload, want the previous key", got)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "pairing.static_key"); n != 0 {
		t.Fatalf("config_file_changed records for pairing.static_key = %d, want 0", n)
	}
}

// REQ-broker-pairing-031: Set rejects the secret key and changes neither the
// file, the memory value nor the audit log.
func TestREQBrokerPairing031_SetSecretRejected(t *testing.T) {
	c, path := newValueCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", "")
	installCoreForTest(t, c)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Set(Actor{}, "pairing.static_key", secretValue("fedcba9876543210")); !errors.Is(err, ErrSecretKey) {
		t.Fatalf("Set(static_key) error = %v, want ErrSecretKey", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("the config file changed after a rejected secret Set:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records = %d, want 0", n)
	}
	if got := Secret("pairing.static_key"); got != "0123456789abcdef" {
		t.Errorf("Secret = %q, want the unchanged key", got)
	}
}

// REQ-broker-pairing-032: Secret returns a value replaced in the file without
// requiring a separate reload, and the audit record keeps only the key.
func TestREQBrokerPairing032_SecretReload(t *testing.T) {
	c, path := newWatchCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", false, nil)
	installCoreForTest(t, c)

	if got := Secret("pairing.static_key"); got != "0123456789abcdef" {
		t.Fatalf("Secret = %q, want the file value", got)
	}
	writeConfigBody(t, path, "pairing:\n  static_key: \"fedcba9876543210\"\n")
	if got := Secret("pairing.static_key"); got != "fedcba9876543210" {
		t.Fatalf("Secret = %q after the file changed, want fedcba9876543210", got)
	}

	e, ok := gapEvent(t, c, EventConfigFileChanged, "pairing.static_key")
	if !ok {
		t.Fatal("no config_file_changed record for pairing.static_key")
	}
	if e.Old != nil || e.New != nil {
		t.Errorf("secret audit old/new = %+v/%+v, want both nil", e.Old, e.New)
	}
}

// REQ-broker-pairing-033: deleting the key from the file makes Secret return
// the empty string.
func TestREQBrokerPairing033_SecretDelete(t *testing.T) {
	c, path := newWatchCore(t, "pairing:\n  static_key: \"0123456789abcdef\"\n", false, nil)
	installCoreForTest(t, c)

	if got := Secret("pairing.static_key"); got != "0123456789abcdef" {
		t.Fatalf("Secret = %q, want the file value", got)
	}
	writeConfigBody(t, path, "broker:\n  public: false\n")
	if got := Secret("pairing.static_key"); got != "" {
		t.Fatalf("Secret = %q after the key was deleted, want empty", got)
	}
	v, err := Get("pairing.static_key")
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != KindSecret || v.SecretIsSet {
		t.Fatalf("Get(static_key) = %+v, want an unset secret", v)
	}
	if n := countEvent(t, c, EventConfigFileChanged, "pairing.static_key"); n != 1 {
		t.Fatalf("config_file_changed records for pairing.static_key = %d, want 1", n)
	}
}

// REQ-broker-discovery-016: the periodic re-enumeration replaces the list and
// notifies the advertised-address subscribers. Removing reevaluateAdvertised
// or its notification makes this test fail.
func TestREQBrokerDiscovery016_ReenumerateNotifies(t *testing.T) {
	routes := &gapRoutes{}
	routes.set(gapAddr(t, "192.0.2.10"))
	c, _ := newWatchCore(t, "", false, routes)
	installCoreForTest(t, c)

	if got := Advertised(); len(got) != 1 || got[0].Addr != "192.0.2.10:17443" {
		t.Fatalf("initial advertised addresses = %v", got)
	}

	type change struct {
		old []AdvertisedAddr
		new []AdvertisedAddr
	}
	changes := make(chan change, 1)
	SubscribeAdvertised(func(old, new []AdvertisedAddr) {
		changes <- change{old: old, new: new}
	})

	routes.set(gapAddr(t, "192.0.2.20"))
	c.reevaluateAdvertised()

	select {
	case got := <-changes:
		if len(got.old) != 1 || got.old[0].Addr != "192.0.2.10:17443" {
			t.Errorf("notification old = %v, want [192.0.2.10:17443]", got.old)
		}
		if len(got.new) != 1 || got.new[0].Addr != "192.0.2.20:17443" {
			t.Errorf("notification new = %v, want [192.0.2.20:17443]", got.new)
		}
	case <-time.After(eventTimeout):
		t.Fatal("re-enumeration did not notify the advertised-address subscribers")
	}

	if got := Advertised(); len(got) != 1 || got[0].Addr != "192.0.2.20:17443" {
		t.Fatalf("advertised addresses = %v after re-enumeration", got)
	}
}

// REQ-broker-config-032: Set rejects an invalid listen/advertise combination
// and leaves the file, memory and audit log unchanged.
func TestREQBrokerConfig032_SetRejectsCombination(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  listen: \"203.0.113.5:17443\"\n", "")
	installCoreForTest(t, c)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Set(Actor{}, "broker.advertise", stringValue(":17444")); !errors.Is(err, ErrInvalidCombination) {
		t.Fatalf("Set(advertise=:17444) error = %v, want ErrInvalidCombination", err)
	}
	if v, _ := Get("broker.advertise"); v.String != "" {
		t.Errorf("broker.advertise = %q after a rejected Set, want empty", v.String)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("the config file changed after a rejected combination:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records = %d, want 0", n)
	}
}

// REQ-broker-config-032: reload rejects an invalid listen/advertise
// combination and keeps the previous group.
func TestREQBrokerConfig032_ReloadRejectsCombination(t *testing.T) {
	c, path := newWatchCore(t, "", false, nil)
	writeConfigBody(t, path, "broker:\n  listen: \"\"\n  advertise: example.com\n")
	c.rescanOnce()

	listen := gapValue(t, c, "broker.listen")
	advertise := gapValue(t, c, "broker.advertise")
	if listen.String != ":17443" {
		t.Errorf("broker.listen = %q after a rejected reload, want :17443", listen.String)
	}
	if advertise.String != "" {
		t.Errorf("broker.advertise = %q after a rejected reload, want empty", advertise.String)
	}
	if n := countEvent(t, c, EventConfigFileChanged, ""); n != 0 {
		t.Errorf("config_file_changed records = %d, want 0", n)
	}
}

// REQ-broker-config-036: Set rejects more than eight configured addresses and
// keeps the previous value.
func TestREQBrokerConfig036_SetRejectsTooMany(t *testing.T) {
	c, path := newValueCore(t, "", "")
	installCoreForTest(t, c)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Set(Actor{}, "broker.advertise_extra", stringListValue(gapExtraItems(9))); !errors.Is(err, ErrTooManyAddresses) {
		t.Fatalf("Set(advertise_extra) error = %v, want ErrTooManyAddresses", err)
	}
	if v, _ := Get("broker.advertise_extra"); len(v.Strings) != 0 {
		t.Errorf("broker.advertise_extra = %v after a rejected Set, want empty", v.Strings)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("the config file changed after a rejected Set:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records = %d, want 0", n)
	}
}

// REQ-broker-config-036: reload rejects more than eight configured addresses
// and keeps the previous group.
func TestREQBrokerConfig036_ReloadRejectsTooMany(t *testing.T) {
	c, path := newWatchCore(t, "", false, nil)
	var body strings.Builder
	body.WriteString("broker:\n  advertise_extra:\n")
	for _, item := range gapExtraItems(9) {
		body.WriteString("    - ")
		body.WriteString(item)
		body.WriteString("\n")
	}
	writeConfigBody(t, path, body.String())
	c.rescanOnce()

	extra := gapValue(t, c, "broker.advertise_extra")
	if len(extra.Strings) != 0 {
		t.Errorf("broker.advertise_extra = %v after a rejected reload, want empty", extra.Strings)
	}
	if n := countEvent(t, c, EventConfigFileChanged, ""); n != 0 {
		t.Errorf("config_file_changed records = %d, want 0", n)
	}
}

// Set persistence failure keeps the in-memory value and writes no audit record.
func TestREQBrokerConfig002_SetWriteFailureKeepsMemoryAndAudit(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  public: false\n", "")
	installCoreForTest(t, c)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c.writeFile = func(string, []byte, Mode) error { return ErrFileWrite }

	if _, err := Set(Actor{}, "broker.public", boolValue(true)); !errors.Is(err, ErrFileWrite) {
		t.Fatalf("Set error = %v, want ErrFileWrite", err)
	}
	if v, _ := Get("broker.public"); v.Bool {
		t.Errorf("broker.public = true after a failed write, want false")
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records = %d after a failed write, want 0", n)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("the config file changed after a failed write")
	}
}

// Set persists values of every non-string kind.
func TestREQBrokerConfig002_SetPersistsIntDurationStringList(t *testing.T) {
	c, path := newValueCore(t, "", "")
	installCoreForTest(t, c)

	if _, err := Set(Actor{}, "send.link_retries", intValue(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := Set(Actor{}, "send.request_timeout", durationValue(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := Set(Actor{}, "broker.advertise_extra", stringListValue([]string{"a.example", "b.example"})); err != nil {
		t.Fatal(err)
	}

	if v, _ := Get("send.link_retries"); v.Kind != KindInt || v.Int != 3 {
		t.Errorf("send.link_retries = %+v, want int 3", v)
	}
	if v, _ := Get("send.request_timeout"); v.Kind != KindDuration || v.Duration != 30*time.Second {
		t.Errorf("send.request_timeout = %+v, want 30s", v)
	}
	if v, _ := Get("broker.advertise_extra"); v.Kind != KindStringList || len(v.Strings) != 2 || v.Strings[0] != "a.example" || v.Strings[1] != "b.example" {
		t.Errorf("broker.advertise_extra = %+v, want the two addresses", v)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseConfigFile(data)
	if err != nil {
		t.Fatalf("the written file does not parse: %v\n%s", err, data)
	}
	if got := doc.values["send.link_retries"].Int; got != 3 {
		t.Errorf("file send.link_retries = %d, want 3", got)
	}
	if got := doc.values["send.request_timeout"].Duration; got != 30*time.Second {
		t.Errorf("file send.request_timeout = %s, want 30s", got)
	}
	if got := doc.values["broker.advertise_extra"].Strings; len(got) != 2 || got[0] != "a.example" || got[1] != "b.example" {
		t.Errorf("file broker.advertise_extra = %v, want the two addresses", got)
	}
}

// Set rejects an unknown key, changing nothing.
func TestREQBrokerConfig002_SetUnknownKeyRejected(t *testing.T) {
	c, path := newValueCore(t, "", "")
	installCoreForTest(t, c)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Set(Actor{}, "no.such.key", stringValue("x")); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("Set(unknown key) error = %v, want ErrUnknownKey", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("the config file changed after an unknown-key Set")
	}
	if n := countEvent(t, c, EventConfigSet, ""); n != 0 {
		t.Errorf("config_set records = %d, want 0", n)
	}
}

// Set with a value equal to the current one still writes and audits, because
// the design gives same-value writes no exception.
func TestREQBrokerConfig002_SetSameValueStillAudits(t *testing.T) {
	c, _ := newValueCore(t, "broker:\n  public: false\n", "")
	installCoreForTest(t, c)

	old, err := Set(Actor{}, "broker.public", boolValue(false))
	if err != nil {
		t.Fatalf("same-value Set error = %v", err)
	}
	if old.Kind != KindBool || old.Bool {
		t.Errorf("same-value Set returned old = %+v, want false", old)
	}
	e, ok := gapEvent(t, c, EventConfigSet, "broker.public")
	if !ok {
		t.Fatal("same-value Set wrote no config_set record")
	}
	if e.Old == nil || e.Old.Bool || e.New == nil || e.New.Bool {
		t.Errorf("same-value audit old/new = %+v/%+v, want false/false", e.Old, e.New)
	}
	if n := countEvent(t, c, EventConfigSet, "broker.public"); n != 1 {
		t.Errorf("config_set records = %d, want 1", n)
	}
}
