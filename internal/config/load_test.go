package config

import (
	"bytes"
	"errors"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fixedRoutes struct {
	addrs []netip.Addr
	err   error
}

func (f fixedRoutes) DefaultRouteAddresses() ([]netip.Addr, error) {
	return f.addrs, f.err
}

func oneRoute(t *testing.T, addr string) addressSource {
	t.Helper()
	return fixedRoutes{addrs: []netip.Addr{netip.MustParseAddr(addr)}}
}

// newLoadTestCore builds a core confined to home with the runtime and data
// directory checks replaced, so nothing touches the host.
func newLoadTestCore(t *testing.T, home string, routes addressSource) *core {
	t.Helper()
	c := newCore()
	c.skipDataCheck = true
	c.noBackground = true
	c.checkRuntime = func() (string, error) { return home, nil }
	c.homeDir = func() (string, error) { return home, nil }
	c.euid = func() int { return os.Geteuid() }
	c.hostname = func() (string, error) { return "hostA", nil }
	c.accountName = func() (string, error) { return "tester", nil }
	c.now = func() time.Time { return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC) }
	c.routesSrc = watchTestRoutes{}
	if routes != nil {
		c.routesSrc = routes
	}
	t.Cleanup(c.close)
	return c
}

func writeLoadTestConfig(t *testing.T, home, body string) string {
	t.Helper()
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestLoadDefaults checks that loading an absent configuration file yields the
// defaults, the default alias and the default-route advertised address.
func TestLoadDefaults(t *testing.T) {
	home := t.TempDir()
	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	if err := c.load(ModeUser, ""); err != nil {
		t.Fatalf("load() error = %v", err)
	}

	if got := c.values["broker.listen"].String; got != ":17443" {
		t.Errorf("broker.listen = %q, want :17443", got)
	}
	if want := (listenAddr{set: true, all: true, port: 17443}); c.effectiveListen != want {
		t.Errorf("effectiveListen = %+v, want %+v", c.effectiveListen, want)
	}
	if got := c.values["broker.alias"].String; got != "hostA.tester" {
		t.Errorf("broker.alias = %q, want hostA.tester", got)
	}
	want := []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "192.0.2.20:17443"}}
	if !sameAddresses(c.advertised, want) {
		t.Errorf("advertised = %+v, want %+v", c.advertised, want)
	}
	if wantPath := filepath.Join(home, ".peers", "config.yaml"); c.paths.ConfigFile != wantPath {
		t.Errorf("ConfigFile = %q, want %q", c.paths.ConfigFile, wantPath)
	}
	if c.audit == nil {
		t.Error("audit log was not opened")
	}
}

// TestREQBrokerConfig028_DefaultAndEmptyListen checks that the default
// configuration listens on every address at port 17443 and that an explicitly
// empty listen address listens on nothing.
func TestREQBrokerConfig028_DefaultAndEmptyListen(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		home := t.TempDir()
		c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
		if err := c.load(ModeUser, ""); err != nil {
			t.Fatalf("load() error = %v", err)
		}
		if !c.effectiveListen.set || !c.effectiveListen.all || c.effectiveListen.port != 17443 {
			t.Fatalf("effectiveListen = %+v, want every address at 17443", c.effectiveListen)
		}
	})

	t.Run("empty disables listening", func(t *testing.T) {
		home := t.TempDir()
		writeLoadTestConfig(t, home, "broker:\n  listen: \"\"\n")
		c := newLoadTestCore(t, home, fixedRoutes{err: errors.New("routes must not be enumerated")})
		if err := c.load(ModeUser, ""); err != nil {
			t.Fatalf("load() error = %v", err)
		}
		if c.effectiveListen.set {
			t.Fatalf("effectiveListen = %+v, want not listening", c.effectiveListen)
		}
		if len(c.advertised) != 0 {
			t.Fatalf("advertised = %+v, want an empty list", c.advertised)
		}
		if got := c.values["broker.alias"].String; got != "hostA.tester" {
			t.Fatalf("broker.alias = %q, want hostA.tester", got)
		}
	})
}

// TestREQBrokerConfig025_ListenOverride checks that a listen override replaces
// the configured value for this run and is never written back to the file.
func TestREQBrokerConfig025_ListenOverride(t *testing.T) {
	home := t.TempDir()
	path := writeLoadTestConfig(t, home, "broker:\n  listen: \":17443\"\n")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	if err := c.load(ModeUser, ":17444"); err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if c.effectiveListen.port != 17444 {
		t.Fatalf("effectiveListen.port = %d, want 17444", c.effectiveListen.port)
	}
	if got := c.values["broker.listen"].String; got != ":17444" {
		t.Fatalf("broker.listen = %q, want the override :17444", got)
	}
	if len(c.advertised) != 1 || c.advertised[0].Addr != "192.0.2.20:17444" {
		t.Fatalf("advertised = %+v, want the override port", c.advertised)
	}
	if c.lastIgnoredListen != ":17443" {
		t.Fatalf("lastIgnoredListen = %q, want the file value :17443", c.lastIgnoredListen)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("the config file changed after load with an override:\nbefore %q\nafter  %q", before, after)
	}
}

// TestREQBrokerConfig025_OverrideSkipsInvalidFileListen checks that under an
// override the file's broker.listen is not validated.
func TestREQBrokerConfig025_OverrideSkipsInvalidFileListen(t *testing.T) {
	home := t.TempDir()
	writeLoadTestConfig(t, home, "broker:\n  listen: 123\n")

	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	if err := c.load(ModeUser, ":17444"); err != nil {
		t.Fatalf("load() with override error = %v, want nil", err)
	}
	if got := c.values["broker.listen"].String; got != ":17444" {
		t.Fatalf("broker.listen = %q, want :17444", got)
	}

	plain := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	err := plain.load(ModeUser, "")
	if err == nil {
		t.Fatal("load() without an override accepted a non-string broker.listen")
	}
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("load() error = %v, want ErrInvalidValue", err)
	}
}

// TestLoadRejectsInvalidValue checks that loading refuses an invalid value and
// names the key.
func TestLoadRejectsInvalidValue(t *testing.T) {
	home := t.TempDir()
	writeLoadTestConfig(t, home, "log:\n  level: verbose\n")
	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	err := c.load(ModeUser, "")
	if err == nil {
		t.Fatal("load() accepted an unknown log level")
	}
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("load() error = %v, want ErrInvalidValue", err)
	}
	if !strings.Contains(err.Error(), "log.level") {
		t.Fatalf("load() error = %v, want it to name log.level", err)
	}
}

// TestLoadRejectsCrossKeyRules checks that loading refuses values that violate
// a cross-key rule and names the offending key.
func TestLoadRejectsCrossKeyRules(t *testing.T) {
	tests := []struct {
		name string
		body string
		key  string
	}{
		{
			name: "reconnect_max below reconnect_min",
			body: "link:\n  reconnect_max: 500ms\n",
			key:  "link.reconnect_max",
		},
		{
			name: "keepalive_timeout not below the interval",
			body: "link:\n  keepalive_timeout: 15s\n",
			key:  "link.keepalive_timeout",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			writeLoadTestConfig(t, home, tc.body)
			c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
			err := c.load(ModeUser, "")
			if err == nil {
				t.Fatal("load() accepted values that violate a cross-key rule")
			}
			if !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("load() error = %v, want ErrInvalidValue", err)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("load() error = %v, want it to name %s", err, tc.key)
			}
		})
	}
}

// TestREQBrokerConfig032_StartupRejectsCombination checks that an illegal
// listen/advertise combination stops startup with ErrInvalidCombination.
func TestREQBrokerConfig032_StartupRejectsCombination(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "specific listener with a port-only advertise",
			body: "broker:\n  listen: \"127.0.0.1:17443\"\n  advertise: \":18443\"\n",
		},
		{
			name: "not listening with advertise set",
			body: "broker:\n  listen: \"\"\n  advertise: \"a.example:443\"\n",
		},
		{
			name: "not listening with advertise_extra set",
			body: "broker:\n  listen: \"\"\n  advertise_extra: [a.example]\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			writeLoadTestConfig(t, home, tc.body)
			c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
			err := c.load(ModeUser, "")
			if err == nil {
				t.Fatal("load() accepted an illegal combination")
			}
			if !errors.Is(err, ErrInvalidCombination) {
				t.Fatalf("load() error = %v, want ErrInvalidCombination", err)
			}
		})
	}
}

// TestLoadREQBrokerConfig036_TooManyConfigured checks that more than eight
// configured addresses are rejected at startup.
func TestLoadREQBrokerConfig036_TooManyConfigured(t *testing.T) {
	var body strings.Builder
	body.WriteString("broker:\n  advertise_extra:\n")
	for i := 0; i < 9; i++ {
		body.WriteString("    - host")
		body.WriteString(string(rune('a' + i)))
		body.WriteString(".example\n")
	}
	home := t.TempDir()
	writeLoadTestConfig(t, home, body.String())
	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	err := c.load(ModeUser, "")
	if err == nil {
		t.Fatal("load() accepted nine configured addresses")
	}
	if !errors.Is(err, ErrTooManyAddresses) {
		t.Fatalf("load() error = %v, want ErrTooManyAddresses", err)
	}
}

// TestLoadREQBrokerConfig030_PortOnly checks that with a port-only advertise
// the default-route addresses carry the configured port.
func TestLoadREQBrokerConfig030_PortOnly(t *testing.T) {
	home := t.TempDir()
	writeLoadTestConfig(t, home, "broker:\n  advertise: \":18443\"\n")
	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	if err := c.load(ModeUser, ""); err != nil {
		t.Fatalf("load() error = %v", err)
	}
	want := []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "192.0.2.20:18443"}}
	if !sameAddresses(c.advertised, want) {
		t.Fatalf("advertised = %+v, want %+v", c.advertised, want)
	}
}

// TestREQBrokerConfig033_ExtraAppended checks that advertise_extra entries are
// appended and an entry without a port takes the advertise or listen port.
func TestREQBrokerConfig033_ExtraAppended(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []AdvertisedAddr
	}{
		{
			name: "extra after the default-route address",
			body: "broker:\n  advertise_extra: [b.vpn.example]\n",
			want: []AdvertisedAddr{
				{Kind: AdvertisedKindDirect, Addr: "192.0.2.20:17443"},
				{Kind: AdvertisedKindDirect, Addr: "b.vpn.example:17443"},
			},
		},
		{
			name: "extra takes the advertise port",
			body: "broker:\n  advertise: \":18443\"\n  advertise_extra: [b.vpn.example]\n",
			want: []AdvertisedAddr{
				{Kind: AdvertisedKindDirect, Addr: "192.0.2.20:18443"},
				{Kind: AdvertisedKindDirect, Addr: "b.vpn.example:18443"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			writeLoadTestConfig(t, home, tc.body)
			c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
			if err := c.load(ModeUser, ""); err != nil {
				t.Fatalf("load() error = %v", err)
			}
			if !sameAddresses(c.advertised, tc.want) {
				t.Fatalf("advertised = %+v, want %+v", c.advertised, tc.want)
			}
		})
	}
}

// TestREQBrokerConfig031_HostAddresses checks that a host in broker.advertise
// becomes the first entry and a bare host takes the listen port.
func TestREQBrokerConfig031_HostAddresses(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []AdvertisedAddr
	}{
		{
			name: "host and port",
			body: "broker:\n  advertise: \"a.example:443\"\n",
			want: []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "a.example:443"}},
		},
		{
			name: "host only uses the listen port",
			body: "broker:\n  advertise: \"a.example\"\n",
			want: []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "a.example:17443"}},
		},
		{
			name: "IPv6 host keeps its brackets",
			body: "broker:\n  advertise: \"[2001:db8::1]:443\"\n",
			want: []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "[2001:db8::1]:443"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			writeLoadTestConfig(t, home, tc.body)
			c := newLoadTestCore(t, home, fixedRoutes{err: errors.New("routes must not be enumerated")})
			if err := c.load(ModeUser, ""); err != nil {
				t.Fatalf("load() error = %v", err)
			}
			if !sameAddresses(c.advertised, tc.want) {
				t.Fatalf("advertised = %+v, want %+v", c.advertised, tc.want)
			}
		})
	}
}

// TestLoadRoutesFailureOnlyWhenNeeded checks that a route enumeration failure
// does not stop startup and that a combination needing routes starts with an
// empty advertised list and a log entry.
func TestLoadRoutesFailureOnlyWhenNeeded(t *testing.T) {
	t.Run("a configured host does not need the default route", func(t *testing.T) {
		home := t.TempDir()
		writeLoadTestConfig(t, home, "broker:\n  advertise: \"a.example:443\"\n")
		c := newLoadTestCore(t, home, fixedRoutes{err: errors.New("no route table")})
		if err := c.load(ModeUser, ""); err != nil {
			t.Fatalf("load() error = %v, want nil", err)
		}
	})

	t.Run("a route failure starts with an empty list and is logged", func(t *testing.T) {
		home := t.TempDir()
		var logs bytes.Buffer
		c := newLoadTestCore(t, home, fixedRoutes{err: errors.New("no route table")})
		c.logger = slog.New(slog.NewTextHandler(&logs, nil))
		if err := c.load(ModeUser, ""); err != nil {
			t.Fatalf("load() error = %v, want nil: a route failure must not stop startup", err)
		}
		if len(c.advertised) != 0 {
			t.Fatalf("advertised = %+v, want an empty list", c.advertised)
		}
		if !strings.Contains(logs.String(), "default route") {
			t.Fatalf("log = %q, want an error naming the default route", logs.String())
		}
	})
}

type flakyLoadRoutes struct {
	mu    sync.Mutex
	addrs []netip.Addr
	err   error
}

func (f *flakyLoadRoutes) DefaultRouteAddresses() ([]netip.Addr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]netip.Addr(nil), f.addrs...), f.err
}

func (f *flakyLoadRoutes) recover(addrs ...netip.Addr) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addrs = append([]netip.Addr(nil), addrs...)
	f.err = nil
}

// TestLoadRoutesEmptyThenFilledByReenumeration checks that re-enumeration
// fills in the addresses that startup left empty.
func TestLoadRoutesEmptyThenFilledByReenumeration(t *testing.T) {
	home := t.TempDir()
	routes := &flakyLoadRoutes{err: errors.New("no route table")}
	c := newLoadTestCore(t, home, routes)
	if err := c.load(ModeUser, ""); err != nil {
		t.Fatalf("load() error = %v, want nil", err)
	}
	if len(c.advertised) != 0 {
		t.Fatalf("advertised = %+v, want an empty list after the route failure", c.advertised)
	}

	routes.recover(netip.MustParseAddr("192.0.2.20"))
	c.reevaluateAdvertised()

	want := []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "192.0.2.20:17443"}}
	if !sameAddresses(c.advertised, want) {
		t.Fatalf("advertised = %+v, want %+v", c.advertised, want)
	}
}

// TestLoadAlias checks that a configured alias is kept and that without one the
// default combines the hostname and account.
func TestLoadAlias(t *testing.T) {
	t.Run("a configured alias is kept", func(t *testing.T) {
		home := t.TempDir()
		writeLoadTestConfig(t, home, "broker:\n  alias: build-box\n")
		c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
		if err := c.load(ModeUser, ""); err != nil {
			t.Fatalf("load() error = %v", err)
		}
		if !c.explicitAlias {
			t.Error("explicitAlias = false, want true")
		}
		if got := c.values["broker.alias"].String; got != "build-box" {
			t.Fatalf("broker.alias = %q, want build-box", got)
		}
		if got := c.effectiveAlias(); got != "build-box" {
			t.Fatalf("effectiveAlias() = %q, want build-box", got)
		}
	})

	t.Run("without a configured alias the default combines hostname and account", func(t *testing.T) {
		home := t.TempDir()
		c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
		if err := c.load(ModeUser, ""); err != nil {
			t.Fatalf("load() error = %v", err)
		}
		if c.explicitAlias {
			t.Error("explicitAlias = true, want false")
		}
		if got := c.effectiveAlias(); got != "hostA.tester" {
			t.Fatalf("effectiveAlias() = %q, want hostA.tester", got)
		}
	})
}

// TestREQBrokerConfig023_LoadDoesNotRequireWritePermission checks that a
// write-protected configuration file does not stop startup.
func TestREQBrokerConfig023_LoadDoesNotRequireWritePermission(t *testing.T) {
	home := t.TempDir()
	path := writeLoadTestConfig(t, home, "broker:\n  alias: build-box\n")
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatal(err)
	}
	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	if err := c.load(ModeUser, ""); err != nil {
		t.Fatalf("load() error = %v, want nil for a read-only config file", err)
	}
	if got := c.effectiveAlias(); got != "build-box" {
		t.Fatalf("effectiveAlias() = %q, want build-box", got)
	}
}

// TestLoadCreatesAuditLogWith0600 checks that loading creates the audit log
// with mode 0600.
func TestLoadCreatesAuditLogWith0600(t *testing.T) {
	home := t.TempDir()
	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	if err := c.load(ModeUser, ""); err != nil {
		t.Fatalf("load() error = %v", err)
	}
	info, err := os.Stat(c.paths.AuditLog)
	if err != nil {
		t.Fatalf("audit log stat error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("audit log mode = %#o, want 0600", got)
	}
}
