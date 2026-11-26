package pairing

import (
	"errors"
	"reflect"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

func settingsTestConfig() configSettings {
	return configSettings{
		paths: func() config.PathSet {
			return config.PathSet{DataDir: "/var/lib/test-peers"}
		},
		get: func(key string) (config.Value, error) {
			switch key {
			case "broker.alias":
				return config.Value{Kind: config.KindString, String: "hostA"}, nil
			case "broker.listen":
				return config.Value{Kind: config.KindString, String: "192.0.2.10:17443"}, nil
			case "pairing.max_failures":
				return config.Value{Kind: config.KindInt, Int: 7}, nil
			case "pairing.max_attempts_per_minute":
				return config.Value{Kind: config.KindInt, Int: 3}, nil
			default:
				return config.Value{}, errors.New("unexpected config key " + key)
			}
		},
		secret: func(key string) string {
			if key != "pairing.static_key" {
				return ""
			}
			return "correct-horse-battery"
		},
		advertised: func() []config.AdvertisedAddr {
			return []config.AdvertisedAddr{
				{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:17443"},
				{Kind: config.AdvertisedKindDirect, Addr: "[2001:db8::1]:17443"},
			}
		},
		hostname: func() string { return "hostA" },
		audit:    func(config.Event) {},
	}
}

// TestConfigSettingsReadsDocumentedConfig checks that configSettings returns the
// configured values for the settings pairing reads.
func TestConfigSettingsReadsDocumentedConfig(t *testing.T) {
	s := settingsTestConfig()

	dir, err := s.DataDir()
	if err != nil {
		t.Fatalf("DataDir() error = %v", err)
	}
	if dir != "/var/lib/test-peers" {
		t.Errorf("DataDir() = %q, want /var/lib/test-peers", dir)
	}

	alias, err := s.Alias()
	if err != nil {
		t.Fatalf("Alias() error = %v", err)
	}
	if alias != "hostA" {
		t.Errorf("Alias() = %q, want hostA", alias)
	}

	hostname, err := s.Hostname()
	if err != nil {
		t.Fatalf("Hostname() error = %v", err)
	}
	if hostname != "hostA" {
		t.Errorf("Hostname() = %q, want hostA", hostname)
	}

	listen, err := s.ListenAddress()
	if err != nil {
		t.Fatalf("ListenAddress() error = %v", err)
	}
	if listen != "192.0.2.10:17443" {
		t.Errorf("ListenAddress() = %q, want 192.0.2.10:17443", listen)
	}

	key := s.StaticKey()
	if key != "correct-horse-battery" {
		t.Errorf("StaticKey() = %q, want correct-horse-battery", key)
	}

	failures, err := s.MaxFailures()
	if err != nil {
		t.Fatalf("MaxFailures() error = %v", err)
	}
	if failures != 7 {
		t.Errorf("MaxFailures() = %d, want 7", failures)
	}

	attempts, err := s.MaxAttemptsPerMinute()
	if err != nil {
		t.Fatalf("MaxAttemptsPerMinute() error = %v", err)
	}
	if attempts != 3 {
		t.Errorf("MaxAttemptsPerMinute() = %d, want 3", attempts)
	}

	want := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:17443"},
		{Kind: config.AdvertisedKindDirect, Addr: "[2001:db8::1]:17443"},
	}
	addrs, err := s.Advertised()
	if err != nil {
		t.Fatalf("Advertised() error = %v", err)
	}
	if !reflect.DeepEqual(addrs, want) {
		t.Errorf("Advertised() = %+v, want %+v", addrs, want)
	}
}

// TestConfigSettingsAdvertisedReturnsCopy checks that changing a returned address
// list leaves the next call's list unchanged.
func TestConfigSettingsAdvertisedReturnsCopy(t *testing.T) {
	s := settingsTestConfig()
	first, err := s.Advertised()
	if err != nil {
		t.Fatalf("Advertised() error = %v", err)
	}
	first[0].Addr = "203.0.113.1:1"
	second, err := s.Advertised()
	if err != nil {
		t.Fatalf("Advertised() error = %v", err)
	}
	if second[0].Addr != "192.0.2.10:17443" {
		t.Errorf("second Advertised()[0].Addr = %q, want the original address", second[0].Addr)
	}
}

// TestConfigSettingsAuditForwards checks that Audit passes the event through to
// the configured audit function.
func TestConfigSettingsAuditForwards(t *testing.T) {
	var got config.Event
	s := settingsTestConfig()
	s.audit = func(e config.Event) { got = e }
	want := config.Event{Event: config.EventPairingSucceeded, Detail: "ok"}
	s.Audit(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Audit() forwarded %+v, want %+v", got, want)
	}
}

// TestConfigSettingsEmptyDataDir covers the state Open reports as ErrNotOpen.
func TestConfigSettingsEmptyDataDir(t *testing.T) {
	s := configSettings{paths: func() config.PathSet { return config.PathSet{} }}
	if _, err := s.DataDir(); !errors.Is(err, errNoDataDir) {
		t.Fatalf("DataDir() error = %v, want errNoDataDir", err)
	}
}

// TestConfigSettingsKindChecks checks that a value of the wrong kind is rejected
// with an error.
func TestConfigSettingsKindChecks(t *testing.T) {
	t.Run("alias wants string", func(t *testing.T) {
		s := configSettings{get: func(string) (config.Value, error) {
			return config.Value{Kind: config.KindInt, Int: 1}, nil
		}}
		if _, err := s.Alias(); err == nil {
			t.Fatal("Alias() with a KindInt value returned no error")
		}
	})
	t.Run("limits want int", func(t *testing.T) {
		s := configSettings{get: func(string) (config.Value, error) {
			return config.Value{Kind: config.KindString, String: "5"}, nil
		}}
		if _, err := s.MaxFailures(); err == nil {
			t.Fatal("MaxFailures() with a KindString value returned no error")
		}
		if _, err := s.MaxAttemptsPerMinute(); err == nil {
			t.Fatal("MaxAttemptsPerMinute() with a KindString value returned no error")
		}
	})
}

// TestConfigSettingsPropagatesGetError checks that a configuration read error is
// returned to the caller.
func TestConfigSettingsPropagatesGetError(t *testing.T) {
	want := errors.New("config unavailable")
	s := configSettings{get: func(string) (config.Value, error) { return config.Value{}, want }}
	if _, err := s.Alias(); !errors.Is(err, want) {
		t.Fatalf("Alias() error = %v, want %v", err, want)
	}
	if _, err := s.ListenAddress(); !errors.Is(err, want) {
		t.Fatalf("ListenAddress() error = %v, want %v", err, want)
	}
	if _, err := s.MaxFailures(); !errors.Is(err, want) {
		t.Fatalf("MaxFailures() error = %v, want %v", err, want)
	}
	if _, err := s.MaxAttemptsPerMinute(); !errors.Is(err, want) {
		t.Fatalf("MaxAttemptsPerMinute() error = %v, want %v", err, want)
	}
}

// TestConfigKindName covers the kind names used in configuration errors.
func TestConfigKindName(t *testing.T) {
	cases := []struct {
		kind config.ValueKind
		want string
	}{
		{config.KindUnset, "unset"},
		{config.KindString, "string"},
		{config.KindBool, "bool"},
		{config.KindInt, "int"},
		{config.KindDuration, "duration"},
		{config.KindStringList, "string list"},
		{config.KindSecret, "secret"},
		{config.ValueKind(250), "unknown"},
	}
	for _, c := range cases {
		if got := configKindName(c.kind); got != c.want {
			t.Errorf("configKindName(%d) = %q, want %q", c.kind, got, c.want)
		}
	}
}

// TestConfigSettingsZeroValueUsesConfigAPI checks that the production settings
// read the hostname through the config package.
func TestConfigSettingsZeroValueUsesConfigAPI(t *testing.T) {
	s := newConfigSettings()
	if _, ok := s.(configSettings); !ok {
		t.Fatalf("newConfigSettings() = %T, want configSettings", s)
	}
	got, err := s.Hostname()
	if err != nil {
		t.Fatalf("Hostname() error = %v", err)
	}
	if got != config.Hostname() {
		t.Errorf("Hostname() = %q, want config.Hostname() = %q", got, config.Hostname())
	}
}
