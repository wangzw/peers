package link

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

type durationCase struct {
	key  string
	name string
	got  func(configSettings) time.Duration
	want time.Duration
}

func defaultDurationCases() []durationCase {
	return []durationCase{
		{"link.dial_timeout", "DialTimeout", func(s configSettings) time.Duration { return s.DialTimeout() }, 5 * time.Second},
		{"link.reconnect_min", "ReconnectMin", func(s configSettings) time.Duration { return s.ReconnectMin() }, time.Second},
		{"link.reconnect_max", "ReconnectMax", func(s configSettings) time.Duration { return s.ReconnectMax() }, 60 * time.Second},
		{"link.keepalive_interval", "KeepaliveInterval", func(s configSettings) time.Duration { return s.KeepaliveInterval() }, 15 * time.Second},
		{"link.keepalive_timeout", "KeepaliveTimeout", func(s configSettings) time.Duration { return s.KeepaliveTimeout() }, 10 * time.Second},
	}
}

func newTestConfigSettings(values map[string]config.Value, logBuf *bytes.Buffer) configSettings {
	return configSettings{
		get: func(key string) (config.Value, error) {
			v, ok := values[key]
			if !ok {
				return config.Value{}, config.ErrNotLoaded
			}
			return v, nil
		},
		advertised: func() []config.AdvertisedAddr {
			return []config.AdvertisedAddr{
				{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.1:17443"},
				{Kind: "relay", Addr: "relay.example:9"},
			}
		},
		hostname: func() string { return "fallback-host" },
		logger:   slog.New(slog.NewJSONHandler(logBuf, nil)),
	}
}

// TestConfigSettingsReadsKeys checks that each Settings method reads its key
// and that Advertised returns a copy.
func TestConfigSettingsReadsKeys(t *testing.T) {
	values := map[string]config.Value{
		"broker.alias":            {Kind: config.KindString, String: "alice"},
		"link.dial_timeout":       {Kind: config.KindDuration, Duration: 3 * time.Second},
		"link.reconnect_min":      {Kind: config.KindDuration, Duration: 750 * time.Millisecond},
		"link.reconnect_max":      {Kind: config.KindDuration, Duration: 45 * time.Second},
		"link.keepalive_interval": {Kind: config.KindDuration, Duration: 20 * time.Second},
		"link.keepalive_timeout":  {Kind: config.KindDuration, Duration: 8 * time.Second},
	}
	var buf bytes.Buffer
	var audited []config.Event
	s := newTestConfigSettings(values, &buf)
	s.audit = func(e config.Event) { audited = append(audited, e) }

	if got := s.Alias(); got != "alice" {
		t.Errorf("Alias() = %q, want %q", got, "alice")
	}
	for _, d := range []durationCase{
		{"link.dial_timeout", "DialTimeout", func(s configSettings) time.Duration { return s.DialTimeout() }, 3 * time.Second},
		{"link.reconnect_min", "ReconnectMin", func(s configSettings) time.Duration { return s.ReconnectMin() }, 750 * time.Millisecond},
		{"link.reconnect_max", "ReconnectMax", func(s configSettings) time.Duration { return s.ReconnectMax() }, 45 * time.Second},
		{"link.keepalive_interval", "KeepaliveInterval", func(s configSettings) time.Duration { return s.KeepaliveInterval() }, 20 * time.Second},
		{"link.keepalive_timeout", "KeepaliveTimeout", func(s configSettings) time.Duration { return s.KeepaliveTimeout() }, 8 * time.Second},
	} {
		if got := d.got(s); got != d.want {
			t.Errorf("%s() = %v, want %v", d.name, got, d.want)
		}
	}

	addrs := s.Advertised()
	if len(addrs) != 2 || addrs[0].Addr != "192.0.2.1:17443" || addrs[1].Kind != "relay" {
		t.Errorf("Advertised() = %+v, want the seam's list", addrs)
	}
	addrs[0].Addr = "changed"
	if again := s.Advertised(); again[0].Addr != "192.0.2.1:17443" {
		t.Errorf("Advertised() shares its backing array: second call = %+v", again)
	}

	s.Audit(config.Event{Event: config.EventLinkUp})
	if len(audited) != 1 || audited[0].Event != config.EventLinkUp {
		t.Errorf("Audit forwarded %+v, want one EventLinkUp", audited)
	}

	if buf.Len() != 0 {
		t.Errorf("valid values logged errors: %s", buf.String())
	}
}

// TestConfigSettingsFallsBackOnError checks that defaults replace values that
// are unavailable or of the wrong kind, and that each fallback is logged.
func TestConfigSettingsFallsBackOnError(t *testing.T) {
	keys := []string{"broker.alias"}
	for _, d := range defaultDurationCases() {
		keys = append(keys, d.key)
	}

	check := func(t *testing.T, s configSettings, buf *bytes.Buffer) {
		t.Helper()
		if got := s.Alias(); got != "fallback-host" {
			t.Errorf("Alias() = %q, want %q", got, "fallback-host")
		}
		for _, d := range defaultDurationCases() {
			if got := d.got(s); got != d.want {
				t.Errorf("%s() = %v, want %v", d.name, got, d.want)
			}
		}
		for _, key := range keys {
			if !strings.Contains(buf.String(), key) {
				t.Errorf("log output does not mention %q: %s", key, buf.String())
			}
		}
	}

	t.Run("not loaded", func(t *testing.T) {
		var buf bytes.Buffer
		s := configSettings{
			get:      func(string) (config.Value, error) { return config.Value{}, config.ErrNotLoaded },
			hostname: func() string { return "fallback-host" },
			logger:   slog.New(slog.NewJSONHandler(&buf, nil)),
		}
		check(t, s, &buf)
	})

	t.Run("wrong kind", func(t *testing.T) {
		var buf bytes.Buffer
		s := configSettings{
			get: func(string) (config.Value, error) {
				return config.Value{Kind: config.KindBool, Bool: true}, nil
			},
			hostname: func() string { return "fallback-host" },
			logger:   slog.New(slog.NewJSONHandler(&buf, nil)),
		}
		check(t, s, &buf)
	})
}
