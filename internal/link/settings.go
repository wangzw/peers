package link

import (
	"log/slog"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// configSettings is the Settings view over the process-wide configuration; a
// nil function field means the corresponding config package function.
type configSettings struct {
	get        func(string) (config.Value, error)
	advertised func() []config.AdvertisedAddr
	hostname   func() string
	audit      func(config.Event)
	logger     *slog.Logger
}

// ConfigSettings returns a Settings backed by the process-wide configuration.
func ConfigSettings() Settings { return configSettings{} }

func (s configSettings) value(key string) (config.Value, error) {
	if s.get != nil {
		return s.get(key)
	}
	return config.Get(key)
}

func (s configSettings) advertisedAddrs() []config.AdvertisedAddr {
	if s.advertised != nil {
		return s.advertised()
	}
	return config.Advertised()
}

func (s configSettings) hostnameValue() string {
	if s.hostname != nil {
		return s.hostname()
	}
	return config.Hostname()
}

func (s configSettings) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

// Alias returns the effective broker alias, falling back to the host name when
// broker.alias is missing, not a non-empty string, or cannot be read.
func (s configSettings) Alias() string {
	v, err := s.value("broker.alias")
	if err != nil {
		s.log().Error("link: cannot read broker.alias", "key", "broker.alias", "error", err)
		return s.hostnameValue()
	}
	if v.Kind != config.KindString || v.String == "" {
		s.log().Error("link: invalid broker.alias", "key", "broker.alias", "kind", v.Kind)
		return s.hostnameValue()
	}
	return v.String
}

// Advertised returns a copy of the advertised address list.
func (s configSettings) Advertised() []config.AdvertisedAddr {
	addrs := s.advertisedAddrs()
	out := make([]config.AdvertisedAddr, len(addrs))
	copy(out, addrs)
	return out
}

func (s configSettings) duration(key string, fallback time.Duration) time.Duration {
	v, err := s.value(key)
	if err != nil {
		s.log().Error("link: cannot read "+key, "key", key, "error", err)
		return fallback
	}
	if v.Kind != config.KindDuration || v.Duration <= 0 {
		s.log().Error("link: invalid "+key, "key", key, "kind", v.Kind)
		return fallback
	}
	return v.Duration
}

// DialTimeout returns link.dial_timeout, or 5s.
func (s configSettings) DialTimeout() time.Duration {
	return s.duration("link.dial_timeout", 5*time.Second)
}

// ReconnectMin returns link.reconnect_min, or 1s.
func (s configSettings) ReconnectMin() time.Duration {
	return s.duration("link.reconnect_min", time.Second)
}

// ReconnectMax returns link.reconnect_max, or 60s.
func (s configSettings) ReconnectMax() time.Duration {
	return s.duration("link.reconnect_max", 60*time.Second)
}

// KeepaliveInterval returns link.keepalive_interval, or 15s.
func (s configSettings) KeepaliveInterval() time.Duration {
	return s.duration("link.keepalive_interval", 15*time.Second)
}

// KeepaliveTimeout returns link.keepalive_timeout, or 10s.
func (s configSettings) KeepaliveTimeout() time.Duration {
	return s.duration("link.keepalive_timeout", 10*time.Second)
}

// Audit forwards an audit event to the configuration's audit log.
func (s configSettings) Audit(e config.Event) {
	if s.audit != nil {
		s.audit(e)
		return
	}
	config.Audit(e)
}
