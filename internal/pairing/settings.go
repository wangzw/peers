package pairing

import (
	"errors"
	"fmt"

	"github.com/wangzw/peers/internal/config"
)

var errNoDataDir = errors.New("the configuration has no data directory")

// Alias is read when a handshake starts, never while the peer table lock is
// held; the commit recheck and ResolveAlias read the SetOwnAlias cache instead.
type settings interface {
	DataDir() (string, error)
	Alias() (string, error)
	Hostname() (string, error)
	Advertised() ([]config.AdvertisedAddr, error)
	ListenAddress() (string, error)
	StaticKey() string
	MaxFailures() (int, error)
	MaxAttemptsPerMinute() (int, error)
	Audit(config.Event)
}

type configSettings struct {
	paths      func() config.PathSet
	get        func(key string) (config.Value, error)
	secret     func(key string) string
	advertised func() []config.AdvertisedAddr
	hostname   func() string
	audit      func(config.Event)
}

func newConfigSettings() settings { return configSettings{} }

var _ settings = configSettings{}

func (s configSettings) pathSet() config.PathSet {
	if s.paths != nil {
		return s.paths()
	}
	return config.Paths()
}

func (s configSettings) value(key string) (config.Value, error) {
	if s.get != nil {
		return s.get(key)
	}
	return config.Get(key)
}

func (s configSettings) secretValue(key string) string {
	if s.secret != nil {
		return s.secret(key)
	}
	return config.Secret(key)
}

func (s configSettings) advertisedValue() []config.AdvertisedAddr {
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

func (s configSettings) auditValue(e config.Event) {
	if s.audit != nil {
		s.audit(e)
		return
	}
	config.Audit(e)
}

func (s configSettings) DataDir() (string, error) {
	dir := s.pathSet().DataDir
	if dir == "" {
		return "", errNoDataDir
	}
	return dir, nil
}

func (s configSettings) Alias() (string, error) {
	return s.stringValue("broker.alias")
}

func (s configSettings) Hostname() (string, error) {
	return s.hostnameValue(), nil
}

func (s configSettings) Advertised() ([]config.AdvertisedAddr, error) {
	addrs := s.advertisedValue()
	return append([]config.AdvertisedAddr(nil), addrs...), nil
}

func (s configSettings) ListenAddress() (string, error) {
	return s.stringValue("broker.listen")
}

// pairing never stores a copy, so every handshake lookup reads the current
// value and a reload that changes or removes the key applies to the next
// handshake.
func (s configSettings) StaticKey() string {
	return s.secretValue("pairing.static_key")
}

func (s configSettings) MaxFailures() (int, error) {
	return s.intValue("pairing.max_failures")
}

func (s configSettings) MaxAttemptsPerMinute() (int, error) {
	return s.intValue("pairing.max_attempts_per_minute")
}

func (s configSettings) Audit(e config.Event) {
	s.auditValue(e)
}

func (s configSettings) stringValue(key string) (string, error) {
	v, err := s.value(key)
	if err != nil {
		return "", err
	}
	if v.Kind != config.KindString {
		return "", fmt.Errorf("cannot read config key %s: it has kind %s, want string", key, configKindName(v.Kind))
	}
	return v.String, nil
}

func (s configSettings) intValue(key string) (int, error) {
	v, err := s.value(key)
	if err != nil {
		return 0, err
	}
	if v.Kind != config.KindInt {
		return 0, fmt.Errorf("cannot read config key %s: it has kind %s, want int", key, configKindName(v.Kind))
	}
	return int(v.Int), nil
}

func configKindName(k config.ValueKind) string {
	switch k {
	case config.KindUnset:
		return "unset"
	case config.KindString:
		return "string"
	case config.KindBool:
		return "bool"
	case config.KindInt:
		return "int"
	case config.KindDuration:
		return "duration"
	case config.KindStringList:
		return "string list"
	case config.KindSecret:
		return "secret"
	default:
		return "unknown"
	}
}
