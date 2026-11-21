package pairing

import (
	"errors"
	"fmt"

	"github.com/wangzw/peers/internal/config"
)

// errNoDataDir reports that config did not yield a data directory. Open wraps
// it as ErrNotOpen; keeping it local lets this file stand on its own.
var errNoDataDir = errors.New("the configuration has no data directory")

// settings is the narrow view of configuration pairing needs. Production code
// uses configSettings; tests use a fake with mutable fields, so pairing's logic
// never depends on config's package-level singleton or on config.Load.
//
// Alias is read when a handshake starts, never while the peer table lock is
// held. The commit recheck and ResolveAlias read the value the Broker pushed
// through SetOwnAlias instead.
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

// configSettings adapts the documented config API to settings. It is the only
// place in pairing that calls config's read functions.
//
// Each field is a seam for tests. A nil field falls back to the package
// function, so both configSettings{} and a partially populated value work.
// Open uses the zero value and therefore always reads the live configuration.
type configSettings struct {
	paths      func() config.PathSet
	get        func(key string) (config.Value, error)
	secret     func(key string) string
	advertised func() []config.AdvertisedAddr
	hostname   func() string
	audit      func(config.Event)
}

// newConfigSettings returns the production adapter.
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

// DataDir returns the directory holding identity.key, identity.crt and
// peers.json. An empty path means config has not been loaded.
func (s configSettings) DataDir() (string, error) {
	dir := s.pathSet().DataDir
	if dir == "" {
		return "", errNoDataDir
	}
	return dir, nil
}

// Alias returns this Broker's self-reported alias, taken from broker.alias. It
// is called when a handshake starts and must not be called under the peer table
// lock; the handshake commit recheck reads the SetOwnAlias cache instead.
func (s configSettings) Alias() (string, error) {
	return s.stringValue("broker.alias")
}

// Hostname returns the sanitized system hostname used when displaying pairing
// keys. config handles the sanitization.
func (s configSettings) Hostname() (string, error) {
	return s.hostnameValue(), nil
}

// Advertised returns a copy of the advertised address list.
func (s configSettings) Advertised() ([]config.AdvertisedAddr, error) {
	addrs := s.advertisedValue()
	return append([]config.AdvertisedAddr(nil), addrs...), nil
}

// ListenAddress returns broker.listen. An empty value means the Broker does not
// listen and cannot accept a pairing.
func (s configSettings) ListenAddress() (string, error) {
	return s.stringValue("broker.listen")
}

// StaticKey returns the current preset static pairing key. pairing never
// stores a copy: every handshake lookup reads the value through this method, so
// a reload that changes or removes it applies to the next handshake.
func (s configSettings) StaticKey() string {
	return s.secretValue("pairing.static_key")
}

// MaxFailures returns pairing.max_failures, the number of failed verifications
// after which a one-time key is discarded.
func (s configSettings) MaxFailures() (int, error) {
	return s.intValue("pairing.max_failures")
}

// MaxAttemptsPerMinute returns pairing.max_attempts_per_minute, the per-minute
// cap on unmatched pairing requests from one source. All sources together are
// capped at ten times this.
func (s configSettings) MaxAttemptsPerMinute() (int, error) {
	return s.intValue("pairing.max_attempts_per_minute")
}

// Audit forwards one event to the audit log.
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

// configKindName names a config value kind for error messages. config.ValueKind
// has no String method.
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
