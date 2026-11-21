package broker

import (
	"time"

	"github.com/wangzw/peers/internal/config"
)

// ConfigSource is the broker's injection point for configuration. Its method
// set mirrors the exported functions of the config package, so the production
// implementation delegates to the process-wide config singleton while tests
// substitute a fake. The broker never calls the config package directly.
type ConfigSource interface {
	// CheckRuntimeDir creates and verifies the runtime directory.
	CheckRuntimeDir() error
	// AccountLock returns the path of the account lock file.
	AccountLock() string
	// AccountLockCandidates returns the account lock path at every runtime
	// directory this account's Broker may use.
	AccountLockCandidates() []string
	// Load loads the configuration for the given mode.
	Load(mode config.Mode, listenOverride string) error
	// Close releases the configuration singleton. It is idempotent.
	Close()
	// Paths returns the paths derived by Load; it returns the zero value
	// before Load.
	Paths() config.PathSet
	// Get returns the effective value of a key.
	Get(key string) (config.Value, error)
	// All returns every configuration entry.
	All() []config.Entry
	// Set validates and stores a value, returning the effective value.
	Set(actor config.Actor, key string, v config.Value) (config.Value, error)
	// ParseValue parses text according to the YAML type of the key.
	ParseValue(key, text string) (config.Value, error)
	// Validate registers a validation function for a key. The function runs
	// under the config lock and must not call exported config functions.
	Validate(key string, check func(old, new config.Value) error)
	// ProbeListen checks whether the Broker can listen on addr.
	ProbeListen(addr string) error
	// CheckLowPort applies the privileged-port rule to the loaded
	// configuration. The broker calls it in the listen-binding step.
	CheckLowPort() error
	// Subscribe registers a callback for changes to a key. It panics if
	// called before Load.
	Subscribe(key string, onChange func(old, new config.Value))
	// SubscribeAdvertised registers a callback for changes to the advertised
	// addresses.
	SubscribeAdvertised(onChange func(old, new []config.AdvertisedAddr))
	// Audit appends an event to the audit log.
	Audit(e config.Event)
	// ReadAudit returns the events recorded since the given time.
	ReadAudit(since time.Time) ([]config.Event, error)
}

// packageConfig implements ConfigSource by delegating to the config package.
type packageConfig struct{}

var _ ConfigSource = packageConfig{}

// PackageConfig returns the ConfigSource backed by the config package. The
// broker's unit tests never use it, because the config package is a
// process-wide singleton whose runtime and home directories cannot be
// redirected.
func PackageConfig() ConfigSource { return packageConfig{} }

func (packageConfig) CheckRuntimeDir() error { return config.CheckRuntimeDir() }

func (packageConfig) AccountLock() string { return config.AccountLock() }

func (packageConfig) AccountLockCandidates() []string { return config.AccountLockCandidates() }

func (packageConfig) Load(mode config.Mode, listenOverride string) error {
	return config.Load(mode, listenOverride)
}

func (packageConfig) Close() { config.Close() }

func (packageConfig) Paths() config.PathSet { return config.Paths() }

func (packageConfig) Get(key string) (config.Value, error) { return config.Get(key) }

func (packageConfig) All() []config.Entry { return config.All() }

func (packageConfig) Set(actor config.Actor, key string, v config.Value) (config.Value, error) {
	return config.Set(actor, key, v)
}

func (packageConfig) ParseValue(key, text string) (config.Value, error) {
	return config.ParseValue(key, text)
}

func (packageConfig) Validate(key string, check func(old, new config.Value) error) {
	config.Validate(key, check)
}

func (packageConfig) ProbeListen(addr string) error { return config.ProbeListen(addr) }

func (packageConfig) CheckLowPort() error { return config.CheckLowPort() }

func (packageConfig) Subscribe(key string, onChange func(old, new config.Value)) {
	config.Subscribe(key, onChange)
}

func (packageConfig) SubscribeAdvertised(onChange func(old, new []config.AdvertisedAddr)) {
	config.SubscribeAdvertised(onChange)
}

func (packageConfig) Audit(e config.Event) { config.Audit(e) }

func (packageConfig) ReadAudit(since time.Time) ([]config.Event, error) {
	return config.ReadAudit(since)
}
