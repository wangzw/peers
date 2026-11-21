package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// Mode selects the deployment form of the Broker. It decides the locations of
// the configuration file, data directory and audit log.
type Mode int

const (
	// ModeUser is a Broker serving one account.
	ModeUser Mode = iota
	// ModeSystem is a Broker serving every account on the host.
	ModeSystem
)

// ValueKind is the type of a configuration value.
type ValueKind uint8

const (
	KindUnset ValueKind = iota
	KindString
	KindBool
	KindInt
	KindDuration
	KindStringList
	KindSecret
)

// Value is a configuration value. Only the field selected by Kind is
// meaningful. Strings is never shared with the caller: Get, All and Set copy
// it. For KindSecret, String holds the value only inside this package; Get and
// All never return it.
type Value struct {
	Kind        ValueKind
	String      string
	Bool        bool
	Int         int64
	Duration    time.Duration
	Strings     []string
	SecretIsSet bool
}

// Entry is one configuration key and its effective value.
type Entry struct {
	Key   string
	Value Value
}

// Actor identifies who initiated a change or an event. UIDKnown reports whether
// UID is the effective account of the local caller; a caller that could not
// establish an identity leaves it false and is treated as unprivileged, so the
// zero value cannot pass a root-only rule. UID is used for rules that depend on
// whether the caller is root.
type Actor struct {
	UIDKnown bool
	UID      uint32
	Account  string
	Agent    string
}

// AdvertisedAddr is one entry in the advertised address list.
type AdvertisedAddr struct {
	Kind string
	Addr string
}

// AdvertisedKindDirect is the only address kind this version produces.
const AdvertisedKindDirect = "direct"

// PeerRef identifies a peer Broker.
type PeerRef struct {
	Fingerprint [32]byte
	Alias       string
}

// EventKind is the closed set of audit event types.
type EventKind string

const (
	EventConfigSet          EventKind = "config_set"
	EventConfigFileChanged  EventKind = "config_file_changed"
	EventPairingKeyCreated  EventKind = "pairing_key_created"
	EventPairingSucceeded   EventKind = "pairing_succeeded"
	EventPairingFailed      EventKind = "pairing_failed"
	EventLinkUp             EventKind = "link_up"
	EventLinkDown           EventKind = "link_down"
	EventUnpaired           EventKind = "unpaired"
	EventConnectionRejected EventKind = "connection_rejected"
	EventRejectedByPeer     EventKind = "rejected_by_peer"
	EventLocalAliasAssigned EventKind = "local_alias_assigned"
	// EventConfigRejected records an external configuration change that was
	// refused, so no value was adopted.
	EventConfigRejected EventKind = "config_rejected"
	EventUpdateApplied  EventKind = "update_applied"
	EventUpdateFailed   EventKind = "update_failed"
)

var allEventKinds = map[EventKind]bool{
	EventConfigSet:          true,
	EventConfigFileChanged:  true,
	EventPairingKeyCreated:  true,
	EventPairingSucceeded:   true,
	EventPairingFailed:      true,
	EventLinkUp:             true,
	EventLinkDown:           true,
	EventUnpaired:           true,
	EventConnectionRejected: true,
	EventRejectedByPeer:     true,
	EventLocalAliasAssigned: true,
	EventConfigRejected:     true,
	EventUpdateApplied:      true,
	EventUpdateFailed:       true,
}

// Event is one audit record.
type Event struct {
	Time    time.Time
	Event   EventKind
	Actor   string
	Account string
	Peer    *PeerRef
	Key     string
	Old     *Value
	New     *Value
	Count   uint64
	Detail  string
	// MergeKey is used only to decide whether two events merge; it is not part
	// of the public record. For connection_rejected it takes precedence over
	// Peer.Fingerprint, which the other side chooses and so cannot identify a
	// source.
	MergeKey string
}

// PathSet is the set of files this package uses.
type PathSet struct {
	RuntimeDir string
	// RuntimeDirFallback is set when RuntimeDir is the fallback location
	// because another account had taken the preferred runtime directory. It
	// names the preferred directory and the account that owns it, for the
	// startup log.
	RuntimeDirFallback *RuntimeDirFallback
	ConfigFile         string
	DataDir            string
	AuditLog           string
}

// RuntimeDirFallback describes the preferred runtime directory that another
// account had taken when the fallback location was chosen.
type RuntimeDirFallback struct {
	// TakenPath is the preferred runtime directory that could not be used.
	TakenPath string
	// OwnerUID is the account that owns TakenPath, or -1 when it is a symbolic
	// link or not a directory and its owner is not inspected.
	OwnerUID int
}

var (
	ErrNotLoaded          = errors.New("the configuration is not loaded")
	ErrAlreadyLoaded      = errors.New("the configuration is already loaded")
	ErrUnknownKey         = errors.New("unknown configuration key")
	ErrSecretKey          = errors.New("a secret key can only be set in the file")
	ErrListenOverridden   = errors.New("the listen address is fixed by the command line")
	ErrInvalidValue       = errors.New("invalid value")
	ErrInvalidCombination = errors.New("invalid listen/advertise combination")
	ErrTooManyAddresses   = errors.New("too many advertised addresses")
	ErrFileWrite          = errors.New("cannot write the configuration file")
	// ErrLowPortDenied reports that a system Broker refused a listen port
	// below 1024 because the change was not made by root or the configuration
	// file can be written by another account.
	ErrLowPortDenied = errors.New("a listen port below 1024 requires root and a configuration file that only root can write")
	// ErrDataUnavailable reports that the data directory or the audit log
	// cannot be used. A Broker started with --allow-degraded runs diagnostics
	// only for this class instead of exiting.
	ErrDataUnavailable = errors.New("the data directory or audit log is unavailable")
)

// DataUnavailableError classifies a failed Load whose cause is the data
// directory or the audit log. Every other startup check has already passed:
// the paths are resolved, the runtime directory is usable, the file was read
// and parsed, and the values and the advertised addresses were prepared. The
// error text names no configuration value. LocalSocketGroup and
// LocalSocketMode carry the loaded broker.local_socket_group and
// broker.local_socket_mode, so a system Broker can bind its local socket with
// the configured group and permissions while it runs diagnostics only.
type DataUnavailableError struct {
	Paths            PathSet
	LocalSocketGroup string
	LocalSocketMode  string
	Err              error
}

func (e *DataUnavailableError) Error() string {
	if e.Err == nil {
		return ErrDataUnavailable.Error()
	}
	return ErrDataUnavailable.Error() + ": " + e.Err.Error()
}

// Unwrap matches both ErrDataUnavailable and the concrete cause.
func (e *DataUnavailableError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrDataUnavailable}
	}
	return []error{ErrDataUnavailable, e.Err}
}

// global is the package singleton. Tests install their own core through
// installTestCore, which serializes the replacement with globalTestMu.
var global struct {
	mu   sync.RWMutex
	core *core
}

func currentCore() *core {
	global.mu.RLock()
	defer global.mu.RUnlock()
	return global.core
}

// Load resolves the paths for mode and loads the configuration. A non-empty
// listenOverride replaces broker.listen for this run only.
func Load(mode Mode, listenOverride string) error {
	global.mu.Lock()
	defer global.mu.Unlock()
	if global.core != nil {
		return ErrAlreadyLoaded
	}
	if err := takeAccountInitErr(); err != nil {
		return err
	}
	c := newCore()
	if err := c.load(mode, listenOverride); err != nil {
		c.close()
		return err
	}
	global.core = c
	return nil
}

// Close stops the background goroutines and closes the audit log. It is
// idempotent and is safe to call when nothing is loaded.
func Close() {
	global.mu.Lock()
	c := global.core
	global.core = nil
	global.mu.Unlock()
	if c != nil {
		c.close()
	}
}

// Paths returns the paths of the loaded instance, or a zero PathSet before
// Load.
func Paths() PathSet {
	c := currentCore()
	if c == nil {
		return PathSet{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paths
}

// Get returns the effective value of one key. Secret keys return
// KindSecret with SecretIsSet and no value.
func Get(key string) (Value, error) {
	c := currentCore()
	if c == nil {
		return Value{}, ErrNotLoaded
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getLocked(key)
}

// KeyNames returns every configuration key in lexicographic order. It reads
// only the package's key table, so it works before Load and without a Broker.
func KeyNames() []string {
	return allKeyNames()
}

// All returns every known key in lexicographic order.
func All() []Entry {
	c := currentCore()
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allLocked()
}

// Set validates one value and persists it, updates memory, writes an audit
// record and queues the notifications.
func Set(actor Actor, key string, v Value) (Value, error) {
	c := currentCore()
	if c == nil {
		return Value{}, ErrNotLoaded
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.setLocked(actor, key, v)
}

// Secret returns the value of a secret key, or an empty string for any other
// key or before Load.
func Secret(key string) string {
	c := currentCore()
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.secretLocked(key)
}

// Advertised returns a copy of the advertised address list.
func Advertised() []AdvertisedAddr {
	c := currentCore()
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyAddresses(c.advertised)
}

// Hostname returns the sanitized system hostname. It returns an empty string
// when os.Hostname fails, after logging the error.
func Hostname() string {
	if c := currentCore(); c != nil {
		return sanitizeHostnameFrom(c.hostname, c.logger)
	}
	return sanitizeHostnameFrom(os.Hostname, defaultLogger())
}

// SubscribeAdvertised registers a callback for changes of the advertised list.
func SubscribeAdvertised(onChange func(old, new []AdvertisedAddr)) {
	c := currentCore()
	if c == nil {
		panic("config: SubscribeAdvertised called before Load")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if onChange == nil {
		panic("config: SubscribeAdvertised with a nil callback")
	}
	c.advSubs = append(c.advSubs, onChange)
}

// Validate registers an extra check for one key. Set and reload call it
// before committing. The check runs while the package lock is held, so it must
// not call the package API.
//
// A check for broker.listen must only probe whether the address can be bound,
// for example with ProbeListen; it must not keep the listener it opens. The
// change is committed after the check returns, and the real rebind happens
// later in the subscriber that reacts to the change.
func Validate(key string, check func(old, new Value) error) {
	c := currentCore()
	if c == nil {
		panic("config: Validate called before Load")
	}
	if check == nil {
		panic("config: Validate with a nil callback for " + key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := lookupKey(key); !ok {
		panic("config: Validate for unknown key " + key)
	}
	c.validators[key] = append(c.validators[key], check)
}

// ProbeListen reports whether this process can bind addr, which uses the
// broker.listen syntax. It binds a TCP listener and closes it before returning,
// so no listener survives the check and the address stays free for the real
// listener. The result is only a probe: another process can take the address
// between the probe and the rebind. An empty addr means "do not listen" and
// always succeeds.
func ProbeListen(addr string) error {
	if addr == "" {
		return nil
	}
	if _, err := parseListen(addr); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("cannot bind %q: %w", addr, err)
	}
	if err := ln.Close(); err != nil {
		return fmt.Errorf("cannot close the probe listener on %q: %w", addr, err)
	}
	return nil
}

// ListenFromFile returns the effective broker.listen value of the configuration
// file at path: the value in the file, or the default when the key is absent.
// A missing file is an empty configuration and yields the default; an invalid
// value is reported as an error. The function only reads: it neither loads the
// package nor creates any path, so the service installer can check the address
// before it registers anything.
func ListenFromFile(path string) (string, error) {
	doc, err := readConfigFile(path)
	if err != nil {
		return "", err
	}
	if err, bad := doc.invalid["broker.listen"]; bad {
		return "", err
	}
	value := ""
	if doc.present["broker.listen"] {
		value = doc.values["broker.listen"].String
	} else {
		spec, ok := lookupKey("broker.listen")
		if !ok {
			return "", errors.New("broker.listen is not a known key")
		}
		value = defaultValue(spec, defaultContext{}).String
	}
	if _, err := parseListen(value); err != nil {
		return "", wrapInvalid("broker.listen", err.Error())
	}
	return value, nil
}

// Subscribe registers a callback for changes of one key.
func Subscribe(key string, onChange func(old, new Value)) {
	c := currentCore()
	if c == nil {
		panic("config: Subscribe called before Load")
	}
	if onChange == nil {
		panic("config: Subscribe with a nil callback for " + key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := lookupKey(key); !ok {
		panic("config: Subscribe for unknown key " + key)
	}
	c.subs[key] = append(c.subs[key], onChange)
}

// Audit appends one record. A failure to write is logged and does not fail the
// operation that produced the event.
func Audit(e Event) {
	c := currentCore()
	if c == nil {
		defaultLogger().Error("config_audit_before_load", "audit_event", string(e.Event))
		return
	}
	c.auditAppend(e)
}

// AppendAudit writes one record to the audit log at path without loading the
// configuration. The Broker uses Audit; this entry point is for a process that
// is not the Broker, such as the detached launchd reload helper, which records
// an outcome after the caller has already received the update response. An
// event that would merge in a running Broker is written as one record.
func AppendAudit(path string, e Event) error {
	if path == "" {
		return errors.New("the audit log path is empty")
	}
	a, err := openAudit(path, time.Now, slog.Default())
	if err != nil {
		return err
	}
	defer func() { _ = a.close() }()
	return a.append(e)
}

// ReadAudit returns the records at or after since in time order. Mergeable
// events accumulate in an in-memory window instead of being written one by
// one; the audit file stays append-only and receives one record when the
// window closes. ReadAudit returns the records already in the file together
// with any pending in-memory window, in time order.
func ReadAudit(since time.Time) ([]Event, error) {
	c := currentCore()
	if c == nil {
		return nil, ErrNotLoaded
	}
	return c.readAudit(since)
}

func wrapInvalid(key, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrInvalidValue, key, reason)
}

func copyValue(v Value) Value {
	if v.Strings != nil {
		v.Strings = append([]string(nil), v.Strings...)
	}
	return v
}

func copyAddresses(in []AdvertisedAddr) []AdvertisedAddr {
	if in == nil {
		return nil
	}
	return append([]AdvertisedAddr(nil), in...)
}
