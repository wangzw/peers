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

// Mode is the Broker's deployment form; it decides the paths of the
// configuration file, data directory and audit log.
type Mode int

const (
	// ModeUser is a Broker serving one account.
	ModeUser Mode = iota
	// ModeSystem is a Broker serving every account on the host.
	ModeSystem
)

// ValueKind is the type of a configuration value.
type ValueKind uint8

// The ValueKind constants are the types a configuration value can hold.
const (
	KindUnset ValueKind = iota
	KindString
	KindBool
	KindInt
	KindDuration
	KindStringList
	KindSecret
)

// Value is a configuration value; only the field selected by Kind is
// meaningful.
type Value struct {
	// Kind is the type of the value.
	Kind ValueKind
	// String is the value for KindString, and the text of a secret key.
	String string
	// Bool is the value for KindBool.
	Bool bool
	// Int is the value for KindInt.
	Int int64
	// Duration is the value for KindDuration.
	Duration time.Duration
	// Strings is the value for KindStringList; callers receive a copy.
	Strings []string
	// SecretIsSet reports whether a secret key has a value. Get and All never
	// return the value; it is read with Secret.
	SecretIsSet bool
}

// Entry is one configuration key and its effective value.
type Entry struct {
	// Key is the configuration key's name.
	Key string
	// Value is the key's effective value.
	Value Value
}

// Actor identifies who initiated a change or an event.
type Actor struct {
	// UIDKnown reports whether UID is the local caller's effective account; a
	// caller that could not establish an identity leaves it false and is
	// treated as unprivileged.
	UIDKnown bool
	// UID is the caller's effective account, used by rules that depend on root.
	UID uint32
	// Account is the caller's account name.
	Account string
	// Agent is the initiating agent's address, empty when not an agent.
	Agent string
}

// AdvertisedAddr is one entry in the advertised address list.
type AdvertisedAddr struct {
	// Kind is the address kind, deciding how another Broker reaches this one.
	Kind string
	// Addr is the address spelling for Kind; a direct address is host:port.
	Addr string
}

// AdvertisedKindDirect is the only address kind this version produces.
const AdvertisedKindDirect = "direct"

// PeerRef identifies a peer Broker.
type PeerRef struct {
	// Fingerprint is the peer Broker's fingerprint.
	Fingerprint [32]byte
	// Alias is the peer Broker's alias.
	Alias string
}

// EventKind is the closed set of audit event types.
type EventKind string

// The EventKind constants name each audit event type.
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
	EventConfigRejected     EventKind = "config_rejected"
	EventUpdateApplied      EventKind = "update_applied"
	EventUpdateFailed       EventKind = "update_failed"
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
	// Time is the event time.
	Time time.Time
	// Event is the event type.
	Event EventKind
	// Actor is the initiating agent's address, empty when not an agent.
	Actor string
	// Account is the initiating account, empty when triggered by a peer, a file
	// change or the system.
	Account string
	// Peer is the related Broker, nil when unrelated.
	Peer *PeerRef
	// Key is the configuration key of a change.
	Key string
	// Old is the value before the change, nil when not a change.
	Old *Value
	// New is the value after the change, nil when not a change.
	New *Value
	// Count is the number of merged events, zero when not merged.
	Count uint64
	// Detail is supplementary text such as a failure reason.
	Detail string
	// MergeKey decides only whether two events merge and is not part of the
	// public record; for connection_rejected it takes precedence over
	// Peer.Fingerprint, which the other side chooses.
	MergeKey string
}

// PathSet is the set of files this package uses.
type PathSet struct {
	// RuntimeDir is the runtime directory holding the local socket and the
	// account lock.
	RuntimeDir string
	// RuntimeDirFallback describes why RuntimeDir is the fallback location; nil
	// when RuntimeDir is the preferred one.
	RuntimeDirFallback *RuntimeDirFallback
	// ConfigFile is the configuration file's path.
	ConfigFile string
	// DataDir is the data directory's path.
	DataDir string
	// AuditLog is the audit log's path.
	AuditLog string
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

// The exported errors are sentinels for callers to match with errors.Is.
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
	// below 1024 because the caller is not root or another account can write
	// the configuration file.
	ErrLowPortDenied = errors.New("a listen port below 1024 requires root and a configuration file that only root can write")
	// ErrDataUnavailable reports that the data directory or the audit log
	// cannot be used; a Broker started with --allow-degraded runs diagnostics
	// only instead of exiting.
	ErrDataUnavailable = errors.New("the data directory or audit log is unavailable")
)

// DataUnavailableError is a failed Load whose cause is the data directory or
// the audit log. Every other startup check has passed, so a system Broker can
// still bind its local socket with the loaded group and permissions while it
// runs diagnostics only. The error text names no configuration value.
type DataUnavailableError struct {
	// Paths are the resolved paths of the failed load.
	Paths PathSet
	// LocalSocketGroup is the loaded broker.local_socket_group.
	LocalSocketGroup string
	// LocalSocketMode is the loaded broker.local_socket_mode.
	LocalSocketMode string
	// Err is the underlying cause.
	Err error
}

// Error returns the sentinel text followed by the cause when there is one.
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

// global is the package singleton; tests replace it through installTestCore
// under globalTestMu.
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
// listenOverride replaces broker.listen for this run only. It returns
// ErrAlreadyLoaded when a configuration is already loaded, a
// *DataUnavailableError when the data directory or audit log cannot be used,
// and other errors when the configuration is invalid.
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

// Get returns the effective value of one key, or ErrNotLoaded before Load and
// ErrUnknownKey for a key that does not exist. Secret keys return KindSecret
// with SecretIsSet and no value.
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
// record and queues the notifications. It returns ErrNotLoaded before Load,
// ErrUnknownKey or ErrSecretKey for the key, ErrListenOverridden or
// ErrLowPortDenied for broker.listen, ErrInvalidValue when a value fails its
// own check or two link keys disagree, ErrInvalidCombination when
// broker.listen and broker.advertise or broker.advertise_extra disagree,
// ErrTooManyAddresses when more than eight addresses are advertised, and
// ErrFileWrite when the file cannot be written.
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
		return c.hostnameString()
	}
	return sanitizeHostnameFrom(os.Hostname, defaultLogger())
}

// SubscribeAdvertised registers a callback for changes of the advertised
// address list; it panics before Load or with a nil callback.
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

// Validate registers an extra check for one key. Set and reload call it before
// committing, while the package lock is held, so it must not call this
// package's other exported functions. A check for broker.listen must only probe
// whether the address can be bound, for example with ProbeListen, and must not
// keep the listener it opens; the real bind happens later in the subscriber. It
// panics before Load, with a nil check or for an unknown key.
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

// probeListenNet is net.Listen for ProbeListen; tests replace it to observe the
// listener a probe opens.
var probeListenNet = net.Listen

// ProbeListen reports whether this process can bind addr, which uses the
// broker.listen syntax. It binds and closes a TCP listener, so no listener
// survives the check and the address stays free for the real listener; another
// process can still take it before the rebind. An empty addr means not
// listening and always succeeds.
func ProbeListen(addr string) error {
	if addr == "" {
		return nil
	}
	if _, err := parseListen(addr); err != nil {
		return err
	}
	ln, err := probeListenNet("tcp", addr)
	if err != nil {
		return fmt.Errorf("cannot bind %q: %w", addr, err)
	}
	if err := ln.Close(); err != nil {
		return fmt.Errorf("cannot close the probe listener on %q: %w", addr, err)
	}
	return nil
}

// ListenFromFile returns the effective broker.listen value of the file at path,
// using the default when the key is absent. A missing file is an empty
// configuration; an invalid value is an error. It only reads: it does not load
// the package or create any path.
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

// Subscribe registers a callback for changes of one key; it panics before Load,
// for an unknown key or with a nil callback.
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
// is not the Broker, such as the detached launchd reload helper. An event that
// would merge in a running Broker is written as one record.
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

// ReadAudit returns the records at or after since in time order, including any
// merge window still in memory.
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
