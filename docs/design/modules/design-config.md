# config

`config` stores the [Broker](../../glossary.md)'s configuration and audit log, and provides the location of each kind of file under the two deployment forms. Configuration is a set of named, typed keys with defaults, settable by a user editing the file and readable and modifiable through the local interface; the audit log is an append-only record of events. Other packages read the values and paths they need from here and hand it the events that must leave a trace. It references no other package of this project.

## Responsibilities

- Provide the paths of the runtime directory, configuration file, data directory, audit log and account lock by account and deployment form ([ADR-broker-deployment-modes](../../adr/adr-broker-deployment-modes.md)).
- Load the configuration file at startup, taking defaults for missing keys, refusing to start and naming the key when a key's type or value is invalid; refuse to start and name both items when the combination of listen-address and advertised-address configuration is invalid, refuse to start and name the item when the advertised address list exceeds 8 items, and do not commit such runtime changes (`REQ-broker-config-032`, `REQ-broker-config-036`).
- Take the listen address given by a startup parameter as this run's value without writing it into the configuration file, so that `broker.listen` in the file has no effect in this run; during that time a call through the local interface that modifies `broker.listen` fails (`REQ-broker-config-025`, `REQ-broker-config-029`).
- Derive the default alias from the system hostname and the account running the Broker when no alias is configured (`REQ-broker-pairing-002`).
- Derive this run's advertised address list from the listen address, `broker.advertise` and `broker.advertise_extra`, taking the addresses of the interface that holds the default route when listening on every address, for use by [link](design-link.md), [pairing](design-pairing.md) and [directory](design-directory.md) ([ADR-advertised-address](../../adr/adr-advertised-address.md), `REQ-broker-config-030`, `REQ-broker-config-031`, `REQ-broker-config-033`, `REQ-broker-config-035`).
- Re-enumerate the local addresses every `broker.address_check_interval` and notify subscribers when the advertised address list changes (`REQ-broker-discovery-016`).
- Provide the system hostname, for `pairing` to show the local address in the result of creating a pairing key (`REQ-broker-pairing-028`).
- Store secret configuration keys, which are set only through the configuration file and whose values are not returned by the local interface, so that a read returns no value and neither audit nor logs record the value, and validate the length of a pre-provisioned static key ([ADR-static-pairing-key](../../adr/adr-static-pairing-key.md), `REQ-broker-pairing-029` to `REQ-broker-pairing-031`).
- Provide reading by key and a validated modification: after validation passes, persist, update memory, write an audit record and notify the callers subscribed to that key (`REQ-broker-config-001`, `REQ-broker-config-002`, `REQ-broker-config-006`).
- Watch the configuration file, adopting each legal new value item by item after another program changes the file, keeping the old value for an illegal item, and writing an audit record for each key that adopted a new value (`REQ-broker-config-017`).
- Generate the configuration file template by deployment form, listing every configuration key, for the service installation to write a not-yet-existing configuration file (`REQ-broker-config-040`).
- Append audit records, merge repeated rejection events from the same source, and provide reading in time order (`REQ-broker-config-007` to `REQ-broker-config-009`).

### Non-Responsibilities

- Does not decide a tool's read/write split or read-only marker; [mcpserver](design-mcpserver.md) owns that.
- Does not require user confirmation. Whether a change needs confirmation is decided by Claude Code's permission settings ([ADR-config-through-tools](../../adr/adr-config-through-tools.md)).
- Does not decide whether the caller may read or write the configuration. The configuration file's file permission is the basis of management permission ([ADR-management-by-config-file-permission](../../adr/adr-management-by-config-file-permission.md)); [broker](design-broker.md) makes that decision before calling this package, and this package only provides the configuration file's path.
- Does not bind the listen address; [link](design-link.md) owns that, and `broker` owns handling a bind failure.
- Does not store the peer table or the identity key; [pairing](design-pairing.md) owns those. Does not store the directory; [directory](design-directory.md) owns that.
- Does not read the project configuration file `.peers.yaml`; `mcpserver` owns that.
- Does not know whether a key's value is feasible in another package's view. Such checks are registered by [broker](design-broker.md) as validation functions.
- Does not emit the run log. The run log is written by each package through the standard logging interface, and this package only provides its level setting.

## External Interface Contract

Called by the project's other packages; the signatures are stable. A configuration key's name and meaning are a stable promise to the user, and adding a key does not change the meaning of an existing key.

```go
// Load determines the paths by mode and loads the configuration; a non-empty
// listenOverride replaces broker.listen, valid only for this run, during which
// Set refuses changes to broker.listen and changes to this item in the
// configuration file are not adopted.
func Load(mode Mode, listenOverride string) error
func Paths() PathSet                     // runtime directory, configuration file, data directory, audit log
func AccountLock() string                // the account lock path; does not depend on Load

// Template generates the configuration file template by mode: a key with a fixed
// default is written with its value, a key without one is written as a comment.
func Template(mode Mode) []byte

func Get(key string) (Value, error)
func All() []Entry
func Set(actor Actor, key string, v Value) (old Value, err error)

// Secret returns the value of a secret configuration key, empty when unset; it is
// called only by pairing when looking up a key during the handshake.
func Secret(key string) string

// AdvertisedAddr is one item of the advertised address list. Kind is the address
// kind, deciding how another Broker reaches this Broker through this item; Addr is
// the address spelling for that kind. This version produces only the kind "direct",
// for which Addr is "host:port", an IPv6 host in brackets.
type AdvertisedAddr struct {
    Kind string
    Addr string
}

// Advertised returns this run's advertised address list, at most 8 items; empty
// when not listening. Every item in the list is a direct address item.
func Advertised() []AdvertisedAddr

// ValidDialHost reports whether a host without brackets is an IP address or domain
// name this system can dial. broker.advertise, broker.advertise_extra and a peer's
// self-reported addresses share this rule, so a value the configuration accepts but
// pairing would drop cannot appear, and vice versa.
func ValidDialHost(host string) bool

// Hostname returns the system hostname, with characters outside the alias
// character set replaced by "-".
func Hostname() string

// SubscribeAdvertised registers a change notification for the advertised address
// list; each notification is delivered in the order it occurred.
func SubscribeAdvertised(onChange func(old, new []AdvertisedAddr))

// Actor is the initiator of a change or an event: the system account, and the
// agent address when initiated by an agent.
type Actor struct {
    Account string
    Agent   string
}

// Validate registers an additional check for a key; Set and file reload call it
// before committing, and an error means no commit. The check runs while the
// package's mutex is held, and must not call this package's other exported
// functions; when another package must decide, it may call only that package's
// queries that take only their own lock, such as pairing's AliasConflict and
// ResolveAlias.
func Validate(key string, check func(old, new Value) error)

// Subscribe registers a change notification for a key; each notification is
// delivered in commit order.
func Subscribe(key string, onChange func(old, new Value))

func Audit(e Event)
func ReadAudit(since time.Time) ([]Event, error)

// Close stops the watch and notification goroutines and closes the audit log;
// callable repeatedly, a no-op when not loaded.
func Close()

// ParseValue parses the text in a SetConfig request into a Value by the key's type,
// checking only the type, not the value or the combination.
func ParseValue(key, text string) (Value, error)

// CheckRuntimeDir creates and checks the runtime directory's owner and permission,
// for broker to call before taking the account lock.
func CheckRuntimeDir() error

// ProbeListen probes whether addr can be bound, with addr in broker.listen's
// spelling. It binds and immediately closes, keeping no listener, so after the
// check passes the port can still be bound by the real listener. An empty address
// means not listening, which always succeeds.
func ProbeListen(addr string) error

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

// KeyKind is a configuration key's type, with values defined by the package's key
// table; outside the package it need not be constructed.
type KeyKind uint8

// Value is a typed representation of a configuration value, where only the field
// for its Kind is meaningful. Strings is a copy owned by the caller. A secret
// configuration key's value is kept only inside the package; Get and All return
// KindSecret and SecretIsSet.
type Value struct {
    Kind        ValueKind
    String      string
    Bool        bool
    Int         int64
    Duration    time.Duration
    Strings     []string
    SecretIsSet bool
}

type Entry struct {
    Key   string
    Value Value
}

// Event is one audit record. MergeKey takes part only in merge decisions and is not
// a public field of the audit file.
type Event struct {
    Time     time.Time
    Event    EventKind
    Actor    string
    Account  string
    Peer     *PeerRef
    Key      string
    Old      *Value
    New      *Value
    Count    uint64
    Detail   string
    // MergeKey takes part only in merge decisions and is not a public field of the
    // audit file; connection_rejected takes precedence over Peer.Fingerprint.
    MergeKey string
}

type PathSet struct {
    RuntimeDir string
    ConfigFile string
    DataDir    string
    AuditLog   string
}
```

Semantics of calling an exported function before loading: `Get`, `Set` and `ReadAudit` return `ErrNotLoaded`; `All` and `Advertised` return `nil`; `Secret` returns the empty string; `Paths` returns the zero value; `Validate`, `Subscribe` and `SubscribeAdvertised` are assembly errors and panic directly; `Audit` writes a run log at error level and drops the event; `Close` does nothing. `Load` succeeds only once; a second call returns `ErrAlreadyLoaded`. After `Close` the package returns to the unloaded state and the exported functions behave as above: `Set` returns `ErrNotLoaded`, `Secret` returns the empty string, the rest as when unloaded. Errors are exported as sentinel values (`ErrNotLoaded`, `ErrAlreadyLoaded`, `ErrUnknownKey`, `ErrSecretKey`, `ErrListenOverridden`, `ErrInvalidValue`, `ErrInvalidCombination`, `ErrTooManyAddresses`, `ErrFileWrite`), which callers tell apart with `errors.Is`. When the audit log is unavailable, `Load` wraps the error with a distinguishable sentinel and exports that sentinel; when the data directory is unavailable, `broker`'s assembler wraps the error with the same sentinel while opening the identity, peer table and directory. `broker` uses `errors.Is` against this sentinel to decide whether to enter diagnostics only; the rest of `Load`'s failures carry no such marker.

The low-port check at startup does not belong to `Load`. After `Load` succeeds and before the listen address is bound, `broker` calls a check function this package exports; it returns an error when `broker.listen`'s port is below 1024 while the configuration file is writable by the group or others, treated as an unavailable listen address.

The table below is the complete set of configuration keys:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `broker.alias` | string | see "Default alias" | The alias this Broker reports, 1 to 24 characters drawn from letters, digits, `.`, `-`, `_`; a configuration longer than 24 characters or containing other characters is invalid (`REQ-broker-pairing-002`, `REQ-broker-pairing-003`, `REQ-broker-pairing-037`) |
| `broker.public` | bool | `false` | Whether public (`REQ-broker-discovery-001`) |
| `broker.listen` | string | `:17443` | The address and port actually bound; an empty host part means every local address, an explicitly configured empty value means not listening ([ADR-listen-by-default](../../adr/adr-listen-by-default.md), `REQ-broker-config-028`) |
| `broker.advertise` | string | empty | The advertised address configuration, with only the legal spellings empty, `host`, `host:port`, `:port`; see "Advertised address" ([ADR-advertised-address](../../adr/adr-advertised-address.md), `REQ-broker-config-030` to `REQ-broker-config-032`) |
| `broker.advertise_extra` | string list | empty | Additional advertised addresses, each `host` or `host:port` (`REQ-broker-config-033`) |
| `broker.address_check_interval` | duration | `1m` | The interval for re-enumerating the local addresses (`REQ-broker-discovery-016`) |
| `broker.local_socket_group` | string | empty | The group of the system Broker's local socket; empty inherits the runtime directory's group, ignored for a user Broker; the group must exist in the account database (on Linux queried via getent under the NSS configuration, see [ADR-account-database-source](../../adr/adr-account-database-source.md)), and a runtime change takes effect immediately (`REQ-broker-config-027`) |
| `broker.local_socket_mode` | string | `0666` | The file permission of the system Broker's local socket; fixed at `0600` for a user Broker (`REQ-broker-config-027`) |
| `broker.offline` | bool | `false` | Whether offline; when true, refuses messages from peers while links and the directory work as usual, and a runtime change takes effect immediately (`REQ-broker-config-065`, `REQ-agent-messaging-039`) |
| `pairing.static_key` | string, secret | empty | The pre-provisioned static key, 16 to 128 characters, in effect once it has a value ([ADR-static-pairing-key](../../adr/adr-static-pairing-key.md), `REQ-broker-pairing-008`, `REQ-broker-pairing-009`, `REQ-broker-pairing-029`) |
| `pairing.max_failures` | integer | `5` | The number of verification failures allowed before a one-time pairing key is invalidated |
| `pairing.max_attempts_per_minute` | integer | `10` | The pairing requests per source per minute allowed that match no pairing key; the cap across all sources combined is 10 times this (a fixed multiple); successful requests that match the pre-provisioned static key are not counted |
| `topics.auto_join` | bool | `true` | Whether to join the default topic automatically (`REQ-topics-020`) |
| `directory.reconcile_interval` | duration | `10m` | The interval of periodic reconciliation |
| `directory.max_records` | integer | `1000` | The cap on total directory records (`REQ-broker-discovery-011`) |
| `directory.max_records_per_link` | integer | `250` | The cap on records accepted over one link (`REQ-broker-discovery-015`) |
| `directory.max_topics_per_publisher` | integer | `16` | The combined cap on one publisher's public topic declarations and unknown-kind records (cards and revocations not counted) |
| `send.link_retries` | integer | `5` | The retry count when the link is not established (`REQ-agent-messaging-017`) |
| `send.link_backoff` | duration | `500ms` | The wait before the first retry, doubling after each, at most 8 seconds per wait |
| `send.request_timeout` | duration | `20s` | The budget of one send, that is the total time limit of the send call (`REQ-agent-messaging-017`, `REQ-agent-messaging-018`); a `send_message` issued from a Claude Code session is also bound by the tool's own fixed 30-second cap, and when this value is above 30 seconds the excess does not apply to calls issued from a session, see "Tool Call Timeouts" in [mcpserver](design-mcpserver.md) |
| `send.inbox_retries` | integer | `2` | The retry count when the inbox is known not written (`REQ-agent-messaging-018`) |
| `send.inbox_backoff` | duration | `200ms` | The wait between inbox write retries |
| `send.inbox_timeout` | duration | `5s` | The deadline of one inbox write |
| `send.dedupe_window` | duration | `5m` | The retention time of a deduplication table entry |
| `link.dial_timeout` | duration | `5s` | The connection deadline for each address while dialing; a `connect_broker` issued from a Claude Code session is also bound by the tool's own fixed 30-second cap, and when this value is above that cap the excess does not apply to calls issued from a session, while the terminal command `peers broker connect` has no cap of its own and this value applies in full, see "Tool Call Timeouts" in [mcpserver](design-mcpserver.md) |
| `link.reconnect_min` | duration | `1s` | The initial backoff interval for re-establishing the link |
| `link.reconnect_max` | duration | `60s` | The maximum backoff interval for re-establishing the link |
| `link.keepalive_interval` | duration | `15s` | The keepalive probe interval; the gRPC client's effective floor is 10 seconds, so a value below 10 seconds probes at 10 seconds |
| `link.keepalive_timeout` | duration | `10s` | The reply deadline of a keepalive probe |
| `log.level` | string | `info` | The run log level |
| `update.check` | bool | `true` | Whether to periodically check GitHub for a newer release; once off, the Broker sends no check request (`REQ-update-001`) |
| `update.check_interval` | duration | `24h` | The interval of the periodic check, with a floor of `1h`, a value below the floor being invalid (`REQ-update-001`) |

The default value of `broker.listen` is only one value in the default configuration; the code treats this port specially in no way. The validity period of a one-time pairing key, and the validity period and renewal interval of a directory record are fixed by the requirements (`REQ-broker-pairing-006`, `REQ-broker-discovery-007`, `REQ-broker-discovery-008`) and are not configuration keys; the 1 000 000-byte cap on a message encoded into an inbox frame is likewise fixed (`REQ-agent-messaging-026`), and the 10-minute retention of an agent's previous name is a constant, both covered in [broker](design-broker.md). The other defaults in the table are initial values chosen by the implementation.

Two cross-key constraints are checked by this package itself: `link.reconnect_max` is not less than `link.reconnect_min`, and `link.keepalive_timeout` is less than `link.keepalive_interval`. They are pure value rules; both `Set` and file reload check a group of keys together against candidate values: when the combination is legal the group adopts the new values together, and when illegal every key in the group keeps its old value. Comparing key by key against a not-yet-updated same-group key would make the result depend on key-name lexicographic order; for example, changing legal `15s`, `10s` to `5s`, `3s` would adopt only one of them, yielding a combination the user never wrote, so the decision is made by group. Constraints that another package must decide (whether the listen address can be bound, whether the alias conflicts with a peer) are still done by the validation functions `broker` registers.

### Values in SetConfig

The `value` in a `SetConfig` request is a string carrying the text the model gave verbatim, not pre-encoded by the key's type. The Broker hands it to `ParseValue` to parse by the key's type: a string, bool, integer or duration key's value is a YAML scalar, such as `10m` or `true`; a string-list key's value is a YAML flow sequence, such as `[a, b]`. `ParseValue` requires the input to be a single YAML document with one root node whose tag matches the key's type; an unknown key returns `ErrUnknownKey`, and an extra document, a type mismatch or a list that is not a flow sequence returns `ErrInvalidValue`, with the error naming the key and the reason. A secret configuration key is parsed as a string scalar and then refused by `Set` with `ErrSecretKey`. Parsing checks only the type; alias length, port range, the 8-item cap and the combination rules are decided by `Set`. The parse result is turned into a `Value` and handed to `Set`; when a string cannot be parsed by the key's type, the call fails with `invalid_argument` and changes no state.

The `GetConfig` reply, the `SetConfig` reply and the values in audit records are still represented by `ConfigValue`. `Set` receives a parsed `Value`, not a string.

## Dependencies

`config` references no other package of this project, matching the module dependency graph's direction.

## Core Data Structures

The location of each kind of file is decided by the deployment form:

| Content | User | System |
|---|---|---|
| Configuration file | `~/.peers/config.yaml` | `/etc/peers/config.yaml` |
| Data directory: identity, peer table, directory, name ownership | `~/.peers/` | `/var/lib/peers/` |
| Audit log | `~/.peers/audit.log` | `/var/log/peers/audit.log` |

The runtime directory holds the local socket `broker.sock` and the account lock `broker.lock`. The runtime directory depends only on the process's account and the filesystem's state, not on the form, environment variables, configuration content or listen address: root's is `/run/peers/` on Linux and `/var/run/peers/` on macOS, so a system Broker and root's user Broker contend for the same lock; other accounts' preferred location is `/tmp/peers-<uid>/`, where `<uid>` is the process's account. When the preferred location already exists and is a symbolic link, is not a directory, or is owned by another account, it is treated as occupied by another account and the fallback location is used: on Linux `/run/user/<uid>/peers/`, on macOS `peers/` under the per-user temporary directory given by `getconf`'s `DARWIN_USER_TEMP_DIR`. The fallback location's parent directory must be owned by this account, not a symbolic link, and give the group and others no permission at all; when the preferred location is owned by this account but its permission is too broad it does not count as occupied and is still refused under the permission rule, because the account can itself correct it. When neither is available, startup is refused, with the error writing the occupier and reason of the preferred location and the exit code following the value used for a runtime directory failure. This rule depends only on the filesystem's state and the process's account and reads no environment variable; the Broker, the MCP server, the terminal commands, `service install`'s precheck and `service remove` use the same set of rules. An occupier can delete and recreate the preferred location, so different processes of the same account may compute different locations at different times; for this reason the Broker checks the account lock at both the preferred and fallback locations at startup and refuses to start if either is held by a running instance of this account, and the MCP server and terminal commands probe the local socket at the same two locations, adopting only the one that passes the process-credentials check, adopting the one whose account lock is held when both pass, and adopting the one chosen by the location rule when neither is held (`REQ-broker-config-013`). Root's runtime directory permission is `0755`; the system Broker's local socket has its group and permission set by `broker.local_socket_group` and `broker.local_socket_mode` after binding, which decides which accounts may connect; a user Broker's socket is fixed at `0600`; another account's runtime directory permission is `0700`. The runtime directory's owner must be this account, and startup is refused otherwise. The above location rules hold for release builds. The test build with the `peers_testenv` build tag is an exception: when `PEERS_TEST_RUNTIME_ROOT` is non-empty the runtime directory root takes its value, and when `PEERS_TEST_HOME` is non-empty the account home directory takes its value; when both are empty it has no effect, and release builds do not contain this code.

A user Broker's data directory permission is `0700`, and the files in it `0600`. The system configuration file is created by `service install --system` when absent, owned by root, grouped to the management group and permissioned `0660`, with content `Template(ModeSystem)`, and an existing file is left unchanged by installation; the management group defaults to `peers` and may be set with `--admin-group` ([ADR-admin-group](../../adr/adr-admin-group.md), `REQ-broker-config-040`, `REQ-broker-config-043`). The user configuration file is created by `service install` when absent, owned by this account and permissioned `0600`, with content `Template(ModeUser)`, and an existing file is left unchanged by installation. An administrator granting a group read or write permission on it delegates reading the configuration or management operations to that group's accounts. When the configuration file is absent and `Set` creates it, the system form also uses `0600` file permission: a system Broker does not know the management group and cannot copy the installation's group and `0660` to add the group.

The configuration file's structure corresponds one-to-one with key names, for example `broker.alias` corresponds to `alias` under the top-level `broker`. A key absent from the file takes its default, and a missing file is equivalent to an empty file. `Set` changes only the one key it modifies, keeping the rest of the file unchanged; when the file is absent it is created under the account running the Broker with file permission `0600`, creating the containing directory when absent too, the user directory `0700` and the system directory `0755`.

Default alias: when `broker.alias` is not configured, the system Broker takes the first segment of the host's system hostname (the part before the first `.`), and the user Broker takes the string formed in order by the hostname's first segment, `.` and the owning account name. The hostname comes from `os.Hostname()` preserving its case; the account name is the account running the Broker, looked up through the account database, the same source as the audit `account` field; when the account name cannot be found or is empty, the account-name part uses `os.Geteuid()`'s decimal uid, for example `hostA.1000`, unique on the host and the same every time, and at the same time writes a run log at Warn level saying the account name was not found and the uid is used instead. The alias is independent of the listen address, listen port and advertised address. Characters in the hostname and account name outside the alias character set are replaced by `-`. When the hostname cannot be obtained or is empty, the hostname part is `broker`. The total length does not exceed 24 characters (`REQ-broker-pairing-037`): when too long, first shorten the hostname part, keeping at least 1 character; if still too long, shorten the account-name part, and when the account-name part uses the uid, shorten the uid. After shortening, drop the trailing `.` and `-`, and when the result is empty use `broker`. When `broker.alias` is configured, its value is used and this section's rule does not apply.

Listen address: when `broker.listen`'s host part is empty, listen on every local address, including IPv6 addresses when IPv6 is enabled; when a host part is written, bind that address literally, where `0.0.0.0` and `::` are treated as every address when computing the advertised addresses. The port must not be 0.

When the system Broker's `broker.listen` port is below 1024, it takes effect only when the configuration file is writable by root alone: the group and others have no write permission and the access control list grants no additional write. When a call through the local interface sets `broker.listen` to a low port and the caller is not root, `broker` returns `permission_denied`, saying a low port requires root to modify and the configuration file must be writable only by root. When the configuration file is writable by the group or others, hot reload does not adopt a new low port and keeps the old value, recording in the run log at Warn level and appending a `config_rejected` audit record. At startup this rule does not change `Load`'s result but is checked by `broker` before binding the listen address: a foreground run exits with code 4, and a service with `--allow-degraded` enters local-only degraded run. The rejection message has two forms. `SetConfig`'s rejection says a low port requires root and a configuration file writable only by root, the configuration file is now writable by other accounts, and the fix is `chmod g-w <configuration file>` (and removing an access control list entry that grants write), or using a port of 1024 or above. Startup and hot reload share one message: it writes the current value of `broker.listen` in the file, says this value may have been written by another account able to write the file, that the value should be checked for what is wanted before tightening permission, and gives the same fix of `chmod g-w` or using a port of 1024 or above. A user Broker is not affected by this rule ([ADR-admin-group](../../adr/adr-admin-group.md), `REQ-broker-config-043`, `REQ-broker-config-058`).

Advertised address: this run's advertised address list is derived from the effective listen address and `broker.advertise` by the table below, with each `broker.advertise_extra` item appended after them ([ADR-advertised-address](../../adr/adr-advertised-address.md), `REQ-broker-config-030`, `REQ-broker-config-031`, `REQ-broker-config-033`).

| Listen address | `broker.advertise` | The part of the list derived from this |
|---|---|---|
| Not empty | `host:port` | Only this item |
| Not empty | `host` | That host paired with the listen port |
| A specific address | empty | The listen address paired with the listen port (`REQ-broker-config-035`) |
| Every address | empty | The addresses of the interface that holds the default route, each paired with the listen port |
| Every address | `:port` | The addresses of the interface that holds the default route, each paired with that port |
| Empty, not listening | empty | Empty |

A combination outside the table is invalid, for example listening on a specific address while `broker.advertise` gives only a port, or not listening while `broker.advertise` is non-empty; when not listening, `broker.advertise_extra` must also be empty (`REQ-broker-config-032`). The combination is judged by this run's effective listen address, and when the listen address comes from a startup parameter it is authoritative. An item of `broker.advertise_extra` without a port takes the port used by the advertised addresses: when `broker.advertise` writes a port it takes that, otherwise the listen port. A host is an IP address or domain name, an IPv6 address written in brackets. A domain name is split by `.` into labels, each 1 to 63 ASCII letters, digits, `-` or `_`, not starting or ending with `-`, the whole name at most 253 bytes; a fully qualified name may carry one trailing `.`, which is dropped during validation, and two trailing dots leave an empty label, rejected as invalid. A host that does not satisfy this rule is rejected when loading the configuration and does not wait to fail at use. Duplicate items in the list keep only their first occurrence.

Each item of the advertised address list has a kind, deciding how another Broker reaches this Broker through this item. The Go interface represents an item as `AdvertisedAddr`, `Kind` being the kind and `Addr` the address spelling under that kind. This version produces only the direct kind, valued `direct`, whose address is written `host:port` and means a connection can be established directly to that address, an IPv6 host in brackets; the list this package derives contains only direct items. The configuration text has no syntax for expressing a kind: the legal spellings of `broker.advertise` and `broker.advertise_extra` are only the forms listed in the table above, any other spelling being invalid, so this version's configuration text has no expressible unknown kind. The wire representation of a kind belongs to the implementation. How to handle an unknown kind in a list another Broker tells or a card carries is covered in [link](design-link.md), [pairing](design-pairing.md) and [directory](design-directory.md).

The addresses of the interface that holds the default route: the IPv4 address on the interface that holds the default IPv4 route with the smallest metric, excluding link-local addresses in `169.254.0.0/16`; when a default IPv6 route exists, additionally the global unicast IPv6 address on the interface of the route with the smallest metric, excluding link-local and temporary addresses. A default IPv4 route is judged by both destination and mask being zero and the route being enabled, not requiring the gateway flag, so a route that only names a device (for example `default dev wg0`) also counts. When there is no default route this part is empty. Addresses on other networks the host is attached to are not included. Failing to enumerate the default routes at startup does not refuse startup: this part of the addresses is empty, a run log at error level records it, and the periodic enumeration fills it in on a later round.

The list has at most 8 items. A configuration where the one item containing a host in `broker.advertise` plus the number of `broker.advertise_extra` items exceeds 8 is invalid (`REQ-broker-config-036`); when the addresses of the interface that holds the default route push the list past 8 items, the later addresses in that part are dropped, and every `broker.advertise_extra` item is kept.

This package re-enumerates the local addresses every `broker.address_check_interval`, recomputes the list after `broker.listen`, `broker.advertise` or `broker.advertise_extra` change, and notifies `SubscribeAdvertised`'s subscribers when the list differs from before (`REQ-broker-discovery-016`). The local address a peer sees on the connection does not take part in the computation.

The list in a card and the display when creating a pairing key are covered in [directory](design-directory.md) and [pairing](design-pairing.md) respectively.

Secret configuration keys: `pairing.static_key` is one, and this class of key follows the rules below ([ADR-static-pairing-key](../../adr/adr-static-pairing-key.md)):

- It can be set only by editing the configuration file. `Set` refuses every modification of it with `invalid_argument`, saying the item can be set only in the configuration file (`REQ-broker-pairing-031`).
- `Get` and `All` give only whether it is set and return no value; the value reaches `pairing` only through `Secret` (`REQ-broker-pairing-030`).
- Reload after the configuration file is changed applies to it as usual; setting, replacing or deleting need no restart (`REQ-broker-config-017`, `REQ-broker-pairing-032`, `REQ-broker-pairing-033`). Its `config_file_changed` audit record notes only that this item changed, carrying no `old` or `new`; no run log, audit log or error message shows its value (`REQ-broker-config-008`, `REQ-broker-pairing-030`).
- The pre-provisioned static key's length must be between 16 and 128 characters: an invalid value at startup refuses startup and names the key, and an invalid value at reload keeps this item's old value (`REQ-broker-pairing-029`). The value is stored as given, with no case or format normalization.
- The value must be written as a YAML string, a value written as a number or another type being invalid; when it contains characters that YAML would otherwise interpret, such as ` #`, it must be quoted.

The audit log has one JSON object per line:

| Field | Meaning |
|---|---|
| `time` | The event time, in RFC 3339 format |
| `event` | The event type, whose values are in the table below |
| `actor` | The initiating agent address; empty when not initiated by an agent, or when triggered by a peer, a file change or the system |
| `account` | The initiating system account; empty when triggered by a peer, a file change or the system (`REQ-broker-config-007`) |
| `peer` | The related Broker's fingerprint and alias; empty when unrelated to a Broker |
| `key`, `old`, `new` | The key of a configuration change and its values before and after; a secret configuration key has only `key` |
| `count` | The number of merged events; absent when not merged |
| `detail` | Supplementary information such as a failure reason |

The event types are a closed set:

| Event | Trigger |
|---|---|
| `config_set` | A configuration change completed through the local interface |
| `config_file_changed` | A key adopted a new value after the configuration file was changed by another program |
| `pairing_key_created` | A one-time pairing key was created |
| `pairing_succeeded` | Pairing succeeded, with one record each for the initiator and the acceptor |
| `pairing_failed` | Pairing failed, recording the reason |
| `link_up` | The link was established |
| `link_down` | The link went down |
| `unpaired` | The pairing was removed |
| `connection_rejected` | A connection from an unknown fingerprint was rejected |
| `rejected_by_peer` | This side's connection was rejected by the peer as "not a peer" |
| `local_alias_assigned` | The system generated a local alias for a peer |
| `config_rejected` | A configuration file reload rejected a value that would give the management group more than the ability to edit the file |
| `update_applied` | The Broker completed a self-update, recording the caller's account and the old and new versions |
| `update_failed` | The Broker's self-update failed, recording the reason; on the second phase failing, Detail begins with `second phase:` |

A `config_file_changed` or `config_rejected` triggered by directly editing the configuration file has no account attribution, with `account` and `actor` empty; a change through the local interface records the caller's account. The audit log does not record a pairing key's value, message bodies or the sending and receiving of messages (`REQ-broker-config-008`). A secret configuration key's change records only the key name, see "Secret configuration keys".

## Core Algorithms and Flows

### Generating the Configuration File Template

`Template(mode Mode) []byte` generates by deployment form a configuration file template listing every configuration key, for the service installation to write a not-yet-existing configuration file (`REQ-broker-config-040`).

The template is generated from the package's key table, which is its only source; no hand-written template constant exists. Each key in the key table carries one English line of description matching the key's "Meaning" column in the configuration key table.

Sections are ordered fixed: `broker`, `pairing`, `topics`, `directory`, `send`, `link`, `log`, with keys sorted by name within a section. The file begins with an explanatory comment: the system form reuses the existing paragraph about file permission, and the user form writes a separate paragraph saying the file is for this account alone.

A key with a fixed default is written in two lines: the line above is `# <description>` and the line below is `<leaf>: <default>`. The value is written through the YAML encoder, or quoted by an equivalent rule, so that every effective value of `Load(Template(mode))` equals that key's default, for example `broker.listen`'s `:17443` must be quoted. The empty-default `broker.advertise`, `broker.advertise_extra`, `broker.local_socket_group` and `pairing.static_key`, and `broker.alias` whose default is computed at runtime, are written as comments in the form `# <leaf>: <example>`, with the next line saying "no fixed default" or "the default is computed at runtime". The example must pass the key's validation. The template does not write an empty string as an effective value: an empty string means something else on some keys, for example `broker.listen`'s empty string means not listening, differing from the default.

The user template likewise lists `broker.local_socket_group` and `broker.local_socket_mode`, noting that these two take effect for the system form only.

### Changing a Key

`Set` does the following in order, doing none of the rest if one fails:

- First adopt, by the same set of rules as "Watching the Configuration File", any external change on disk not yet adopted, and write a `config_file_changed` audit record for each key that adopted a new value. Validation therefore starts from the current value on disk, and an external change saved during the debounce period is not swallowed by this write.
- Check that the key exists and is not a secret configuration key, that the value's type and range are legal and its combination with other keys is legal, then call the validation function registered for that key. When changing `broker.listen`, `broker.advertise` or `broker.advertise_extra`, check the new value together with the other two's current values against the combination rule and item cap in "Advertised address".
- Read the configuration file's current content, replace only this key, and rewrite it in place: truncate then write the whole content and sync to disk. The write-back rewrites the whole file, keeping the rest of the file unchanged, so the original file's owner, group, permission bits and access control list stay the same. When the file is absent it is created as this account with file permission `0600`; when the containing directory is absent it is created too, the user directory `0700` and the system directory `0755`. When the key is absent from the file, it is written by the placeholder-line rule in the next paragraph. The write path opens the configuration file with `O_NOFOLLOW`, so when it is a symbolic link the write fails and the original file is unchanged; this rule relies on the configuration directory `/etc/peers` being owned by root with the group not writable ([ADR-admin-group](../../adr/adr-admin-group.md), `REQ-broker-config-043`).
- Update the value in memory.
- Append a `config_set` audit record.
- Put this change into the notification queue.

When the key is absent from the file, `Set` first finds, within the section the key belongs to, a placeholder line matching `^\s*#\s*<leaf>:`. At the first match it replaces that line with the effective `<leaf>: <new value>`, keeping the position and indentation and the explanatory comment above it; when none is found, it appends to the section's end or the file's end as before. Apart from this replacement, the rest of the file is untouched (`REQ-broker-config-040`).

The read and the write-back are not atomic: content written by another program in this window is overwritten and lost by this write-back, a cost accepted for the in-place rewrite. When the configuration file on disk is empty or absent, the write-back contains only the modified key and the file's original comments; other keys remain only in memory, in effect from memory for this run and back to their defaults after a restart.

When writing the file fails it returns `internal`, with memory and audit unchanged: this package does its best to write the original bytes back; regardless of whether the write-back succeeds, it changes the file hash recorded in memory to the hash of the current content on disk, so this process does not adopt the empty file or truncated content it left as an external change.

The notification queue is drained by one goroutine calling subscribers in commit order, so two modifications of the same key take effect in their modification order. Validation functions serve cases another package must decide and are registered by `broker`: a new value of `broker.listen` only does a bind probe, the validation function binding and immediately closing, keeping no listener, and after the change commits a subscriber binds for real; a failed probe means the change does not commit and the existing listener is unaffected (`REQ-broker-config-034`). When the new value equals the current value it does not probe and accepts the change directly: the running listener still holds the address, so a probe would necessarily fail. Between the probe and the real bind there is a time gap, during which the port may be taken by another process. The validation function runs while the package's mutex is held and must not call this package's other exported functions. When `broker.alias`'s new value equals an effective alias of a committed peer or an alias reserved by a handshake in progress, the change does not commit; the validation function calls `pairing`'s `AliasConflict`, which takes only `pairing`'s lock. When this run's listen address comes from a startup parameter, `Set`'s modification of `broker.listen` is refused with `invalid_argument` before validation, the error saying this run's listen address is given by a startup parameter and neither the configuration file's nor the memory value changes (`REQ-broker-config-029`).

Subscribers make a change take effect immediately, and subscription is handed to each package after `broker`'s assembly completes: a change to `broker.alias` makes each link receive a `PeerInfo` frame and makes `directory` reissue its card, and at the same time calls `pairing`'s `SetOwnAlias` to push the new local alias, with `broker` also pushing the current value once during assembly; a change to `broker.listen` makes `link` listen on the new address; a change to the advertised address list, whether from modifying `broker.listen`, `broker.advertise` or `broker.advertise_extra` or from the periodic re-enumeration, is delivered through `SubscribeAdvertised`, making each link on which this side is the [acceptor](../../glossary.md) receive a `PeerInfo` frame and making `directory` reissue its card; a change to `broker.public` makes `directory` publish or withdraw the local records, and on becoming public `broker` separately redeclares by each agent's public topic set. `pairing.static_key` has no subscriber, and `pairing` reads the new value at the next handshake's key lookup; before `Secret` returns the value it reads the configuration file and compares the content hash, reloading when the content differs, so a delete or replacement still takes effect when a file-change notification is lost. Each interval, count and cap takes its new value at the next use.

The take-effect timing of `link.keepalive_interval` and `link.keepalive_timeout` differs from the other intervals: [link](design-link.md) reads them on the acceptor side when first listening and creating the gRPC server, so a modification takes effect only after the acceptor side restarts; the dialer side applies them to the next outgoing connection. Changing only the host part of `broker.listen` (for example from `:17443` to `127.0.0.1:17443`) is refused: when validation probes binding with the new value, `link`'s old listener still holds the same port, so the probe fails; changing this item requires changing the port at the same time, or editing the configuration file and restarting the Broker, and on a failed change it keeps listening on the original address (`REQ-broker-config-034`).

### Watching the Configuration File

This package watches the configuration file, and reload is driven only by filesystem notifications ([REF-fsnotify](../../references.md)), with no periodic rescan. When the file content changes it reparses and compares key by key with the value in memory:

- Parse fails or an empty file is read: no change is adopted, the value in memory is unchanged, and a run log at error level records the reason. An empty file includes the window between truncation and write when another program rewrites in place; only the startup load treats an empty file as an empty configuration. When the truncation falls exactly on a line boundary, the remaining piece may still be a legal YAML document, and reload adopts it as an external change that deleted the other keys, so keys that no longer appear take their defaults; this is a cost accepted for judging changes by file content alone.
- Parse succeeds: decide key by key. A key whose value is legal and passes its validation function adopts the new value; a key that is illegal or fails its validation function keeps its old value, recorded at error level. Keys of a paired constraint are decided as a group, adopted together when the combination is legal and all kept otherwise, with the result independent of key-name lexicographic order. `broker.listen`, `broker.advertise` and `broker.advertise_extra` are decided as another group: check the combination rule and item cap against the whole after the change, adopting them together when legal and keeping all three otherwise. When a new value of `broker.listen` has a port below 1024 while the configuration file is writable by the group or others, the new value is not adopted and the old value is kept, recording in the run log at Warn level and appending a `config_rejected` audit record (`REQ-broker-config-043`).
- For each key that adopted a new value, update memory, append a `config_file_changed` audit record and put it into the notification queue.

This package does not handle file changes while it writes the file itself; after the write it records the file content's hash as adopted, producing no record. When a file-change notification is lost, `Secret` compares the file content hash before returning the value as a backstop, so the absence of a periodic rescan does not let an old key stay in effect forever.

When this run's listen address comes from a startup parameter, `broker.listen` does not take part in comparison or validation: its value changing in the file is not adopted, writes no audit record, and is recorded once in the run log; other keys reload as usual (`REQ-broker-config-017`).

When `Load` fails, this package does not start watching. A Broker in degraded run watches the configuration file's directory itself, re-runs the startup steps when the file changes, and after `Load` succeeds this package watches as usual ("Degraded Run" of [broker](design-broker.md)).

### Audit Log Representation and Merging

One JSON object per line, UTF-8. `time` is a UTC RFC 3339 nanosecond time; `event` is the event type's string value; `actor`, `account`, `key` and `detail` are omitted when empty. `peer` is `{"fingerprint":"<64 lowercase hex digits>","alias":"..."}`. `old` and `new` are typed objects `{"kind":"...","value":...}`, where `kind` is `string`, `bool`, `int`, `duration` or `string_list`: a duration's `value` is a string such as `10m`, an integer's `value` is a JSON number. The audit file is config's private format; `GetConfig`'s and `SetConfig`'s replies use `ConfigValue`, corresponding to the types here, with a secret configuration key represented separately by whether it is set. A secret configuration key's change keeps only `key` and writes no `old` or `new`. When opening the audit file, if the end is not a newline, append one first; a half line left by a crash still cannot be parsed but does not swallow the next record.

`connection_rejected` and a `pairing_failed` caused by too-frequent attempts or an incompatible protocol version are merged in memory before being written. The merge key is made of the event type, the source identifier and the reason: `connection_rejected` uses the source the caller gave, that is an IPv4 address or an IPv6 /64 prefix, using the peer fingerprint when there is no source; this kind of `pairing_failed` uses the merge key the caller gave and the reason in `detail`, and `pairing` fills in the source prefix when the attempt rate is exceeded or the protocol version is incompatible (a /32 for IPv4, a /64 for IPv6), written `source:<prefix>`, with `detail` giving only the reason. Events of the same source and same reason merge into one, and different reasons of the same source merge separately, each keeping its own reason, so one record's `detail` never stands for another class of failure. On one merge key, the first event opens a 10-minute window counted from that event's time, and later events do not slide the window. Later events within the window are accumulated in memory only and not written. When the window expires or `Close` runs, the window is summarized into one record appended to the file, with the record's time being the window's first event time: a window with only one event carries no `count`, and with more than one `count` is the number of events. Ordinary windows in memory number at most 256, counting all mergeable events together. An event for an existing window merges as usual; when the merge key is not in the table and ordinary windows are full, the event merges into the summary window of its event kind. Each mergeable event kind has at most one summary window, not counted in the 256. The summary window starts from its first event and lasts the same 10 minutes, and on expiry writes one record whose time and event type are the first event's, `peer` empty, `detail` `other sources`, and `count` the number of merged events. When a summary window opens it writes a run log at Warn level `config_audit_merge_overflow`, with field `audit_event` the event type and `limit` 256. `ReadAudit` returns the records in the file plus the window records still in memory, sorted by time. Expiry scheduling sorts by deadline and merge key, and windows with the same deadline are flushed in merge-key order; appending an event and scheduling a timer do not fully sort by the count of pending windows. Expiry is driven by one timer, always set at the earliest deadline. The timer fires on the monotonic clock and may fire slightly before the wall-clock deadline; when a firing finds no window expired, the implementation re-arms a timer for the remaining time, and the window is still flushed after its deadline. Flushing therefore does not depend on later events: as long as a mergeable window remains, one timer is set at the earliest deadline. The audit file is always append-only and never rewrites an existing line. The merge key lives only in memory and is not written to the file. When the process crashes before a window is flushed, that window's records are lost, including a window with only one event; this is a cost accepted for keeping the file append-only. Audit events produced during and after `Close` must not be silently dropped: when the audit log is closed or not yet opened, the event is written to the run log at error level at least.

`link_up` and `link_down` are not merged: the peer is already authenticated and falls within the trusted scope ("Authentication and Authorization Boundaries" of [architecture](../architecture.md)).

### Implementation Conventions

The conventions below are details the design does not fix and the implementation decides:

- `All` returns in key-name lexicographic order; the lists and string slices returned by `Get`, `All` and `Set` are copies.
- The keys of `Validate` and `Subscribe` must be in the key table and `Load` must have completed; an unknown key or a not-loaded state is an assembly error and panics directly.
- A second call to `Load` returns `ErrAlreadyLoaded`.
- The value set of `log.level` is `debug`, `info`, `warn`, `error`, and an unrecognized value is invalid.
- `broker.advertise` and `broker.advertise_extra` refuse `0.0.0.0` and `::` as hosts, since another Broker cannot connect to a wildcard address.
- `broker.local_socket_mode` accepts an unquoted integer spelling (such as `0666`) and validates the value as octal; other string keys written as numbers are still invalid.
- A same-value `Set` writes the file, writes audit and sends a notification as usual, because it is a change completed through the local interface.
- When the current configuration file cannot be parsed structurally, `Set` fails and does not overwrite the file; one key's invalid value does not block modification of other keys.
- A key repeated in any section (including unknown sections and keys), a top-level known section that is not a mapping, and the YAML merge key `<<` are all treated as parse errors; the rest of the unknown sections and keys are kept in the file, take no part in loading and are not modified by `Set`.
- Anchors and aliases in the configuration file (`&name`, `*name`) are treated as parse errors, and nesting of mappings and sequences beyond 32 levels likewise; the check only walks the node tree and does not follow alias pointers, so a self-referential alias does not form a cycle and an alias that would grow exponentially when expanded is not expanded. Each read of the configuration file reads at most 1 MiB plus 1 byte, and exceeding 1 MiB is treated as a read error.
- When listening on a specific address without `broker.advertise` configured, the implicit listen-address item counts toward the 8-item cap; when the addresses from the default route exceed the cap, the later items in that part are dropped, and every `broker.advertise_extra` item is kept.
- The grouping basis of audit merging includes the event type, so two different event types are not merged just because their strings are equal.
- Summary windows are separated by event kind, each mergeable event kind having at most one.
- The watch debounce duration and the address-enumeration periodic ticker are injected through package fields; tests drive reload and periodic enumeration with a fake ticker and do not depend on real waits.
- Each platform's `defaultRouteAddresses` has a smoke test that calls it directly and asserts only `err == nil`; the macOS IPv4 and IPv6 route fixtures come from real `route -n get default` output collected on a macOS runner in CI run 44329785438: IPv4 is a normal route description with `gateway: 192.168.64.1` and `interface: en0`; IPv6 has no default route and its output is `route: writing to routing socket: not in table` with exit code zero; there is also a test for the `ifconfig` failure branch.

## Error and Failure Semantics

| Situation | Behaviour |
|---|---|
| At startup the configuration file cannot be parsed or contains an invalid value | Refuse to start, also refusing with `--allow-degraded`; when unparsable the error gives a line and column, or names the offending section or key, or states the reason, without quoting a value from the file; when it contains an invalid value it names the key and the reason, without a secret configuration key's value |
| The configuration file exceeds 1 MiB | Reading stops at 1 MiB plus 1 byte, exceeding it being treated as a read error: at startup refuse to start, also refusing with `--allow-degraded`; at runtime keep every old value and record at error level |
| At startup the data directory or audit log is unavailable | `Load` returns an error; with `--allow-degraded` `broker` enters diagnostics-only degraded run, otherwise startup is refused |
| At startup `broker.listen`'s port is below 1024 and the configuration file is writable by the group or others | `broker` refuses before binding the listen address: a foreground run exits with code 4, and with `--allow-degraded` it enters local-only degraded run |
| At startup the listen address and advertised address configuration combination is invalid, or the advertised address configuration exceeds 8 items | Refuse to start; on an invalid combination the error names both `broker.listen` and `broker.advertise` or `broker.advertise_extra`, and on more than 8 items it names `broker.advertise_extra` |
| At startup enumerating the default route fails | Do not refuse startup; this part of the addresses is empty, recorded at error level, and filled in on a later round |
| The owner or permission of the runtime directory or the account-lock directory does not match | Refuse to start, the error naming the directory |
| The `value` in a `SetConfig` request cannot be parsed by the key's type | Return `invalid_argument`, naming the key and the reason, and change no state |
| `Set`'s key is absent, the value is invalid, or a validation function refuses | Return `invalid_argument` and change no state |
| `Set` modifies a secret configuration key | Return `invalid_argument`, saying the item can be set only in the configuration file, and change no state |
| `Set` makes the listen address and advertised address configuration combination invalid, or makes the advertised address configuration exceed 8 items | Return `invalid_argument`, saying which item conflicts, and change no state |
| `Set` modifies `broker.listen` while this run's listen address comes from a startup parameter | Return `invalid_argument`, saying this run's listen address is given by a startup parameter, and change no state |
| Writing the configuration file fails | Return `internal`, the value in memory unchanged, no audit record; this package does its best to write the original bytes back, and when the write-back also fails the file may be empty or incomplete. Regardless of whether the write-back succeeds, this package changes the file hash recorded in memory to the hash of the current content on disk, so this process does not adopt what it left; when the process crashes or the write-back also fails, the next startup loads the file's current content, and when the truncation falls exactly on a line boundary the file may be read as a legal but incomplete configuration, a cost accepted for the in-place rewrite |
| The configuration file read at runtime is empty | Treat as unparsable, keep every old value and record at error level |
| At runtime the configuration file is changed to invalid content, including an invalid combination | The invalid item keeps its old value, the other legal items are adopted as usual, and the run log records it |
| This run's listen address comes from a startup parameter while `broker.listen` in the configuration file changes | Not adopted, other keys reload as usual, and the run log records it |
| Writing the audit log fails | The operation that triggered it completes as usual, and the run log records at error level |
| An event arrives while the audit log is closed or not yet opened | The event is written to the run log at error level and then dropped, not silently lost |

An account able to write the configuration file can rewrite it directly without the local interface, which is exactly what management permission being based on that file's write permission means; in the user form the audit log and configuration file belong to the same account, whose other processes can rewrite them, and this package does not guard against that path. In the system form the audit log is writable by root alone.

## Concurrency and Transaction Boundaries

`Set`'s and file reload's actions are serialized by one mutex, and subscribers are called outside the lock by the notification goroutine. A validation function registered for a key runs before `Set` and file reload commit, while the lock is held, so it must not call this package's other exported functions. `Get` reads the value in memory without touching disk. Appending to the audit log is protected by a separate lock, and each record is written as a complete line in one write call.

The locks of `config` and `pairing` have a fixed order: take `config`'s lock first, then `pairing`'s; the reverse is forbidden. While holding its own lock, `pairing` calls no function of `config`. While holding its lock, `config`'s validation function calls only `pairing`'s `AliasConflict` and `ResolveAlias`, which take only `pairing`'s lock. Between `broker.alias`'s validation passing and a subscriber pushing the new value into `pairing` there is a narrow window: a handshake committed within the window may take an effective alias equal to the new local alias, and after the window closes `pairing`'s commit recheck restores the refusal of this name. The window's length depends on the notification queue's backlog, a cost accepted for eliminating a lock-order inversion.

## Implementation Status

| Capability | Status | Code |
|---|---|---|
| The paths of both forms and the account lock path | Implemented | `internal/config/paths.go` (`resolvePaths`, `prepareDataDirs`, `CheckRuntimeDir`, `AccountLock`) |
| Configuration loading, defaults and validation | Implemented | `internal/config/keys.go`, `internal/config/file.go`, `internal/config/load.go` (`load`, `adoptValues`, `validateLoadedValues`) |
| Startup parameter overriding the listen address and the default alias | Implemented | `internal/config/load.go`, `internal/config/alias.go` (`adoptValues`, `defaultAlias`) |
| Computing the advertised address list and combination validation | Implemented | `internal/config/advertise.go`, `internal/config/routes_common.go`, `internal/config/routes_linux.go`, `internal/config/routes_darwin.go`, `internal/config/routes_other.go` (`computeAdvertised`, `platformAddressSource`, `defaultRouteAddresses`) |
| Reading secret configuration keys, the modification restriction and length validation | Implemented | `internal/config/keys.go`, `internal/config/state.go`, `internal/config/api.go` (`validateValue`, `secretLocked`, `Secret`, `setLocked`) |
| Periodic re-enumeration of local addresses and change notification | Implemented | `internal/config/state.go`, `internal/config/load.go`, `internal/config/notify.go` (`startAddressTickerLocked`, `reevaluateAdvertised`, `newNotifier`) |
| Reading and modifying the configuration | Implemented | `internal/config/api.go`, `internal/config/state.go`, `internal/config/file.go` (`Get`, `Set`, `setLocked`, `setKey`) |
| Additional validation and change notification | Implemented | `internal/config/api.go`, `internal/config/notify.go` (`Validate`, `ProbeListen`, `Subscribe`, `newNotifier`) |
| Watching the configuration file | Implemented | `internal/config/watch.go`, `internal/config/state.go` (`startWatch`, `startWatchWithDebounce`, `reloadFromDisk`, `reloadLocked`) |
| Generating the configuration file template | Implemented | `internal/config/template.go` (`Template`, `templateExamples`, `templateSections`) |
| Replacing the comment placeholder line when modifying a configuration key | Implemented | `internal/config/file.go` (`replacePlaceholderLine`, `setKey`) |
| Appending, merging and reading the audit log | Implemented | `internal/config/audit.go` (`openAudit`, `append`, `mergeLocked`, `flushExpiredLocked`, `read`, `terminatePartialLine`) |
| The `broker.offline` configuration key | Implemented | `internal/config/keys.go` (`broker.offline` in `keyTable`) |
| Classifying `Load` failures and the low-port check before binding | Implemented | `internal/config/api.go` (`ErrDataUnavailable`, `DataUnavailableError`), `internal/config/lowport.go` (`CheckLowPort`, `checkLowPortAtStartup`) |
