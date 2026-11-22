# inbox

`inbox` is the only package in the system that touches Claude Code's internal formats. It looks up a session's record in the [session registry](../../glossary.md), getting the session's name, status and inbox socket path, and watches the registry directory for changes; it writes one signature-wrapped message frame into an inbox socket; it also provides two pure computations, name normalization and signature cleaning. Neither the registry nor the frame format carries a public compatibility promise, so the dependency on them is gathered into one package, and a Claude Code version change only needs a change here. This package always acts as the account of its own process: in a user Broker it runs inside the Broker process; in a system Broker the lookup, watch, reread and write all run inside the target account's helper process, see [broker](design-broker.md).

## Responsibilities

- Look up a session record in the session registry under the given configuration directory by the inbox socket path or process id the caller gives, and return the session's current information (`REQ-agent-messaging-001`, `REQ-agent-messaging-036`).
- Watch the session registry directory for file changes and tell the caller the records that changed; reread one record at the caller's request (`REQ-agent-messaging-005`, `REQ-agent-messaging-010`).
- Write a message into the given session's inbox socket, wrapping it with the signature the caller gives, and classify the result as written, not written or uncertain (`REQ-agent-messaging-022`, `REQ-agent-messaging-023`).
- When writing, send no authentication line ([ADR-inbox-delivery-without-auth-token](../../adr/adr-inbox-delivery-without-auth-token.md), `REQ-agent-messaging-024`) and carry no session identifier (`REQ-agent-messaging-033`).
- Provide computations that involve no I/O: the byte count of a message encoded into a frame, the normalized result of a session name, and the result of a signature after the receiver cleans it (`REQ-agent-messaging-003`, `REQ-agent-messaging-029`, `REQ-agent-messaging-037`).
- Decide whether this process runs within a Claude Code session's process tree, for `broker` to confirm at startup that it is not a descendant of any session.

### Non-Responsibilities

- Does not decide whether a message should be delivered, and does no visibility check or deduplication; [topic](design-topic.md) and [broker](design-broker.md) own those.
- Does not retry. The write result is returned as-is, and `broker` decides whether to retry.
- Does not receive delivery status sent back by a session, and binds no socket for a remote agent; the system does not use delivery status ([ADR-inbox-write-is-success](../../adr/adr-inbox-write-is-success.md)).
- Does not register a session. Which sessions participate is decided by the MCP server process's registration, see [mcpserver](design-mcpserver.md); whether a session name is usable and whether an agent exists are decided by `broker`.
- Does not check the inbox socket's owner; `broker` owns that.
- Does not switch accounts and does not fork a helper process. Under the system form, under which account this package runs is decided and arranged by [broker](design-broker.md).
- Does not read the key files of the sessions in the same directory, and does not read another account's session registry.
- Does not decide the signature's content. The signature is assembled by `broker` and passed in.

## External Interface Contract

The interface is for `broker` to call only; the implementation is adjusted with changes to Claude Code's internal formats, and the signatures stay stable.

```go
// Lookup looks up a session record in the session registry under configDir; an empty
// configDir means this account's default location. It takes the record whose
// messagingSocketPath equals socketPath; when the same path appears in several
// records it takes the one with the latest modification time. It returns ErrNoRecord
// when no record is found.
func Lookup(configDir, socketPath string) (Session, error)

// Reread rereads the record s is in. It returns ErrNoRecord when the record has been
// deleted, is no longer a regular file, or exceeds the size cap; it returns
// ErrPartial when the regular file's content cannot be parsed (half-written or
// empty), and the caller keeps the previous value.
func Reread(s Session) (Session, error)

// Watch watches the registry directory under configDir, calling back with the process
// id that owns the changed record when a record in it is added, rewritten or deleted;
// it calls back with 0 when which record it is cannot be known.
func Watch(configDir string, onChange func(pid int)) (stop func(), err error)

// Deliver writes one message to the session with signature sig.
// Returning nil means written; ErrNotWritten means definitely not written; ErrUncertain
// means the frame may have been received; ErrBadSignature means sig is empty or cannot be
// recognized by the receiver as-is, and nothing was written.
func Deliver(ctx context.Context, s Session, sig, text, msgID string, timeout time.Duration) error

// FrameSize returns the byte count of this message encoded into a frame.
func FrameSize(sig, text string) int

// MaxNameBytes is the longest byte count of an agent name that can be registered,
// published to the directory or accepted from a peer.
const MaxNameBytes = 128

// NormalizeName returns the session name normalized by the rule Claude Code uses before
// matching session names. When the result exceeds MaxNameBytes bytes it is truncated on a
// UTF-8 character boundary, and a "-" left at the end after truncation is dropped.
func NormalizeName(name string) string

// CleanSignature returns the result of sig after the receiver cleans it.
func CleanSignature(sig string) string

// InSession reports whether this process runs within a Claude Code session's process tree.
func InSession() bool

type Session struct {
    ConfigDir    string // the configuration directory the record is in
    PID          int    // the session process id, that is the record's file name
    Name         string // the session name, taken as-is from the record
    Status       string // taken as-is from the record, may be empty
    SocketPath   string // taken from the record, not assembled here; empty before the inbox is bound
    PeerProtocol int    // taken from the record's peerProtocol; 0 when missing or not a positive integer
}
```

`Deliver` returning nil means exactly this: the frame was written to the socket and the receiver closed the connection. Whether the session then processes it immediately, queues it, keeps it for user approval or drops it, `inbox` gets no such information and does not query it; this matches `REQ-agent-messaging-015`'s definition of success. When a message that inbound controls let through reaches an idle session, the session begins processing it itself (`REQ-agent-messaging-021`), which is the behaviour of Claude Code's receive side ([REF-claude-code-cross-session-messaging](../../references.md)).

`Status`'s values are decided by Claude Code, the known values being `idle`, `busy`, `waiting` and `shell`; other values and a missing value are recorded by `broker` as unknown.

## Dependencies

The frame format, signature wrapping and address encoding use claude-code-socket-transport ([REF-claude-code-socket-transport](../../references.md), [ADR-reuse-socket-transport-library](../../adr/adr-reuse-socket-transport-library.md)), whose source is vendored into this repository's `third_party` directory at the commit registered in the references file and is not updated with upstream automatically. That library implements the protocol version 1 derived from Claude Code v2.1.233. The session registry is not read through that library; this package reads it itself. Changes to the registry directory are learned through the operating system's file-change notifications. `inbox` references no other package of this project; the write deadline is passed in by the caller.

The following options are fixed when calling that library:

| Option | Value | Effect |
|---|---|---|
| `Client.NoAuth` | `true` | Sends no authentication line and reads no target session's token file ([ADR-inbox-delivery-without-auth-token](../../adr/adr-inbox-delivery-without-auth-token.md)) |
| `Client.Timeout` | the write deadline the caller gives | The time cap of one write; the effective deadline is the earlier of it and the caller context's deadline |
| send interface | send by socket address | The library does not look up the target through the registry, so it does not fill in a session identifier automatically |
| `Message.FromName` | the signature the caller gives | The receiver displays the sender from it |
| `Message.From` | empty | Provides no reply address and receives no delivery status ([ADR-inbox-write-is-success](../../adr/adr-inbox-write-is-success.md)) |
| `Message.SessionID` | empty | Carries no session identifier: a frame that carries one inconsistent with the receiver's current session identifier is silently dropped by the receiver without notifying the sender, and the session identifier changes right after the session runs `/clear`; Claude Code's built-in send tool carries none either |
| `Message.Priority` | `next` | Hands to the model at the next processable moment |
| `Message.MsgID` | the message identifier the caller gives | Consistent with the message identifier on the link, for correlating logs |

## Core Algorithms and Flows

### Looking Up a Session Record

The session registry is the `sessions` directory under the configuration directory, one file per session process, the file name being the process id plus `.json`; the process id is taken from the file name, and a `pid` field in the record content is not used. The configuration directory takes the value the caller gives, and an empty value takes `.claude` under this account's home directory. It reads every record in the directory and takes the one whose `messagingSocketPath` equals the given socket path; when the same path appears in several records it takes the one with the latest modification time. A record that is half-written, empty or unparsable is skipped on read, taking no part in matching and not affecting the reading of the other records. It returns `ErrNoRecord` when no usable record exists. The configuration directory and socket path are both reported by the MCP server process and are not verified by this package; they only decide which of this account's own records it reads, and this package reads as this account and cannot read what another account does not allow it. While a session lasts, `messagingSocketPath` in its record does not change, and is empty before binding.

### Lenient Parsing

The record content depends on three fields, `name`, `status` and `messagingSocketPath`, and also reads `peerProtocol` for the caller; every other field is ignored, and a missing field is not an error. Claude Code writes a record in place when rewriting it, not by atomic replacement, so a read may encounter half-written content in a regular file: when the JSON cannot be parsed (including an empty file) it returns `ErrPartial`, and the caller keeps the previous value, waiting for the next change notification. When the record file is absent, is not a regular file (for example a symbolic link, FIFO, directory, device or socket), or exceeds the size cap (256 KiB), it returns `ErrNoRecord`, the caller treats the session as ended, and `Lookup` skips such records. `peerProtocol` is only read, and this package does not judge compatibility by it; when it is missing or not a positive integer `PeerProtocol` is 0, which does not affect the record's other fields or make the record incomplete; whether to warn is decided by [broker](design-broker.md).

### Watching and Rereading

`Watch` installs one watch per configuration directory, calling back the process id owning the record that changed. The system has no periodic check, and a record's change is learned only through notifications; the caller also calls `Reread` on demand before using a record, see [broker](design-broker.md).

The watch target is the deepest directory in which registry changes can currently be observed: when the registry directory exists it watches it, when only the configuration directory exists it watches the configuration directory, and when neither the registry directory nor the configuration directory exists it watches the nearest existing ancestor instead. Startup and runtime use the same selection and installation logic: at startup the configuration directory may not exist, `Watch` does not error but watches its nearest ancestor and calls back once with process id 0, and later as the configuration directory and the registry directory appear the watch is moved back down layer by layer; when even the ancestor directory cannot be watched, `Watch` returns an error. When the registry directory or the configuration directory itself is deleted or renamed, the operating system drops the corresponding watch, even if the path has already reappeared by the time the event is handled. On receiving such an event, `Watch` does not rely on its own recorded watch state but treats the watch as invalid directly: it reselects the target and installs a watch, then reports the difference once by snapshot. After installing a watch it checks the directory once more, so a directory that appeared between the check and the install can also be watched. When the configuration directory is deleted, or during runtime no watchable directory is found, it calls back once with process id 0, then recovers through a change of the ancestor, and does not silently fail.

### Name Normalization

`NormalizeName` is the same as the normalization Claude Code's built-in send tool applies before matching session names: drop control and format characters, drop leading and trailing whitespace, convert to lowercase, and replace whitespace with `-`. When the result exceeds `MaxNameBytes` (128 bytes) it is truncated on a UTF-8 character boundary to no more than that length, then a trailing `-` is removed. `MaxNameBytes` is the longest name byte count the Broker uses when registering, publishing a roster or accepting a peer's member entry. The result is used as the agent's name, and whether the result is usable is decided by `broker`.

### Signature Cleaning and Validation

Before recognizing the signature wrapper, the receiver cleans the sender's name: it deletes double quotes `"`, angle brackets `<` and `>`, and backslash `\`, and deletes every code point that `strconv.IsPrint` judges unprintable, including control characters, format characters, surrogates, line separators, paragraph separators, the private use area, unassigned code points and non-ASCII whitespace; then it drops leading and trailing whitespace and truncates to 64 code points. `CleanSignature` computes by this rule, derived from program analysis on Claude Code v2.1.327. Measured on v2.1.311, the character set the receiver deletes is a subset of the set `CleanSignature` deletes: characters the receiver keeps but `CleanSignature` deletes include a U+00A0 inside the name, a private-use U+E000, an unassigned U+0378, a printable U+3164 and the backslash. `CleanSignature` deleting more is the safe direction, and the system does not send a name the receiver would change. The receiver puts the signature into a JSON string to display it, and the characters listed above all need escaping in a JSON string; the cleaned signature contains none of them, so `strconv.Quote`'s result is just a double quote on each side, and the name the receiver sees is exactly the one the sender wrote.

When the cleaned result is not identical to the original text, the receiver does not recognize the whole signature wrapper; the message is delivered as usual, but the receiver cannot see who the sender is. The dependent library cleans once more by its own rule before writing and truncates beyond 200 bytes.

`Deliver` validates the signature before writing: only when `sig` is non-empty, `CleanSignature(sig)` is exactly the same as `sig`, and `sig` does not exceed 200 bytes, does it write; otherwise it returns `ErrBadSignature` and writes nothing. An empty signature, if written, would make the library add no signature wrapper and the receiver see no sender, so an empty signature is equally invalid. A signature that passes validation is changed by neither the library's cleaning nor the receiver's cleaning.

### Process Tree Detection

Claude Code puts the inbox socket path of the session it belongs to into the environment variable `CLAUDE_CODE_MESSAGING_SOCKET` of the commands and MCP server processes it starts. `InSession` first looks at this variable in its own process; when non-empty it reports that this process is within a session's process tree. When the variable is empty, it walks up the parent chain to pid 1: if any ancestor's environment contains a non-empty instance of this variable, or an ancestor's pid is in this account's session registry (the configuration directory's `sessions/<pid>.json`, the configuration directory being `CLAUDE_CONFIG_DIR` or this account's default `~/.claude` when unset), it also reports being within a session's process tree. An unreadable ancestor environment, for example due to insufficient permission, is skipped and the walk continues upward; when an ancestor's parent process id cannot be read the walk stops and it reports not being in a session, because no ancestor remains to check and a guess would only misjudge a process outside a session as inside. The chain length is capped at 256 levels, preventing malformed data from causing a loop. Only process information is read, and no signal is sent; Linux takes the parent process id from `/proc/<pid>/stat` and the environment from `/proc/<pid>/environ`, and macOS uses `sysctl`'s `kern.proc.pid` and `kern.procargs2`. The test build (`peers_testenv`) provides the environment variable `PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK`, which when set disables only the check along the parent chain and leaves the check of this process's own environment unchanged, and release builds ignore the variable entirely; the reason is that the test runner is itself within a session's process tree, and a Broker child process started by the test sandbox would otherwise be misjudged as inside a session and refuse to start.

The Broker therefore cannot start within a session's process tree, and a Broker started inside a session fails with exit code 6, see [broker](design-broker.md). When the process is first detached to init with `setsid` or a double fork before starting, the session is no longer in the ancestor chain and this check cannot find it; whether such a process is still regarded by Claude Code as a child of the session depends on Claude Code's way of deciding ([ADR-inbox-delivery-without-auth-token](../../adr/adr-inbox-delivery-without-auth-token.md)).

### Writing

Connect to the socket, write one line of JSON, half-close, and wait for the receiver to close the connection. The body the receiver sees looks like:

```
<cross-session-message from-name="api-worker@hostA">
body
</cross-session-message>
```

The envelope's opening and closing tags appearing in the body, case-insensitively, are both handled by `escapeBody` (`internal/inbox/frame.go`) inserting a backslash after `<`, preserving the original case: `</cross-session-message>` becomes `<\/cross-session-message>` and `<Cross-Session-Message` becomes `<\Cross-Session-Message`. A tag already following a backslash is left alone. This is the first line of defence, done before the body is handed to the dependent library. The receiver adds a fixed piece of text outside the envelope; this system's envelope opening and closing tags do not take part in the backslash insertion, and the envelope tag sequences in the body get another backslash added. This is the second, independent line of defence, not conflicting with the first. The basis of both layers' behaviour is program analysis and measurement on Claude Code v2.1.311.

Known limitation: `escapeBody` handles only these two tag texts themselves, not HTML entities, zero-width characters or homoglyphs. These forms do not change the receiver's recognition of the signature and envelope and can only change how the body displays in the session, so they are not handled separately.

The write result's classification: a connection failure, or an error before any byte is written, is not written; the whole line written and the receiver closed the connection is written; every other case, including a timeout waiting for the close after writing, is uncertain. The dependent library does not report the number of bytes written when it errors during the write phase, so a failure in the write phase is always treated as uncertain; a connection failure and an encoding failure before writing are still not written. While waiting for the receiver to close the connection after the frame is written, cancellation of the caller context does not interrupt the wait, and cancellation takes effect when the write deadline expires.

### Frame Size

The receiver disconnects when a line exceeds 1 MiB without a newline (REF-claude-code-socket-transport). `FrameSize` applies to the body the same escaping as writing, then computes by the actual encoding: the signature-wrapped body is encoded as a JSON string, in which `<`, `>`, `&`, quotes, backslashes and control characters are counted at their escaped length and other characters at their UTF-8 byte count, plus the length of the frame's other fields. The result matches the byte count the dependent library actually writes. `broker` compares its result with the 1 000 000-byte cap.

## Error and Failure Semantics

| Situation | Return | Explanation |
|---|---|---|
| No matching record in the registry | `ErrNoRecord` | The session has not bound an inbox, has no registry record, the configuration directory is wrong, the record is no longer a regular file or exceeds the size cap, or the session has ended |
| The record is a regular file whose content is half-written or empty and cannot be parsed | `ErrPartial` | The caller keeps the previous value |
| The signature is empty, would change after the receiver cleans it, or exceeds 200 bytes | `ErrBadSignature` | Not written; such a signature, if written, would let the receiver deliver the message as usual but not see the sender |
| The socket cannot connect, or an error before writing | `ErrNotWritten` | Can be retried |
| A failure in the write phase, or failing to confirm the receiver closed the connection after writing | `ErrUncertain` | The frame may have been received, and a retry may make the session receive two copies |
| The receiver read the frame but then dropped it | nil | `inbox` cannot know, including an inbound policy rejection, exceeding a rate limit, deduplication because the content is identical to the same sender's previous message, or a full queue; a message this system writes is identified at the receiver by the writing process as the sender, and the rules and values are in [ADR-no-broker-rate-limiting](../../adr/adr-no-broker-rate-limiting.md) |

Writing is not idempotent: the same message written twice is received twice by the session, and the receiver's deduplication takes effect only when the same sender sends identical content within 30 seconds. The system's deduplication is done by `broker` before writing.

When a Claude Code version update makes the frame format or registry format incompatible, it shows as `Lookup` finding no record, or `Deliver` returning nil while the session receives no message. The case where a session receives no message cannot be discovered by `inbox` itself and relies on verification with a real session.

## Implementation Status

| Capability | Status | Code |
|---|---|---|
| Looking up and leniently parsing a session record | Implemented | [`internal/inbox/session.go`](../../../internal/inbox/session.go) (`Lookup`, `Reread`, `parseRecord`, `protocolField`) |
| Watching the registry directory and rereading | Implemented | [`internal/inbox/watch.go`](../../../internal/inbox/watch.go) (`Watch`, `watcher`, `scanAndNotify`) |
| Writing a message and classifying the result | Implemented | [`internal/inbox/deliver.go`](../../../internal/inbox/deliver.go) (`Deliver`, `classifySendError`) |
| Computing frame size, name normalization and signature cleaning | Implemented | [`internal/inbox/frame.go`](../../../internal/inbox/frame.go) (`FrameSize`), [`internal/inbox/name.go`](../../../internal/inbox/name.go) (`NormalizeName`), [`internal/inbox/signature.go`](../../../internal/inbox/signature.go) (`CleanSignature`) |
| Process tree detection | Implemented | [`internal/inbox/insession.go`](../../../internal/inbox/insession.go) (`InSession`) |
