# Present no authentication token when writing to the inbox socket

## Context

Claude Code lets a process writing to the [inbox socket](../glossary.md) present the target session's authentication token on the first line; on macOS and Linux that line is optional ([REF-claude-code-cross-session-messaging](../references.md)). A sender that presents the token falls into some permission category. A sender that does not present the token is still trusted when its process relationship shows it is a child of the target session itself; a sender that meets neither condition is treated as an undeclared permission category: an ordinary session accepts it directly, while a session running with permission confirmation skipped holds the message for user approval when no inbound policy is set. The Broker writing to a session can read the target session's token. The messages this system writes come from other agents, including agents on other hosts.

## Options Considered

### Present the target session's token

The Broker reads the target session's token and presents it on the write. The library it depends on does this by default ([REF-claude-code-socket-transport](../references.md)).

**Rejected**: A remote agent's message would enter the session as a trusted local program, and a session running with permission confirmation skipped would no longer hold it for user approval. Text from another host is untrusted input to the local session and does not deserve a local program's treatment.

### Present no token

Send no authentication line on the write, and let the target session's inbound controls treat the sender as an undeclared permission category.

**Adopted**; see Decision.

## Decision

The Broker presents no authentication token when writing to the inbox socket and does not read the session's token file.

## Consequences

Messages this system delivers are subject in full to the target session's inbound controls, and the accept, hold-for-approval or refuse inbound policy the user sets for a session applies to this system as well.

**Costs accepted:**

- A session running with permission confirmation skipped and no inbound policy set requires user approval for every message, and unattended messages are dropped when the hold period expires.
- A native Windows inbox connection requires the authentication line, so this decision keeps the system from delivering on native Windows.
- The library it depends on presents the token by default, so it must be explicitly disabled at the call site; omitting that violates this decision and raises no error.
- Presenting no token is not enough on its own: when the Broker process is a descendant of some session, Claude Code treats the messages it writes as sent by that session's own child process and inbound controls are bypassed. The Broker must therefore not be a descendant of any session: a user Broker starts from the user service management mechanism or from a terminal the user runs outside any session; a system Broker runs as a system service, and the process it delivers through is outside every session's process tree.
- At startup the Broker walks the parent process chain, reading each ancestor's environment variables and this account's session registry, to confirm that it is not inside a session's process tree. When the process is reparented under init with `setsid` or a double fork before it starts, the ancestor chain no longer contains a session, and this check cannot find it; whether Claude Code still treats such a process as a session's child depends on how Claude Code decides.
