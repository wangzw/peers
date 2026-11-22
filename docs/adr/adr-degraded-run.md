# Do not exit on a partial startup failure after the account lock

## Context

When a [Broker](../glossary.md) runs as a service, the service manager restarts it after it exits: systemd units carry `Restart=always` and `RestartSec=5`, and launchd carries `KeepAlive`. When the listen address is taken by another account's Broker on the same host, or the data directory or audit log cannot be read or written, the process exits every few seconds and the fault stays only in the service log. A tool call in a session then gets "no Broker available", whose startup instructions do not apply to an installed service; the install command can only relay the startup failure from the service log.

The registration of local agents, topics and local messages do not depend on the link's listening. At most one instance per account is guaranteed by the account lock, and only the lock-holding process can bind the local socket; a failure before the account lock is taken, such as being inside a session's process tree, an existing instance holding the lock, or a non-root run in the system form, leaves no service that can be safely provided. When the configuration file cannot be read, fails to parse or has an invalid value, the Broker cannot know the listen address or the group and permissions of the local socket; deciding who can connect to the local socket from such a configuration is more dangerous than providing no service. A Broker run in a terminal foreground shows the failure reason directly to whoever runs it.

## Options Considered

### Exit on any startup failure

**Rejected**: under a service manager it restarts repeatedly, the fault is invisible to sessions and the install command, and the local features of a second account on the same host go down with it.

### Keep running on failure without reporting it

**Rejected**: for the same reason [ADR-listen-by-default](adr-listen-by-default.md) rejects "switch to not listening", the Broker looks fine while actually missing a feature, and the fault is hard to find.

### Always offer diagnostics only

Without distinguishing the failed step, report every failure only on the local socket.

**Rejected**: a taken listen address is the most common failure, and local features do not depend on listening; treating it as every failure would make a second account's sessions on the same host completely unusable. When the configuration file is not trusted, offering diagnostics only would also decide the local socket's permissions from a misread value.

### Decide degradation by whether it runs under a service manager

At startup, ask the service manager for the service's main process PID and degrade only when it equals this process, without a command-line flag.

**Rejected**: the decision depends on whether the service manager can be queried in the earliest startup stage, and the result is uncertain when the query fails; a Broker started by a container or another supervisor cannot degrade; and the behavior cannot be read directly from the service definition.

### Bind the local socket first on every start, then run the other steps

**Not adopted**: a successful start does not need to bind early, and binding early would make the local socket answer before the Broker is ready, so the install command and the MCP server process would both have to distinguish "starting". For degradation, binding the local socket at the point of failure suffices, which does not conflict with this option, and it can be changed to bind early if needed.

### Allow a degraded run by a flag, with local-only and diagnostics-only degrees

**Adopted**; see Decision.

## Decision

`peers service run` gains the flag `--allow-degraded`. With it, a listen bind failure after the account lock is taken does not make the process exit: the Broker provides all local features but does not provide pairing-key generation. When the data directory or audit log is unavailable, the local socket offers diagnostics only. Both degrees report the failed step, the reason and how to fix it, and retry at once when the configuration file changes; diagnostics only also retries once a minute, and the listen bind retries with backoff, recovering after success without a restart. When the configuration file cannot be read, fails to parse or has an invalid value, `broker.local_socket_group` points at a group that does not exist, or assembly or component startup fails for a reason unrelated to the data directory, the process still exits even with the flag. A failure before the account lock is taken also still exits. The service definition written by `service install` carries this flag, and a foreground run does not carry it by default.

## Consequences

A Broker running as a service no longer restarts repeatedly because the listen address is taken; sessions and the terminal see the specific reason; and a second Broker on the same host with a conflicting listen port can still serve local agents.

**Costs accepted:**

- A Broker in a degraded run looks healthy to the service manager, so monitoring that only watches the service state cannot find the fault; it must watch `peers service status` or the run log.
- One binary has two startup semantics, separated by the flag; foreground and service startup failures each need their own tests.
- Recovery re-runs the startup steps within the process, so component startup and teardown must be repeatable in one process.
- A service installed before the flag was added is not rewritten with it on upgrade; the installation must be run again to carry the flag, and until then its behavior is unchanged.
- In local only, peers for which this side is the acceptor cannot connect in, and sends to them fail after waiting within the budget; the user must check the status to learn why.
- When the configuration file cannot be read or fails to parse, the process still exits and the service manager restarts it repeatedly, so the fault can be seen only in the service log and on standard error; this is the price of not letting an untrusted configuration decide the diagnostics service.
