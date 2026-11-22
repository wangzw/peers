# Deploy a Broker in two forms, system and user

## Context

Claude Code restricts each session's inbox socket and session registry to the account the session belongs to ([REF-claude-code-cross-session-messaging](../references.md)), so a [Broker](../glossary.md) under an ordinary account can deliver only to sessions of the same account. A common practice in production is to run the daemon as a system service, installed and maintained by an administrator. On a personal development machine, the user wants to use it under an ordinary account without involving an administrator. One host may have several accounts each running sessions.

## Options Considered

### User only, one per account

A Broker always runs under an ordinary account and serves only sessions of the same account.

**Rejected**: on a multi-account host, each account would be installed, configured and paired separately, and an administrator could not provide one Broker for the whole host.

### System only, one per host

A Broker always runs with privileges as a system service and serves every account on the host.

**Rejected**: installation and startup both need privileges, so a user on a personal development machine must first perform a system installation and cannot use it under an ordinary account alone.

### Start a user Broker on demand from the MCP server process

When a session finds no usable Broker, that session's MCP server process starts a user Broker and detaches it from the session's process tree, so a personal user need not install or start a Broker in advance.

**Rejected**: the Broker's lifecycle is the responsibility of the service manager or the user, and the MCP server only connects. Starting on demand hands the Broker's startup and its failure handling to the session; once the user has installed a user Broker as a service, it already starts at login and keeps running, so there is no problem here to solve.

### Allow several instances under one account

One account starts several Brokers with different configurations or listen addresses.

**Rejected**: a session of the account cannot tell which instance it belongs to; the same session may be registered on two instances and appear in the network under two identities, and it may receive duplicate messages.

### Put the user Broker's runtime directory under `/run/user/<uid>` by default

The runtime directory of a non-root account is taken to be `/run/user/<uid>/peers`, and `/tmp/peers-<uid>` is no longer used.

**Rejected**: logind deletes `/run/user/<uid>` when the account's last session ends. Using it by default would mean that an account's Broker and a later session see different locations at different times, and both the single-instance constraint for one account and "always find the local socket at the same place" would no longer hold. It is enabled only when `/tmp/peers-<uid>` is occupied by another account, but that occupant can delete and recreate the directory at any time, so processes of one account would still compute different locations at different times. For this reason, when starting, a Broker checks the account lock in both the preferred and the fallback location, and refuses to start under the same account if a running instance holds either; the MCP server and terminal commands probe the local socket in both locations and use only the one that passes the process-credential check; when both pass, they use the one whose account lock is held, that is, where the Broker is running, and when neither is held, they use the one selected by the location rule. Changes back and forth in occupancy therefore neither let two instances arise under one account nor leave a session unable to find the running Broker.

### Two forms coexist, at most one instance per account

A system Broker runs with privileges as a system service, at most one per host, and serves agents under the accounts on the host that may use it. A user Broker runs under an ordinary account and serves only agents under the same account, and one host may have several accounts each running one. At most one instance runs under one account. A user Broker is installed as a service by the user and starts at login, or is started by the user in a terminal outside a session; a system Broker is installed and started only by an administrator. The MCP server process only connects and does not start any Broker.

**Adopted**; see Decision.

## Decision

A Broker has two deployment forms, system and user. A system Broker serves agents under several accounts on the host, and a user Broker serves only agents under the same account. At most one Broker instance runs under one account, whichever configuration it starts with. Each instance has its own fingerprint, peers, directory and configuration, and is an independent Broker in the network. The Broker's lifecycle is the responsibility of the service manager that starts it or of the user; the MCP server process only connects and does not start a Broker.

## Consequences

An administrator can provide one Broker for the whole host, and a personal user can use it without privileges. A session of one account is registered on only one instance.

**Costs accepted:**

- The two forms differ in file locations, permission checks and delivery method, so each must be implemented and tested separately.
- A system Broker runs with privileges and must read the session registries of the accounts and deliver to their sessions; to do so it keeps one helper process per served account that switches to that account, and these operations run under that account's identity.
- The account lock of a non-privileged account lives in a directory named after the account under `/tmp`, and other local accounts can create this directory first. When the preferred directory is occupied by another account, that account's user Broker uses the platform's per-user location (on Linux `/run/user/<uid>`, on macOS the per-user temporary directory); when that location is also unavailable, for example when the account has no logind session on Linux, the user Broker refuses to start.
- A system Broker puts the agents of several accounts under one Broker: agents of different accounts are visible to each other whenever they share a topic, and they share one fingerprint, one set of peers and one alias.
- Several Brokers on one host are unrelated in the network and pair separately; another host that wants to talk to two accounts on this host may have to pair twice.
- When an account has both a system and a user Broker available, the MCP server process chooses one in a fixed order, and the user must know which one their session is registered on.
- The user must first install the user service, or start a Broker in a terminal outside a session, before a session can register; until then, a tool call returns only an error explaining how to start a Broker.
- Instance uniqueness is judged by account and host and cannot rely on a file under the home directory, because the home directory may be shared by several hosts.
