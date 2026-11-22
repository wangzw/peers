# System Overview

## What the System Is

peers lets Claude Code sessions running on different hosts discover each other and send text messages. A session that enables this system's tools registers with a [Broker](../glossary.md) on its host and becomes an agent. The user pairs the Brokers on two hosts, and after pairing, agents on both sides that share a topic can see each other and exchange messages.

By deployment form, a Broker is either a system Broker or a user Broker. A system Broker runs with privileges as a system service, at most one per host, and serves agents under several accounts on that host. A user Broker runs under an ordinary account and serves only that account's agents; it is usually installed as a service, starts at login and keeps running, and the user can also start it manually in a terminal. The MCP server process only connects to a Broker and never starts one. Each Broker instance has its own identity, peers and configuration and is an independent Broker on the network; an account runs at most one instance.

Messages enter the receiving session as Claude Code cross-session messages ([REF-claude-code-cross-session-messaging](../references.md)): an idle session starts processing a message on its own, and a busy session reads it in a gap in its current work. The receiving session's inbound controls and permission confirmation apply as usual.

## Capabilities

Once an agent joins a topic, it can list the online agents in the same topic and send a point-to-point message to any of them. A topic decides both visibility and permission to communicate; an agent in no topic is invisible to other agents.

The user pairs two Brokers with a pairing key; pairing is the only way to establish a link between Brokers; the link is established by the dialer and re-established by it after a disconnect. A Broker can be made public so that other Brokers in the connected network can discover it. A Broker's configuration can be read and modified in a session, and changes leave an audit record; whether they can be read and modified depends on whether the session's account has read and write permission on the Broker's configuration file.

At the configured interval, a Broker checks whether this project has a new official release on GitHub. When one is available, the terminal status output and the first tool result in a session show a hint; the user runs the update command in the terminal, and after confirmation it automatically downloads, verifies and replaces the binary, and restarts a service that was installed as one.

## Scope Boundary

The system is responsible for registering and naming local agents, topic membership, pairing between Brokers and encrypted links, discovery of public Brokers and public topics, synchronous point-to-point text message delivery, the two deployment forms of a Broker and the service scope of each, reading, writing and auditing of Broker configuration, and notification of and terminal updates to the Broker's own official releases.

### Out of Scope

- Forwarding messages through a third Broker. Two Brokers cannot communicate without a link between them, and two hosts both behind NAT cannot reach each other.
- A centralized registry.
- Caching, persisting, offline redelivery and asynchronous delivery of messages. On a failed send, the sending agent decides whether to resend.
- Status reporting after delivery. Whether the receiver reads, processes or replies to a message is up to the receiving agent, and a reply is another message.
- Broadcast. Sending to several agents is done by the caller making one call per agent.
- Transfer of files, session history and context. A message carries plain text only.
- Send rate limiting and message loop detection, which the receiving side's inbound controls in Claude Code handle.
- Interoperability with external A2A agents ([REF-a2a](../references.md)).
- Native Windows.
- Replacing or modifying Claude Code's built-in cross-session messaging. Both coexist: a local session that is not registered as an agent can communicate only through Claude Code's built-in capability.
- Running several Broker instances under one account.
- An administrator list or roles set up outside file permissions.
- Installation without user confirmation. The version check runs automatically; installation is confirmed by the user in the terminal, or by a parameter the user explicitly passes to skip the question.
- Distribution and upgrades through a system package manager.

## Actors

| Actor | Authority |
|---|---|
| User | The person running Claude Code sessions on the host under some operating-system account. Approves or rejects operations an agent initiates through Claude Code's permission confirmation. When the account has write permission on the Broker's configuration file, the user decides whether to pair and whether to make the Broker public; with read permission, the user can view the Broker's full status and audit log on the host. |
| Administrator | The person who installs and runs the system Broker. Decides by file permission which accounts may use it and which accounts may read or modify its configuration. |
| agent | A registered Claude Code session, operated by the model in the session through tools. Joining and leaving topics, listing agents, topics and Brokers, and sending messages are open to every agent; reading configuration, modifying configuration, generating pairing keys, initiating pairing and unpairing require the owning account's read and write permission on the configuration file. Each tool call is further subject to the permission settings of its session. |
| Peer Broker | Another Broker that has been paired. It can exchange with this Broker the rosters of shared topics, cards, public topic declarations and messages, and cannot read or modify this Broker's configuration. |
