# Start conversations through Claude Code's built-in send tool when its session list shows the agent

## Context

Claude Code's built-in cross-session send tool reaches the sessions its list shows: this account's sessions on this host and, over Remote Control, the same user's sessions on other machines ([REF-claude-code-cross-session-messaging](../references.md)). A session registered with this system is still a Claude Code session, so an [agent](../glossary.md) can be reachable both through the built-in tool and through this system's `send_message`.

A system [Broker](../glossary.md) serves several accounts on one host, and the sessions of the other accounts are not in this account's list ([ADR-broker-deployment-modes](adr-broker-deployment-modes.md)). Messages this system writes share one sender budget at the receiver and fall outside Claude Code's loop detection ([ADR-no-broker-rate-limiting](adr-no-broker-rate-limiting.md)). A received message names its sender only by the [agent address](../glossary.md) in the signature, with no reply address ([ADR-inbox-write-is-success](adr-inbox-write-is-success.md)). On a system Broker, another account's agent can carry the name of a session of this account ([ADR-name-ownership-by-account](adr-name-ownership-by-account.md)). The model picks the send tool from the MCP server's instructions.

## Options Considered

### Contact every agent address through this system

Every agent is contacted with `send_message`, whether or not its session appears in Claude Code's built-in session list.

**Rejected**: a session the built-in tool reaches receives its messages through the Broker, inside the shared sender budget and outside Claude Code's loop detection.

### Prefer the built-in tool for agents on the same host

Any agent on this host is contacted with the built-in tool, and `send_message` reaches only agents on other hosts.

**Rejected**: it misses the same user's Remote Control sessions on other machines, and on a system Broker it includes other accounts' agents on this host, whose sessions the built-in tool cannot reach.

### Prefer the built-in tool for agents whose session the built-in list shows

A conversation with an agent whose session appears in Claude Code's built-in session list starts with the built-in session tools, unless the user names the agent by its agent address or asks for peers; `send_message` starts every other conversation. A reply to a message whose sender is an agent address goes through `send_message` to that address verbatim.

**Adopted**; see Decision.

### Map an agent address the user gives to a listed session

When the user names an agent by an agent address whose alias is this Broker's and whose name, without a disambiguator, equals a listed session's name, the conversation starts with the built-in tools, to that session by its name.

**Rejected**: an agent address is the user's explicit choice of the agent and of this system's channel. The match is by name, and on a system Broker another account's agent can own that name, a collision `list_agents` shows only when both agents share a topic with this session.

### Reply through the built-in tool when the sender's name matches a listed session

A reply to a message from an agent address is sent with the built-in tool when a session with the sender's name appears in this account's built-in session list.

**Rejected**: the received message carries no reply address, so the match is by name. On a system Broker, another account's agent can carry the name of a session in this account's list, and the reply reaches that session instead of the sender.

## Decision

Adopt Claude Code's built-in session list as the criterion for the send tool. The model starts a conversation with an agent whose session appears in that list through Claude Code's built-in session tools, even when the agent is also registered with this system. `send_message` starts the conversation instead when the user names the agent by its agent address, when the user explicitly asks to send with peers, as for broadcasts, or when the agent's session is not in that list. A reply to a message whose sender is an agent address goes through `send_message` to that address verbatim, even when a session with the same name appears in the list.

## Consequences

Choose the send tool by whether Claude Code's built-in session list shows the other session, not by whether it is on this host. A session the list does not show is still reached through `send_message`.

**Costs accepted:**

- The rule is carried out by the model, and only the instruction text is tested; whether the model follows it cannot be verified deterministically.
- The model consults Claude Code's built-in session list before starting a conversation.
- One conversation can use both channels, so its messages are split between Claude Code's built-in messaging and this system.
- A user who names a listed session by the agent address `list_agents` shows gets this system's delivery, inside the shared sender budget and outside Claude Code's loop detection; the built-in tools are used only when the session is named otherwise, such as by its session name.

## Invalidation Conditions

The built-in list shows sessions its send tool cannot reach, or this system's frames carry a reply address.
