# Scope agent visibility by topic

## Context

After two [Brokers](../glossary.md) pair, each host may run several sessions doing unrelated work, some of it in sensitive repositories. A rule is needed to decide which local agents a peer's agents can see and which they can message. Pairing is itself a trust decision the user made, and a peer is not a stranger.

## Options Considered

### Visible to all, excluded one by one

Every registered agent is visible to all connected peers, and the user excludes sensitive sessions one by one.

**Rejected**: safety depends on the user remembering to exclude them. Two unrelated sessions being visible to each other serves no purpose, since the agents have no reason to communicate.

### Visible only when declared one by one

A session explicitly declares itself externally visible at startup.

**Rejected**: that decides only whether an agent is visible, not to whom, and it gives no reason to communicate; every declared agent is still visible to every other.

### Distinguish visibility by peer

Specify for each agent which peers it is visible to.

**Rejected**: visibility would be tied to hosts, while the usual reason to communicate is shared work, not a shared host. Every agent would maintain its own peer list.

### Filter by skill tags

The roster carries each agent's skill tags for the caller to filter on.

**Rejected**: tags only help filtering and do not limit visibility; their source also has no definite basis.

### Limit by topic

An agent joins a topic identified by name, and two agents are visible to each other and can message each other only when they share at least one topic. Knowing the name is enough to join, with no administrator, invitation or approval. A topic is private by default.

**Adopted**; see Decision.

## Decision

A topic is the sole scope of visibility and communication permission. An agent in no topic is invisible to other agents. Joining a topic takes only knowing its name.

## Consequences

Visibility follows shared work, not the host: two agents on one host and two agents on different hosts fall under the same rule. The same rule decides both whether one agent can see another and whether it can send to it, and across hosts each end of the link checks it. The roster is organized by topic and carries no skill tags.

**Costs accepted:**

- A private topic's secrecy rests only on its name being hard to guess, a leaked name makes it public, and there is no way to remove a member that has joined.
- An agent that joins a topic is visible to every connected peer that has a member in that topic and cannot be visible to only some of them.
- A topic is not a broadcast channel, so sending to every member of a topic takes a separate call from the caller for each.
