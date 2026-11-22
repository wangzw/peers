# Identify an agent by its inbox socket, with the name taken from the session name

## Context

A [Broker](../glossary.md) must tell its agents apart, give each agent a name, and separate agents that share a name with a disambiguator. This identity must stay the same for the session's whole life, and the address must match the session the user sees in Claude Code.

Facts observed on Claude Code v2.1.327:

- The session name is given by the user or generated automatically by Claude Code. When a renamed session collides with another live session, Claude Code makes one of them switch to a suffixed name, so collisions on one host are rare, but automatically generated names can still be the same.
- Claude Code's built-in send tool normalizes a name before matching it: it removes control and format characters, trims whitespace, lowercases, and replaces whitespace with `-`. Its built-in session list does not show sessions whose name contains `@` or `*`.
- After a session clears its conversation, the inbox socket path and session name are unchanged, while the session identifier becomes new; the user still sees the same session.
- The identifier in square brackets in the Claude Code session list is computed as the first 12 hex digits of SHA-256 of the string `session:` followed by the inbox socket path, taken as the shortest prefix, at least 6 digits, that distinguishes it among all candidates; it does not change with a rename or with clearing the conversation.

## Options Considered

### Use the session identifier as identity

An agent is distinguished by Claude Code's session identifier.

**Rejected**: the session identifier changes once the session clears its conversation, while the inbox socket, session name and the session the user sees are unchanged.

### Use the process id and process start marker as identity

An agent is distinguished by the session process's process id and start marker, and the disambiguator is derived from the two.

**Rejected**: it needs the process start marker separately, and the derived identifier differs from the one the user sees in the Claude Code list.

### Substitute another name when the session name is unusable

When the session name is empty or contains characters the Claude Code list does not show, use the process id or a generated name as the agent's name.

**Rejected**: the agent address would no longer match the session name the user sees in Claude Code.

### Use the inbox socket path as identity, with a required name and a disambiguator matching Claude Code's

An agent is distinguished by the inbox socket path, its name is the normalized session name, a session with an unusable name is not registered, and the disambiguator is computed from the socket path by the same algorithm as the Claude Code session list.

**Adopted**; see Decision.

## Decision

An agent's identity is the session's inbox socket path. An agent's name is the session name normalized by Claude Code's name-matching rules; it is a precondition for registration, and there is no substitute name. The disambiguator uses the same algorithm as the Claude Code session list.

## Consequences

Clearing the conversation does not affect an agent's address or membership. Several MCP server processes of one session belong to one agent. The session name the user sees in Claude Code is the name other agents see. On a system Broker, the disambiguator of a same-named agent that does not belong to the name-owning account is 12 digits, which may differ from the identifier Claude Code's session list shows for it ([ADR-name-ownership-by-account](adr-name-ownership-by-account.md)).

**Costs accepted:**

- When Claude Code changes its identifier algorithm, or enables a way to compute the identifier from the session identifier, the two identifiers no longer match.
- A session whose name contains `@` or `*`, and a session with no session registry record, cannot take part.
- A rename changes the address, so a party holding the old address must fetch it again.
- On a system Broker, a cross-account name collision is handled by the name-ownership rule: an account that registers later can be found only by an address with the 12-digit disambiguator, and its address and signature no longer match the identifier seen in the Claude Code session list.
