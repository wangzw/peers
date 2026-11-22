# Own each name by the account that registered it first

## Context

A system [Broker](../glossary.md) serves several accounts on one host, and agents under different accounts can see each other in one topic. An agent address consists of a name, a disambiguator when needed, `@` and an alias, and without a disambiguator it locates an agent by name alone.

In this form, one account on the host can do three kinds of things to other accounts:

- Interception: after the agent of the account that registered a name leaves, another account registers an agent with the same name; a party that keeps sending to the address without a disambiguator delivers the message into the later account's session.
- Impersonation: another account registers an agent with the same name as the owning account's, and the receiver sees a signature identical to the owning account's agent.
- Identifier collision: the disambiguator is a prefix of `SHA-256("session:" + inbox socket path)`, at least 6 digits. An account can choose socket paths under its own sessions and, with about 2^24 offline computations, make the 6-digit identifier equal the victim's, and after the victim leaves the old address uniquely matches the attacker.

A user Broker serves one account, so none of these cases exists.

The address format and the signature format keep their current form. [ADR-agent-identity-by-inbox-socket](adr-agent-identity-by-inbox-socket.md) requires the disambiguator to use the same algorithm as the Claude Code session list, and this decision does not overturn it.

## Options Considered

### Put the account into the address

Write the address as `name@account@alias` or the like, locating by account and name together.

**Rejected**: it changes the address format, invalidates every address already held, and conflicts with the current composition of an address.

### Own names by account, keeping the address format

The system Broker records which account first registered each name. The first registrant keeps the address without a disambiguator, and other accounts' same-named agents can be found only by an address with a disambiguator; an address without a disambiguator delivers only to the owning account's agent; a name is released after 7 idle days.

**Adopted**; see Decision.

### Declare mutual trust between accounts and tighten admission

An administrator declares which accounts trust each other, allows the same name only between trusting accounts, and decides who may use the system Broker on that basis.

**Rejected**: it adds per-account trust configuration and management operations, and whatever the default, it leaves a gap or makes a common use unusable; it also does not handle identifier collisions within one account.

### Derive the disambiguator with a salt

Derive the disambiguator from the account, a private salt and the socket path, so that others cannot compute it offline.

**Rejected**: it conflicts with the decision in [ADR-agent-identity-by-inbox-socket](adr-agent-identity-by-inbox-socket.md). That decision requires the disambiguator to use the same algorithm as the Claude Code session list, so the identifier a user sees elsewhere matches the one they see in Claude Code; a salted derivation would overturn this. This decision keeps the current algorithm and only makes a same-named agent that does not belong to the owning account, and an agent whose name does not fit and whose address must be shortened, take the full 12-digit identifier. Salted derivation can serve as defense in depth and be reconsidered after that decision is rewritten.

## Decision

Adopt name ownership by account, with the address and signature formats unchanged.

- The system Broker records which account first registered each name. When registering or renaming to a name, if the name has no unexpired owner, it is recorded as the current account; if it has an unexpired owner, it is not changed.
- An agent of the owning account carries no disambiguator in its address and signature when its name is unique under the owning account; a same-named agent of another account always takes the first 12 digits of the agent identifier, whether or not the name looks unique to peers.
- When the name does not fit and the name part of the address must be shortened, the disambiguator is fixed to the first 12 digits of the agent identifier. When an address with a disambiguator is parsed, a shortened name matches only under this full identifier and only when the name in the address equals the name obtained by rendering the address at some legal alias length (1 to 24 bytes) for that agent; the alias length is chosen by the viewer, and the address a peer sees and uses to reply need not be rendered with the local current alias, so it is not required to equal the rendering under the local current view. An identifier shorter than 12 digits matches only an exactly identical name or the name-owning account's agent, and cannot use shortening to bypass ownership.
- An address without a disambiguator resolves among the owning account's agents under the current rules. When the owning account has no visible same-named agent but another account has a visible same-named agent, return `not_found` with text saying the name belongs to another account, without giving the other account's agent address.
- When no agent holds a name, it is released 7 days after the last holder leaves or renames; a held name is not released.
- The owner table is persisted in `name-owners.json` in the data directory, at most 1024 entries per account, with every uid that has no account entry sharing one quota, and at most 65536 entries in all; when it overflows, the oldest unheld entries are evicted first.
- Each item of `list_agents` shows the account name of the registering account, for display only, and it takes no part in addressing or resolution.

## Consequences

Once the owning account has registered a name, another account's agent keeps a 12-digit disambiguator in its address even when it no longer coexists with a same-named agent under its own account, until the ownership is released.

**Costs accepted:**

- The first registrant gets the address without a disambiguator. An account can register a name while no one holds it and so keep the short address of that name for a long time, until it is idle for 7 days.
- After a name has been idle for 7 days, another account can take it, and the previously held address without a disambiguator may thereafter resolve to another account's agent.
- An agent that does not belong to the owning account can still share the victim's 12-digit identifier prefix, needing about 2^48 hashes, or about 2^48 repeated online registrations. The 12-digit identifier is a prefix of the current algorithm, and this decision accepts this residue.
- A membership entry sends the registering account's account name to peers, which discloses a local account name to them. The account name is for display only, but it is still local information visible to peers, and the user accepts this cost (the website's security note is updated separately by the site unit; this unit does not change the website).
- The account name a peer reports about itself in a membership entry is untrusted and is for display only.
- The owner table is a new persistent file. When the file cannot be parsed, the Broker starts from an empty table, the ownership information is lost, and short addresses may be taken by other accounts.
- When the owner table is full (1024 entries for one account, or 65536 entries in all) and all of that account's entries are held, a new name is not recorded, and this name temporarily returns to the old rule without an owner table: the address without a disambiguator may be taken by another account until the table frees space.

## Invalidation Conditions

The decision's premise no longer holds when the address format changes to carry the account, or when [ADR-agent-identity-by-inbox-socket](adr-agent-identity-by-inbox-socket.md) is rewritten to allow a private derivation of the disambiguator.
