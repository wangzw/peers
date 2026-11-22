# Derive the default alias from the hostname and account

## Context

A Broker's [alias](../glossary.md) appears after the `@` in an agent address and is used for display and addressing, not for authentication. When `broker.alias` is not configured, the alias must be derived automatically from the runtime environment. The listen port has nothing to do with a Broker's identity, yet the alias changes with it. A system Broker serves the whole host and a user Broker serves one account, so the two must differ in their default alias; and several Brokers on one host should be recognizable as belonging to the same host when listed.

## Options Considered

### Hostname plus listen port

When there is a listen address, take the system hostname, `-` and the listen port; when there is none, take only the system hostname.

**Rejected**: the alias changes with the listen port, so changing the port changes the agent address that peers see; the port has nothing to do with the Broker's identity and should not be part of the default alias.

### Omit the default port

Agree on one port, omit the port from the alias when it is that port, and still include the port when it is another port.

**Rejected**: the alias still depends on the port, only with a special case for one port; among several Brokers on one host, the one on another port follows a different rule from the rest; changing the port still changes the alias. The port does not enter the alias, so no port gets a special case.

### Parenthesis form

Take `buildbox` for the system form and `buildbox(alice)` for the user form, appending the account name to the hostname in parentheses.

**Rejected**: parentheses are already used in the `hostname(address:port)` address display form, so using them again in an alias makes the two hard to tell apart; passing an alias in a shell needs quoting; and the alias would need a wider character set to hold parentheses, beyond the current letters, digits, `.`, `-` and `_`.

### Account first

The user alias consists of the account name, `.` and the hostname, for example `alice.buildbox`.

**Rejected**: when a peer lists the Brokers on one host sorted by alias, the account-first form scatters the instances of one host to different positions; hostname first keeps them together.

### Hostname first, with the account name appended for the user form

The system form takes the first segment of the hostname, and the user form consists of the first segment of the hostname, `.` and the account name, for example `buildbox` and `buildbox.alice`.

**Adopted**; see Decision.

## Decision

When a Broker has no configured alias, the alias for the system form is the first segment of the host's system hostname (the part before the first `.`), and the alias for the user form consists of the first segment of the hostname, `.` and the owning account name. The alias is independent of the listen address, the listen port and the advertised address. Characters in the hostname and account name that are not in the alias character set are replaced with `-`.

## Consequences

The default alias does not change with the listen port, the listen address or the advertised address. The system Broker on one host and the user Brokers of its accounts get stable aliases distinct from one another, and a user alias also shows the account running it; when sorted by alias, the Brokers on one host are adjacent.

**Costs accepted:**

- After upgrading from v0.1.0, the alias and agent address of a Broker with no configured alias change. A peer handles this under its existing alias-change rule (`REQ-broker-pairing-026`); when the alias conflicts with an existing one, a local alias is generated, and existing agent addresses do not thereby point at another Broker.
- The user alias contains the account name, so an agent address shows the account running the Broker.
- When the first segment of the system hostname is the same on two hosts, the user Brokers with the same account name on them get the same alias. Aliases are not globally unique, and peers tell them apart by local alias.
- The system and user forms use different default alias rules, so both must be implemented and tested separately.

## Invalidation Conditions

The decision no longer holds when the first segment of the system hostname no longer reliably represents one host, or when the default alias must be unique across hosts.
