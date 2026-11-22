# Listen by default and keep the address on conflict

## Context

For two [Broker](../glossary.md)s to pair and establish a link, at least one must be listening. The address told to the other side, the broker card and the advertised address recorded in the peer table all carry a port, which by default is the listen port ([ADR-advertised-address](adr-advertised-address.md)), and the side that initiates pairing uses it to dial again after a disconnect ([ADR-initiator-redials](adr-initiator-redials.md)). One host may have several accounts each running a Broker, and they cannot bind the same address and port. An unpaired connection is refused, and listening by itself grants no access.

## Options Considered

### Do not listen by default

A Broker binds no port until configured and can only connect out to other Brokers.

**Rejected**: pairing needs at least one side listening, so not listening by default forces every user to change configuration before anyone can connect to them. Unpaired connections are refused anyway; not listening buys no protection worth the cost.

### Switch to another port when the address is taken

When the configured listen address cannot be bound, the Broker uses another available port.

**Rejected**: the address told to the other side, the broker card and the advertised address saved by peers all carry the port, so switching automatically makes them change on every start, and the addresses already given out and the peers' reconnect targets become invalid.

### Switch to not listening when the address is taken

When the configured listen address cannot be bound, the Broker starts as usual, only it does not listen.

**Rejected**: the user configured listening but the Broker in fact does not listen and does not report it, so others cannot connect while the local side looks fine, and the fault is hard to find. Not listening but reporting the fault and retrying is covered by [ADR-degraded-run](adr-degraded-run.md).

### Make a special case for one port

Agree on a default port and omit it from the alias, the displayed address and so on when that port is used.

**Rejected**: the alias and address of one Broker would be written differently depending on the port value, and among several instances on one host one would always follow a different rule from the rest.

### Listen by default, and do not switch addresses when the address cannot be bound

The default configuration listens on a fixed port on all of the host's addresses, including IPv6 addresses when IPv6 is enabled, so a Broker listens without configuration; that port is an ordinary value in the default configuration and the system gives it no special treatment. A listen address can be specified separately at startup. When the address given in the configuration or at startup cannot be bound, the Broker does not switch ports: a foreground start fails and reports that the address is unavailable, and one with the degraded-run flag enters a degraded run. When the user explicitly configures an empty listen address, the Broker does not listen.

**Adopted**; see Decision.

## Decision

A Broker's default configuration listens on a fixed port on all of the host's addresses, including IPv6 addresses when IPv6 is enabled. The listen address can be changed in the configuration or specified at startup; the system gives no port a special case. When the listen address cannot be bound, the Broker does not switch ports: a foreground start fails, and one with the degraded-run flag does not listen, reports the fault and retries as described in [ADR-degraded-run](adr-degraded-run.md).

## Consequences

A newly started Broker can be reached by other hosts and accept pairing without configuration. The port in the address told to the other side, the broker card and the peer-saved advertised address stays the same across starts.

**Costs accepted:**

- As soon as a Broker starts, the host has one more port open on all of its addresses, and on IPv6 addresses when IPv6 is enabled. Anyone who can connect to it can see its certificate; a user who does not want this must explicitly configure the listen address as empty or restrict it to one address.
- When a second account's Broker on one host runs with the default configuration, its listen address is taken, and that account's user must give it another address or port. On a new install or reinstall, when the preflight finds the address taken by another process, the installation fails and registers no service (`REQ-broker-config-064`). A service already installed enters a degraded run when it starts again, and local agents keep communicating while peers cannot connect in; a foreground start fails, and a tool call in a session returns an error saying no Broker is available, until the user changes the configuration and restarts it.
- The default alias carries no port; see [ADR-default-alias-host-and-account](adr-default-alias-host-and-account.md).
- The listen port is open to any host that can reach it before authentication; such hosts can open unauthenticated connections and use up the unauthenticated-connection allowance. Narrowing this exposure by source is covered by [ADR-unauthenticated-connection-limits](adr-unauthenticated-connection-limits.md).
