# Use direct links between Brokers only

## Context

For two [Broker](../glossary.md)s to communicate, at least one side must be able to connect to the other. Some hosts sit behind NAT and have no advertised address the other side can connect to. Trust between Brokers is established by the user pairing them one pair at a time, and a Broker authenticates only the peers it has paired with.

## Options Considered

### Relay through a third Broker

When A and C have no link, B, which is connected to both, relays the roster and messages. Two hosts both behind NAT can reach each other through one reachable host.

**Rejected**: the link layer would need routing and loop protection. A and C have never paired, so A cannot authenticate C's messages relayed through B; it could only trust B's account of C, or a separate end-to-end authentication would have to be built.

### A centralized registry

Every Broker connects to a central service that maintains a network-wide list of Brokers for each Broker to query for discovery.

**Not adopted**: it needs a separately deployed component. When one of a pair of hosts is reachable, the other initiates pairing and a direct link already covers it. Discovery can be left to a long-running public Broker through the publication and propagation of broker cards; relaying at the connection layer is outside this decision, and the two relayed Brokers still communicate over an end-to-end authenticated link on which the relay cannot see or change the content, so it does not amount to a Broker relaying messages or rosters. Either can be added without changing this decision.

### Direct links only, with discovery left to card propagation

Messages travel only over the link between sender and receiver. A public Broker publishes its card so that a Broker with no link can learn that it exists and its advertised address, and the user then decides whether to pair.

**Adopted**; see Decision.

## Decision

Brokers connect directly only. A Broker does not relay messages or rosters: messages and rosters travel only over the link between two Brokers that are peers. How the underlying connection of a link is established is outside this decision.

## Consequences

A message crosses at most one link, and trust and links correspond one to one: every Broker that can send a message to a local agent is one the user has paired. The link layer needs no routing table, hop count or loop detection.

**Costs accepted:**

- Two hosts that both lack an advertised address the other can connect to cannot communicate.
- When several hosts must all reach one another, the user pairs each pair once.
- A Broker learned from a card cannot communicate directly; it must be paired first.

## Invalidation Conditions

The decision no longer holds when communication between hosts that are both unreachable becomes a common case and relaying at the connection layer cannot satisfy it.
