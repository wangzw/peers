# Keep Brokers private by default

## Context

The directory can pass a [Broker](../glossary.md)'s fingerprint, alias and advertised address to Brokers that have no link with it. Pairing with a host means trusting that host only; it does not mean being willing to let that host's other peers learn about this one. The advertised address points at the host the Broker runs on, and that host may be the user's personal machine.

## Options Considered

### Public by default

A Broker publishes its broker card by default, and one that does not want to be public turns it off.

**Rejected**: without the user knowing, the host's address propagates to the whole connected network with a single pairing.

### Private by default, public only after explicit configuration

A Broker does not publish its card by default and publishes it only after the user explicitly makes it public. A private Broker still receives other Brokers' cards.

**Adopted**; see Decision.

## Decision

A Broker is private by default and publishes its card only after the user explicitly makes it public. A private Broker's information is not passed on to other Brokers by its peers.

## Consequences

Whether the connected network knows about it is an explicit choice made by each Broker's user. A private Broker can still discover public Brokers.

**Costs accepted:**

- A newly installed Broker is found by no one. To be found, it must either be made public or have its advertised address communicated outside the system to the other side by the user.
- A private Broker cannot declare a topic public: the declaration must be signed by the declarer to prevent forgery, and the signature carries its fingerprint.
- Private is not hidden. Its peers know its fingerprint and alias, and a peer that initiates pairing with it also knows its advertised address; a host that can reach its listen port can see its certificate.
