# Default the advertised address to the default route's interface address

## Context

A [Broker](../glossary.md) listens on every local address by default ([ADR-listen-by-default](adr-listen-by-default.md)). Such a Broker has several local addresses and does not know which of them another host can connect to. What needs the advertised address is the side that connects to it: the dialer connects to the acceptor at the address the user gives it, the dialer connects to the other side again after the link drops ([ADR-initiator-redials](adr-initiator-redials.md)), and a Broker that receives a broker card learns from the card where the publisher is. The Broker that receives a card and the publisher's peer are not necessarily on the same network, and the same private address can point to different hosts on different networks. The card is signed by the publisher, and a Broker that handles it on the way cannot change its content ([ADR-signed-directory-reconciliation](adr-signed-directory-reconciliation.md)); the card of a public Broker travels through the whole connected network, and the addresses in it expose which networks the host has joined. Most hosts have no domain name and no port forwarding; a few sit behind a proxy or port forwarding, and other Brokers have to connect to the address and port of the proxy or forwarding device.

## Options Considered

### Let the peer return the address it sees on the connection, and the publisher completes the card from it

The advertised address carries only a port by default. A peer that has a connection to the publisher tells it the publisher address it sees on the connection, and the publisher fills the host into the card from it and signs.

**Rejected**: the one that needs the address is a third party that receives the card, whose network differs from the returning peer's, and the address the returning peer sees may not be reachable from the third party; the returned address is unverified, yet the publisher would sign it into its own card.

### Let the Broker that propagates the card attach the address it observes

The Broker that propagates the card attaches beside it the publisher address it observes on the connection, and the receiver connects to the publisher from it.

**Rejected**: the attached note is not inside the publisher's signature, so a Broker that handles it on the way can change it; the address the attaching party sees may likewise not be reachable from a farther Broker.

### Default to the system host name

When unconfigured, use the system host name plus the listening port as the advertised address, write it into the card and show it when a pairing key is generated.

**Rejected**: the system host name may not resolve from another host.

### Publish the addresses of every local interface and let the connecting side try them one by one

The advertised address includes the address on each local interface by default, and the connecting side tries them one by one, so one of them is bound to work.

**Rejected**: this publishes every network the host has joined to the whole connected network, including the addresses of virtual networks and private networks.

### Publish only the default route's interface address by default, with the option to replace it or append addresses

The advertised address is a set of addresses. When unconfigured it holds only the address on the interface with the default route; the user can replace it with a given address, or append addresses after it. The connecting side tries them one by one in order and checks the certificate fingerprint after connecting.

**Adopted**; see Decision.

## Decision

The advertised address is a set of addresses; when unconfigured it holds only the address on the interface with the default route, each address paired with the listening port, and it can be replaced with given addresses or have addresses appended. The card carries the publisher's whole set of advertised addresses, inside the publisher's signature. The acceptor tells the dialer its whole set of advertised addresses, and the dialer does not report its own addresses. The local address a peer sees on the connection is neither used, saved nor propagated.

## Consequences

Most hosts need no advertised address configuration. The addresses in a card are settled and signed by the publisher itself, regardless of who forwarded the card. By default it publishes only the host's address on the network of the default gateway, and the other networks the host has joined do not travel with the card. A private address repeated across networks does not cause a wrong connection: a host that presents a different fingerprint is abandoned.

Each item in the advertised address list carries a kind, and the only kind so far is direct; a Broker skips an item whose kind it does not recognize, and the card and the rest of the handshake stay valid. Adding a new way to reach a Broker only adds a kind, and a Broker that does not recognize it still accepts and propagates a card that carries it, so Brokers of different versions can coexist.

**Costs accepted:**

- The address on the default route's interface is unreachable outside a network that translates addresses; in that case the advertised address has to be configured as a given address, or addresses have to be appended.
- The card of a public Broker publishes its whole set of advertised addresses to every Broker in the connected network.
- After a local address changes and before the new card reaches everywhere, other Brokers hold the old address.
- On the first pairing there is no card to use, and the user tells the other side the connection address.
- Even when the fingerprint differs, the host the dialer connects to has already seen this side's certificate and fingerprint in the TLS handshake.

## Invalidation Conditions

This decision stops holding when most hosts sit behind address translation and the address on the default route's interface is mostly unreachable.
