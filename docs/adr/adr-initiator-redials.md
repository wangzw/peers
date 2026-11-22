# Let the side that initiates pairing re-establish the link

## Context

Once a link between two [Broker](../glossary.md) instances that are peers is established it is symmetric in both directions ([ADR-grpc-stream-links](adr-grpc-stream-links.md)), and after it drops one side has to start the connection again. A peer pair should have only one link: roster exchange and directory reconciliation both work per link, so two connections that coexist either repeat the exchange or one of them has to be closed. At pairing the dialer connected to the other side at the address the user gave and with the pairing key, which shows that this direction can connect; the opposite direction may not, because the dialer may sit behind address translation or may not listen at all. After a Broker restart or a network interruption, both ends of the link notice the drop almost at the same time.

## Options Considered

### Either side that knows the other's address may start again, with a rule to choose between two coexisting connections

After pairing each side saves the other's advertised address, and after the link drops every side that knows the other's address connects; when two connections are established at once, a rule the two ends agree on keeps one of them.

**Rejected**: this needs a rule to choose between coexisting connections and a guarantee that both ends reach the same result; the acceptor would also have to save and maintain the dialer's advertised address. Both ends reconnecting at once after the same drop is the norm, so the rule would be used after every drop, not as an exception.

### The side that initiates pairing starts again, and the acceptor only waits

At pairing the two roles are fixed: the side that initiates pairing is the dialer, and the side that accepts pairing is the acceptor. After the link drops only the dialer reconnects, and the acceptor does not connect to the other side on its own.

**Adopted**; see Decision.

## Decision

The side that initiates pairing is the dialer, responsible for establishing the link and establishing it again after a drop; the side that accepts pairing is the acceptor, which does not connect to the other side on its own and only waits for the dialer to connect in. The roles are fixed at pairing, and the same pair of Brokers fixes them again by the new initiator when it pairs again. When the acceptor receives a new connection from the same peer, it takes the new one and closes the old one.

## Consequences

A peer pair has only one link, and the two ends do not need a rule to choose between coexisting connections. The acceptor does not need to know the dialer's address, and the dialer does not report it. That pairing completes shows the dialer can reach the acceptor, and reconnecting follows the same direction. The address the dialer uses to reconnect is told to it by the acceptor ([ADR-advertised-address](adr-advertised-address.md)).

**Costs accepted:**

- When the acceptor changes its address after the link drops and before it is re-established, and the dialer does not get the new address from a broker card, the dialer cannot reach it, the link cannot recover on its own and the pair has to pair again. When the acceptor's address changes, the link usually drops before the next enumeration of local addresses, so the new address is not told in time; an acceptor whose address changes has to configure its advertised address by domain name to avoid this.
- The roles are fixed at pairing. When the side with the stable address is the dialer and the other side moves, the pair has to pair again to swap roles.
- While the link is down, a message the acceptor sends can only wait within its send budget for the dialer to connect in.
- When the dialer unpairs, the acceptor is not notified and still lists the other side as a peer, until the acceptor's user unpairs too.
- When the last step of the pairing handshake is lost, or two Brokers initiate pairing with each other at once using each other's keys, both may take themselves to be the dialer, the link cannot be established because the roles disagree, and the pair has to pair again.

## Invalidation Conditions

This decision stops holding when it becomes common for the acceptor to change its address while the link is down and the link mostly has to be recovered by pairing again.
