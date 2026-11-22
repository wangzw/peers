# Propagate public information as signed records through hop-by-hop reconciliation

## Context

To connect to a [Broker](../glossary.md), a user first has to know that it exists and how to connect to it. The system has no registry; Brokers have only links established pairwise. The broker card of a public Broker and a public topic's declaration have to become known across the whole connected network, and the Brokers that handle them along the way must not be able to alter them. Rosters and agent information are more sensitive and should be sent only to peers that are already connected.

## Options Considered

### Propagate only one hop

A Broker tells its other peers only about the peers it is directly linked with, and passes nothing further.

**Rejected**: public then becomes public to only some Brokers, and which ones depends on how the links happen to be connected. A public Broker should be visible across the whole connected network.

### Gossip-style propagation

A Broker that receives new information randomly picks several peers and forwards it, for some rounds and then stops ([REF-epidemic-algorithms](../references.md)).

**Rejected**: it does not guarantee that every Broker eventually receives it. The network is small and information changes little, so there is no need to trade reliability for propagation speed.

### Propagate the roster with the card

Propagate agent information together with the card, so that Brokers with no link can also see agents elsewhere.

**Rejected**: agent information should be sent only to peers that are already connected.

### Propagate online status

Propagate and probe whether each other is online between Brokers.

**Rejected**: whether a link is established is each Broker's own local fact, and whether a Broker is directly reachable is stated by whether its card has an advertised address; neither needs to agree across the network.

### Signed records with hop-by-hop reconciliation

Public information is represented as records signed by the publisher with a version number and an expiry time, and the publisher re-signs them periodically. Brokers reconcile pairwise over their links, filling in the missing and updating the older from each other, and pass newly accepted records on to their other peers. A withdrawal is represented by publishing a signed record with a higher version. The reconciliation and withdrawal approach is taken from anti-entropy and the death certificate in REF-epidemic-algorithms.

**Adopted**; see Decision.

## Decision

A card and a public topic's declaration are represented as records signed by the publisher with a version number and an expiry time, reconciled hop by hop over links and spread across the whole connected network; the publisher re-signs periodically, and a withdrawal is represented by a signed record with a higher version. The roster and online status do not propagate with the records.

## Consequences

Public Brokers and public topics eventually become visible everywhere in the connected network, and the Brokers that handle them along the way cannot tamper with or forge them. Once a publisher has been offline longer than the validity period, its records disappear on their own.

**Costs accepted:**

- A public Broker's advertised address is visible to every Broker in the connected network, including those the user does not know.
- A withdrawal is not an immediate network-wide deletion: each Broker keeps the withdrawal record until it expires on its own, and the old record stays visible until the withdrawal reaches that Broker.
- Expiry is judged by each Broker's local clock, so clock skew makes the same record disappear at different times in different places. The version number also comes from the publisher's clock, and the publisher must ensure that the version numbers it issues only increase; it cannot simply take the current time when the clock goes backwards.
- A signature proves only that a record was issued by the publisher it carries, not that the publisher is trustworthy. Any peer can produce a large number of validly signed records, so directory capacity must be limited both per source link and per publisher; otherwise one peer could fill the whole network's directory. The per-source-link limit is counted on the link that carries a forwarded record after the second hop: a large number of records produced by one peer and relayed through another Broker will fill that link's limit, causing legitimate new records relayed through it to be refused. Replacing a record with the same identifier is not subject to the count limit, but each Broker accepts it at most once every 60 seconds, so a publisher that keeps re-signing at a high rate can propagate only at that rate. The identifier of a new version held back by the replacement interval is recorded as pending refetch, and it is fetched once when the existing version has been accepted for a full 60 seconds, so legitimate consecutive changes are delayed by at most one replacement interval. The same holds for withdrawals: the existing version was accepted before the withdrawal was signed, its 60-second acceptance moment is no later than 60 seconds after the withdrawal was signed, it is fetched immediately on expiry, and the withdrawal takes effect on the Broker that receives it directly within 60 seconds of signing. A withdrawal propagates hop by hop, and each hop waits no longer than one replacement interval; the second hop and beyond add the propagation delay of the preceding hops, so a withdrawal does not take effect network-wide within 60 seconds of signing, and it takes effect later the farther it is from the publisher.
- Every Broker stores the whole network's public records, and the record count grows with the number of public Brokers and public topics.
- A card is only a hint: whether the address a card carries is actually reachable, and whether the other side is willing to pair, are not guaranteed.
