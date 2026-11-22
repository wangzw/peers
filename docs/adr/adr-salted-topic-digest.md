# Compare shared topics by a salted digest

## Context

The two ends of a link should exchange rosters only for the [topics](../glossary.md) where both have members, and the name of a private topic that only one side has should stay unknown to the other. A default topic's name is the repository address, which is easy to guess. A link's peer is a Broker the user paired, but it may be compromised or itself malicious; echoing back a received set of digests takes no topic name, so the digests themselves must not become a way for a peer to obtain a local roster.

## Options Considered

### Exchange topic names in plain text

The two sides report to each other the names of the topics where they have members and take the intersection.

**Rejected**: a private topic one side alone holds would hand its name straight to the other side.

### Exchange fixed digests

The two sides report digests of topic names, and equal digests mean equal names.

**Rejected**: the digests do not change with the link, so a peer can precompute digests of common repository addresses and compare them offline, and can replay digests obtained on one link on another.

### Exchange a salted digest over only the salt and the topic name

When the link is established the two sides agree on a salt and exchange a digest over "salt plus topic name".

**Rejected**: the digest does not distinguish direction, so a peer that echoes back the received digest set unchanged matches exactly the set the local side computed for it. The local side then sends a roster for every topic, and the peer can forge members under those digests without knowing any topic name at all.

### Exchange a salted digest bound to the sender's fingerprint

When the link is established the two sides agree on a salt and exchange a digest over the salt, the sender's fingerprint and the topic name. The sender's fingerprint is the sender's certificate fingerprint, the same value the peer sees on the link.

**Adopted**; see Decision.

### Private set intersection

The two sides compute their shared topics directly by private set intersection and exchange no digest at all.

**Rejected**: an offline dictionary attack is not the threat this decision guards against. A digest is valid only for one establishment of one link and in one direction, a peer can already probe guessed names online, which is listed as an accepted cost, and the cryptographic components and complexity that private set intersection introduces buy no extra protection against a paired Broker.

## Decision

Adopt two digests per topic, one for each direction, both `SHA-256(salt ‖ sender fingerprint ‖ topic name)`, where `‖` denotes concatenation. A side sending computes with its own fingerprint, and a side comparing received digests computes the local topic of the same name with the peer's fingerprint; each side computes the intersection on its own and exchanges rosters only for the topics in it. The salt is fixed at 64 bytes and the fingerprint at 32 bytes, so the concatenation is unambiguous.

## Consequences

A peer cannot derive from a digest a topic name this side alone holds. A digest is bound to the salt agreed for this link and the sender's fingerprint: it cannot be precomputed, cannot be replayed on another link, and a digest for one direction cannot match in the other. A received digest set echoed back does not intersect the set the local side computed for the peer, so no topic is added and no member can be forged under those digests.

The digest field's meaning changed and the link protocol version rose with it. A Broker of this version cannot establish a link with 0.5.0 or earlier, and each side refuses the connection as it does for the existing version-incompatible state; on the acceptor side `list_brokers` shows version incompatibility with the peer's version, while the dialer side shows only version incompatibility.

**Costs accepted:**

- It does not prevent probing a specific name: a peer can have its own agent join a guessed name and see whether the digests match, confirming whether this side has a member in it. A peer is a paired Broker and a topic name is not treated as a secret, so this is accepted.
- Every rebuild of a link re-exchanges all digests and rosters.
