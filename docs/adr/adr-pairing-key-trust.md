# Establish trust between self-signed certificates with a pairing key

## Context

Two [Broker](../glossary.md) instances do not know each other on their first connection: each has to confirm that the other is the one the user wants to connect to, and has to prevent a man in the middle from impersonating it. Connections after that have to be encrypted and have to recognize the other side. The user starts pairing from within a session. The information pairing needs has to pass from one user to another and should be easy to copy and paste.

## Options Considered

### A team certificate authority

Each Broker holds a certificate issued by a team certificate authority, and on connection both sides verify the certificate chain and the host name.

**Not adopted**: this needs a facility that issues and distributes certificates first, and a new host has to request a certificate before it joins. The existing use case is two users establishing trust pair by pair, and a pairing key already suffices; the facility is not needed. It can be added as another trust source beside the pairing key and does not replace this decision.

### Ship the full certificate fingerprint with the key and use a longer key

The acceptor gives the dialer the full certificate fingerprint together with a 128-bit key. The dialer verifies the other side's certificate directly by the fingerprint, and the key serves only to prove the dialer's own identity to the other side.

**Rejected**: what has to be passed is too long to copy and paste easily.

### A short code plus a password-authenticated key exchange

The user types a short code by hand, and the handshake uses a password-authenticated key exchange protocol resistant to offline guessing.

**Rejected**: a short code has too little randomness and can only be compensated for by a dedicated protocol, which is complex to implement. The pairing key and the address already have to be copied to the other side, and copying and pasting is the norm, so nothing has to be short enough to type by hand.

### A medium-length key plus two-way fingerprint binding

Each Broker uses a self-signed certificate and takes the public key fingerprint as its identity. The acceptor generates a random 80-bit key, and the user hands it to the dialer together with the acceptor's address, with the key and the address passed separately. In the handshake each side uses the key to compute a message authentication code over "its own fingerprint and the fingerprint it sees for the other side" and shows it to the other; once that passes, both sides save the other's fingerprint and recognize only fingerprints after that.

**Adopted**; see Decision.

## Decision

A Broker uses a self-signed certificate and takes its fingerprint as its identity; the first connection uses the pairing key for mutual proof that binds both sides' fingerprints, and after that each side recognizes only the saved fingerprint.

For the pre-provisioned static key among pairing keys, which can be reused, see [ADR-static-pairing-key](adr-static-pairing-key.md).

## Consequences

No certificate authority is needed. A user can complete pairing from within a session, and the pairing key is a piece of copyable text. An alias serves only for display, and the default alias carries no port, see [ADR-default-alias-host-and-account](adr-default-alias-host-and-account.md); neither the alias nor the advertised address takes part in authentication.

**Costs accepted:**

- The pairing key has to travel through a trusted channel outside the system, and eavesdropping on that channel amounts to leaking the key.
- A one-time pairing key has 16 characters and is awkward to pass on by voice or to type by hand.
- There is no unified revocation path: when a host is no longer trusted, it has to be unpaired separately on every Broker that was paired with it.
- Pairing one Broker means trusting the names and topics of the agents it reports under itself; the system cannot verify facts on the other host.
- A self-signed certificate has to be shown before the fingerprint is checked: the certificate of a listening Broker is visible to any host that can reach the listening port, so the fingerprint of a Broker that is not public may be learned by those hosts too.

## Invalidation Conditions

This decision stops holding when pairing mostly happens inside an organization that already has a certificate authority and is no longer started by a user from within a session on the spot.
