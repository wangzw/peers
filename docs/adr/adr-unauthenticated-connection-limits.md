# Limit unauthenticated connections by source

## Context

A [Broker](../glossary.md) listens on every local address by default ([ADR-listen-by-default](adr-listen-by-default.md)). An inbound connection is unauthenticated until the TLS handshake completes and the first frame checks out, so any host that can reach the listening port can establish such a connection and keep it around for a while. When these connections share slots without distinction, one source can crowd out the connections of other sources and of already-paired peers; and before authentication, nothing separately caps the data a single connection can make the host buffer.

## Options Considered

### Share one cap across all unauthenticated connections, closing the oldest when the cap is reached

All unauthenticated connections share one cap, and when the cap is reached the oldest is closed, regardless of source.

**Rejected**: it does not distinguish sources. A source that keeps opening connections can crowd out the connections of other sources and of already-paired peers one by one, while keeping its own connections within the cap.

### Refuse new connections when the table is full

Once the number of unauthenticated connections reaches the cap, every new connection is closed and existing connections are unaffected.

**Rejected**: slots go first come, first served, so a source that fills the cap can hold every slot for a long time, and a new legitimate connection has no chance even to try. An attacker only has to keep established connections alive and need not keep opening them.

### Limit by source and set known peers apart

Divide sources by the network prefix of the other address and give each source its own cap; a connection whose fingerprint is already in this side's peer table once TLS completes moves into a known-peer pool and is not subject to the per-source cap or the global cap. At the same time cap the bytes read from one connection before authentication and the HTTP/2 header list.

**Adopted**; see Decision.

### Limit the acceptance rate by source

Put a token bucket on each source's acceptance rate, and reject or delay connections that exceed the rate.

**Not adopted**: this does not conflict with limiting by source, but it needs a table that grows with the number of sources and has to be cleaned periodically. The per-source cap already confines a single source's rotation to its own slots, so no extra rate limit is needed.

## Decision

Unauthenticated connections are limited by source. The source is the network prefix of the other address: for IPv4 the /32 of that address, for IPv6 the /64 it lies in, an IPv4-mapped IPv6 address is treated as IPv4, the IPv6 zone is dropped, and connections whose address has no IP address are grouped into one source together. The per-source cap is 8 and the global cap is 64. When one source exceeds the per-source cap, the oldest connection of that source is closed; when all sources together exceed the global cap, the oldest connection in the source with the most connections is closed, and when connection counts tie, the source whose oldest connection was established earliest is chosen. A connection whose TLS handshake has completed and whose certificate fingerprint is in this side's peer table moves into the known-peer pool and does not count toward the two caps, with at most 2 per fingerprint, and when that is exceeded the oldest connection of that fingerprint is closed. Before authentication, the bytes read from one connection (including the TLS handshake, the HTTP/2 preface and frames) are capped at 64 KiB, and the HTTP/2 header list at 16 KiB. The message cap after authentication stays at 4 MiB.

## Consequences

A source's unauthenticated connections consume only its own slots and can no longer crowd out the connections of other sources. Once TLS completes and the fingerprint is checked, an already-paired peer moves into the known-peer pool, and a connection from a stranger source can no longer crowd it out. Before authentication, both the bytes a single connection can make the host buffer and the header list size are bounded.

**Costs accepted:**

- When an attacker controls more than 64 sources and keeps rotating them at high speed, it can still make a new pairing from a stranger source fail, and can crowd out an already-paired peer before that peer completes the TCP and TLS handshakes; a paired peer after TLS completes is unaffected. Under IPv6 a user usually holds a /64 alone, and an attacker can obtain several /64s or a larger block.
- Several hosts that share one /64 count as one source and together fall under the cap of 8. Brokers in one LAN or one carrier allocation that reconnect to the same remote at once may crowd out each other's slots and recover through backoff and retry.
- User Brokers under several accounts on the same host that reconnect to the same remote at once share one source, and their TCP and TLS stage combined may exceed 8 connections, with the excess retried after backoff.

## Invalidation Conditions

- When many Brokers behind the same NAT or the same /64 reconnect at once and the per-source cap keeps normal reconnection or pairing from completing for a long time, this decision needs to be re-evaluated.
- If persistent rotation across more than 64 sources becomes a common attack in real deployments, limiting the acceptance rate by source can supplement this decision without changing it.
