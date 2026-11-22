# Limit authenticated peers by source

## Context

A Broker authorizes a peer to exchange the roster of agents that share a topic and directory records, and to deliver messages to visible agents. Authentication establishes only who the peer is; it does not carry a right to occupy the local host's resources without limit. An established link can keep sending state frames and request frames, and can keep delivering messages, and these are the normal channels after authentication.

Without a cap, the link's read side leaves the state frame queue unbounded and spawns a handler goroutine for every request frame; the dedup table shares a single whole-table cap of 65536 items; and delivery has no quota divided by source. A single paired peer can therefore make the host run out of memory for its state frames, spawn unbounded goroutines for its requests, fill the dedup table so that delivery for every other peer returns busy, or drain the token bucket that the Claude Code receiver shares by writing process so that the session drops messages from other sources. [ADR-no-broker-rate-limiting](adr-no-broker-rate-limiting.md) identifies the sender only by writing process, and the receiver cannot tell which peer a delivery comes from, so limiting by source can only be done by the Broker. [ADR-unauthenticated-connection-limits](adr-unauthenticated-connection-limits.md) covers connections before authentication; this record covers peers after it.

## Options Considered

### Trust paired peers and set no cap

Treat an authenticated peer as fully trusted and set no cap on the memory, goroutines, dedup table entries or receiver quota it occupies.

**Rejected**: paired does not mean trusted. A peer that runs out of control or turns malicious can exhaust the host's resources and, through the shared dedup table and token bucket, affect every other peer, which is exactly the problem the audit pointed out.

### Stop reading to apply back pressure, without disconnecting

When a cap is exceeded, do not close the connection but only stop reading frames, letting the transport's flow control carry the pressure back to the peer.

**Rejected**: when state frames exceed the cap, stopping reading stalls the responses queued behind them, and a call in a state frame handler that waits for a response on the same link deadlocks with it. The link protocol also has no generic busy response, and adding a frame type requires raising the protocol version. When request frames exceed the cap, reading may stop, because no existing request handler waits for a response on the same link; when state frames exceed the cap, the only option is to close the connection.

### Use one global shared cap

Do not meter by source, and let all peers share one state frame queue cap, one request concurrency cap and one dedup table cap.

**Rejected**: first come, first served. Once one peer fills the shared cap, every other peer and every legitimate burst is affected, which is exactly the problem the audit pointed out.

### Limit by source, closing on a state frame overflow and stopping reading on a request overflow

Meter the link state frame queue, request handler concurrency, dedup table entries and delivery quota separately by source; close the connection when state frames overflow, and have the routing goroutine stop reading the connection when request handler slots run out.

**Adopted**; see Decision.

### Divide the delivery quota by remote agent name

Meter the delivery quota by remote agent name instead of by source peer.

**Rejected**: a remote agent name is self-reported by the peer, so dividing by it lets the peer decide for itself how much quota it may use.

### Add a separate error code or busy response frame for over-limit

Add a dedicated error code for exceeding a cap, or add a response frame that means "busy".

**Rejected**: adding a frame requires raising the protocol version. Adding an error code either gives an existing value a new meaning or adds an enum value and regenerates the proto; the former makes the same value mean different failures in different versions, and the latter requires upgrading the sender's and receiver's local protocol, error code allowlist and message together. Neither is necessary: a full cap and the directory's record-count limit are both quota exhaustion, so the existing `quota_exceeded` can be reused, with no new proto value, and the MCP side already has a message for this error code and can report it to the model as it is.

## Decision

Authenticated peers are limited by source: the Broker limits separately, for each peer, the link state frame queue, concurrent request handling, dedup table entries and session quota it may occupy, and exceeding any one cap affects only that peer. A single connection's state frames, queued and being handled together, come to at most 4096 frames and at most 64 MiB of payload in total, and exceeding either cap closes the connection; each peer runs at most 64 request handlers at once, the peer's several connections share that allotment, a new connection waits for a slot while the old handlers of the connection it replaces are still running, and reconnecting cannot stack up a second share of concurrency; the dedup table keeps its whole-table cap of 65536 items, with at most 4096 items per source; and each (source peer fingerprint, target agent) pair has a token bucket with capacity 10 that refills 1 item every 10 seconds, and no message is written to the target session when no token is available. A token bucket is removed by peer when the pair is unpaired and by target when the agent ends, and is not removed when a link drops, so a reconnecting peer cannot refresh its quota; a full bucket counts as no bucket and is dropped by the token-taking path, and the table size does not exceed the number of paired peers times the number of local agents. Over-limit reuses `quota_exceeded`; when the whole table is full it keeps `inbox_unwritable`, with the explanation `Not sent: the Broker is temporarily holding too many messages`. These values are constants, and no configuration item is added.

## Consequences

A peer that exceeds a cap loses only its own connection, dedup table share and session quota, and other peers and other local agents are unaffected. When state frames exceed the cap the connection is closed rather than left unread, so a call in a state frame handler that waits for a response does not deadlock. When request handler slots run out, reading stops without adding a frame, so the protocol version stays the same. Reusing `quota_exceeded` adds no proto value and needs no code regeneration; this version's sender has already included this code in the set of errors a peer may return, so what the receiver says is passed on to the model as `Not sent: Broker <alias> said: ...`, rather than reported as `delivery_unknown` for an error outside the set.

**Costs accepted:**

- Several peers that collude can still together drain the token bucket the Claude Code receiver shares by writing process. Limiting by source only holds a single peer to about one third of the receiver's capacity and about one fifth of its refill rate (receiver capacity 30 and 0.5 per second, single peer 10 and 1 per 10 seconds).
- The dedup table's whole-table cap is still 65536 items, with 4096 per source, so 16 colluding malicious peers can still fill it. After that every peer's new message id is refused with `inbox_unwritable`, explained as `Not sent: the Broker is temporarily holding too many messages`, until entries expire or are cleaned. This failure is bounded and retryable, and the peers taking part must already be paired.
- A legitimate peer's burst that exceeds the state frame cap is disconnected and reconnects after backoff. The cap is chosen to be far above a normal burst at link establishment (roster and directory reconciliation), but unusually large legitimate traffic can still be disconnected wrongly.
- These values are tied to the Claude Code receiver's rules. After the receiver adjusts its capacity or refill rate, the share left for a source may become outdated.
- The dedup table counts by source and the token buckets are in-memory state, cleared when the Broker restarts; this adds bookkeeping to the delivery path.

## Invalidation Conditions

- When a normal peer's startup burst or directory reconciliation exceeds the state frame cap in measurement and is disconnected, the cap's values are no longer appropriate.
- When the Claude Code receiver's token bucket capacity or refill rate is adjusted so that it no longer holds a fixed proportion to a single peer's share, or falls below the 10-item capacity and 1 per 10 seconds reserved here for a single peer.
- When a single peer needs more than 4096 recent message records for legitimate use, or more than 10 items of burst delivery quota.
- When several peers colluding to drain the shared token bucket becomes a common problem in real deployments, and a shared quota mechanism on the sending side or across peers has to be added.
