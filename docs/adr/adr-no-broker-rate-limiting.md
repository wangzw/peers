# Do not rate-limit or detect message loops in the Broker

## Context

Two agents can keep replying to each other and consume both sides' model usage. On the receiving side Claude Code rate-limits cross-session messages per sender, drops duplicate messages within a short interval and caps the number of pending messages queued; its built-in cross-session send tool refuses to send when the target session's inbox is full, and it imposes no other send-rate limit ([REF-claude-code-cross-session-messaging](../references.md)). Messages from this system enter a session through the [inbox socket](../glossary.md), so these receiving-side mechanisms apply to them as well.

The receiving-side rules observed on Claude Code v2.1.327 are as follows, and Claude Code can adjust the numbers remotely:

- Identifying the sender: by the reply address when the frame has one, otherwise by the process id of the writing process.
- Rate limiting: one token bucket per sender, with capacity 30, refilling 0.5 per second.
- Deduplication: when the same sender sends a message identical to its previous one within 30 seconds, the later one is dropped without notifying the sender.
- Queueing: at most 50 unprocessed cross-session messages.
- Loop detection: Claude Code's own messages record the path they have traveled in the signature wrapper, and loop detection relies on that record.

These rules identify the sender by the writing process, and every message the Broker delivers enters the session through the same writing process, so the receiving side cannot tell which paired peer they come from. A paired peer can also run out of control or be malicious: one peer can fill the token bucket the receiving side prepares for this writing process, so messages from other sources are dropped after their send returned success. Only the Broker knows which peer each delivery came from, so limiting by source can only be done by the Broker.

## Options Considered

### Have the Broker rate-limit per sender and detect loops

The Broker caps each local agent's send rate, messages carry a hop count and a call chain, and an over-limit or looping message is refused.

**Rejected**: A message crosses one link only, so a hop count means nothing; call-chain detection cannot stop two agents talking back and forth, which is not a loop. Adding a send-rate cap on the sending side would make this system's sending behave differently from Claude Code's built-in send tool, while the receiving side already rate-limits.

### Do not rate-limit in the Broker

The Broker imposes no send-rate limit and carries no hop count or call chain. The receiving side's inbound controls constrain loops, and the tool description asks the model not to reply merely to thank or acknowledge.

**Adopted**; see Decision.

## Decision

The Broker does not rate-limit sends or detect message loops; Claude Code's receiving-side inbound controls carry both. For deliveries from a peer, the Broker limits by (peer, session), so one peer cannot exhaust the quota the receiving side shares, as recorded in [ADR-peer-resource-limits](adr-peer-resource-limits.md).

## Consequences

Messages on a link carry no hop count or call chain. Send limits match Claude Code's built-in cross-session send.

**Costs accepted:**

- Messages this system writes carry no reply address, so the receiving side identifies the sender by the writing process: every message written through the same user Broker, or through the same account helper process of a system Broker, belongs to one sender regardless of which agent it came from, and shares one token bucket. The Broker's inbound limit lets each peer take at most a part of it, but several peers colluding can still exhaust the bucket.
- When one agent sends two identical messages to the same session within 30 seconds, the receiving side drops the later one even though the send call already returned success. Different signatures make different content, so different agents do not cause each other's messages to be dropped this way.
- Messages from this system carry no record of the path they traveled, so Claude Code's built-in loop detection does not act on them; the system has no loop defense of its own, and only the receiving side's rate limiting constrains loops.
- These receiving-side rules and numbers are not a public commitment, and the system's behavior changes when Claude Code adjusts them.
- Whether the model follows the request not to reply merely to thank and not to resend identical content within a short interval depends on how the tool description works.

## Invalidation Conditions

This decision stops holding once measurement shows the receiving side's rate limiting is not enough to stop two agents from going back and forth. The Broker's inbound limit already handles one peer squeezing the shared quota; when the limit's numbers fall out of step with Claude Code's receiving-side rules, those numbers need re-evaluation.
