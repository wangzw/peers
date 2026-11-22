# Deliver synchronously without buffering or resending

## Context

A send can fail because the sender's [Broker](../glossary.md) cannot reach the receiver's Broker, or because the receiver's Broker cannot write the message to the target session. A write usually fails because the target session has exited. A message between agents is usually a request or a notice for the moment, and a message that arrives later may no longer mean anything.

## Options Considered

### A persistent inbox and outbound queue

The receiver writes the message to disk before delivering it, the sender puts the message into a persistent queue per peer while the peer is offline and resends in order after reconnecting, and when no terminal result arrives within a set time after delivery the message is resent, with a separate notice to the sender on failure.

**Rejected**: the Broker would need persistent storage, a resend state machine and failure notices, and the time for the sender to get a result is uncertain. A late message can trigger the receiver's action at the wrong time.

### A short in-memory queue

Fail at once when the target is offline; when the link drops, queue the message in memory for several minutes and notify the sender of failure on timeout.

**Rejected**: the result is still undetermined when the send call returns, and a later notice is still needed; only the persistent queue is replaced with an in-memory one.

### Retry synchronously and return an error on failure

The send call makes every attempt before it returns. It retries a fixed number of times when the peer is unreachable and a few times when the write to the target session fails; once those are exhausted it returns an error and the system handles the message no further.

**Adopted**; see Decision.

## Decision

Sending is synchronous throughout: after a bounded number of retries the call returns success, not delivered or an unknown result when it cannot tell whether the message was written. The system does not buffer, persist, resend or notify asynchronously.

## Consequences

The Broker stores no messages and has no inbox, outbound queue or failure notice. The result the send call returns is final, and whether to resend is the sending agent's decision.

**Costs accepted:**

- A message sent while the peer is briefly offline fails and is not delivered automatically after the peer recovers.
- The send call blocks until it gets a result, for at most one send's overall time limit.
- When the delivery request has gone out and the reply is lost, the sender gets no definite result and learns only that the message may have been delivered; if the agent resends then, the target session may receive the message twice.
- The retries within one call can make the receiving Broker receive the same message several times, and the receiver must deduplicate by message identifier; the deduplication record is in memory only, so the receiving Broker may write the message again if it restarts between retries.
