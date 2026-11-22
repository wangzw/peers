# Refuse peer delivery while the Broker is offline

## Context

A [Broker](../glossary.md) needs a state that lets the user stop receiving messages from other hosts for a while without unpairing or removing links. Links, membership and directory reconciliation must keep working, and neither messages between local agents nor messages sent to a peer may stop. Message delivery is synchronous, so a send call must get a definite result: success means the message was written to the target session, and a write that cannot happen returns not delivered. The receiver must answer every delivery request.

## Options Considered

### Close every link

On going offline, close the links to every peer.

**Rejected**: Peers keep reconnecting with backoff, directory reconciliation and membership exchange stop, and this conflicts with the requirement that links keep working.

### Turn the Broker private

On going offline, make the Broker private.

**Rejected**: Being private affects only the propagation of the broker card and public topic declarations, and paired peers still deliver as before, so it does not achieve the refusal.

### Refuse local messages as well

On going offline, refuse messages between local agents too.

**Rejected**: Local messages cross no link and have nothing to do with a temporary stop on messages from other hosts; refusing them also keeps sessions on the same host from working together.

### Silently drop peer deliveries and return success

**Rejected**: Success means the message was written to the target session, and a silent drop makes the sender believe the message was delivered, violating the send-result contract.

### Refuse only peer deliveries and report an error code

**Adopted**; see Decision.

## Decision

The Broker adds a boolean `broker.offline` kept in configuration. When it is true, the Broker answers `broker_offline` after receiving a peer's delivery request and before writing the dedup table; the description carries this host's effective alias and the reason for the refusal, with this Broker as the source Broker. The message does not enter the dedup table, no audit is written, and one run log line is recorded at Debug level. Links are established and re-established, topic membership is exchanged, directory reconciliation and new pairings all complete as usual. Sends between local agents and sends to a peer do not check this item. A peer learns that this Broker is offline from `broker_offline` in the send result.

## Consequences

The user can pause receiving messages from other hosts while keeping links and pairings. The sender gets a definite refusal rather than a timeout or an unknown result.

**Costs accepted:**

- Agents on the offline side can send messages but receive no reply from the other side; two-way communication requires going online first.
- Link state, the directory and the result of `list_brokers` do not reflect going offline, and a peer learns it only from a refused send; finding out whether a peer is offline takes sending a message.
- A sender that has not been upgraded treats an unknown error code as `internal` and can see the reason only from the description text.
- Going offline writes no audit and leaves only a Debug-level run log line; frequent deliveries leave no audit record when refused.
