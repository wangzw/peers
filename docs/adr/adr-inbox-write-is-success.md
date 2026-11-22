# Count a successful inbox socket write as sent

## Context

After a message is written to the target session's [inbox socket](../glossary.md), Claude Code may hand it to the model at once, queue it because the session is busy, hold it for user approval or drop it under the inbound policy ([REF-claude-code-cross-session-messaging](../references.md)). When the sender includes a local socket in the message as a reply address, the session reports these states back to it, and a refusal or expiry may occur several minutes after the write ([REF-claude-code-socket-transport](../references.md)). Sending is a synchronous call, so the point at which it returns and what it returns must be settled.

## Options Considered

### Wait for the terminal state before returning

The send call waits until the session reports the message delivered, refused or expired.

**Rejected**: When a message is held for user approval, the call blocks until the user acts or the message expires, which can take minutes.

### Wait briefly and return the first state

After the write, wait a few seconds and return the first state received to the sender; on timeout return an unknown state, and report no later change.

**Rejected**: The target session may be working on a previous turn, so a state obtained shortly after the write does not show how the message was finally handled. The receiving Broker would also have to bind a local socket for each message to receive the state. Whether the other side handled the message is upper-layer semantics, expressed by the other agent's reply.

### Bind a proxy inbox for a remote agent

The receiving Broker binds a local socket for each remote agent, serving as the message's reply address, both receiving the reported states and letting the target session reply directly with Claude Code's built-in send tool.

**Rejected**: The Broker would bind and maintain a local socket for each remote agent, while a reply can be made with this system's send tool.

### Return success on a successful write

When the message is written to the target session's inbox socket, the send call returns success, carries no reply address and receives no reported state. A reply is another message the target agent sends on its own.

**Adopted**; see Decision.

## Decision

A message written to the target session's inbox socket counts as sent. The system ignores the delivery state the session reports and binds no local socket for a remote agent.

## Consequences

A successful send means the same as with Claude Code's built-in cross-session send: the message reached the session, which does not mean the model read it. After the write, the receiving Broker keeps no state about the message beyond the deduplication record.

**Costs accepted:**

- The sender cannot tell whether the message was handed to the model, held for user approval or dropped under the inbound policy. When the target session is set to refuse, the message disappears silently.
- After reading a frame, the receiving side also applies per-sender rate limiting and deduplication, and a dropped message is not reported to the sender: a message over the rate limit and the second of two identical messages one agent sends to the same session within 30 seconds both disappear silently after the send has already returned success ([ADR-no-broker-rate-limiting](adr-no-broker-rate-limiting.md)).
- The sender learns that the message was handled only from the other side's reply, and the other side may not reply.
- A message the target session receives carries no reply address, so the model must use this system's send tool and address the reply to the agent address in the signature; whether the model does so depends on how the tool description works, and its default is to reply with Claude Code's built-in send tool, which cannot reach such an address.
