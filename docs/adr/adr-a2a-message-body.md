# Reuse A2A's Message structure as the link message body

## Context

What the system passes between [Brokers](../glossary.md) is plain text, and both the sender and the receiver are Claude Code sessions. Interoperation between agents already has an open protocol, A2A ([REF-a2a](../references.md)), which defines data structures such as Message and HTTP-based endpoints. The first phase's scope is cross-host communication between Claude Code sessions and excludes interoperation with other kinds of agents, but the design must leave room for interoperation with A2A agents later. A link message format is hard to replace once Brokers of several versions coexist.

## Options Considered

### Provide the standard A2A endpoints

The Broker implements A2A's HTTP endpoints, and external A2A agents can connect directly.

**Not adopted**: It exceeds the scope of cross-host communication between Claude Code sessions, since the endpoints, discovery documents and task semantics would all have to be implemented and verified. This option is not mutually exclusive with the adopted one.

### A custom message body

The body of a link delivery request is a single text field.

**Rejected**: On A2A interoperation the link message body would need a different structure, and Brokers of the old and new versions would need a conversion.

### Reuse only the Message data structure

The body of a link delivery request is an A2A Message with a single text part. The system implements none of A2A's endpoints, discovery or task semantics.

**Adopted**; see Decision.

## Decision

The body of a link delivery request uses A2A's Message structure with a single text part; the system implements no other part of the A2A protocol.

## Consequences

When A2A endpoints are provided, the link message body does not need a different structure.

**Costs accepted:**

- The system cannot interoperate with external A2A agents, because reusing a data structure is not protocol compatibility.
- The message body has one more layer than a text field, and only its text part is used.
- The Message structure evolves with A2A versions, the system stays fixed on the version it adopted, and interoperation may still need adaptation.

## Invalidation Conditions

This decision's "implement no other part" stops holding for the endpoints and discovery that message exchange needs, once a use case appears that calls for interoperation with A2A agents that are not Claude Code sessions; that interoperation is limited to exchanging messages, a reply is another message, and it involves none of A2A's tasks, streaming or push notifications.
