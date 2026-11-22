# Carry links over a gRPC bidirectional stream

## Context

Two [Broker](../glossary.md) instances that are peers need a long-lived connection, started by one of them ([ADR-initiator-redials](adr-initiator-redials.md)); once it is established, both sides have to be able to send the other the roster, the records in the directory and delivery requests on their own initiative, and have to be able to match a request with its response. The connection has to be encrypted and has to detect connections that have become dead. The outbound network of the deployment environment places no restriction on the protocol.

## Options Considered

### WebSocket over TLS

Each text frame is a JSON object. It uses only the HTTP/1.1 upgrade mechanism and can pass through an outbound firewall that allows only HTTPS and a proxy that supports only HTTP/1.1.

**Rejected**: the frame format, the distinction between types and the matching of request with response all have to be defined and checked in-house. Its strength is passing through restricted networks, and the deployment environment has no such restriction.

### A gRPC bidirectional stream

A single bidirectional streaming method carries every frame, the frames are defined with Protocol Buffers, and gRPC provides flow control and keepalive while its transport provides TLS.

**Adopted**; see Decision.

## Decision

A link is a gRPC bidirectional stream carried over TLS, one stream between two Brokers that are peers.

## Consequences

Frames have typed definitions, and adding a frame type does not affect existing frames. Flow control, keepalive probes and connection reuse do not have to be implemented in-house. The local interface also uses gRPC, so the system has one remote call mechanism throughout.

**Costs accepted:**

- gRPC depends on HTTP/2. When the outbound side has a proxy that supports only HTTP/1.1, or a proxy that intercepts TLS, the link cannot be established.

## Invalidation Conditions

This decision stops holding when the target deployment environment has an outbound network that allows only HTTP/1.1 or intercepts TLS.
