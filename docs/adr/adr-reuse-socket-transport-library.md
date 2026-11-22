# Reuse claude-code-socket-transport to write the inbox socket

## Context

Claude Code's documentation describes how a program can write messages to a session's [inbox socket](../glossary.md) and gives the format of the authentication line, but it does not give the fields of the message frame, the format of the signature wrapper, or the structure of the session registry ([REF-claude-code-cross-session-messaging](../references.md)). These formats can only be derived from Claude Code's actual behavior, and they come with no compatibility promise. The signature wrapper is especially error-prone: the receiver re-serializes the parse result and compares it with the original, so attribute order and whitespace must match exactly.

## Options Considered

### Implement it in-house

The system only needs "connect, write one line, close", so write the frame construction itself.

**Rejected**: frame fields, the exact format of the signature wrapper, the encoding rules for addresses, and the escaping of the closing tag in the body would all have to be rediscovered and verified separately, while ready-made implementations and tests for these details already exist.

### Reuse claude-code-socket-transport

Use this Go library to construct the frame, the signature wrapper, address encoding and writing ([REF-claude-code-socket-transport](../references.md)), vendor the source at a fixed commit into this repository, and keep every call to it in one package.

**Adopted**; see Decision.

## Decision

Use claude-code-socket-transport to construct and write inbox frames, vendor the source at a fixed commit into this repository, and keep calls to it in one package. The session registry is not read through the library: the system depends on only a few of its fields, must keep the previous result when it reads a half-written record, and therefore reads it itself.

## Consequences

The system does not maintain knowledge of Claude Code's internal formats itself. When Claude Code's format changes, the scope of change is limited to updating the vendored source and one package.

**Costs accepted:**

- The library is a personally maintained project, and its protocol implementation is based on Claude Code v2.1.233, behind the version in use; when upstream stops updating it, the vendored source must be maintained here.
- The library's default behavior differs from this system's requirements and must be set explicitly item by item when called: by default it reads and presents the target session's token, the opposite of [ADR-inbox-delivery-without-auth-token](adr-inbox-delivery-without-auth-token.md).
- The library has more functionality than needed, including receiving delivery status, reading the session registry and enumerating all sessions, and the unused parts enter the repository along with the source.
- The library's rule for cleaning the name in a signature differs from current Claude Code: it truncates at 200 bytes, while the Claude Code receiver truncates at 64 code points and does not recognize the whole signature wrapper when cleaning changes the name. Signature generation and validation are therefore the system's own responsibility, not the library's cleaning logic; the system must ensure the signature is unchanged after the receiver's cleaning and still points back uniquely to the sender.

## Invalidation Conditions

When Claude Code publishes the formats of the message frame and the session registry with a compatibility promise, or when the library is no longer compatible with the Claude Code version in use and no one upstream fixes it, this decision no longer holds.
