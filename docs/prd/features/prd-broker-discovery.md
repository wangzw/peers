# Broker discovery

## Problem Statement

When a user wants to connect to a [Broker](../../glossary.md), they need to know which Brokers are on the network and whether each can be reached. At the same time, a Broker that does not want to be known must not be propagated merely because it paired with someone.

## Scope

This document covers a Broker being public or not, the propagation and expiry of cards within the connected network, and the listing and status of known Brokers.

### Out of Scope

- Propagation of agents and rosters, which [topic](prd-topics.md) specifies.
- Active probing of whether a Broker is reachable.
- Automatically initiating pairing based on discovery results.

## Actors

| Actor | Authority |
|---|---|
| User | Decides whether the local Broker is public. |
| agent | Lists every Broker the local Broker knows and their status. |
| Peer | Passes to this Broker the cards it holds. |

## Functional Requirements

### REQ-broker-discovery-001

A Broker that is not explicitly set public in configuration MUST be not public.

- **Acceptance**: Brokers A and B started with the default configuration are paired, and C is connected to B with no link to A; C's result of listing known Brokers does not contain A.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryPrivateByDefault`

### REQ-broker-discovery-002

Once set public, a Broker MUST publish a card whose content is its fingerprint, alias and advertised address list, and the list may be empty.

- **Acceptance**: After it is set public, the result of a Broker with no link to it listing known Brokers contains it, with the fingerprint, alias and advertised address list matching its actual values; for a public Broker whose listen address is configured empty, its advertised address list in the result is empty.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryCardReachesWholeNetwork`; `test/e2e/discovery_test.go` `TestDiscoveryPublicWithoutListen`

### REQ-broker-discovery-003

A public Broker's card MUST reach every Broker in the connected network, not only its peers.

- **Acceptance**: A is connected to B, B to C and C to D, with no links among the other pairs; after A is set public, D's result of listing known Brokers contains A.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryCardReachesWholeNetwork`; `test/e2e/journey_public_broker_test.go` `TestJourneyConnectPublicBroker/main`

### REQ-broker-discovery-004

A Broker MUST NOT pass any information about another non-public Broker to a Broker other than that Broker's peers.

- **Acceptance**: Non-public A is connected to B and B to C, with no link between A and C; neither the return result of any tool on C nor C's log shows A's fingerprint, alias or advertised address.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryPrivateByDefault`

### REQ-broker-discovery-005

A non-public Broker MUST likewise be able to learn the cards of other Brokers in the connected network.

- **Acceptance**: Non-public A is connected to B, and B holds the card of public Broker C; A's result of listing known Brokers contains C.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryPrivateBrokerLearnsCards`

### REQ-broker-discovery-006

A Broker MUST reject cards whose content a relay has altered, and cards not published by the Broker the card names.

- **Acceptance**: A peer passes a card whose advertised address list was altered, and a card that uses another's fingerprint; the receiver's result of listing known Brokers does not contain either card.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_hostile_test.go` `TestDirectoryRejectsTamperedCards`

### REQ-broker-discovery-007

A card's validity period MUST be 7 days, counted from its publication or most recent renewal.

- **Acceptance**: A card still appears in other Brokers' results on the sixth day after publication; the publisher then stays offline, and on the eighth day the card no longer appears.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_expiry_test.go` `TestCardExpiresAfterSevenDays`

### REQ-broker-discovery-008

An online public Broker MUST renew its card once a day.

- **Acceptance**: A public Broker stays online for 10 days, and during this time its card always appears in other Brokers' results.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_expiry_test.go` `TestCardRenewedDaily`

### REQ-broker-discovery-009

After a Broker changes from public to not public, its card MUST disappear within 60 seconds from the known-Broker results of the Brokers continuously connected to it, and MUST NOT reappear because another Broker still holds the old card. A Broker not connected to it at the time is not covered by this and receives the revocation only after it reconnects and reconciles.

- **Acceptance**: After A becomes public its card reaches C; A changes to not public, and after 60 seconds C's result no longer contains A; then a Broker holding A's old card is reconnected to the network, and C's result still does not contain A.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryRevokeWithin60s`

### REQ-broker-discovery-010

A Broker MUST NOT accept cards from a source other than a peer.

- **Acceptance**: An unpaired Broker cannot make any card appear in this Broker's result of listing known Brokers.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_hostile_test.go` `TestDirectoryRejectsUnpairedSource`

### REQ-broker-discovery-011

The number of records the directory keeps MUST have a configurable cap. After the cap is reached, a card passed by a Broker that has no card yet MUST be rejected, and renewal of stored cards is unaffected.

- **Acceptance**: Set the cap to 3 and have a peer pass 5 distinct valid cards; this Broker's result of listing known Brokers contains 3 of them; when renewals of these 3 cards arrive they are accepted, and they remain in the result past their original validity period.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_expiry_test.go` `TestDirectoryTotalLimitKeepsRenewals`

### REQ-broker-discovery-012

An agent MUST be able to list every Broker the local Broker knows, including all peers and the non-peers whose cards this Broker holds. Each entry gives that Broker's advertised address list; for a peer, it also states whether this side is its dialer or its acceptor.

- **Acceptance**: This Broker has two non-public peers, one paired by this side and the other having paired with this side; this Broker also holds a non-peer's card. The result contains all three Brokers, each entry carrying that Broker's advertised address list, empty for one whose advertised address is unknown; the peer paired by this side is marked as this side being the dialer, and the peer that paired with this side is marked as this side being the acceptor.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-discovery-012 peers listed` covers peers and roles; the non-peer card half is covered by `test/e2e/discovery_test.go` `TestDiscoveryCardReachesWholeNetwork`.

### REQ-broker-discovery-013

Each listed Broker MUST be marked with exactly one status: connected, connectable or unreachable. Connected means there is currently a link to it; connectable means there is currently no link and at least one of its advertised addresses is known; unreachable means there is currently no link and none of its advertised addresses is known.

- **Acceptance**: Construct Brokers for each of these situations, and the status in the result corresponds one-to-one with the situation.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-discovery-013 three statuses`

### REQ-broker-discovery-014

Each listed Broker MUST be marked as to whether it is currently a peer of this Broker.

- **Acceptance**: The Brokers marked as peers in the result exactly match this Broker's current peers; after unpairing, that Broker is no longer marked as a peer.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-discovery-014 peer flag` covers peers; the non-peer half is covered by `test/e2e/discovery_test.go` `TestDiscoveryCardReachesWholeNetwork`.

### REQ-broker-discovery-015

The number of records accepted over one peer MUST have a separate configurable cap, so that a single peer cannot fill the directory.

- **Acceptance**: Set the directory cap to 10 and the per-peer cap to 4; one peer passes 10 valid records and only 4 are accepted, and cards passed later by another peer are still accepted.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_hostile_test.go` `TestDirectoryPerLinkLimit`

### REQ-broker-discovery-016

After a public Broker's advertised address list changes, it MUST republish its card so that other Brokers in the connected network get the new list.

- **Acceptance**: Public Broker A is connected to B and B to C, with no link between A and C; without restarting A, change the address of the interface carrying A's default route from `198.51.100.4` to `198.51.100.9`, and afterward A's advertised address list in C's result of listing known Brokers becomes `198.51.100.9:17443`.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_address_test.go` `TestCardFollowsAddressChange`

### REQ-broker-discovery-017

When a card contains an address kind this Broker does not recognize, the Broker MUST ignore that address and still accept and propagate the card, and the remaining addresses in the card remain usable.

- **Acceptance**: B is connected to C; B's peer A passes a valid card whose advertised address list contains one item of an address kind B does not recognize and one `198.51.100.4:17443`. B's result of listing known Brokers contains the card's publisher with an advertised address list of `198.51.100.4:17443` and a status of connectable; C's result of listing known Brokers likewise contains the publisher.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_hostile_test.go` `TestDirectoryUnknownAddressKind`

### REQ-broker-discovery-018

When a card contains a direct address item written in a nonconforming way, this Broker MUST ignore that address item and still accept and propagate the card, and the remaining addresses in the card remain usable.

- **Acceptance**: B is connected to C; B's peer A passes a valid card whose advertised address list contains one address with an ESC character and one `198.51.100.4:17443`. B's result of listing known Brokers contains the card's publisher with an advertised address list of only `198.51.100.4:17443`; C's result of listing known Brokers likewise contains the publisher.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_hostile_test.go` `TestDirectoryMalformedDirectAddress`; unit tests `TestValidDirectAddr` in `internal/directory/validate_test.go` and `TestBrokersSkipsMalformedDirectAddresses` in `internal/directory/views_test.go`

## Data Requirements

The directory is stored on the local disk and can still be listed after the Broker restarts. A card carries only a fingerprint, an alias and an advertised address list, and no agents, topic membership or configuration.

## Assumptions and Dependencies

- A public Broker's card publishes its advertised address list to every Broker in the connected network.
- Whether a Broker is directly reachable is decided by whether this Broker knows its advertised addresses, and knowing the advertised addresses does not guarantee that the Broker can actually be reached.
- After the local address changes and before the new card reaches everywhere, other Brokers hold the old advertised address list. A card is not a basis for trust, and pairing with a public Broker still needs its pairing key.
