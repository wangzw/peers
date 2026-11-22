# Two hosts pair for the first time

## Who

The user on host A and the user on host B, who may be the same person. Each person has a Claude Code session with this system's tools enabled on their own host and can approve tool calls in that session. Both accounts have write permission on the configuration file of the Broker their sessions register with.

## Precondition

- The [Broker](../../glossary.md) on both hosts is running, and they are not yet paired with each other.
- Host A's Broker listens with the default configuration, and host B can reach the address of the interface A's default route uses. Host B may be behind NAT.
- There is a trusted channel outside the system between the two users that can carry a pairing key.

## Steps

1. A's user asks the agent in the session to generate a one-time pairing key, and approves it in the permission confirmation.
2. A's user sees the pairing key and its expiry time given by the agent, along with the hostname and address of A's Broker.
3. A's user hands the address and the pairing key to B's user through a channel outside the system; the two may go through different channels.
4. B's user asks the agent in the session to initiate pairing with this address and pairing key, and approves it in the permission confirmation.
5. B's user sees the successful pairing result, which carries A's alias and fingerprint.
6. Each user asks the agent to list Brokers, and both see the other marked as a peer with a status of connected. B's user sees A's advertised address list and sees that this side is the dialer; A's user sees that this side is the acceptor.

## Outcome

A and B are each other's peers and are connected.

## Alternate Paths

- The pairing key is used only after 10 minutes: B's user sees the key expired error at step 5, and neither side stores the other's identity. A's user starts again from step 1.
- A's alias is the same as that of a peer B already has: B's user sees a result asking for a local alias at step 5, and after specifying one the pairing completes.
- B cannot reach the address shown at step 2, for example because A is behind port forwarding: A's user hands an address B can reach to B's user at step 3, and B's user uses it at step 4.
- The network between the two hosts goes down and comes back after pairing: without any action by either side, the two users list Brokers again and both see the other's status as connected.

## Requirements Exercised

- `REQ-broker-pairing-005`
- `REQ-broker-pairing-006`
- `REQ-broker-pairing-010`
- `REQ-broker-pairing-011`
- `REQ-broker-pairing-004`
- `REQ-broker-pairing-018`
- `REQ-broker-pairing-028`
- `REQ-broker-discovery-012`
- `REQ-broker-discovery-013`
- `REQ-broker-discovery-014`
- `REQ-broker-config-028`
- `REQ-broker-config-030`

## End-to-End Test

`test/e2e/journey_pair_test.go` `TestJourneyPairBrokers`

Among the alternate paths above, "The pairing key is used only after 10 minutes" is not covered by this journey's end-to-end test and is covered by the unit tests of `internal/pairing`. Verification that depends on a real Claude Code session awaits user approval.
