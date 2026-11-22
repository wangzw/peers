# Discover and connect to a public Broker

## Who

The user on host C, who wants to communicate with an agent on a host A that is not yet paired; host A's user cooperates to complete the pairing. Each person has a Claude Code session with this system's tools enabled on their own host and can approve tool calls in that session. Both accounts have write permission on the configuration file of the Broker their sessions register with.

## Precondition

- A's [Broker](../../glossary.md) is connected to B's Broker, C's Broker is connected to B's Broker, and A and C are not yet paired.
- A's Broker is set to public, listens with the default configuration, and C can reach the address of the interface A's default route uses.
- On A, an agent has joined the topic `infra-help` publicly, with a description.
- There is a trusted channel outside the system between A's user and C's user.

## Steps

1. C's user asks the agent to list Brokers and sees A's alias, fingerprint and advertised address list, with a status of connectable and not a peer.
2. C's user asks the agent to list topics and sees the public topic `infra-help` and its description.
3. C's user contacts A's user through a channel outside the system and asks to pair.
4. A's user asks their agent to generate a one-time pairing key, approves it, and hands the key to C's user.
5. C's user asks the agent to initiate pairing with A's advertised address seen at step 1 and this key, and after approval sees the pairing succeed, with A's fingerprint in the result matching the one seen at step 1.
6. C's user asks the agent to join `infra-help`.
7. C's user asks the agent to list the visible agents and sees the agent on A that joined `infra-help`.
8. C's user asks the agent to send it a message and sees the successful send result.

## Outcome

C and A are each other's peers and are connected. The agent on C and the agent on A that share `infra-help` see each other and can exchange messages.

## Alternate Paths

- At step 5 the other side actually presents a fingerprint different from the one seen at step 1: C's user sees a fingerprint warning in the pairing result and decides from it whether to unpair.
- A's listen address is configured empty: at step 1 A's advertised address list is empty and the status is unreachable. Pairing is then initiated by A's user: C's user generates a pairing key and hands it and C's Broker's address to A's user, and this address must be one A can reach.

## Requirements Exercised

- `REQ-broker-discovery-002`
- `REQ-broker-discovery-003`
- `REQ-broker-discovery-012`
- `REQ-broker-discovery-013`
- `REQ-broker-discovery-014`
- `REQ-topics-001`
- `REQ-topics-005`
- `REQ-topics-007`
- `REQ-topics-010`
- `REQ-topics-011`
- `REQ-topics-013`
- `REQ-broker-pairing-005`
- `REQ-broker-pairing-010`
- `REQ-broker-pairing-023`
- `REQ-agent-messaging-008`
- `REQ-agent-messaging-013`

## End-to-End Test

`test/e2e/journey_public_broker_test.go` `TestJourneyConnectPublicBroker`
