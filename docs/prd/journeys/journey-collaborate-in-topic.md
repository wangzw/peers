# Collaborate across hosts in one topic

## Who

The user on host A and the user on host B, each working in the same project repository on their own host, able to approve tool calls in their sessions.

## Precondition

- Host A's [Broker](../../glossary.md) and host B's Broker are paired and connected. Both Brokers run as system Brokers with the default configuration, have no alias configured and no local alias set; the system hostnames of the two hosts are `hostA` and `hostB`, so A calls B's Broker by the alias `hostB` and B calls A's Broker by the alias `hostA`.
- Both hosts have a workspace containing the project repository, and the `origin` remotes of the two workspaces point to the same repository, possibly spelled differently.
- The project configuration file declares no topic and does not turn off auto-join.

## Steps

1. A's user starts a session with this system's tools enabled in the workspace, named `api-worker`.
2. B's user starts a session with this system's tools enabled in the workspace, named `frontend`.
3. A's user asks the agent to list the visible agents, and sees `frontend@hostB` and the shared topic, named after the repository address.
4. A's user asks the agent to send a message to `frontend@hostB` saying that an interface has changed.
5. A's user sees the successful send result.
6. B's user sees their idle session start processing a message signed by `api-worker@hostA` on its own.
7. After B's session finishes processing, it sends a reply to `api-worker@hostA`.
8. A's user sees their session receive a message signed by `frontend@hostB`.

## Outcome

The sessions on the two hosts see each other without any topic operation, and a message and its reply each entered the other's session, every message carrying the sender's agent address.

## Alternate Paths

- B's session is busy at step 6: the message is read in a gap in or after the session's current work, and the result A sees at step 5 is unchanged.
- B's session's inbound policy is to wait for user approval: A still sees success at step 5, B's user sees a message awaiting approval, and if B's user does not approve, A receives no notification and no reply.
- The two workspaces' `origin` remotes point to different repositories: the list at step 3 does not include the other side. Once either user learns the name of the other's topic outside the system and asks the agent to join that topic, the flow continues from step 3.

## Requirements Exercised

- `REQ-agent-messaging-001`
- `REQ-agent-messaging-003`
- `REQ-agent-messaging-004`
- `REQ-agent-messaging-008`
- `REQ-agent-messaging-009`
- `REQ-agent-messaging-013`
- `REQ-agent-messaging-015`
- `REQ-agent-messaging-016`
- `REQ-agent-messaging-021`
- `REQ-agent-messaging-022`
- `REQ-agent-messaging-024`
- `REQ-agent-messaging-025`
- `REQ-topics-001`
- `REQ-topics-005`
- `REQ-topics-007`
- `REQ-topics-016`
- `REQ-topics-017`
- `REQ-broker-pairing-002`

## End-to-End Test

`test/e2e/journey_collaborate_test.go` `TestJourneyCollaborateInTopic`

Among the alternate paths above, "B's session's inbound policy is to wait for user approval" is not covered by this journey's end-to-end test; it depends on a real Claude Code session, and the related verification awaits user approval.
