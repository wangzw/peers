# Agent messaging

## Problem Statement

Claude Code sessions on different hosts need to know which sessions they can communicate with and to deliver a text message into the other's session.

## Scope

This document covers how a session registers as an [agent](../../glossary.md), agent naming and addressing, the agent list, sending and delivery results for point-to-point messages, the scope of a message when the user asks to send to all agents or to broadcast, which send tool the model is directed to use for an agent that Claude Code's built-in cross-session send tool can reach, and the refusal of peer messages while a Broker is offline.

### Out of Scope

- The visibility rules themselves, which [topic](prd-topics.md) specifies.
- The system's own broadcast capability, file transfer, status reporting after delivery, offline redelivery, and sender-side rate limiting and loop detection.

## Actors

| Actor | Authority |
|---|---|
| agent | Lists the visible agents and sends messages to visible agents. |
| User | Approves or rejects an agent's send through Claude Code's permission confirmation, and through the receiving session's inbound controls decides whether to receive messages. |

## Functional Requirements

### REQ-agent-messaging-001

When a session enables this system's tools and its session name is usable, the system MUST register it as a local agent without extra user action.

- **Acceptance**: Start a session with the tools enabled; when this session calls the capability to list agents, the return result contains its own agent address.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-001 self address in list`

### REQ-agent-messaging-002

A session that has not enabled this system's tools MUST NOT appear in any agent's list and MUST NOT be a send target.

- **Acceptance**: Start a session without the tools on the same host; other agents' lists do not contain it, and a send targeting its session name returns an error.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-002 session without tools`

### REQ-agent-messaging-003

An agent's name MUST be the result of normalizing its Claude Code session's session name by the rules Claude Code uses to match session names: remove control and format characters, trim leading and trailing whitespace, convert to lowercase, and replace whitespace with `-`.

- **Acceptance**: Start a session named `api-worker` and the name part of its agent address is `api-worker`; start a session named ` API Worker ` and the name part is `api-worker`.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-003 name normalization`

### REQ-agent-messaging-004

An agent address MUST be made up in order of these parts: the agent's name; the disambiguator when the name is not enough to uniquely identify the agent on its Broker, when the name belongs to another account, or when the name cannot appear verbatim in a signature or does not fit; `@`; and the alias the Broker of the party seeing the address uses for the Broker hosting the agent.

- **Acceptance**: Host A calls host B's Broker by the alias `hostB`. When there is only one agent named `api-worker` on B, the address seen on A is `api-worker@hostB`; when there are two agents of the same name and the same account on B, the two addresses seen on A both start with `api-worker` and end with `@hostB`, each carrying a different disambiguator in between; when two same-named agents belong to different accounts, the one belonging to the account that owns the name carries no disambiguator, and the other account's carries the 12-character disambiguator (`REQ-agent-messaging-047`).
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-004 remote refs`

### REQ-agent-messaging-005

After a session is renamed, the system MUST use the new name as the agent's name, and the old address becomes invalid.

- **Acceptance**: After the session is renamed, another agent on the same host lists agents and the result shows the new name; a send targeting the old address returns an error.
- **Status**: Implemented
- **E2E test**: `test/e2e/session_lifecycle_test.go` `TestLocalSessionChanges/REQ-agent-messaging-005 rename`

### REQ-agent-messaging-006

An agent's disambiguator MUST use the same algorithm Claude Code uses to display the identifier for the same session in its session list, computed from the session's inbox socket path; when the candidate set is the same the two values are equal, and when the candidate set differs this Broker's value is a prefix of that identifier. It MUST stay the same after the session is renamed and its conversation is cleared. On a system Broker, a same-named agent whose name belongs to another account takes a 12-character disambiguator.

- **Acceptance**: Under Claude Code's default configuration, start a session with the tools enabled named `a<b`; in the list its address carries a disambiguator, and this disambiguator is a prefix of the identifier in square brackets for that session in Claude Code's session list; rename the session to `c<d` and then clear the conversation, and its disambiguator is unchanged.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-006 ref algorithm`

### REQ-agent-messaging-007

When the send target's name corresponds to several agents on the target Broker and carries no disambiguator, the send MUST fail and state that it is ambiguous.

- **Acceptance**: Sending to two same-named agents by name alone returns an error that indicates the ambiguity; sending with one of their disambiguators succeeds, and only that agent receives it.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-007 ambiguous`

### REQ-agent-messaging-008

An agent MUST be able to list all online agents visible to it, including those on the local host and those on connected peer Brokers. Visibility is specified by `REQ-topics-005`.

- **Acceptance**: On two connected hosts, one agent each is in some topic, and either side's list contains the other.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-008 lists remote`

### REQ-agent-messaging-009

Each entry in the list MUST give the agent's address and the topics it shares with the caller.

- **Acceptance**: The caller and some agent share topics `t1` and `t2`; the entry's address is correct and the shared topics are exactly `t1` and `t2`.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-009 shared topics`

### REQ-agent-messaging-010

When the list gives an agent's status, the value MUST be taken verbatim from that session's status in Claude Code's session registry.

- **Acceptance**: One agent's session is running a lengthy tool call and another agent's session is idle; when the list gives the statuses of these two entries, the one running the tool call is busy and the idle one is idle.
- **Status**: Implemented
- **E2E test**: `test/e2e/session_lifecycle_test.go` `TestLocalSessionChanges/REQ-agent-messaging-010 status`

### REQ-agent-messaging-011

The list result MUST contain the caller's own agent address.

- **Acceptance**: Any agent calling the list produces a result with an address marked as the caller itself, whose name part equals the normalized session name.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-011 self entry`

### REQ-agent-messaging-012

After a session ends, the system MUST remove its agent from the local list and the lists of connected peers within 5 seconds.

- **Acceptance**: End an agent's session; 5 seconds later call the list once locally and once on a peer, and neither result contains that agent.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-012 removal reaches peer`; `test/e2e/session_lifecycle_test.go` `TestLocalSessionChanges/REQ-agent-messaging-012 local end`

### REQ-agent-messaging-013

An agent MUST be able to send a plain-text message to an agent address.

- **Acceptance**: Send text targeting the address of some visible agent, and this text appears in that agent's session.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-013 plain text`

### REQ-agent-messaging-014

When the sender and the target cannot see each other, the send MUST fail and the message MUST NOT enter the target session.

- **Acceptance**: Send between two agents that share no topic; the sender gets an error and the message does not appear in the target session.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-014 not visible`

### REQ-agent-messaging-015

A send call MUST return success only after the message has been handed to the target session's inbox socket. Success does not mean the model in the target session has read or processed the message.

- **Acceptance**: Set the target session's inbound policy to wait for user approval and send to it; at the moment the call returns success, the message has appeared in the target session's list awaiting approval.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-015 success only after inbox`

### REQ-agent-messaging-016

After a send call returns, the system MUST NOT send the sender any follow-up notification about this message.

- **Acceptance**: Send to a session that leaves the message for user approval and the sender gets success; whether the user then approves, rejects or ignores it, the sender's session shows no notification from the system.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-016 no follow-up`

### REQ-agent-messaging-017

When the Broker hosting the target is unreachable, the send call MUST return a not delivered error within the send time limit; the send time limit is given by the Broker's configuration.

- **Acceptance**: Configure both Brokers' send time limits to 10 seconds, disconnect the network between the two hosts and wait for the link to go down, then send one message each from the dialer's host and the acceptor's host; both calls return a peer unreachable error within 10 seconds, and after the network is restored neither message appears in the target session.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_remote_test.go` `TestRemoteSendFaults/REQ-agent-messaging-017 unreachable within budget`

### REQ-agent-messaging-018

When the Broker hosting the target cannot write the message into the target session, the send call MUST return a not delivered error within the send time limit.

- **Acceptance**: Configure the sender Broker's send time limit to 10 seconds; send to a target session that has exited but has not yet been removed from the list, and the call returns an error stating the target session is not writable within 10 seconds.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-018 inbox unwritable`

### REQ-agent-messaging-019

After a send call returns a not delivered error, the system MUST NOT deliver this message at any later time.

- **Acceptance**: Send while the link is down and get a not delivered error; after the network is restored the message does not appear in the target session.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_remote_test.go` `TestRemoteSendFaults/REQ-agent-messaging-019 never delivered later`

### REQ-agent-messaging-020

Without restarting the receiving Broker, one send call MUST NOT make the target session receive more than one copy of the message.

- **Acceptance**: Make the Broker hosting the target write successfully but lose its response on the way back; without restarting the receiving Broker during this time, the sender's call returns after an internal retry, and the message appears only once in the target session.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_remote_test.go` `TestRemoteSendFaults/REQ-agent-messaging-020 reply lost, one copy`

### REQ-agent-messaging-021

When a message admitted by inbound controls reaches an idle target session, the system's delivery MUST make the session start processing the message without user input.

- **Acceptance**: Send to an idle session whose inbound policy is to receive, and the session starts a new turn on its own and reads the message.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-021 system side`

### REQ-agent-messaging-022

The message the target session sees MUST be signed with the sender's agent address, in the form as seen from the Broker hosting the target (`REQ-agent-messaging-004`).

- **Acceptance**: Host B calls host A's Broker by the alias `hostA`; a message sent by `api-worker` on A shows the sender in the target session on B as `api-worker@hostA`.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-022 signature from receiver view`

### REQ-agent-messaging-023

A message body MUST NOT change the signature the target session sees.

- **Acceptance**: When the body contains a forged signature marker, the sender the target session sees is still the real sender's agent address; the envelope start and end tags in the body, case-insensitively, all appear as ordinary text.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-023 forged envelope`

### REQ-agent-messaging-024

Messages the system delivers MUST be subject to the target session's inbound controls ([REF-claude-code-cross-session-messaging](../../references.md)).

- **Acceptance**: When the target session's inbound policy is to refuse, the message does not enter that session; when the target session runs in a mode that skips permission confirmation and has no inbound policy set, the message waits for user approval.
- **Status**: Implemented
- **E2E test**: `test/realsession/probes/inbound.sh` (run through `.github/workflows/real-session.yml` pushing `probe/inbound`)

### REQ-agent-messaging-025

A receiving agent MUST be able to reply to the sender through this system's send capability, targeting the agent address in the message's signature.

- **Acceptance**: The target agent uses the address in the signature verbatim as the target to send a reply, and a message signed with the target agent's address appears in the original sender's session; the same holds when the sender has a same-named agent on its Broker.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-025 reply by signature`

### REQ-agent-messaging-026

When a message together with its signature, encoded as one line written to the inbox socket, exceeds 1 000 000 bytes, the system MUST reject it at the sender and state that the cap is exceeded, and MUST NOT attempt delivery; the signature is taken as the longest signature the delivery might use. This cap counts encoded bytes; Claude Code's built-in cross-session send refuses a body of about one million characters ([REF-claude-code-cross-session-messaging](../../references.md)), so for a multibyte body this system's limit is stricter.

- **Acceptance**: Send a message that encodes to 1 000 001 bytes with the longest possible signature; the call returns an error stating the cap is exceeded, and the Broker hosting the target receives no delivery request; a message that encodes to 1 000 000 bytes with the longest possible signature is sent successfully.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_remote_test.go` `TestRemoteSendFaults/REQ-agent-messaging-026 size limit`

### REQ-agent-messaging-027

When the system cannot determine whether the message has been written to the target session, the send call MUST return a result different from not delivered and state that the message may already have been delivered.

- **Acceptance**: Make the Broker hosting the target write successfully and make its response and every retry's response fail to reach the sender; the call's result marks the delivery as unknown rather than not delivered.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_remote_test.go` `TestRemoteSendFaults/REQ-agent-messaging-027 delivery unknown`

### REQ-agent-messaging-028

Between two agents on the same host that can see each other, the send behavior MUST be the same as across hosts.

- **Acceptance**: Two agents in the same topic on the same host exchange messages through this system, and the meaning of signatures, success and errors is the same as across hosts.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_local_test.go` `TestLocalSend/REQ-agent-messaging-028 same as remote`

### REQ-agent-messaging-029

When an agent's name differs from the original after the receiving session cleans the signature, both that agent's address and its signature MUST carry the disambiguator.

- **Acceptance**: An agent started with the session name `a<b` sends a message to an agent on another host; the target session sees a signature carrying the disambiguator, and the target agent replies verbatim to the signature and the reply reaches the `a<b` session; when there is another agent named `ab` on the same host, the reply does not reach it.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-029 cleaned name gets ref`

### REQ-agent-messaging-030

A session whose session name is unusable MUST NOT register as an agent, the system MUST NOT substitute another name, and it MUST state the reason to that session. A session name is unusable when it is empty after normalization, empty after signature cleaning, or contains `@` or `*`. The part of a session name beyond 128 bytes after normalization is truncated. When a registered session is renamed to an unusable name, its registration is revoked.

- **Acceptance**: Start a session with the tools enabled named `a@b`: other agents' lists show no entry for it; when it calls list agents it gets an error stating that the session name contains `@` and so cannot register. After renaming the session to `ab`, an entry named `ab` appears in other agents' lists. Start a session with the tools enabled whose name exceeds 128 bytes: an entry for it appears in other agents' lists, its name is no more than 128 bytes, and the truncation falls on a UTF-8 character boundary; the same truncated name appears in the agent list on the paired peer Broker.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-030 unusable names`, and `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-030 long name is truncated for both sides`

### REQ-agent-messaging-031

When a send targeting an agent's previous name fails, the error MUST give the agent's current name.

- **Acceptance**: Rename the session of agent `api-worker` to `api-v2`, then immediately send to `api-worker@hostA`; the returned error gives `api-v2`, and the message does not enter that session.
- **Status**: Implemented
- **E2E test**: `test/e2e/session_lifecycle_test.go` `TestLocalSessionChanges/REQ-agent-messaging-031 old name error`

### REQ-agent-messaging-032

When listing agents, an agent renamed within the last 10 minutes MUST note its previous name.

- **Acceptance**: Rename an agent's session from `api-worker` to `api-v2`; another agent that can see it immediately lists agents, and the entry's name is `api-v2` and notes the previous name `api-worker`.
- **Status**: Implemented
- **E2E test**: `test/e2e/session_lifecycle_test.go` `TestLocalSessionChanges/REQ-agent-messaging-032 previous name`

### REQ-agent-messaging-033

After a session clears its conversation, its agent's address and topic membership MUST stay unchanged, and messages that arrived earlier and have not been processed MUST NOT be lost because of the clear.

- **Acceptance**: An agent that joined topic `t1` clears the conversation in its session; other agents' lists show its address and shared topic unchanged, and a message sent to the original address enters the session; when the session is busy and a message arrives, then the conversation is cleared, the clear takes effect only after this arrived message is processed, and this message is not lost.
- **Status**: Implemented
- **E2E test**: `test/realsession/probes/ref-clear.sh` (run through `.github/workflows/real-session.yml` pushing `probe/ref-clear`)

### REQ-agent-messaging-034

After the user disables this system's MCP service in a session, that session's agent MUST be deregistered; after it is re-enabled the session MUST register again with the same agent identity and name, the address's disambiguator is computed from the agent set at that time, and it rejoins only the default topic, with topics manually joined before disabling not restored.

- **Acceptance**: Disable this system's MCP service in the session and another agent immediately lists agents, and the result does not contain it; after re-enabling, it reappears in the list with the same name as before disabling, and the topics manually joined before disabling no longer appear.
- **Status**: Implemented
- **E2E test**: `test/e2e/session_lifecycle_test.go` `TestLocalSessionChanges/REQ-agent-messaging-034 disable and enable`

### REQ-agent-messaging-035

A call a subagent initiates through this system's tools MUST be made under the agent identity of its main session, and a subagent MUST NOT become an independent agent.

- **Acceptance**: A subagent in a session calls send, and the sender the target session sees is the main session's agent address; while the subagent runs, other agents' lists have only one entry for that session. The same holds when a subagent's definition declares this system's MCP service itself.
- **Status**: Implemented
- **E2E test**: `test/e2e/registration_test.go` `TestLocalRegistration/REQ-agent-messaging-035 second MCP process`

### REQ-agent-messaging-036

A session running headless or in a host such as an IDE, after enabling this system's tools, MUST become an agent just like an interactive session.

- **Acceptance**: Run a session with the tools enabled headless or as the main session in an IDE; while it runs, the list of another agent in the same topic contains it, and a message it sends to that agent enters the other's session.
- **Status**: Implemented
- **E2E test**: `test/realsession/probes/headless.sh` (run through `.github/workflows/real-session.yml` pushing `probe/headless`)

### REQ-agent-messaging-037

A system-generated signature MUST be recognizable by the receiving session as the sender. When the agent address exceeds the length that can be signed, the system MUST shorten the name part and append the full 12-character disambiguator, so that the signature still points uniquely to the sender. When parsing an address with a disambiguator, the name part matches if it equals the shortened name the agent renders under some valid alias length (1 to 24 bytes), and is not required to equal the result the receiver renders under its own alias: when the two hosts set different alias lengths for each other, the truncation points differ, and a reply to the signature verbatim MUST still be delivered.

- **Acceptance**: An agent started with a 70-character session name sends a message to an agent on another host; the target session shows the sender with a signature carrying the 12-character disambiguator, whose name part is the start of the session name; the target agent replies verbatim to the signature and the reply reaches the original sender.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-agent-messaging-037 truncated signature`

### REQ-agent-messaging-038

When the local Broker is unavailable or registration has not completed, a tool call MUST return an error stating the reason, and the session MUST NOT thereby require the user to reconnect the MCP service.

- **Acceptance**: Make another process occupy the default listen address and start this account's Broker in the terminal; startup fails and states that the listen address is unavailable; then start a session with the tools enabled: Claude Code shows this system's MCP service as connected, and a tool call returns an error stating that there is no available Broker and how to start one. End the process occupying the address and start the Broker; without reconnecting the MCP service in Claude Code, the next agent listing succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/broker_process_test.go` `TestBrokerProcessInContainer/REQ-agent-messaging-038 recover without reconnect`

### REQ-agent-messaging-039

An offline Broker MUST refuse every message from peers with `broker_offline`, writing to no session.

- **Acceptance**: After B goes offline, an agent on A that shares a topic with an agent on B sends to it; the call returns an error starting with `Not sent` and with error code `broker_offline`, stating that B reported it; the target session on B has not received the message. After B comes online the same send succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/offline_test.go` `TestBrokerOffline/REQ-agent-messaging-039 peer delivery is refused`

### REQ-agent-messaging-040

While a Broker is offline, the link between it and its peers, re-establishing the link, exchanging topic membership and reconciling the directory MUST go on as usual, and new pairings MUST also complete as usual.

- **Acceptance**: After B goes offline, when A lists Brokers B is still connected; disconnect and restore the network between the two hosts, and the link is re-established; an agent on B that newly joins a shared topic appears in the list of an agent on A; when B is public, C can still receive B's card through A. Another host initiates pairing with B using a valid pairing key and succeeds, and the link is established; a send to an agent on B is still refused per `REQ-agent-messaging-039`.
- **Status**: Implemented
- **E2E test**: `test/e2e/offline_test.go` `TestBrokerOffline/REQ-agent-messaging-040 links and topics continue`, `TestBrokerOffline/pairing works while offline` and `TestBrokerOfflineDirectory` (three nodes: after the public peer goes offline, a third Broker still receives its card and newly public topics through the shared peer)

### REQ-agent-messaging-041

A Broker being offline MUST NOT affect sends between local agents, nor sends from local agents to agents on peers.

- **Acceptance**: After B goes offline, two agents on B exchange messages successfully; an agent on B sends to an agent on A successfully; an agent on A replies to an agent on B and is refused per `REQ-agent-messaging-039`.
- **Status**: Implemented
- **E2E test**: `test/e2e/offline_test.go` `TestBrokerOffline/REQ-agent-messaging-041 local and outgoing sends continue`

### REQ-agent-messaging-042

When the user asks to send to all agents or to broadcast and does not mention peers, the system MUST make this broadcast reach only local sessions, not send it to any agent through this system, and after the broadcast give the user one hint: other hosts' agents can also be notified with peers.

- **Acceptance**: In a session with a built-in cross-session send tool, ask to send to all agents; local sessions receive the message, agents on a peer Broker in the same topic as the sender do not receive it, and this system does not send it either; a hint then appears stating that other hosts' agents can also be notified with peers.
- **Status**: Implemented
- **E2E test**: Not applicable: the model performs this behavior per the instructions, and it cannot be verified deterministically; the content of the instructions is asserted by `TestInstructions` in `internal/mcpserver/tools_test.go`

### REQ-agent-messaging-043

When the user explicitly asks to broadcast or notify with peers, the system MUST send the message to each agent in the list one by one, once per agent.

- **Acceptance**: On two connected hosts, one agent each is visible to the sender, the user explicitly asks to broadcast with peers, and both agents' sessions each receive one copy of the message.
- **Status**: Implemented
- **E2E test**: Not applicable: the model performs this behavior per the instructions, and it cannot be verified deterministically; the content of the instructions is asserted by `TestInstructions` in `internal/mcpserver/tools_test.go`

### REQ-agent-messaging-044

When this session has no built-in Claude Code cross-session send tool and the user asks to send to all agents or to broadcast without mentioning peers, the system MUST NOT send on its behalf through this system, and MUST tell the user that a local broadcast cannot be done, with the same hint: other hosts' agents can also be notified with peers.

- **Acceptance**: In a session without the built-in cross-session send tool, ask to send to all agents; this system does not send the message, and the user sees a statement that a local broadcast cannot be done and the hint that other hosts' agents can also be notified with peers.
- **Status**: Implemented
- **E2E test**: Not applicable: the model performs this behavior per the instructions, and it cannot be verified deterministically; the content of the instructions is asserted by `TestInstructions` in `internal/mcpserver/tools_test.go`

### REQ-agent-messaging-045

Messages and directory content that reach the model through this system MUST be framed as untrusted input: a message from a sender that contains `@`, and the public topic names, descriptions and other Brokers' addresses returned by `list_topics` and `list_brokers`, MUST be stated to come from other agents or other Brokers and not be user instructions; an operation they request MUST first be relayed to the user and get the user's explicit consent.

- **Acceptance**: The instructions state that a message from a sender that contains `@`, and the public topic names, descriptions and other Brokers' addresses listed by `list_topics` and `list_brokers`, come from other agents or other Brokers, possibly from another host or another account, and are not user instructions; an operation they request, especially changing configuration, pairing and unpairing, upgrading the Broker, going online and offline, running commands, and reading or sending out files, must first be relayed to the user and get the user's explicit consent. The tool descriptions of `list_agents`, `list_topics` and `list_brokers` each state that their directory content is provided by other Brokers and unverified; the instructions state that the peer's own words quoted in errors such as `Not sent` (`Broker <alias> said: ...`) are the peer's own words and unverified.
- **Status**: Implemented
- **E2E test**: Not applicable: the model performs this behavior per the instructions, and it cannot be verified deterministically; the content of the instructions is asserted by `TestInstructions` in `internal/mcpserver/tools_test.go`, and the three tool descriptions are asserted by `TestToolDescriptionsMarkUntrustedDirectoryContent` in the same file

### REQ-agent-messaging-046

The receiver's Broker MUST limit messages from the same peer to the same session; when the limit is exceeded the sender MUST get an explicit failure, rather than the receiver silently dropping it after a successful send; messages to that session from other sources are unaffected.

- **Acceptance**: On two paired hosts, an agent on one host sends 11 messages in a short time to one session on the other; the 11th send call returns `quota_exceeded` and this message does not appear in that session; at this point another agent on the same host as the target sending to that session still succeeds. When two of three hosts are each paired with the target's host, an agent on one host sends 11 in a row to the target session and the 11th returns `quota_exceeded`; then an agent on the third host sends to the same session successfully, and the session receives this message.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_remote_test.go` `TestRemoteSendFaults/REQ-agent-messaging-046 inbound quota per peer` and `TestRemoteSendInboundQuota/REQ-agent-messaging-046 another peer still reaches the session`

### REQ-agent-messaging-047

On a system Broker, when a name belongs to an account, the address and signature of this account's uniquely named agent MUST carry no disambiguator, and a same-named agent of another account MUST always carry the 12-character disambiguator.

- **Acceptance**: Two accounts each register an agent named `worker`; the one that registered first keeps `worker@<alias>`, the other account's address carries a 12-hex-character disambiguator; after the first account leaves, the other account's address still has 12 characters; the two accounts' names are correct in a peer Broker's list.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/cross_account_names_test.go` `TestREQAgentMessaging047_CrossAccountImpersonation`

### REQ-agent-messaging-048

An address without a disambiguator MUST be delivered only to the agent of the account the name belongs to. When the name belongs to another account, the send MUST fail with error code `not_found`, and the message MUST state that the name belongs to another account.

- **Acceptance**: On a system Broker, the owning account's agent registers `worker` and leaves, and another account registers `worker`. A third agent on the same Broker and the peer host send to `worker@<alias>` over the link, both return `not_found` with a message saying the name belongs to another account, and the other account holding the name receives neither; sending with that account's 12-character disambiguator delivers.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/cross_account_names_test.go` `TestREQAgentMessaging048_CrossAccountInterception`

### REQ-agent-messaging-049

A name MUST be released 7 days after the owning account no longer holds it, after which the account that registers the name takes ownership; it is not released while the owning account still holds it.

- **Acceptance**: Account A's agent registers `worker` and leaves; at 7 days minus 1 second the owner is still A; after a full 7 days and one expiry scan, account B, which currently holds `worker`, takes ownership. While A still has an agent named `worker`, no amount of time releases it.
- **Status**: Implemented
- **E2E test**: Not applicable: the expiry of the name-owner table is verified in unit tests with an injected clock, independent of real time; covered by `TestNameOwnerExpiresAfterSevenDays`, `TestNameOwnerScanExpiresIdleEntry` and `TestNameOwnerHeldEntryDoesNotExpire` in `internal/broker/nameowner_test.go`

### REQ-agent-messaging-050

Each entry of `list_agents` MUST give the account name that registered the agent, and for a peer member give the account name its publisher self-reports. The account name is used only for display and does not take part in addressing, filtering or parsing.

- **Acceptance**: When agents of two accounts are in the same topic, each entry's `account` in `list_agents` is the corresponding account name; a peer member gives the account name the peer self-reports; the filtering results of `topic`, `broker` and `name` are unaffected by the account name.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/cross_account_names_test.go` `TestREQAgentMessaging047_CrossAccountImpersonation`; local and peer members, and falling back to the decimal uid when the account name is missing, are covered by `TestListAgentsEntriesCarryAccount` in `internal/broker/listagents_test.go`

### REQ-agent-messaging-051

The MCP service's instructions MUST tell the model that an address with the same name but a different disambiguator is another agent, and not to forward messages to it as if it were the original agent.

- **Acceptance**: The instructions contain a sentence stating that an address with the same name but a different disambiguator points to another agent and, even if the name is the same as the one the user wants to contact, it must not be forwarded to as if it were the original one.
- **Status**: Implemented
- **E2E test**: Not applicable: the model performs this behavior per the instructions, and it cannot be verified deterministically; the content of the instructions is asserted by `TestInstructions` in `internal/mcpserver/tools_test.go`

### REQ-agent-messaging-052

When this session starts a conversation with an agent that Claude Code's built-in cross-session send tool can reach (the agent's session appears in that tool's session list), and the user neither mentions peers nor names the agent by its agent address, the system MUST direct the model to send with the built-in tool. This system's send tool is for agents the built-in tool cannot reach, for an agent the user names by its agent address, and for a message the user explicitly asks to send with peers.

- **Acceptance**: The instructions state that a conversation with an agent whose session appears in Claude Code's built-in session list is started with Claude Code's built-in session tools, even when the agent is also registered with this system; and that this system's send tool starts the conversation instead when the user names the agent by its agent address, when the user explicitly asks to send with peers, or when the agent's session is not in that list.
- **Status**: Implemented
- **E2E test**: Not applicable: the model performs this behavior per the instructions, and it cannot be verified deterministically; the content of the instructions is asserted by `TestInstructions` in `internal/mcpserver/tools_test.go`

### REQ-agent-messaging-053

A reply to a message whose sender is an agent address MUST go through this system's send tool, to that address verbatim, even when a session with the same name appears in Claude Code's built-in session list.

- **Acceptance**: The instructions state that a reply to a message whose sender contains `@` goes through this system's send tool to the sender's address verbatim, even when a session with the same name appears in Claude Code's built-in session list.
- **Status**: Implemented
- **E2E test**: Not applicable: the model performs this behavior per the instructions, and it cannot be verified deterministically; the content of the instructions is asserted by `TestInstructions` in `internal/mcpserver/tools_test.go`

## Data Requirements

A message body exists in memory only during delivery, and the system does not write it to disk. An agent's registration is kept only while its session exists. Name ownership is kept in the Broker's data directory until the name is released.

## External Interface Requirements

- Claude Code: the system delivers messages through a session's inbox socket, and gets from Claude Code the session name, whether the session is busy or idle, and the location of the inbox socket.
- The model in a session: this document's capabilities are provided as tools ([REF-mcp](../../references.md)).

## Constraints

- The system runs on macOS and Linux.
- The minimum version required for Claude Code cross-session messages is v2.1.224 ([REF-claude-code-cross-session-messaging](../../references.md)), and a session of an earlier version cannot become an agent.

## Assumptions and Dependencies

- Neither the inbox socket message format nor the session registry has a public compatibility commitment, and a Claude Code version update may break registration or delivery.
- The following sessions cannot become agents: a session with no inbox socket bound; a session for which Claude Code keeps no session registry record, including an interactive session started from another session's command line; and a session whose session name is unusable (`REQ-agent-messaging-030`).
- The system uses the account as its isolation boundary: any process under an account can register any of that account's sessions as an agent and call under that agent's identity.
- A session running headless and skipping permission confirmation does not receive this system's messages: the receiving side's Claude Code holds back messages from a sender that has not declared a permission category and waits for user approval; a headless session has no approval interface, so the held-back message is dropped because the sender declared no permission category, while the sender's send has already returned success.
- A message sent by an agent on an offline Broker is delivered as usual, and the other side's reply is refused; for two-way communication, come online first.
- A peer can learn that this Broker is offline only through `broker_offline`; link status, the directory and the result of `list_brokers` do not reflect going offline.
- Send rate limiting, dropping of duplicate messages and the queue cap for pending messages are the receiving side's Claude Code's responsibility, and the system does not change these behaviors. The rules are as follows, and Claude Code may adjust the numbers: the receiver rate-limits and de-duplicates per sender, and when a written message has no reply address it identifies the sender by the writing process; each sender's budget is at most 30, recovering 0.5 per second; when the same sender sends a message identical to the previous one within 30 seconds, the latter is dropped; at most 50 unprocessed cross-session messages are queued; a message enqueued during a turn ends as `remove` when the turn ends and does not become a user record, so the queue cap can only be observed by queue depth. There are these consequences:
  - All messages written into the same session through the same Broker, regardless of which agent they come from, belong to the same sender on the receiving side and share one budget.
  - When the same agent sends two identical messages in a row to the same session within 30 seconds, the latter is dropped by the receiver while the send has already returned success. Different signatures make different content, so different agents do not cause each other's messages to be dropped this way.
  - Claude Code's built-in message loop detection does not apply to this system's messages, and only the rate limiting above constrains messages that go back and forth between agents.
