# Topic

## Problem Statement

After pairing, the two hosts may each have several sessions doing unrelated work. An agent needs a scope that limits who can see it and who can send it messages, and agents working in the same project should be in the same scope without extra action.

## Scope

This document covers joining and leaving a [topic](../../glossary.md), the visibility rules, a topic being public or not, and how the default topic is determined.

### Out of Scope

- topic administrators, invitations and approvals. Knowing the name is enough to join.
- Broadcasting a message to an entire topic.
- Visibility distinguished by peer. An agent in a topic is visible to every connected peer that has a member in that topic.

## Actors

| Actor | Authority |
|---|---|
| agent | Joins and leaves topics, declares a topic public, lists the topics it is in and the known public topics. |
| User | Declares the project's topics in the project configuration file, and turns auto-join off in the Broker configuration or the project configuration file. |

## Functional Requirements

### REQ-topics-001

An agent MUST be able to join a topic by name.

- **Acceptance**: After an agent joins under the name `release`, listing the topics it is in contains `release`.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-001 join`

### REQ-topics-002

Joining a topic MUST NOT require approval from any other agent or the user, and joining a topic no one has joined before MUST likewise succeed.

- **Acceptance**: Joining under a name no one has used before succeeds; another agent joining under the same name also succeeds, and the two see each other.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-002 join new name without approval`

### REQ-topics-003

An agent MUST be able to leave a topic it has joined.

- **Acceptance**: After leaving, listing the topics it is in does not contain that topic; in the list of an agent that shares only this topic with it, it no longer appears.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-003 leave`

### REQ-topics-004

An agent MUST be able to belong to several topics at once.

- **Acceptance**: An agent joins `t1` and then `t2`; listing the topics it is in contains both `t1` and `t2`.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-004 many topics`

### REQ-topics-005

Two agents see each other if and only if they share at least one topic and are on the same Broker or on two connected Brokers.

- **Acceptance**: Two agents in the same topic each appear in the other's list; once one leaves that topic and they share no other topic, they disappear from each other's lists; they likewise disappear after the link between their Brokers goes down.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-topics-005 iff shared and connected`

### REQ-topics-006

An agent that belongs to no topic MUST NOT appear in any other agent's list.

- **Acceptance**: An agent that is registered but has joined no topic does not appear in the list of any agent, local or remote.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-006 no topic means invisible`; `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-topics-006 no topic invisible`

### REQ-topics-007

Topics with the same name on different Brokers MUST be treated as the same topic.

- **Acceptance**: On two connected hosts, one agent each joins a topic under the same name, and the two see each other.
- **Status**: Implemented
- **E2E test**: `test/e2e/visibility_remote_test.go` `TestRemoteVisibility/REQ-topics-007 same name across brokers`

### REQ-topics-008

A topic that is not declared public MUST be not public.

- **Acceptance**: An agent joins a new topic without declaring it public, and the result of listing public topics on other Brokers does not contain it.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryPrivateByDefault`

### REQ-topics-009

The system MUST NOT reveal the name of a non-public topic to a Broker that has no member in that topic.

- **Acceptance**: On host A an agent joins the non-public topic `secret-x`, and on connected host B no agent has joined it; neither the return result of any tool on B nor B's log shows `secret-x`.
- **Status**: Implemented
- **E2E test**: `test/e2e/security_test.go` `TestPrivateTopicNamesStayHome/REQ-topics-009 private topic name`

### REQ-topics-010

When its Broker is public, an agent MUST be able to declare a topic public while joining it. The declaration may or may not carry a description.

- **Acceptance**: After an agent on a public Broker publicly joins a topic and gives a description, the result of listing public topics on connected other Brokers contains the name and the description; without a description, the result contains the name with an empty description.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestPublicTopicVisibleAcrossNetwork`; `test/e2e/journey_public_broker_test.go` `TestJourneyConnectPublicBroker/main`

### REQ-topics-011

A public topic's name and description MUST be visible to agents on every Broker in the connected network, not only the declarer's peers.

- **Acceptance**: A is connected to B and B to C, with no link between A and C; a public topic declared on A appears in C's result of listing public topics.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestPublicTopicVisibleAcrossNetwork`; `test/e2e/journey_public_broker_test.go` `TestJourneyConnectPublicBroker/main`

### REQ-topics-012

A public topic declaration MUST follow the same validity period and renewal rules as a card (`REQ-broker-discovery-007`, `REQ-broker-discovery-008`).

- **Acceptance**: The declarer goes offline and no longer renews; past the validity period, other Brokers' result of listing public topics does not contain that topic.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_expiry_test.go` `TestPublicTopicExpiresWithDeclarer`

### REQ-topics-013

An agent MUST be able to list the topics it has joined and the public topics the local Broker knows.

- **Acceptance**: The result distinguishes "joined" from "public but not joined", and each matches the actual situation.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestPublicTopicVisibleAcrossNetwork`; `test/e2e/journey_public_broker_test.go` `TestJourneyConnectPublicBroker/main`

### REQ-topics-014

A topic's roster MUST NOT be sent to any Broker except a connected peer that also has a member in that topic.

- **Acceptance**: On host A the agent named `api-worker` is in topic `t1`, and on connected host B no agent is in `t1`; neither the return result of any tool on B nor B's log shows `api-worker`. After an agent on B joins `t1`, this agent on A appears in its list.
- **Status**: Implemented
- **E2E test**: `test/e2e/security_test.go` `TestPrivateTopicNamesStayHome/REQ-topics-014 member list`; `test/e2e/security_test.go` `TestEchoedTopicDigestsRevealNothing/REQ-topics-014 echoed digests`

### REQ-topics-015

When auto-join is not turned off and the project configuration file of the session's workspace declares a topic list, an agent MUST automatically join every topic in the list after registration.

- **Acceptance**: The project configuration file declares `t1` and `t2`; an agent started in this project, without any join operation, lists the topics it is in and the result is exactly `t1` and `t2`.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-015 project topics`

### REQ-topics-016

When auto-join is not turned off, the project configuration file declares no topic list, and the session's workspace is a git repository with an `origin` remote, an agent MUST automatically join the topic named after the normalized result of that remote address after registration.

- **Acceptance**: An agent started in a repository whose `origin` is `https://github.com/org/repo`, without any join operation, is in exactly the topic `github.com/org/repo`.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-016 origin topic`

### REQ-topics-017

Different spellings of one repository's address MUST normalize to the same topic name, in the form of the lowercase hostname followed by the repository path, with no protocol, user information, port or `.git` suffix.

- **Acceptance**: `git@github.com:org/repo.git`, `ssh://git@GitHub.com:22/org/repo.git` and `https://github.com/org/repo` all normalize to `github.com/org/repo`.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-017 spellings`

### REQ-topics-018

When the project configuration file declares no topic list and the workspace is not a git repository with an `origin` remote, the system MUST NOT auto-join the agent to any topic.

- **Acceptance**: An agent started in a non-git directory is in no topic and does not appear in any other agent's list.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-018 non-git`

### REQ-topics-019

An auto-joined topic MUST be not public.

- **Acceptance**: After an agent auto-joins the default topic, other Brokers' result of listing public topics does not contain it.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryPrivateByDefault`

### REQ-topics-020

After auto-join is turned off in the Broker configuration, an agent newly registered on that Broker MUST NOT auto-join any topic.

- **Acceptance**: After it is turned off, an agent started in a repository with `origin` is in no topic.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-020 broker auto_join off`

### REQ-topics-021

After auto-join is turned off in the project configuration file, an agent registered in that project MUST NOT auto-join any topic.

- **Acceptance**: With auto-join turned off in the project configuration file and the Broker configuration left on, an agent started in this project is in no topic, and an agent started in another project still auto-joins its default topic.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-021 project auto_join off`

### REQ-topics-022

An agent's topic membership MUST all disappear when its session ends. After a Broker restart, each agent's topic membership is restored once its MCP server process registers again.

- **Acceptance**: After an agent manually joins `t1`, end the session; restart a session in the same workspace and the new agent is in only the auto-joined topics, not `t1`. After restarting the Broker, an agent that had joined topics before the restart restores its original membership once its MCP server process registers again.
- **Status**: Implemented
- **E2E test**: `test/e2e/session_lifecycle_test.go` `TestLocalSessionChanges/REQ-topics-022 membership ends with session and returns after broker restart`

### REQ-topics-023

When its Broker is not public, a call declaring a topic public MUST fail and state that the Broker must first be set public, and the agent MUST NOT join that topic because of this call.

- **Acceptance**: An agent on a non-public Broker joins a topic publicly and the call returns an error; the agent's topics are unchanged, and other Brokers' result of listing public topics does not contain that topic.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestPublicTopicRejectedOnPrivateBroker`

### REQ-topics-024

After a Broker changes from public to not public, its previous public topic declarations MUST disappear from the result of listing public topics on every Broker in the connected network.

- **Acceptance**: A is connected to B and B to C, with no link between A and C; an agent on A joins `infra-help` publicly and C's result of listing public topics contains it; after A changes to not public, while this agent is still in `infra-help`, C's result of listing public topics no longer contains `infra-help`.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestPublicTopicGoneWhenBrokerPrivate`

### REQ-topics-025

When a Broker no longer has any agent that joined a topic publicly, the Broker's public declaration of that topic MUST disappear from other Brokers' result of listing public topics in the connected network.

- **Acceptance**: A is connected to B, only one agent on A joined `infra-help` publicly, and no other Broker in the network declares it; after this agent leaves `infra-help`, B's result of listing public topics no longer contains it; after it publicly joins again it reappears, and after ending this agent's session it disappears again.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestPublicTopicFollowsLastMember`

### REQ-topics-026

When joining a topic without a name, an agent MUST join its session's default topic.

- **Acceptance**: An agent started in a repository whose `origin` is `https://github.com/org/repo` first leaves `github.com/org/repo` by name and then joins without a name; listing the topics it is in contains `github.com/org/repo`; when the project configuration file declares `t1` and `t2`, joining without a name makes the result contain both. When the session has no default topic, the call returns an error stating that the session has no default topic.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-026 join default without name` and `TestLocalTopics/REQ-topics-026 join default with broker auto_join off`

### REQ-topics-027

When leaving a topic without a name, an agent MUST leave its session's default topic, and afterward within this session MUST NOT automatically rejoin it, while a new session auto-joins as usual.

- **Acceptance**: An agent started in a repository whose `origin` is `https://github.com/org/repo` leaves without a name; listing the topics it is in does not contain `github.com/org/repo`; after restarting the Broker, once the MCP server process registers again it still does not; open a new session in the same workspace, and the new agent's topics contain `github.com/org/repo`.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_local_test.go` `TestLocalTopics/REQ-topics-027 leave default without name`

### REQ-topics-028

The number of topics with local members, the number of local members in each topic, and the number of topic changes one account makes in a short time MUST all have caps; when a cap is reached the system MUST reject the call that exceeds it, state to try again later, and not change membership. A system Broker MUST also break down the local topic budget by account, so that one account cannot take up every topic.

- **Acceptance**: When the host already has 512 topics with members, joining the 513th topic fails and states that the limit has been reached; when a topic already has 4096 local members, joining one more agent fails; on a system Broker, when one account already has 128 topics with members, that account joining another new topic fails, while an agent already in one of the account's topics joining again still succeeds; after one account changes topics more than 64 times in a row, the call fails and states to try again later, membership is unchanged, and after waiting one second, 4 more changes succeed; another account's changes are unaffected.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_limits_test.go` `TestTopicChangeLimits` covers the rate limit on one account's topic changes, membership unchanged after a rejection, and recovery after waiting; the number of local topics, the number of local members per topic and the per-account cap of a system Broker are covered by the unit tests `TestJoinTopicCountLimit` and `TestJoinMemberLimit` in `internal/topic/locallimit_test.go`, `TestLinkStaysUpSharingTopicsToLimit` in `internal/topic/linkstability_test.go`, `TestJoinTopicQuotaExceeded`, `TestJoinTopicPerAccountLimit` and `TestUserBrokerHasNoPerAccountLimit` in `internal/broker/topics_rpc_test.go`, and `TestJoinTopicRateLimited` and `TestLeaveTopicRateLimited` in `internal/broker/topicratelimit_test.go`

### REQ-topics-029

A roster sent by any peer MUST NOT make the entries this Broker keeps for that peer grow without bound; a member entry whose name or previous name exceeds the length cap MUST NOT be stored.

- **Acceptance**: A paired peer sends several roster frames in a row under the same shared topic, accumulating more than 4096 new members; this Broker keeps no more than 4096 members for that peer, and an entry with a 129-byte name appears in no result; when five shared topics each have 4096 members, the fifth topic's roster is dropped as a whole frame, and the entries kept for that peer total no more than 16384; when the peer then sends one recognizable member, that member is visible, the link is still connected, and the Broker log records that the roster frame and member entries were dropped.
- **Status**: Implemented
- **E2E test**: `test/e2e/topics_limits_test.go` `TestTopicRosterBounds` covers the cap on a peer's bulk roster, the dropping of a 129-byte name, and the link staying connected after a rejection; the exact boundary of the cumulative count is covered by the unit tests `TestRosterDeltaCumulativeLimit`, `TestRosterMemberNameLength` and `TestPeerRosterCumulativeLimit` in `internal/topic/rosterlimit_test.go`

### REQ-topics-030

A public topic declaration with a nonconforming name or description MUST NOT appear in the result of listing public topics.

- **Acceptance**: Accept over a link a public topic declaration with a 60 KiB description, a declaration whose name contains a newline, and a declaration whose name is 300 bytes; the result of listing public topics does not contain them, and all three records are still forwarded to the next Broker connected to the receiver.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_hostile_test.go` `TestDirectoryPublicTopicReceiveLimits/REQ-topics-030 malformed declarations are not listed`; unit tests `TestPublicTopicsSkipsInvalidDeclarations` in `internal/directory/views_test.go` and `TestValidPublicTopicName` in `internal/directory/validate_test.go`

### REQ-topics-031

For a public topic with the same name, the description MUST be taken first from this Broker's declaration, then from a peer's declaration, and the result MUST give which Broker the description came from and the number of valid declarations of that name; a distant other publisher cannot replace the description given by this Broker or a peer by setting the version number to the maximum value.

- **Acceptance**: This Broker declares `x` (description A, version 1) and another non-peer publisher declares `x` (description B, version `math.MaxUint64`); the result of listing public topics gives description A, the publisher as this Broker, and a declaration count of 2; when a peer and a non-peer both declare the same-named topic, it gives the peer's description with the publisher as that peer; when only a non-peer declares it, it still takes the declaration with the highest version number.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_hostile_test.go` `TestDirectoryPublicTopicReceiveLimits/REQ-topics-031 peer description over a maximum version`; unit tests `TestPublicTopicsPrefersSelfAndPeerDescriptions` in `internal/directory/views_test.go` and `TestListTopicsReportsPublisher` in `internal/broker/topics_rpc_test.go`

## Data Requirements

Topic membership is kept only in memory and not written to disk. A project's topic declarations are kept in the project configuration file and managed with the repository.

## Assumptions and Dependencies

- The confidentiality of a non-public topic is limited to its name being hard to guess. A peer can have its own agent join under a guessed name and thereby learn whether this Broker has a member in that topic.
- The default topic's name is decided by the repository address, and anyone who can access the same repository can derive it.
