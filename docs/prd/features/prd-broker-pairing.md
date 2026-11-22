# Broker pairing and connection

## Problem Statement

Before the [Brokers](../../glossary.md) on two hosts communicate, each user needs to confirm that the other is the one they intend to connect to, that later communication cannot be read by a third party, and that the other's identity cannot be impersonated by a third party. This confirmation must not require a certificate authority set up in advance, nor that both sides have an advertised address the other can reach.

## Scope

This document covers a Broker's identity and alias, the generation and use of pairing keys, pairing, keeping and restoring links, and unpairing.

### Out of Scope

- Connections relayed through a third Broker.
- Trust through certificates issued by a certificate authority.
- The system deciding on its own which Broker to connect to. The first connection is always initiated by the user.

## Actors

| Actor | Authority |
|---|---|
| User | Decides which Broker to pair with, hands the local Broker's address and pairing key to the other user through a channel outside the system, and approves operations an agent initiates through Claude Code's permission confirmation. |
| agent | On the user's request, generates pairing keys, initiates pairing and unpairs. These operations require that the agent's account has write permission on the Broker's configuration file, as `REQ-broker-config-021` specifies. |
| Peer | After pairing, keeps a link with this Broker. |

## Functional Requirements

### REQ-broker-pairing-001

Every Broker MUST have a fingerprint as its identity, and the fingerprint MUST stay the same after the Broker restarts, changes its alias or changes its listen address.

- **Acceptance**: Record the Broker's fingerprint, then restart it, change its alias and change its listen address in turn; each time the fingerprint read afterward is the same as at the start.
- **Status**: Implemented
- **E2E test**: `test/e2e/aliases_test.go` `TestAliases/REQ-broker-pairing-001 fingerprint stable`

### REQ-broker-pairing-002

When a Broker's alias is not configured, a system Broker MUST take the first segment of the host's system hostname (the part before the first `.`), and a user Broker MUST be made up in order of the first segment of the hostname, a `.` and the owning account name. The alias is independent of the listen address, the listen port and the advertised address. Characters in the hostname and the account name that are not in the alias character set are replaced with `-`.

- **Acceptance**: On a host whose hostname is `hostA.example.com`, start a system Broker with the default configuration, and a peer sees addresses of agents under it ending in `@hostA`; start a user Broker under the account `alice`, and the addresses end in `@hostA.alice`. Change the user Broker's listen port to 17444, configure its listen address empty, and configure its advertised address to `a.example:443`; in all three cases the address ending is unchanged.
- **Status**: Implemented
- **E2E test**: `test/e2e/aliases_test.go` `TestAliases`, `TestLongHostnameAlias`; `test/e2e/system/alias_default_test.go` `TestREQBrokerPairing002_DefaultAliasHostAndAccount`

### REQ-broker-pairing-003

A Broker's alias MUST be changeable through configuration.

- **Acceptance**: After the alias is changed to `build-box`, the local agents see their own addresses ending in `@build-box`, and so do peers that have no local alias set for it.
- **Status**: Implemented
- **E2E test**: `test/e2e/aliases_test.go` `TestAliases/REQ-broker-pairing-003 configured alias`

### REQ-broker-pairing-004

When the party initiating pairing finds that the other's alias is the same as the local alias or the alias of an existing peer, the system MUST complete the pairing only after the user specifies a local alias for the new peer.

- **Acceptance**: With a peer whose alias is `macbook`, initiate pairing with another Broker whose alias is also `macbook`, and the system returns a result asking for a local alias; after `macbook-2` is specified the pairing completes, and the two peers are addressed by `@macbook` and `@macbook-2`.
- **Status**: Implemented
- **E2E test**: `test/e2e/aliases_test.go` `TestAliases/REQ-broker-pairing-004 initiator conflict`

### REQ-broker-pairing-005

An agent MUST be able to have the local Broker generate a one-time pairing key.

- **Acceptance**: After the call, it gets a one-time pairing key and its expiry time; another host can initiate pairing with the local Broker's address and this key, without any other information.
- **Status**: Implemented
- **E2E test**: `test/e2e/journey_pair_test.go` `TestJourneyPairBrokers`

### REQ-broker-pairing-006

A one-time pairing key MUST expire 10 minutes after generation.

- **Acceptance**: Pair with it at the ninth minute after generation and it succeeds; generate another and pair with it at the eleventh minute, and it returns a key expired error.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-006 key within validity`; the time limit half is covered by `TestREQBrokerPairing006_NineMinutesSucceedsElevenMinutesExpired` in `internal/pairing/pairkey_expired_test.go` and `TestREQBrokerPairing006_KeyExpiredAtElevenMinutes` in `internal/pairing/pairkey_test.go`

### REQ-broker-pairing-007

A one-time pairing key MUST become invalid after one pairing completes successfully.

- **Acceptance**: Using the same one-time pairing key, initiate pairing from two hosts in turn with the Broker that generated it; the first succeeds and the second returns a key invalid error.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-007 one-time key used once`

### REQ-broker-pairing-008

When the configuration file sets no pre-provisioned static key, only a valid one-time pairing key can make pairing succeed.

- **Acceptance**: A Broker started with the default configuration has no pre-provisioned static key; when another host initiates pairing with any string that is not a valid one-time pairing key, it always returns a key invalid error, and neither side stores the other's identity.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-008 no static key`

### REQ-broker-pairing-009

Once a pre-provisioned static key is set in the configuration file, it MUST be usable for any number of pairings and MUST NOT expire over time.

- **Acceptance**: Set a pre-provisioned static key in the Broker's configuration file; three different hosts pair in turn with the local Broker's address and this key, all succeeding; 11 minutes after it is set, a fourth host pairs with it and likewise succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-009 static key many times`; "still usable after 11 minutes" is covered by `TestREQBrokerPairing009_StaticKeyNotExpired` in `internal/pairing/pairkey_test.go`

### REQ-broker-pairing-010

An agent MUST be able, with the other side's address and pairing key, to have the local Broker initiate pairing with the other.

- **Acceptance**: Calling with the other side's address and one-time pairing key returns pairing success along with the other side's alias and fingerprint; calling with the other side's address and its configured pre-provisioned static key gives the same result; using the displayed address text (`hostname(address:port)`) verbatim as the address gives the same result, with the hostname in the display not used for connecting; the key is still passed separately.
- **Status**: Implemented
- **E2E test**: `test/e2e/journey_pair_test.go` `TestJourneyPairBrokers`; `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-010 static key and plain address`

### REQ-broker-pairing-011

When the pairing key is wrong, expired or already used, pairing MUST fail with the reason stated, and each side MUST NOT store the other's identity.

- **Acceptance**: Initiate pairing with a wrong key and the call returns a key invalid error; afterward neither side's result of listing peers contains the other.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-011 wrong key stores nothing`

### REQ-broker-pairing-012

A third party that does not know the pairing key MUST NOT make pairing succeed even if it can intercept and alter the network traffic between the two sides.

- **Acceptance**: Place between the two Brokers an intermediate node that can read and write all traffic between them but does not know the pairing key, and initiate pairing with the correct pairing key; pairing fails, and neither side stores the identity of the other or of the intermediate node.
- **Status**: Implemented
- **E2E test**: `test/e2e/security_test.go` `TestMITM/REQ-broker-pairing-012 man in the middle`

### REQ-broker-pairing-013

Everything transmitted between Brokers MUST be encrypted.

- **Acceptance**: Capture the network traffic between two connected Brokers, and no plaintext of any message body, agent name or topic name appears in it.
- **Status**: Implemented
- **E2E test**: `test/e2e/security_test.go` `TestLinkEncrypted/REQ-broker-pairing-013 encrypted`

### REQ-broker-pairing-014

The system MUST NOT initiate a connection to a Broker it has not paired with, unless this connection is directly triggered by an agent's pairing call.

- **Acceptance**: After a Broker learns a public Broker's card, with no pairing call, it makes no connection to that Broker's advertised addresses.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryNoDialToCardHolder`; `test/e2e/discovery_address_test.go` `TestNoDialToCardAddress`

### REQ-broker-pairing-015

A connection request from a Broker that neither holds a valid pairing key nor is a peer MUST be rejected.

- **Acceptance**: An unpaired Broker connects without a key, the connection is rejected, and the connected-to side's result of listing peers does not contain it.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-015 unpaired without key rejected`

### REQ-broker-pairing-016

After pairing completes, re-establishing a link between the two peers MUST NOT require a pairing key.

- **Acceptance**: After pairing succeeds, disconnect the network and restore it; the link is re-established without either side providing a pairing key, and the original one-time pairing key has become invalid.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-016 relink without key`

### REQ-broker-pairing-017

A pairing relationship MUST survive a Broker restart.

- **Acceptance**: After pairing, restart one of the Brokers; after the restart, its result of listing peers still contains the other.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-017 survives restart`

### REQ-broker-pairing-018

After the link between peers goes down, the dialer MUST re-establish the link within 120 seconds of the network being restored, without user action.

- **Acceptance**: Disconnect the network between the two hosts and restore it; with no user action, within 120 seconds of restoration both sides' result of listing peers shows connected again.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-018 redial after restore`

### REQ-broker-pairing-019

After a Broker restarts, the system MUST restore links with all peers within 60 seconds of the restart, without user action.

- **Acceptance**: Restart a Broker with two peers, one paired by it and the other having paired with it; with no user action, within 60 seconds of the restart its links with both peers are restored.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinksRestartWithTwoPeers`

### REQ-broker-pairing-020

When the party that initiated pairing can connect out to the other, both sides MUST be able to send messages to each other.

- **Acceptance**: Host B is behind NAT with no advertised address host A can reach, and host A has an advertised address B can reach; after B initiates pairing with A, an agent on A sends a message to an agent on B successfully, and the reverse likewise succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/send_remote_test.go` `TestRemoteSendFaults/REQ-broker-pairing-020 both directions behind NAT`

### REQ-broker-pairing-021

An agent MUST be able to have the local Broker unpair from a peer; after unpairing the link goes down and that Broker is no longer a peer of this one.

- **Acceptance**: After unpairing, the local result of listing peers does not contain the other, and agents on the other Broker disappear from the local agents' lists.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-021 disconnect`

### REQ-broker-pairing-022

After unpairing, the unpaired Broker MUST be rejected when it connects again, until it pairs again.

- **Acceptance**: B initiates pairing with A using A's address and pairing key. After A unpairs from B, B's automatic reconnect is rejected by A; after B re-pairs with a new valid pairing key, the link is restored. When A has a pre-provisioned static key, B re-pairing with it likewise restores the link.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-022 re-pair after unpair`

### REQ-broker-pairing-023

When the fingerprint the other side presents during pairing differs from the fingerprint this Broker previously learned from a card for that advertised address, the system MUST give the user a warning in the pairing result.

- **Acceptance**: This Broker already holds a public Broker's card; when pairing is initiated with the advertised address in that card and the other side actually presents a different fingerprint, the call result contains a fingerprint warning.
- **Status**: Implemented
- **E2E test**: `test/e2e/journey_public_broker_test.go` `TestJourneyConnectPublicBroker/fingerprint_mismatch`

### REQ-broker-pairing-024

When the party accepting a pairing finds that the other's alias is the same as the local alias or the alias of an existing peer, the system MUST generate a local alias for the new peer that duplicates neither, and complete the pairing.

- **Acceptance**: A already has a peer whose alias is `macbook`, and another Broker whose alias is also `macbook` initiates pairing with A using a valid key; the pairing succeeds, and in A's result of listing Brokers the two peers' aliases differ.
- **Status**: Implemented
- **E2E test**: `test/e2e/aliases_test.go` `TestAliases/REQ-broker-pairing-024 acceptor conflict`

### REQ-broker-pairing-025

The system MUST NOT generate a pre-provisioned static key, and the operation that generates pairing keys produces only one-time pairing keys.

- **Acceptance**: On a Broker with no pre-provisioned static key set, generate pairing keys several times; each time the result is a one-time pairing key with an expiry time, and neither the Broker's configuration file nor the data the Broker keeps on disk contains a reusable pairing key; after a pre-provisioned static key is set in the configuration file, pairing with it succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-025 never generates a static key`

### REQ-broker-pairing-026

When a peer's self-reported alias becomes the same as the local alias or another peer's alias, the system MUST generate a non-duplicating local alias for it, and existing agent addresses MUST NOT thereby point to another Broker.

- **Acceptance**: A has two peers whose aliases are `hostB` and `hostC`; after B changes its alias to `hostC`, on A the addresses ending in `@hostC` still point only to agents on C, B's agents appear on A under another alias, and the signatures of the messages they send do not end in `@hostC`.
- **Status**: Implemented
- **E2E test**: `test/e2e/aliases_test.go` `TestAliases/REQ-broker-pairing-026 peer renames into conflict`

### REQ-broker-pairing-027

The acceptor MUST NOT connect out to the other side.

- **Acceptance**: Both hosts run with the default configuration; A generates a pairing key and B initiates pairing with A's address and this key. Disconnect and restore the network between the two hosts; the link is re-established, and during this time A initiates no connection to B. Use a firewall to forbid B from connecting out to A, then disconnect and restore the network; the link is no longer restored, and during this time A still initiates no connection to B.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-027 acceptor never dials`

### REQ-broker-pairing-028

The result of generating a one-time pairing key MUST, besides the key, show the local system hostname and each item in the advertised address list, with the first item first, and the displayed content is not part of the key; when the advertised address list is empty while the Broker is listening, it MUST still generate the key and state that there is no address to show.

- **Acceptance**: On a Broker whose system hostname is `hostA`, whose default-route interface address is `192.0.2.10` and which has no advertised address configured, generate a one-time pairing key; the result gives the key separately from `hostA(192.0.2.10:17443)`, and the key itself contains neither the hostname nor the address; after the advertised address is configured to `a.example:443` and a key is generated, it shows `hostA(a.example:443)`; after adding the advertised address `a.vpn.example` and generating, it shows `hostA(a.example:443)` and `hostA(a.vpn.example:443)` in order; when the host has no default route, listens on every address and has no advertised address configured, generation still succeeds, and the result states that there is no address to show and that the user must tell the other side the address to connect to.
- **Status**: Implemented
- **E2E test**: `test/e2e/journey_pair_test.go` `TestJourneyPairBrokers`; `test/e2e/addresses_test.go` `TestAdvertisedAddresses/REQ-broker-pairing-028 shown addresses`

### REQ-broker-pairing-029

A pre-provisioned static key shorter than 16 characters or longer than 128 characters MUST be rejected; the Broker MUST NOT start with it, and MUST NOT adopt a value changed to such during operation.

- **Acceptance**: Set the pre-provisioned static key to 15 characters in the configuration file and start the Broker; startup fails and the error names this option; set to 16 or 128 characters, it starts and can pair with it. During operation, change it to 129 characters in the configuration file; the Broker does not adopt the new value, pairing with the original key still succeeds, and pairing with the new value fails.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-029 static key length`

### REQ-broker-pairing-030

The result of reading configuration and the runtime log MUST NOT contain the value of the pre-provisioned static key.

- **Acceptance**: Set a pre-provisioned static key in the configuration file and read configuration through a tool; the result only states that it is set; then replace it in the configuration file and complete a pairing with the new value. Searching the result of reading configuration and the runtime log for the old and new values finds neither.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_cluster_test.go` `TestConfigToolsCluster/StaticKeyReadHasNoSecret`

### REQ-broker-pairing-031

The system MUST NOT allow a pre-provisioned static key to be set or modified through a tool.

- **Acceptance**: An agent whose account has write permission on the configuration file sets, modifies or deletes the pre-provisioned static key through a tool; each time it returns an error stating that this option can only be set in the configuration file; neither the configuration file content nor the effective key changes.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_cluster_test.go` `TestConfigToolsCluster/StaticKeyToolsDoNotChange`

### REQ-broker-pairing-032

After a pre-provisioned static key is deleted from the configuration file, the Broker MUST stop accepting pairings with that key without restarting (`REQ-broker-config-017`).

- **Acceptance**: Check in turn: with a pre-provisioned static key set in the configuration file, another host pairs with it successfully; delete this option from the configuration file without restarting the Broker, and afterward a third host initiating pairing with the same key is rejected, with the returned reason being key invalid, and neither side stores the other's identity; the result of reading configuration shows this option unset; the audit log gains one record that this option changed, and the record contains no key value; the peer that previously paired with it is still a peer, and its link with this Broker is unaffected.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_cluster_test.go` `TestConfigToolsCluster/StaticKeyRemovedRejects`

### REQ-broker-pairing-033

After the pre-provisioned static key in the configuration file is changed to another valid value, the Broker MUST, without restarting (`REQ-broker-config-017`), accept only the new value, and pairing with the old value MUST be rejected.

- **Acceptance**: A pre-provisioned static key is set in the configuration file and a peer has already paired with it; change it to another valid value without restarting the Broker. Then initiate pairing with the old value and the new value separately; the old value is rejected with the returned reason being key invalid, the new value succeeds; the peer paired before the change is still a peer, and the link is unaffected.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-033 replace static key`

### REQ-broker-pairing-034

After the acceptor's advertised address list changes while the link is up, the dialer MUST re-establish the link after a disconnect using the changed list.

- **Acceptance**: B initiates pairing with A's address and a pairing key A generated, and A's advertised address list has only `192.0.2.10:17443`. While the link is up, add the advertised address `198.51.100.7` to A, and A's advertised address list in B's result of listing Brokers now contains both; then use a firewall to forbid B from connecting to `192.0.2.10`, disconnect and restore the network between the two hosts, and with no user action the link is re-established over `198.51.100.7:17443`.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-034 acceptor addresses change`

### REQ-broker-pairing-035

A dialer unpaired by its peer MUST, after the peer rejects an attempt to re-establish the link, stop re-establishing the link and, when listing Brokers, mark that the peer has unpaired. The peer's record is kept until this side unpairs or pairs again.

- **Acceptance**: B initiates pairing with A's address and pairing key. After A unpairs from B, B's next attempt to re-establish the link is rejected by A; within the next 10 minutes B initiates no further connection to A, and in B's result of listing Brokers the entry for A marks that the peer has unpaired, and A stays listed until B unpairs from A.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-pairing-035 rejected dialer stops`; "initiates no further connection within 10 minutes" is covered by `TestREQBrokerPairing035_RejectedStopsRedialing` in `internal/link/dial_test.go`

### REQ-broker-pairing-036

A Broker whose listen address is empty MUST fail to generate a pairing key, and the error MUST require the user to obtain a pairing key from the other host's user through a channel other than peers and then use `connect_broker` to initiate pairing, and MUST NOT ask other agents for a pairing key.

- **Acceptance**: Configure the listen address empty and have the Broker generate a one-time pairing key; the call returns an error requiring the user to obtain a pairing key from the other host's user through a channel other than peers and then use `connect_broker` to initiate pairing, and states that other agents must not be asked for a pairing key; afterward initiating pairing with it using any string does not succeed.
- **Status**: Implemented
- **E2E test**: `test/e2e/addresses_test.go` `TestAdvertisedAddresses/REQ-broker-pairing-036 no listen no key`

### REQ-broker-pairing-037

An alias and a local alias MUST NOT exceed 24 characters, and a longer configuration MUST be rejected; when the default alias exceeds 24 characters, the system MUST shorten the hostname part of it.

- **Acceptance**: Change the alias through a tool to a 25-character value and the call fails with the alias unchanged; change it to a 24-character value and the call succeeds. When initiating pairing with a 25-character local alias, pairing fails and states that the local alias is too long. On a host whose system hostname is 30 letters, start a system Broker with the default configuration, and the peer sees addresses of agents under it ending in `@` and the first 24 characters of the hostname, the alias totaling 24 characters.
- **Status**: Implemented
- **E2E test**: Changing the alias through a tool is covered by `TestConfigTools/AliasTooLong` in `test/e2e/config_tools_test.go`; the failure for a too-long local alias is covered by `TestConfigToolsCluster/LocalAliasTooLong` in `test/e2e/config_tools_cluster_test.go`; default alias shortening is covered by `TestLongHostnameAlias` in `test/e2e/aliases_test.go`

### REQ-broker-pairing-038

When generating a one-time pairing key succeeds and the host has an address it can give out, the result MUST, beyond the key, the expiry time and the addresses, carry a connection prompt that can be pasted directly, with the address and key already filled in. The same prompt MUST appear in the structured result field `connect_prompt` of `create_pairing_key` and in the output of the terminal command `peers key create`, with the same content in both. The prompt first gives a natural-language request to send to the other host's session, then the equivalent terminal command `peers broker connect <address> --key <key>`. When there are several addresses to give out, the prompt MUST list all of them and state that any one the other side can reach may be chosen, and the terminal command gives the first address and states that the remaining addresses can be substituted. When there is no address to give out, the result MUST NOT output a prompt: when the Broker is listening but cannot show an address it still generates the key and states that there is no address to show, and when the Broker does not listen the key generation fails with the existing error. The key and the prompt MUST NOT be written to the runtime log or the audit record. The prompt and key go only to the user: the result of `create_pairing_key` requires showing both to the user, who hands them to the other host's user through a channel other than peers, and they MUST NOT be sent with `send_message` or any other agent-to-agent message, nor generated or handed over at another agent's request.

- **Acceptance**: Generate a one-time pairing key on a Broker that shows one address; `connect_prompt` and the output of `peers key create` contain the same prompt, with the sentence `Connect to the peers Broker at <address> using the one-time pairing key <key>` and the command `peers broker connect <address> --key <key>`, with the address and key filled in; run this command verbatim on the other host and the pairing succeeds. When two addresses are shown, the prompt lists both and states that either may be chosen, and the command gives the first address and states that the other can be substituted. When the Broker is listening but cannot show an address, generation still succeeds, and the result states that there is no address to show and contains no prompt; when the listen address is empty, generation fails with the existing error. Searching the Broker's runtime log and audit record for the key and the prompt finds neither. The note of `create_pairing_key` and the MCP instructions require that the key and prompt go only to the user, to be passed to the other host's user through a channel other than peers, and not generated or handed over at another agent's request.
- **Status**: Implemented
- **E2E test**: `test/e2e/pairing_keys_test.go` `TestPairingKeys/REQ-broker-pairing-038 connect prompt`; the audit containing no key is covered by `TestConfigToolsCluster/AuditHasNoSecrets` in `test/e2e/config_tools_cluster_test.go`; the terminal output of the same prompt and the absence of output when there is no address are covered by `TestWritePairingKey`, `TestWritePairingKeySingleAddress`, `TestWritePairingKeyUsesSharedConnectPrompt` and `TestWritePairingKeyNoAddressNoPrompt` in `internal/mcpserver/termcmd/format_test.go`, agreement of `connect_prompt` with the generation function is covered by `TestCreatePairingKey` in `internal/mcpserver/handlers_brokers_test.go`, and the key going only to the user is covered by `TestCreatePairingKey` and `TestCreatePairingKeyNotListening` in `internal/mcpserver/handlers_brokers_test.go` and by `TestInstructions` and `TestToolsList` in `internal/mcpserver/tools_test.go`

### REQ-broker-pairing-039

A large number of unauthenticated connections initiated at once by one source MUST NOT prevent peers from other sources from re-establishing links, nor prevent Brokers from other sources from completing pairing.

- **Acceptance**: Host B's listen port keeps receiving many bare TCP connections from several sources other than A, and the total unauthenticated connections from these sources exceeds B's global cap; during this time the link from A to B goes down and is re-established, recovering within the 120-second limit of `REQ-broker-pairing-018`; during the same time a third source's host initiates pairing with B using a valid one-time pairing key B generated, and the pairing succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinksSourceFlood/REQ-broker-pairing-039 source flood keeps links and pairing`

### REQ-broker-pairing-040

An alias a peer abandons while the peer relationship lasts MUST NOT become another peer's alias.

- **Acceptance**: A has two peers whose aliases are `hostB` and `hostC`; B changes its alias from `hostB` to another name, and C then changes its alias to `hostB`. Afterward in A's result of listing Brokers, C's alias is not `hostB`, a message on A sent to `agent@hostB` is not delivered to an agent on C, and the signature of a message sent by an agent on C does not end in `@hostB`.
- **Status**: Implemented
- **E2E test**: `test/e2e/aliases_test.go` `TestAliases/REQ-broker-pairing-040 released alias stays with its peer`

## Data Requirements

The Broker's identity key, peers' fingerprints and aliases, this side's role in each peer pair, and the advertised addresses of peers this side paired with are stored on the local disk and readable only by the account running the Broker. One-time pairing keys are kept only in memory. A pre-provisioned static key is stored in the Broker's configuration file, and any account that can read the configuration file can read it.

## Assumptions and Dependencies

- A pairing key is handed to the other side by the user through a trusted channel outside the system, and eavesdropping on that channel is equivalent to leaking the key. A Broker's address is not secret and may be passed through a channel different from the key's.
- The addresses shown when generating a pairing key may not be reachable by the other side; when they are not, the user tells the other side an address it can reach. The hostname in the display is for the user to recognize and is not used for connecting.
- The strength of a pre-provisioned static key is up to whoever sets it. Any host that has paired with it knows it; such a host, when it can intercept a new host's connection or steer it to the wrong party, can impersonate the party accepting the pairing and complete the pairing. An account with only read permission on the configuration file can also read it and thereby use it to add a peer to this Broker. Setting a pre-provisioned static key accepts these risks.
- When only one of the two hosts can connect to the other, the pairing must be initiated by the side that can reach the other, and the link is thereafter re-established by it.
- When the acceptor changes its address after the link goes down and before it is re-established, and the dialer has not learned the new address from a card, the dialer cannot reach it, and the remedy is to pair again.
- Pairing with a Broker means trusting its statements about the names and topic memberships of agents under it. The system does not verify facts on the peer host.
- Other processes under the account running the Broker can read the data the Broker keeps on disk.
- When the dialer unpairs, it only closes the link; the acceptor is not notified and sees only that the peer is not connected, and the record is kept; the acceptor's user removes this record themselves.
