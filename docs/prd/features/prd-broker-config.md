# Broker configuration, audit and operation

## Problem Statement

While working in a session, the user needs to view and adjust a [Broker](../../glossary.md)'s configuration without leaving the session to edit files. Some configuration options affect security. Who can read and who can change them should agree with the operating system's permission on the configuration file, and operating through tools is only a convenience; what was changed must also be findable afterward.

A Broker has two deployment forms. In production deployment an administrator runs it as a system service, and one Broker serves several accounts on the host; for personal use it runs under an ordinary account and serves only that account; after the user installs it as a service once, it starts at login and keeps running, without needing a manual start each time. Several Brokers on one host must not interfere with each other, and one account should not have two Brokers at once.

## Scope

This document covers the two deployment forms of a Broker and the service scope of each, a Broker's installation, startup, continuous running and uninstallation, degraded run, going offline and online, how the listen address and advertised addresses are determined, reading and modifying configuration and its permissions, the audit log, and viewing status on the host.

### Out of Scope

- A second confirmation independent of Claude Code's permission mechanism. Whether a tool call needs user confirmation is decided by the Claude Code permission settings of its session; this is independent of the file-permission precondition this document specifies, and the call runs only when both are satisfied.
- An administrator list or roles set up outside file permissions.
- Running several Broker instances under one account.
- Automatically switching to another address or port when the listen address is occupied.
- A peer reading or modifying this Broker's configuration.
- Distributed tracing and metrics export.

## Actors

In this document, account means an operating-system account.

| Actor | Authority |
|---|---|
| agent | Listing agents, listing topics, joining and leaving topics and sending messages require no permission on the configuration file. Reading configuration requires the owning account to have read permission on the Broker's configuration file; modifying configuration, generating pairing keys, initiating pairing and unpairing require write permission. |
| User | Through Claude Code's permission settings, decides which tool calls need confirmation. For a user Broker under their own account, can directly edit the configuration file, view status and the audit log in the local terminal, install it as a service (started immediately after installation and starting at login), or start it manually in the terminal. |
| Administrator | Installs and starts the system Broker, and by file permission decides which accounts may use it and which accounts may read or modify its configuration. |

## Functional Requirements

### REQ-broker-config-001

An agent whose account has read permission on the Broker's configuration file MUST be able to read that Broker's current configuration. The permission precondition is specified by `REQ-broker-config-022`.

- **Acceptance**: The alias, whether it is public, the listen address and whether it auto-joins the default topic in the read result match the Broker's actual behavior.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_test.go` `TestDiscoveryCardReachesWholeNetwork`

### REQ-broker-config-002

An agent whose account has write permission on the Broker's configuration file MUST be able to modify that Broker's options, except the cases `REQ-broker-pairing-031` and `REQ-broker-config-029` specify as not modifiable through tools. The permission precondition is specified by `REQ-broker-config-021`.

- **Acceptance**: Change the alias to a new value; after the call succeeds, reading configuration again gives the new alias.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_test.go` `TestConfigTools/SetAliasIsAudited`

### REQ-broker-config-003

Reading configuration and modifying configuration MUST be authorizable separately.

- **Acceptance**: In Claude Code's permission settings allow only the tool that reads configuration; the agent reading configuration triggers no confirmation, and modifying configuration triggers confirmation.
- **Status**: Implemented
- **E2E test**: `test/realsession/probes/permissions.sh` (run through `.github/workflows/real-session.yml` pushing `probe/permissions`)

### REQ-broker-config-004

A tool with no side effect MUST be declared read-only ([REF-mcp](../../references.md)), and a tool with a side effect MUST NOT be declared read-only. No side effect means neither changing the Broker's state nor delivering messages.

- **Acceptance**: Check the declarations of all tools: listing agents, listing topics, listing Brokers and reading configuration carry the read-only annotation; joining and leaving topics, sending messages, generating pairing keys, initiating pairing, unpairing, modifying configuration, setting online and offline, and upgrading the Broker do not carry the read-only annotation.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_test.go` `TestConfigToolAnnotations`

### REQ-broker-config-005

Generating a pairing key MUST be authorizable independently of modifying configuration.

- **Acceptance**: In Claude Code's permission settings allow only the tool that generates pairing keys; the agent generating a one-time pairing key triggers no confirmation, and modifying configuration triggers confirmation.
- **Status**: Implemented
- **E2E test**: `test/realsession/probes/permissions.sh` (run through `.github/workflows/real-session.yml` pushing `probe/permissions`)

### REQ-broker-config-006

Every modification an agent makes to configuration MUST be written to the audit log, recording the agent that initiated it and its owning account, the option changed, the value before, the value after and the time. For a pre-provisioned static key, only that it changed is recorded, not the value.

- **Acceptance**: After the alias is modified, the audit log gains one record whose content matches this change.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_test.go` `TestConfigTools/SetAliasIsAudited`

### REQ-broker-config-007

Each generation of a one-time pairing key, successful pairing, failed pairing, link establishment, link disconnection and unpairing MUST be written to the audit log, recording the event type, the fingerprints and aliases of the Brokers involved and the time; when an agent initiates it, the initiating agent and its owning account are also recorded.

- **Acceptance**: Run in turn the generation of a one-time pairing key, a failed pairing, a successful pairing, a network disconnect, a network restoration and an unpairing; the audit log gains one record of the corresponding type each, in order; among them, records initiated by an agent carry that agent's address and its owning account.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_cluster_test.go` `TestConfigToolsCluster/PairingAndLinkEventsInOrder`

### REQ-broker-config-008

The audit log MUST NOT record pairing key values or message bodies.

- **Acceptance**: After one pairing and one message send, searching the audit log for the pairing key used and the message body finds neither.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_cluster_test.go` `TestConfigToolsCluster/AuditHasNoSecrets`

### REQ-broker-config-009

A user whose account has read permission on the Broker's configuration file MUST be able to view that Broker's audit log in the local terminal.

- **Acceptance**: Run the command to view the audit log in the terminal as an account with read permission and the output contains all audit records produced earlier; run it as an account without read permission and get an insufficient permission error.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/permission_test.go` `TestREQBrokerConfig009_AuditNeedsReadPermissionOnSystemBroker` (system: the output an account with read permission sees corresponds one-to-one with all the Broker's audit records; an account without read permission is refused); `test/e2e/config_tools_test.go` `TestConfigTools/TerminalAuditCommand` (user: the output corresponds one-to-one with `Oracle.Audit`)

### REQ-broker-config-010

A user MUST be able to view the Broker's status in the local terminal, including registered agents, each agent's topics, and all peers and their connection status. A user whose account has no read permission on the configuration file can see only agents under their own account, and peers and their connection status remain visible as usual.

- **Acceptance**: Register two agents and have them join different topics, and after pairing with one peer disconnect the network; run the status command in the terminal as an account with read permission and the output lists these two agents and their topics and shows that peer as not connected. Run it on a system Broker as an account without read permission and the output contains only this account's own agents and likewise lists that peer and its connection status.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/permission_test.go` `TestREQBrokerConfig010_StatusWithoutReadPermissionShowsOwnAgents` (an account without read permission sees only its own agents and sees the peer as not connected; a non-root account with read permission sees two agents and their topics, and after pairing with the peer stops the peer Broker instead of "disconnecting the network", and the peer is still listed and shown as not connected)

### REQ-broker-config-011

A Broker MUST output its runtime log in a structured format.

- **Acceptance**: Every line of the runtime log can be parsed as one record, with the fields time, level and event name.
- **Status**: Implemented
- **E2E test**: `test/e2e/broker_process_test.go` `TestBrokerProcessInContainer/REQ-broker-config-011 structured log`

### REQ-broker-config-038

The MCP server process MUST NOT start any Broker. When a session finds no available Broker following the order of `REQ-broker-config-026`, a tool call MUST return an error stating how to start a Broker; when the Broker starts afterward, the session can use it without reconnecting the MCP service.

- **Acceptance**: Start a session with the tools enabled on a host with no Broker process; the error a tool call returns states that the service can be installed or the Broker started in the terminal, and no Broker process appears on the host; then start this account's Broker in the terminal, and without reconnecting the MCP service in Claude Code, the next agent listing succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/broker_process_test.go` `TestBrokerProcessLocal/REQ-broker-config-038 no Broker until started`; `test/e2e/broker_process_test.go` `TestBrokerProcessInContainer/REQ-broker-config-024 default address taken` and `TestBrokerProcessInContainer/REQ-agent-messaging-038 recover without reconnect` (terminal startup fails, the session reports an error, and it recovers after a manual start)

### REQ-broker-config-039

A user MUST be able to perform every query and management operation of a Broker in the local terminal: view status, read the audit log, list Brokers, read configuration, modify configuration, set online and offline, generate one-time pairing keys, initiate pairing and unpair. A terminal command and the agent's corresponding operation follow the same permission rules, judged by the account running the command, and write the same audit records.

- **Acceptance**: On a system Broker, an account without write permission on the configuration file generates a pairing key in a session and is refused; run the command to generate a pairing key in the terminal as the administrator and the output gives the key, the expiry time and the local advertised addresses, and another host initiates pairing in the terminal with this information and succeeds, and both sides listing Brokers in the terminal show the other as connected. Also as the administrator, read and modify one option in the terminal and unpair this peer, all succeeding, and the audit log records these operations and the accounts running them in order. Run the command to modify configuration as an account without write permission and it returns insufficient permission and gives the configuration file path.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/term_admin_test.go` `TestREQBrokerConfig039_TerminalAdminCommands`

### REQ-broker-config-040

When installing a service, if the configuration file for that level does not yet exist, it MUST generate a configuration file listing every option: options with a default value are written with the default, options with no fixed default are given as comments, and each option has a one-line explanation. An existing configuration file MUST NOT be changed.

- **Acceptance**: Install a user and a system service separately on hosts with no configuration file; in the generated configuration file every option the design specifies appears exactly once; the values written match the current values obtained when reading configuration. Uncomment one commented-out option and fill in a valid value, and the Broker adopts it without restarting. After modifying a commented-out option with the configuration tool, this option has only one effective value in the file, and other options and comments are unchanged. When the configuration file already exists, installing again leaves its content unchanged.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/config_template_test.go` `TestREQBrokerConfig040_SystemInstallWritesTemplate`, `TestREQBrokerConfig040_UserInstallWritesTemplate`

### REQ-broker-config-041

The release package MUST include an install script, and after extracting it the user can complete the installation with one command: install the binary in a suitable location, install the Broker as a service and start it, and register this system's MCP service in Claude Code for the user running the installation. A system installation has the script gain administrator identity through sudo itself. When fish is installed on the host, the install script MUST also install the fish completion; when the host has no fish it skips this and does not create fish's configuration directory. Running the install script repeatedly MUST NOT fail or register twice.

- **Acceptance**: On a Linux host where this system has never been installed, extract the release package and run the install script as an ordinary account. Afterward in a new terminal, `peers --version` prints the release version, this account's user Broker is running, and `claude mcp get peer` shows it is registered. Run the install script as an ordinary account, answer y to the level prompt and enter the sudo password; the system Broker is running, the binary is in a root-owned directory not writable by group or others, and the account that ran sudo has registered the MCP service in Claude Code. Run each way once more, and both succeed, with the MCP service registered only once. Install on a host with fish and the fish completion file exists; install on a host without fish and the fish completion file does not exist and fish's configuration directory is not created either.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_script_test.go` `TestREQBrokerConfig041_UserInstallScript`, `TestREQBrokerConfig041_SystemInstallScript`, `TestREQBrokerConfig044_InstallScriptLevelPromptAcceptsSystemLevel` (system installation answering y to the level prompt)

### REQ-broker-config-042

A user MUST be able to install online from this project's GitHub official releases with one command: the command downloads this platform's release package from the latest official version, verifies it, and then completes the installation with the install script in the package. A system installation runs the same command and answers y to the level prompt. The user can specify the version to install. When the checksum does not match or the download fails, the installation MUST abort and leave no change on the host.

- **Acceptance**: On a Linux host where this system has never been installed, run one online install command as an ordinary account, and the result is the same as running the install script in the release package locally; run the same command and answer y to the level prompt, and the result is the same as a local system installation. With a published version specified, that version is installed; with a nonexistent version specified, the command exits nonzero with the error `version <v> not found`, and the host has no change. Make the downloaded release package mismatch its checksum, and the command exits nonzero, with no change to the binary, service or MCP registration, and no temporary file left.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/remote_install_test.go` `TestREQBrokerConfig042_RemoteInstallUser`, `TestREQBrokerConfig042_RemoteInstallSystem`, `TestREQBrokerConfig042_RemoteInstallPinnedVersion`, `TestREQBrokerConfig042_RemoteInstallUnknownVersion`, `TestREQBrokerConfig042_RemoteInstallArchiveWithoutInstallScript`, `TestREQBrokerConfig042_RemoteInstallTruncatedScript`, `TestREQBrokerConfig042_RemoteInstallChecksumMismatch`

### REQ-broker-config-013

At most one Broker instance runs under one account. While an instance is running, starting another with any listen address or configuration MUST fail and state that an instance is already running.

- **Acceptance**: On a host with no Broker process, run the command to start the Broker five times at once under the same account; only one Broker process under that account keeps running and the rest exit with an error stating that an instance is already running; then start five sessions with the tools enabled under that account, and all register with this Broker; then start a Broker manually under the same account with another listen address, and startup fails with an error stating that an instance is already running, and the existing Broker is unaffected.
- **Status**: Implemented
- **E2E test**: `test/e2e/broker_process_test.go` `TestBrokerProcessLocal/REQ-broker-config-013 single instance`

### REQ-broker-config-014

After the last agent's session on that Broker ends, the Broker MUST keep running and keep its links with peers.

- **Acceptance**: End every session on the host; the Broker process is still there, and a peer's result of listing Brokers still shows it as connected.
- **Status**: Implemented
- **E2E test**: `test/e2e/links_test.go` `TestLinks/REQ-broker-config-014 broker outlives last session`

### REQ-broker-config-015

A user MUST be able to install the user Broker under their own account as a service: started immediately after installation, and starting at that account's login thereafter.

- **Acceptance**: After installation, without logging in again, this account's Broker process is already running; after logging in again, with no session started, this account's Broker process is already running and has restored links with peers. Both macOS and Linux satisfy this.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/user_service_test.go` `TestREQBrokerConfig015_UserServiceStartsAtLoginAndRestoresLink` (Linux: runs immediately after installation, starts after logging in again and restores the link); `test/e2e/darwinservice/darwin_test.go` `TestUserServiceRestoresLinkAfterRelogin` (macOS: simulates login with bootout/bootstrap, asserting a new pid appears and the peer link is restored to connected)

### REQ-broker-config-016

When a session's MCP server process is incompatible with the version of the running Broker, a tool call MUST fail and prompt the user to restart the Broker, and the system MUST NOT terminate the running Broker on its own.

- **Acceptance**: Connect an incompatible version's MCP server process to the running Broker, and a tool call returns an error prompting to restart the Broker, and the original Broker process and other agents registered with it are unaffected.
- **Status**: Implemented
- **E2E test**: `test/e2e/version_local_test.go` `TestLocalVersionMismatchReq016`, `TestLocalVersionMismatchRecoveryReq016`

### REQ-broker-config-017

After the user directly modifies the Broker's configuration file, the Broker MUST adopt the valid new values without restarting, keep the original values for invalid options, and write one audit record for each option that adopted a new value; for a pre-provisioned static key, only that it changed is recorded, not the value. During a run where the listen address is given by a startup parameter, a change to the listen address option is not covered by this.

- **Acceptance**: While the Broker runs, change the alias to a new value in the configuration file; without restarting the Broker, reading configuration again gives the new value, and the audit log gains one record with the before and after values of this option; when the same modification changes the alias to a new value and another option to an invalid value, the alias adopts the new value and the other option keeps its original value. When running with the startup parameter specifying listen `:17444`, modify both the listen address and the alias in the configuration file: the Broker still listens on port 17444, and the audit log gains only the record for the alias.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_test.go` `TestConfigTools/FileEditHotReload`

### REQ-broker-config-018

A Broker running with privileges as a system service, namely a system Broker, MUST be able to serve agents under several accounts on the host at once.

- **Acceptance**: A system Broker runs on the host, two different accounts each start a session with the tools enabled, and both sessions register with this Broker; after both join the same topic they see each other and can exchange messages.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/multi_account_test.go` `TestREQBrokerConfig018_TwoAccountsShareSystemBroker`

### REQ-broker-config-019

A user Broker MUST refuse registration and calls from other accounts.

- **Acceptance**: Account A runs a user Broker; account B's session's MCP server process and account B's commands in the terminal cannot connect to it, B's agent does not appear on A's Broker, and B cannot read its status, configuration or audit log.
- **Status**: Implemented
- **E2E test**: `test/e2e/accounts_test.go` `TestOtherAccountRejected/REQ-broker-config-019 filesystem layer`, `TestOtherAccountRejected/REQ-broker-config-019 credential layer`, `TestOtherAccountRejected/REQ-broker-config-019 account paths`

### REQ-broker-config-020

A system Broker MUST refuse to register as an agent a session that does not belong to the caller's account.

- **Acceptance**: Account B's process asks the system Broker to register a session of account A, and the session information it reports is exactly the same as that session's own MCP server process reports; the registration is refused, and no agent resulting from it appears in any agent's list.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/foreign_session_test.go` `TestREQBrokerConfig020_ForeignSocketOwnerRejected/unreadable record`, `TestREQBrokerConfig020_ForeignSocketOwnerRejected/record readable but socket owned by another account`

### REQ-broker-config-021

Modifying configuration, setting online and offline, generating pairing keys, initiating pairing and unpairing can be performed only when the caller's account has write permission on the Broker's configuration file; without write permission it MUST return insufficient permission and change no state.

- **Acceptance**: On a system Broker, an account without write permission on the configuration file calls these operations in turn, each returns insufficient permission, and the configuration, peers and valid pairing keys are unchanged; after the administrator grants write permission on the configuration file to the group the account belongs to, the same calls all succeed.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/permission_test.go` `TestREQBrokerConfig021_WriteNeedsConfigWritePermission`

### REQ-broker-config-022

Reading configuration can return configuration content only when the caller's account has read permission on the Broker's configuration file; without read permission it MUST return insufficient permission.

- **Acceptance**: On a system Broker, an account without read permission on the configuration file reads configuration, it returns insufficient permission and the result contains no option value; an account with read permission reads it and it returns the current configuration.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/permission_test.go` `TestREQBrokerConfig022_ReadNeedsConfigReadPermission`

### REQ-broker-config-023

Registering, listing agents, listing topics, listing Brokers, joining and leaving topics, and sending messages MUST NOT require any permission on the configuration file.

- **Acceptance**: On a system Broker, an account with neither read nor write permission on the configuration file starts a session; the session completes registration and successfully performs listing agents, listing topics, listing Brokers, joining a topic, leaving a topic and sending messages.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/permission_test.go` `TestREQBrokerConfig023_NoPermissionStillWorksOnSystemBroker`

### REQ-broker-config-024

A Broker started without degraded run allowed MUST fail to start and report that the address is unavailable when the listen address given by configuration or a startup parameter cannot be bound, and MUST NOT switch to another address or port.

- **Acceptance**: First make another process listen on port 17443 on every local address, then run `peers service run` in the terminal with the default configuration: startup fails with a nonzero exit code, the error states that the address `:17443` is unavailable, and the host has no process of this Broker or its listener on another port left.
- **Status**: Implemented
- **E2E test**: `test/e2e/broker_process_test.go` `TestBrokerProcessInContainer/REQ-broker-config-024 default address taken`

### REQ-broker-config-025

Starting a Broker MUST allow specifying the listen address, replacing the value in configuration.

- **Acceptance**: Keep the configuration at default and start with the listen address `:17444`: the Broker listens on port 17444 on every local address, does not listen on port 17443, and the configuration file content is unchanged.
- **Status**: Implemented
- **E2E test**: `cmd/peers/broker_process_test.go`: `TestBrokerListenFlagOverridesConfig`

### REQ-broker-config-026

When a session has not yet selected a Broker and several Brokers are available, the MCP server process MUST pick the first available in this order: a running user Broker under this account; a system Broker this account is permitted to use.

- **Acceptance**: The host runs both a system Broker and account A's user Broker; the system Broker listens on `:17443` and A's user Broker on `:17444`. A's session registers with A's user Broker; after A's user Broker stops, a newly opened session registers with the system Broker; an account B without a user Broker has its session register with the system Broker. Setting an old removed variable `AGENT_PEER_SOCKET` in the session's environment does not affect the choice.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/select_reregister_test.go` `TestSelectOrderReq026`

### REQ-broker-config-027

A system Broker's administrator MUST be able to limit by file permission which accounts may use it.

- **Acceptance**: The administrator limits the system Broker's use to a group. Sessions of accounts in the group register with it as usual; an account outside the group has set its listen address to `:17444` in its own configuration, its session cannot register with the system Broker, and per `REQ-broker-config-038` it returns an error stating there is no available Broker; after this account starts its own user Broker, the session registers with it.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/socket_group_test.go` `TestREQBrokerConfig027_GroupRestrictsUse`, `TestSystemSocketGroupReappliedOnReload`, `TestSystemSocketGroupUnknownRejected`

### REQ-broker-config-028

Without configuration, a Broker MUST listen on port 17443 on every local address, including IPv6 addresses when IPv6 is enabled; when the listen address is configured empty it does not listen.

- **Acceptance**: On a host with IPv6 enabled, start a Broker with the default configuration, and another host can reach its port 17443 over both the host's IPv4 and IPv6 addresses; after configuring the listen address empty and restarting, it listens on no port and can still connect out to peers it paired with.
- **Status**: Implemented
- **E2E test**: `test/e2e/addresses_test.go` `TestAdvertisedAddresses/REQ-broker-config-028 dual stack and no listen`

### REQ-broker-config-029

During a run where the listen address is given by a startup parameter, a call modifying the listen address through a tool MUST fail with the reason stated, and the configuration file MUST NOT be changed.

- **Acceptance**: Start the Broker with the startup parameter specifying listen `:17444`, then modify the listen address to `:17445` through a tool: the call returns an error stating that this run's listen address is given by a startup parameter; the configuration file content is unchanged and the Broker still listens on port 17444. After restarting without the parameter, the same modification succeeds.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_test.go` `TestConfigTools/ListenFlagBlocksSet`

### REQ-broker-config-030

When a Broker listens on every address and has no advertised address containing a host configured, the advertised address list MUST contain only the address of each default route's interface, plus addresses the user added through configuration.

- **Acceptance**: B runs with the default configuration; the host's default IPv4 route's interface address is `192.0.2.20`, there is another virtual network interface with address `10.211.55.2`, and there is no default IPv6 route. After A pairs with B using B's address and a pairing key B generated, B's advertised address list in A's result of listing Brokers has only `192.0.2.20:17443`. After adding a default IPv6 route to B's host, whose interface's global address is `2001:db8::20`, the list becomes `192.0.2.20:17443` and `[2001:db8::20]:17443`, still without `10.211.55.2`; after configuring B's advertised address to the port-only `:18443`, the ports in the list all change to 18443; after adding the advertised address `b.vpn.example` to B, the list gains `b.vpn.example:18443` at the end.
- **Status**: Implemented
- **E2E test**: `test/e2e/addresses_test.go` `TestAdvertisedAddresses/REQ-broker-config-030 default route addresses`

### REQ-broker-config-031

When an advertised address containing a host is configured, the advertised address list MUST have that address first, followed only by addresses the user added through configuration.

- **Acceptance**: Public Broker B listens on port 17443 and has its advertised address configured to `b.example:443`: B's advertised address list in B's dialer's result of listing Brokers has `b.example:443` first, and so do other Brokers holding B's card, and the list has no default-route interface address. After changing the advertised address to the host-only `b.example`, the first item is `b.example:17443`; after adding the advertised address `b.vpn.example`, the list is `b.example:17443` and `b.vpn.example:17443` in order.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_address_test.go` `TestCardShowsConfiguredAddresses`

### REQ-broker-config-032

When the listen address contains a specific host and the advertised address is port-only, or when the listen address is empty and the advertised address or an added advertised address is non-empty, the Broker MUST refuse to start and point out these two options; such a change during operation MUST fail and keep the original value.

- **Acceptance**: Configure the listen address to `192.0.2.20:17443` and the advertised address to the port-only `:18443`, then start the Broker; startup fails and the error points out both the listen address and the advertised address. Running with listen `192.0.2.20:17443` and no advertised address, modify the advertised address to `:18443` through a tool: the call returns an error, and reading configuration again gives the original value; make the same modification in the configuration file, the Broker does not adopt it, and reading configuration still gives the original value. When the listen address is configured empty and the advertised address or an added advertised address is non-empty, the result is the same.
- **Status**: Implemented
- **E2E test**: modification through a tool during operation is covered by `TestConfigTools/RuntimeAddressRulesThroughTools` in `test/e2e/config_tools_test.go`, the configuration-file path by `TestConfigTools/FileEditRejectsCombination`, and both the tool and configuration-file paths when the listen address is empty by `TestConfigTools/EmptyListenRejectsAddresses`; the startup failure half is covered by `test/e2e/addresses_test.go` `TestAdvertisedAddresses/REQ-broker-config-032 and 036 startup half`

### REQ-broker-config-033

A user MUST be able to configure added advertised addresses, and the added addresses appear in the advertised address list.

- **Acceptance**: After Broker B running with the default configuration adds the advertised addresses `b.vpn.example` and `198.51.100.7:8443`, in B's dialer's result of listing Brokers, B's advertised address list has `b.vpn.example:17443` and `198.51.100.7:8443` in order after the default-route interface address; after B is set public, other Brokers holding B's card list the same list.
- **Status**: Implemented
- **E2E test**: `test/e2e/discovery_address_test.go` `TestCardShowsConfiguredAddresses`

### REQ-broker-config-034

When the listen address is changed during operation to an unbindable address, the change MUST fail and the Broker MUST keep listening on the original listen address.

- **Acceptance**: The Broker listens on `:17444` and another process occupies port 17445 on every local address; modify the listen address to `:17445` through a tool, the call returns an error stating that the address is unavailable, reading configuration again gives `:17444`, and another host can still reach its port 17444; make the same modification in the configuration file and the result is the same.
- **Status**: Implemented
- **E2E test**: `test/e2e/config_tools_test.go` `TestConfigTools/UnbindableListen` (both the tool and the configuration-file paths; local dialing)

### REQ-broker-config-035

When a Broker listens on a specific address and has no advertised address configured, the advertised address list MUST have the listen address first, followed only by addresses the user added through configuration.

- **Acceptance**: Configure the listen address to `192.0.2.20:17443` with no advertised address; B's advertised address list in B's dialer's result of listing Brokers has only `192.0.2.20:17443`; after adding the advertised address `b.vpn.example`, the list is `192.0.2.20:17443` and `b.vpn.example:17443` in order.
- **Status**: Implemented
- **E2E test**: `test/e2e/addresses_test.go` `TestAdvertisedAddresses/REQ-broker-config-035 specific listen`

### REQ-broker-config-036

When the advertised address configuration makes the advertised address list exceed 8 items, the Broker MUST refuse to start and point out this option; such a change during operation MUST fail and keep the original value.

- **Acceptance**: Configure an advertised address containing a host and add 8 advertised addresses, then start the Broker; startup fails and the error points out the added advertised addresses option; when running with a configuration that adds only 7, add one more through a tool, the call returns an error, and reading configuration again gives the original value.
- **Status**: Implemented
- **E2E test**: modification through a tool during operation is covered by `TestConfigTools/RuntimeAddressRulesThroughTools` in `test/e2e/config_tools_test.go`, the configuration-file path by `TestConfigTools/FileEditRejectsAddressOverflow`; the startup failure half is covered by `test/e2e/addresses_test.go` `TestAdvertisedAddresses/REQ-broker-config-032 and 036 startup half`

### REQ-broker-config-037

After the Broker process a session registered with exits, the system MUST register the session again with the same Broker, and MUST NOT re-register it with another Broker.

- **Acceptance**: The host runs both a system Broker and account A's user Broker on different ports, and A's session registers with the user Broker; terminate the user Broker process, and before it restarts, the session's listing agents returns an error stating there is no available Broker, and no other Broker appears on the host; after it is restarted by the user service management mechanism or by the user, without reconnecting the MCP service, the session's next agent listing succeeds, and the session is still registered with this user Broker whose fingerprint is unchanged, and the session's agent does not appear on the system Broker. When account B's session registers with the system Broker, terminate the system Broker process, and before it is restarted by the system service management mechanism, B's user Broker does not appear on the host; after it restarts, B's session registers again with the system Broker.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/select_reregister_test.go` `TestReregisterUserBrokerReq037`, `TestReregisterSystemBrokerReq037`

### REQ-broker-config-043

When installing a system service, the system MUST provide an admin group: create it when it does not exist, and make the newly generated configuration file owned by the group with read and write permission for the group. When the group already exists, has members and was not created by peers earlier, it MUST continue only after the administrator explicitly specifies this group before generating a new configuration file; the admin group's gid MUST NOT be 0. Members of the admin group can perform every operation that needs read and write permission on the configuration file without root or sudo. When installing a system service with the install script in the release package, the script MUST ask whether to add the administrator account running the installation to the admin group, defaulting to not adding; when it cannot ask, it treats it as not adding and gives the command to add.

- **Acceptance**: On a fresh host, run `sh ./install.sh` as an ordinary account, answer y to the level prompt, enter the sudo password and answer y to the admin group prompt; after the account that ran sudo logs in again, it successfully runs, without sudo in the terminal, generating a pairing key, modifying a configuration option and initiating pairing, and the audit records that account; when the prompt is answered with a blank line, or the script runs without a terminal, the account is not added to the admin group, and the output gives the command to add it; run with `--join-admin-group`, it joins without asking; an ordinary account not in the group running the same commands returns insufficient permission. When the configuration file already exists, reinstalling leaves its owning group and permissions unchanged. On a host that already has a `peers` group with members, with no `/var/lib/peers/admin-group` record and no configuration file, run `service install --system` without `--admin-group`; the installation fails, the error lists the members and gives two ways out, `--admin-group peers` and switching to another group name, and the configuration file is not generated; reinstall with `--admin-group peers` and it succeeds, the summary's `admin group:` line marks the group as reused (after explicit specification it no longer reads the members), and the configuration file's owning group is this group. A group with gid 0 is refused even when explicitly specified.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/admin_group_test.go` `TestREQBrokerConfig043_SystemInstallAdminGroup` (the test environment configures sudo with NOPASSWD; the "enter the sudo password" step is covered jointly by statically reading the install script and the sudo re-execution case), `TestREQBrokerConfig043_ExistingAdminGroupNeedsConfirmation` (existing same-named group with members: refused without specifying and does not write the configuration file, succeeds after explicit specification, and the summary only marks it as reused and skips reading members)

### REQ-broker-config-044

A non-root installation without `--system` or `--user` that can prompt in the terminal MUST ask `Install for every account on this host? [y/N]`, complete a system installation through sudo when answered y, and complete a user installation for any other answer.

- **Acceptance**: On a host where this system has never been installed, run `curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh` as an ordinary account: this prompt appears; after answering y and entering the sudo password, the system Broker is running, the binary is in a root-owned directory, and the account running the installation has registered the MCP service in Claude Code; on another host, answer with a blank line, and this account's user Broker is running and the host has no system service. After extracting the release package, run `sh ./install.sh`, and the result is the same.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_script_test.go` `TestREQBrokerConfig044_InstallScriptLevelPromptAcceptsSystemLevel`, `TestREQBrokerConfig044_InstallScriptLevelPromptDefaultsToUser`; `test/e2e/system/remote_install_test.go` `TestREQBrokerConfig042_RemoteInstallSystem`

### REQ-broker-config-045

An installation run as root MUST complete a system installation without asking.

- **Acceptance**: Run an online install command as root with no parameters; the level prompt does not appear during installation, and afterward the system Broker is running and the host has no root user service.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_script_test.go` `TestREQBrokerConfig045_InstallScriptRootDoesNotAsk`

### REQ-broker-config-046

A non-root installation that cannot prompt in the terminal MUST complete a user installation without asking.

- **Acceptance**: Run an online install command as an ordinary account in an environment with no controlling terminal, for example through `setsid` with standard input connected to `/dev/null`: the command waits for no input, and afterward this account's user Broker is running and the host has no system service.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_script_test.go` `TestREQBrokerConfig046_InstallScriptWithoutTerminal`

### REQ-broker-config-047

When a system service is already installed on the host, the prompt of `REQ-broker-config-044` MUST become `Upgrade the system installation? [Y/n]`; a blank line or y upgrades the system installation through sudo; n MUST NOT change the host, and the command exits with 0, stating that the system installation was not changed.

- **Acceptance**: On a host with an older system service installed, run an online install command as an ordinary account: the prompt is this sentence; after a blank line and entering the sudo password, the system Broker runs the new version and the configuration file content is unchanged. On another identical host, answer n: the command exits with 0, the binary, service and configuration are unchanged, and the output states that the system installation was not changed and does not suggest `--user`.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_script_test.go` `TestREQBrokerConfig047_InstallScriptUpgradePrompt`

### REQ-broker-config-048

An installation with `--user` MUST complete a user installation without asking; an installation with `--system` MUST complete a system installation without the level prompt.

- **Acceptance**: Run `curl ... | sh -s -- --user` as an ordinary account; the level prompt does not appear, and afterward the user Broker is running; run `curl ... | sh -s -- --system` as an ordinary account; the level prompt does not appear, and the script completes the system installation through sudo; with both flags together it exits with a usage error and the host has no change; run as root with `--user` and it is refused, prompting to drop `--user`.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_script_test.go` `TestREQBrokerConfig048_InstallScriptBothFlags`, `TestREQBrokerConfig048_InstallScriptUserFlag`, `TestREQBrokerConfig048_InstallScriptSystemFlag`

### REQ-broker-config-049

When installation, upgrade or uninstallation fails, the output MUST state the specific reason, and MUST NOT say only that it cannot connect or did not start.

- **Acceptance**: Induce the following failures separately, and in each case the command exits nonzero and the output contains the content in parentheses: the listen address is occupied by another process (the address, the configuration file path and the option `broker.listen`); `systemctl` or `launchctl` returns failure (the command run and its error output); no permission on the installation directory or the configuration file (the path and the missing permission); the Broker exits during startup (the reason in the `startup_failed` record in the runtime log).
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_port_conflict_test.go` `TestSystemServiceInstallListenPortConflict`, `test/e2e/darwinservice/install_port_test.go` `TestUserServiceInstallRefusesAnOccupiedListenPort` (when the address is occupied the error contains the address, the configuration file path and `broker.listen`); the remaining failure paths are covered by unit tests: `TestInstallLinuxStepFailures`, `TestInstallDarwinBootstrapFailureReportsTheOutput`, `TestCreateConfigRemovesPartialFileOnWriteFailure`, `TestInstallReportsTheStartupFailureOnTimeout` and `TestInstallReportsTheServiceManagerStateOnTimeout` in `internal/broker/service/install_test.go` (command failure, insufficient permission, exit during startup and service manager state), `TestUninstallStopsAfterAFailedStep` and `TestUninstallReportsPathsRemovedBeforeAPurgeFailure` in `cmd/peers/uninstall_cmd_test.go` (the step, paths and reason of a failed uninstallation), and `TestApplyFailureLeavesTargetUnchanged` in `internal/broker/update/apply_test.go` and `TestLatestRejectsAPIError` and `TestLatestRejectsInvalidContent` in `api_test.go` (the reason for a failed upgrade)

### REQ-broker-config-050

After installation or upgrade succeeds, the output MUST give the installation level, the binary path, the version, the configuration file path, the runtime log location, the audit log path, the commands to start, stop and restart the Broker, and the uninstall command.

- **Acceptance**: Complete one user installation and one system installation separately; the output contains each of the above, the paths in it exist on the host, and the start, stop, restart and uninstall commands it gives all succeed when copied and run.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_summary_test.go` `TestREQBrokerConfig050_SystemServiceInstallPrintsSummary`, `TestREQBrokerConfig050_UserServiceInstallPrintsSummary` (Linux: system and user, the paths exist and the configuration is reused, and the `start`, `stop`, `restart` and `uninstall` commands in the summary all run successfully when copied); `test/e2e/darwinservice/install_summary_test.go` `TestREQBrokerConfig050_UserServiceInstallPrintsSummary`, `TestREQBrokerConfig050_SystemServiceInstallPrintsSummary` (macOS: user and system, the same assertions and copying); `test/e2e/system/install_script_test.go` `TestREQBrokerConfig050_InstallScriptUserSummary`, `TestREQBrokerConfig050_InstallScriptSystemSummary` (Linux: user and system installations through the install script in the release package, the summary appears in the script output, and the paths exist and are copied and run)

### REQ-broker-config-051

`peers uninstall` MUST determine the installed level and remove that level's service definition, the binary the service definition starts, the uninstalling account's MCP registration in Claude Code, that level's runtime directory, and the completion files the install script wrote; for a system installation it also removes this project's man pages.

- **Acceptance**: On a host with a completed user installation, run `peers uninstall` and answer the data prompt with a blank line: afterward this account's Broker process does not exist, and the service definition file, the binary and the runtime directory do not exist, and `claude mcp get peer` reports not registered; the configuration file, identity, audit log and service log remain. Run it for a system installation and the result is the same. When the host has both a system and a user installation, a non-root account uninstalls only the user installation, and afterward the output states that the host still has a system installation and `sudo peers uninstall`. Run it again and the command succeeds and reports that nothing is installed.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/uninstall_test.go` `TestUninstallREQBrokerConfig051UserLevel`, `TestUninstallREQBrokerConfig051SystemLevel`, `TestUninstallREQBrokerConfig051NoService`, `TestUninstallREQBrokerConfig051UnrecordedGroup`; `test/e2e/darwinservice/uninstall_test.go` `TestUninstallREQBrokerConfig051UserLevel`

### REQ-broker-config-052

When a non-root account has only a system installation, `peers uninstall` MUST re-run through sudo: re-run directly when `--yes` is given; when there is a terminal, ask first and re-run after y. With no terminal and no `--yes` it reports an error and exits with status 1, and the error message gives `sudo peers uninstall ... --yes` that can be re-run directly.

- **Acceptance**: The host has only a system installation; run `peers uninstall` as an ordinary account without a terminal: the command reports an error with status 1, the error message contains `sudo peers uninstall` and `--yes`, and the host has no change; copy this command, run it and enter the sudo password, and the system installation is removed per `REQ-broker-config-051`. Run the same command on a terminal and after answering y the command re-runs through sudo, and the system installation is likewise removed; run with `--yes` and it re-runs through sudo directly without asking.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/uninstall_test.go` `TestUninstallREQBrokerConfig052NonRootSystemOnly`, `TestUninstallREQBrokerConfig052TwoLevels`

### REQ-broker-config-053

Uninstallation MUST ask `Also remove configuration, identity, audit and service logs? [y/N]`, and remove this data only when answered y; `--yes` skips the prompt and keeps this data. A system installation also removes an admin group whose installation record proves it was created by this project; without an installation record it keeps the admin group and outputs the reason for keeping it and the manual removal command. An uninstallation without `--purge` MUST NOT remove them in other cases. With no terminal and no `--yes`, the command reports an error and exits with status 1, removing nothing, and the same for `--purge`.

- **Acceptance**: Uninstall on a host with a system installation and answer this prompt with a blank line: the configuration file, identity and peer table, and audit log are there, and the admin group still exists; after reinstalling, the Broker's fingerprint is the same as before uninstallation, and existing peers are still peers. On another host, answer y: this data does not exist; with an installation record the admin group does not exist either, and without a record the admin group is kept and the reason for keeping it and the manual removal command are output. With no terminal and no `--yes`, the command exits with status 1 and removes nothing.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/uninstall_test.go` `TestUninstallREQBrokerConfig051UserLevel`, `TestUninstallREQBrokerConfig051SystemLevel` (assertions for keeping data and the admin group)

### REQ-broker-config-054

`peers uninstall --purge` MUST, without the data prompt, remove everything listed in `REQ-broker-config-051` and `REQ-broker-config-053`; afterward none of these paths has this system's files or directories, and the service definition, binary and MCP registration do not exist; the admin group is removed only when the installation record proves this project created it, and without a record it is kept with the reason and the manual removal command output.

- **Acceptance**: On a host with a completed user and system installation, one paired peer and produced audit records, run `peers uninstall --purge`: the command does not ask about the data; afterward the configuration file and configuration directory, data directory, audit log and runtime directory do not exist, the peers service cannot be found in the service manager, and `claude mcp get peer` reports not registered; with an installation record `getent group peers` (on macOS `dscl . -read /Groups/peers`) reports it does not exist, and without a record the admin group is kept and the reason and the manual removal command are output.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/uninstall_test.go` `TestUninstallREQBrokerConfig051UserLevel`, `TestUninstallREQBrokerConfig051SystemLevel`, `TestUninstallREQBrokerConfig051UnrecordedGroup`, `TestUninstallREQBrokerConfig054OtherAccountBroker`

### REQ-broker-config-055

A user MUST be able to start, stop and restart the Broker service with `peers service start`, `stop` and `restart`, with the level acted on specified by `--system` or `--user`, which are mutually exclusive; when neither is given it is decided by the account running the command: root acts on the system service and other accounts act on this account's user service. When the service of that level is not installed, the command MUST fail and state that no service of that level is installed; when a non-root account has only a system installation, the command MUST fail and give `sudo peers service <command> --system`. When, after `start` or `restart`, the service is running but the Broker is in degraded run, the command MUST output the fault and the repair command and exit with exit code 3.

- **Acceptance**: On a host with both a system service and account A's user service, A runs `peers service stop` and A's user Broker stops while the system Broker is unaffected; A runs `sudo peers service restart` and the system Broker runs with a new process id while A's user Broker is still stopped; A runs `peers service start` and the user Broker runs again. When the service of that level is not installed, the command exits nonzero and states that no service of that level is installed; A runs it on a host with only the system service and the command exits with 1 and gives `sudo peers service <command> --system`. First stop A's user service and let another process occupy the configured listen port, then A runs `peers service start`: it outputs the degraded reason and the repair command, the exit code is 3, and the service is running.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/service_control_test.go` `TestServiceControl` (`stop and start act on one level only`, `a user command with only the system service names sudo`, `no installed service names the level`, `start reports a degraded Broker and keeps the service running`; the last case keeps the exit code 3 of `service start` and asserts that the stdout of `peers service status` contains `Broker: degraded (local only):` and `Fix:`); `test/e2e/darwinservice/control_test.go` `TestUserServiceControl`, `TestSystemServiceControl`, `TestServiceControlNotInstalled`, `TestUserServiceControlDegraded` (likewise asserting that the stdout of `peers service status` contains the status line and `Fix:`)

### REQ-broker-config-056

A Broker started without `--allow-degraded` MUST, when any startup step fails, output the reason on standard error and end with a nonzero exit code.

- **Acceptance**: In the terminal with `peers service run`, induce the three failures of an unparseable configuration file, an occupied listen address and an unreadable identity file; each time the process exits with a nonzero exit code, the last line of standard error states the reason, and the runtime directory has no local socket left.
- **Status**: Implemented
- **E2E test**: `test/e2e/degraded_run_test.go` `TestDegradedRun/REQ-broker-config-056 foreground failures exit`

### REQ-broker-config-057

A Broker started with `--allow-degraded`, after acquiring the account lock, MUST NOT exit when the listen address cannot be bound or the data directory or audit log is unavailable, and MUST answer on the local socket; when reading or parsing the configuration file fails, a value is invalid, or another startup failure unrelated to the data directory occurs, it MUST still exit as without the parameter.

- **Acceptance**: Start with `peers service run --allow-degraded` and make the identity file in the data directory unreadable by the current account, or make the directory containing the audit log unwritable: the process keeps running, and `peers service status` can connect to it and output the fault; the same when the listen address is occupied. Write unparseable content into the configuration file and start with the same parameter; the process ends with a nonzero exit code, the same as without the parameter.
- **Status**: Implemented
- **E2E test**: `test/e2e/degraded_run_test.go` `TestDegradedRun/REQ-broker-config-057 diagnostics entry and recovery`

### REQ-broker-config-058

In degraded run where only the listen address cannot be bound, the Broker MUST provide local registration, listing, topic and local message functions as usual, and generating a pairing key MUST fail with the error stating the unbindable address and the option `broker.listen`. When a system Broker's listen port is below 1024 and the configuration file is writable by other accounts, this requirement applies.

- **Acceptance**: With another process occupying port 17443, start the Broker with `--allow-degraded` and the default configuration: two sessions register successfully, and after joining the same topic they exchange messages successfully; a session calling `create_pairing_key` returns `broker_degraded`, with the message containing `:17443` and `broker.listen`; the host does not listen on any port; `peers service status` outputs the fault; sending to a peer for which this side is the acceptor returns `peer_unreachable`, with the degraded reason appended at the end of the message. Also set the system Broker's listen port below 1024 and make the configuration file group-writable; after startup it likewise enters local only degraded run, and the repair hint of `peers service status` contains `chmod g-w` or switching to a port above 1024.
- **Status**: Implemented
- **E2E test**: `test/e2e/degraded_run_test.go` `TestDegradedRun/REQ-broker-config-058 local only keeps local features`; peer_unreachable carrying the degraded reason is covered by the unit test `TestPeerUnreachableReasonOnlyForAcceptor` in `internal/broker/degradedrpc_test.go`; the low-port repair hint containing `chmod g-w` is covered by the unit test `TestDegradedFixHints` in `internal/broker/degradedstate_test.go`

### REQ-broker-config-059

In degraded run where the data directory or audit log is unavailable, `peers service status` MUST output the fault and a repair hint, and every tool call in a session MUST return `broker_degraded` and the reason.

- **Acceptance**: Start with `--allow-degraded` with the audit log's directory unwritable; `peers service status` outputs the audit log's path, the reason and the repair method, with a nonzero exit code; calls to `list_agents`, `send_message` and `get_config` in a session all return `broker_degraded`, with the message matching the reason `peers service status` outputs; the session does not need to reconnect the MCP service.
- **Status**: Implemented
- **E2E test**: `test/e2e/degraded_run_test.go` `TestDegradedRun/REQ-broker-config-059 diagnostics from the audit log`

### REQ-broker-config-060

A Broker with `--allow-degraded` that fails before acquiring the account lock MUST exit as without the parameter.

- **Acceptance**: Start with `--allow-degraded` separately inside a session's process tree, as non-root with `--system`, and while an existing instance holds the account lock; in all three cases the process exits the way `REQ-broker-config-056` describes, and the existing instance is unaffected.
- **Status**: Implemented
- **E2E test**: `test/e2e/degraded_run_test.go` `TestDegradedRun/REQ-broker-config-060 before-lock failures exit`

### REQ-broker-config-061

In degraded run, the Broker MUST retry the failed step immediately when the configuration file changes and once a minute; after a retry succeeds it exits degraded run without restarting the process. On a failed retry it writes the runtime log only when the reason changes.

- **Acceptance**: Start with `--allow-degraded` with the audit log's directory unwritable; after restoring that directory's permission without restarting the Broker, within 60 seconds `peers service status` no longer reports the fault. Also start with the default configuration with port 17443 occupied; after changing `broker.listen` to a free port without restarting the Broker, within 60 seconds `peers service status` no longer reports the fault, the Broker listens on the new port, and the process id is unchanged.
- **Status**: Implemented
- **E2E test**: `test/e2e/degraded_run_test.go` `TestDegradedRun/REQ-broker-config-059 diagnostics from the audit log`, `TestDegradedRun/REQ-broker-config-061 recovery listens on the new port`

### REQ-broker-config-062

In degraded run where the listen address cannot be bound, the Broker MUST retry binding at intervals of no more than 1 minute, and once the address is available, start listening without restarting the process.

- **Acceptance**: Start with `--allow-degraded` while port 17443 is occupied; end the process occupying that port without changing configuration, within 2 minutes another host can reach this Broker's port 17443, `peers service status` no longer reports the fault, and the Broker's process id is unchanged.
- **Status**: Implemented
- **E2E test**: `test/e2e/degraded_run_test.go` `TestDegradedRun/REQ-broker-config-062 listen retry reaches the address`

### REQ-broker-config-063

The service definition `peers service install` generates MUST start the Broker in a way that allows degraded run.

- **Acceptance**: Install the user and system services separately; on Linux the unit file's `ExecStart` and on macOS the plist's `ProgramArguments` both contain `--allow-degraded`; on a host with installation completed, stop the service, let another process occupy the configured listen port, and run `peers service start`: the Broker enters degraded run, the service manager reports the service as running, for at least 30 seconds (six times `RestartSec=5`) the process pid is unchanged and the service manager does not restart it, and `peers service status` reports degraded run.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/service_control_test.go` `TestServiceControl/start reports a degraded Broker and keeps the service running` (also asserting that the user and system unit files contain `--allow-degraded`, keeping the exit code 3 of `service start`, and asserting that the stdout of `peers service status` contains `Broker: degraded (local only):` and `Fix:`); `test/e2e/darwinservice/control_test.go` `TestUserServiceControlDegraded` (also asserting that the plist contains `--allow-degraded`, with the same status-line assertions); the content of the service definition template for both levels is also covered by `internal/broker/service/units_test.go`; the verbatim output of the terminal status line is covered by the "Show degraded run and offline in the terminal and in a session" unit

### REQ-broker-config-064

When the Broker enters degraded run after the service is registered, `peers service install` MUST keep the installed and running service, output the fault and the repair command, and exit with exit code 3. When the precheck before registration finds the listen address occupied, the installation MUST refuse and register no service.

- **Acceptance**: On a host with installation completed, make the audit log's directory unwritable and run `peers service install`: the command exits with 3 and the output states the reason, the path and the repair command; the service definition and binary are both there, the service manager reports the service as running, and `peers service status` reports degraded run. Also, before installation, let another process occupy port 17443: the installation fails and states that the address is unavailable, the service definition is not written, and the host has no such service.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/install_summary_test.go` `TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog`, `test/e2e/system/install_port_conflict_test.go` `TestSystemServiceInstallListenPortConflict/an occupied port fails the install and registers nothing` (Linux: degraded reinstall after the audit log is unavailable, and the precheck port occupation); `test/e2e/darwinservice/install_summary_test.go` `TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog`, `test/e2e/darwinservice/install_port_test.go` `TestUserServiceInstallRefusesAnOccupiedListenPort` (macOS: the same acceptance)

### REQ-broker-config-065

A Broker's offline state MUST be saved in configuration, and after a Broker restart it keeps the state from before the restart.

- **Acceptance**: Set the Broker offline and restart it; `peers service status` still reports offline, and a peer's send to its agents is still refused; change `broker.offline` to `false` in the configuration file without restarting, and the peer's send succeeds, and the audit log gains one record of this option's change.
- **Status**: Implemented
- **E2E test**: `test/e2e/offline_test.go` `TestBrokerOffline/REQ-broker-config-065 state survives restart and a file edit`

### REQ-broker-config-066

A user MUST be able to run `peers service offline` and `peers service online` in the terminal to take the Broker offline and online, with the same permission as modifying configuration.

- **Acceptance**: Run `peers service offline` as an account with write permission on the configuration file and the command succeeds, and `peers service status` reports offline; after running `peers service online` it reports online; run them as an account without write permission and it returns insufficient permission and gives the configuration file path, with the state unchanged; the audit log records each change and the account that ran it.
- **Status**: Implemented
- **E2E test**: `test/e2e/offline_test.go` `TestBrokerOffline/REQ-broker-config-066 terminal commands`; `test/e2e/system/permission_test.go` `TestREQBrokerConfig021_WriteNeedsConfigWritePermission` (without write permission, `offline` and `online` return `permission_denied` and give the configuration file path, and the configuration file is unchanged)

### REQ-broker-config-067

An agent MUST be able to use the tool `set_broker_online` to take the connected Broker offline or online, and this tool MUST be authorizable independently of modifying configuration.

- **Acceptance**: A session calls `set_broker_online` with the parameter `online` false and the call succeeds, taking the Broker offline; true takes it online. When Claude Code's permission settings allow only this tool, calling it triggers no confirmation and `set_config` triggers confirmation. An account without write permission on the configuration file calling it returns `permission_denied`. The tool declaration does not carry the read-only annotation.
- **Status**: Implemented
- **E2E test**: `test/e2e/offline_test.go` `TestBrokerOffline/REQ-broker-config-067 tool changes the state`; `test/e2e/system/permission_test.go` `TestREQBrokerConfig021_WriteNeedsConfigWritePermission` (without write permission it returns `permission_denied`); `test/e2e/config_tools_test.go` `TestConfigToolAnnotations` (declared not read-only). Claude Code authorizing a tool individually and not triggering confirmation when only it is allowed is Claude Code's permission behavior, not verified in end-to-end tests; the declaration level is covered by the tool-declaration test in `internal/mcpserver/tools_test.go`

### REQ-broker-config-068

Viewing status and listing Brokers in the local terminal MUST show whether this Broker is offline.

- **Acceptance**: After the Broker goes offline, the output of `peers service status` and `peers broker list` each has a line stating that this Broker is offline and the command to go online, and in a session the result of `list_brokers` carries this Broker's offline state; after going online these statements disappear.
- **Status**: Implemented
- **E2E test**: `test/e2e/offline_test.go` `TestBrokerOffline/REQ-broker-config-068 status shows the state` (`Status.offline`, `ListBrokers.local.offline`, and the offline status lines of `peers service status` and `broker list`; after going online the status lines disappear)

### REQ-broker-config-069

For unauthenticated connections from one source rejected within the merge window, no matter how many certificates the other side switches, the audit log gains only one record; however many sources are rejected in the same period, the number of records the audit log gains also has a cap.

- **Acceptance**: An unpaired peer initiates one connection each with five different certificates from the same source address, and each is rejected; then read the audit log, and there is only one `connection_rejected` record, with `count` 5 and `detail` carrying the prefix of this source. When forging many connections from different sources, the ordinary records all mergeable events write to the audit log within one 10-minute window total no more than 256, and the excess is merged into one summary record whose `detail` is `other sources`.
- **Status**: Implemented
- **E2E test**: `test/e2e/security_test.go` `TestREQBrokerConfig069_RejectedConnectionsMergeBySource`

## Data Requirements

A Broker's configuration and audit log are stored on the local disk, and who can read and who can change them is decided by their file permissions. A user Broker's configuration file and audit log belong to the account running it, and only that account can read and write them. A system Broker's configuration file by default can be read and written only by root and the admin group. The audit log is append-only and is not deleted by the system.

Each Broker instance has its own configuration and audit log, and several Brokers on the same host share neither. A Broker's offline state is saved in the configuration file; what data uninstallation removes is specified by `REQ-broker-config-051` through `REQ-broker-config-054`.

## External Interface Requirements

- Claude Code: whether a tool call needs user confirmation is decided by Claude Code according to the session's permission settings.
- Operating system: which account the caller belongs to and what permission that account has on the configuration file is decided by what the operating system reports, not by the caller's statement. A system Broker is started by macOS's or Linux's system service management mechanism, and a user Broker, when installed as a service, is started by the user service management mechanism.

## Constraints

- The default configuration listens on port 17443 on every local address; 17443 is only a value in the default configuration, and the system gives this port no special treatment.
- When a second Broker on the same host starts with the default configuration, the listen address is already occupied: on a fresh or reinstall, the precheck finds the address occupied by another process, and the installation fails and registers no service (`REQ-broker-config-064`); when starting an already-installed service afterward, the service enters local only degraded run, and starting in the foreground fails to start. Degraded run provides only local functions, and the user or administrator must specify another address or port for it.

## Assumptions and Dependencies

- The system identifies the account the calling process belongs to, not a person and not a model. Any process under an account can call the Broker with that account's permissions.
- When an account has write permission on the configuration file and has added the tools that modify configuration, generate pairing keys, initiate pairing or upgrade the Broker to Claude Code's auto-approval list, these operations have no confirmation any more. The audit log is the evidence afterward.
- The model in a session can, with the file access permission its session has, directly read and write the configuration file and audit log the account is permitted to access. The system does not guard against this path; Claude Code's permission confirmation for file operations covers it.
- Generating pairing keys, initiating pairing and unpairing do not write to the configuration file, and they likewise require write permission on the configuration file.
- The address of the default route's interface cannot be reached from outside a network that translates addresses; this case needs an advertised address containing a host configured, or added advertised addresses.
- An upgrade only replaces the binary and does not rewrite the service definition. An installed service must have the installation run again to adopt the current version's service definition and startup method.
- An account that can write the Broker's configuration file can modify `broker.offline`, equivalent to running the online and offline commands.
