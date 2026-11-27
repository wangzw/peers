# Architecture Decision Records

This directory records the architecture decisions in force in peers. A decision earns a record here when it is hard to reverse or affects module boundaries, the external interface, the deployment shape, external dependencies or security posture; each record states the constraints that forced the decision, the options considered, the option adopted and the price accepted for it.

## Project naming

| Decision |
|---|
| [Rename the project to peers](adr-rename-peers.md) |

## Topology and participants

| Decision |
|---|
| [Use direct links between Brokers only](adr-direct-links-only.md) |
| [Deploy a Broker in two forms, system and user](adr-broker-deployment-modes.md) |
| [Derive the default alias from the hostname and account](adr-default-alias-host-and-account.md) |
| [Listen by default and keep the address on conflict](adr-listen-by-default.md) |
| [Do not exit on a partial startup failure after the account lock](adr-degraded-run.md) |
| [Register a session as an agent through its MCP server process](adr-register-through-mcp-server-process.md) |
| [Identify an agent by its inbox socket, with the name taken from the session name](adr-agent-identity-by-inbox-socket.md) |
| [Own each name by the account that registered it first](adr-name-ownership-by-account.md) |

## Message delivery

| Decision |
|---|
| [Deliver synchronously without buffering or resending](adr-synchronous-delivery.md) |
| [Count a successful inbox socket write as sent](adr-inbox-write-is-success.md) |
| [Present no authentication token when writing to the inbox socket](adr-inbox-delivery-without-auth-token.md) |
| [Do not rate-limit or detect message loops in the Broker](adr-no-broker-rate-limiting.md) |
| [Reuse A2A's Message structure as the link message body](adr-a2a-message-body.md) |
| [Refuse peer delivery while the Broker is offline](adr-offline-refuses-peer-delivery.md) |
| [Start conversations through Claude Code's built-in send tool when its session list shows the agent](adr-built-in-send-for-listed-sessions.md) |

## Trust and links

| Decision |
|---|
| [Establish trust between self-signed certificates with a pairing key](adr-pairing-key-trust.md) |
| [Take the pre-provisioned static key from the configuration file](adr-static-pairing-key.md) |
| [Carry links over a gRPC bidirectional stream](adr-grpc-stream-links.md) |
| [Default the advertised address to the default route's interface address](adr-advertised-address.md) |
| [Let the side that initiates pairing re-establish the link](adr-initiator-redials.md) |
| [Limit unauthenticated connections by source](adr-unauthenticated-connection-limits.md) |
| [Limit authenticated peers by source](adr-peer-resource-limits.md) |

## Visibility

| Decision |
|---|
| [Scope agent visibility by topic](adr-topic-scoped-visibility.md) |
| [Derive the default topic from the project declaration or the repository address](adr-default-topic-from-repository.md) |
| [Compare shared topics by a salted digest](adr-salted-topic-digest.md) |

## Discovery

| Decision |
|---|
| [Keep Brokers private by default](adr-brokers-private-by-default.md) |
| [Propagate public information as signed records through hop-by-hop reconciliation](adr-signed-directory-reconciliation.md) |

## Configuration

| Decision |
|---|
| [Read and write Broker configuration through tools and leave confirmation to Claude Code](adr-config-through-tools.md) |
| [Gate management operations on the caller's permission on the configuration file](adr-management-by-config-file-permission.md) |
| [Delegate system Broker administration to a group](adr-admin-group.md) |

## Updates

| Decision |
|---|
| [Install self-updates from this project's GitHub releases](adr-self-update.md) |
| [Keep a post-update contract between the old and new binaries](adr-post-update-contract.md) |
| [Reload on macOS in two stages through a detached helper process](adr-launchd-detached-reload.md) |

## Command line

| Decision |
|---|
| [Build the command line on cobra and group commands by noun and verb](adr-cli-framework.md) |

## External dependencies

| Decision |
|---|
| [Read accounts and groups from the system account database](adr-account-database-source.md) |
| [Reuse claude-code-socket-transport to write the inbox socket](adr-reuse-socket-transport-library.md) |

## Website and releases

| Decision |
|---|
| [Index only the website's latest release at the root](adr-versioned-website-indexing.md) |
| [Check the reference pages against the code with package tests](adr-reference-pages-checked-by-tests.md) |
| [Rebuild every published release of the website from git on each deploy](adr-versioned-website-build.md) |
