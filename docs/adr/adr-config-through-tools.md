# Read and write Broker configuration through tools and leave confirmation to Claude Code

## Context

While working in a session, a user needs to view and adjust a [Broker](../glossary.md)'s configuration, generate pairing keys and initiate pairing. Some of these operations affect security: making a Broker public, and pairing with a new host. The model in a session can be induced by prompt injection. Claude Code lets the model change its own configuration in a session and constrains that with per-tool permission confirmation. Whether an account can read and write a Broker's configuration is decided by file permissions ([ADR-management-by-config-file-permission](adr-management-by-config-file-permission.md)); under an account with write permission, the model could already change the Broker's configuration file directly with file operations.

## Options Considered

### Configuration can be changed only through files or terminal commands

The tools provide only reading, and all changes are made by the user in the terminal.

**Rejected**: the user has to leave the session for every adjustment. Since the model can change Claude Code's own configuration under permission confirmation, there is not enough reason to take a stricter approach for the Broker's configuration.

### The Broker sets up its own confirmation

Changing configuration, generating a pairing key and initiating pairing first enter a pending-approval state and return a confirmation code, and they take effect only after the user confirms separately in the terminal, independent of Claude Code's permission settings.

**Rejected**: setting up a second confirmation outside Claude Code's permission mechanism makes the user authorize in two places, and every operation adds a step of leaving the session.

### Distinguish tightening from loosening

A change that tightens security takes effect directly, and a change that loosens security needs confirmation.

**Rejected**: once confirmation is left to Claude Code, the Broker cannot know whether the user confirmed and cannot treat them differently on that basis.

### Separate tools for reading and writing, with confirmation left to Claude Code

Reading configuration and changing configuration are two tools, and generating a pairing key is a separate tool; tools with no side effects are declared read-only ([REF-mcp](../references.md)). Whether each call needs user confirmation is decided by the Claude Code permission settings of the session it is made in. This confirmation and the file-permission decision are independent, and the operation runs only when both pass. The Broker writes every configuration change, pairing and link event to the audit log.

**Adopted**; see Decision.

## Decision

A Broker's configuration is read and changed by an agent through tools, with separate tools for reading and writing marked with the read-only attribute; the pre-provisioned static key is the exception, set only in the configuration file and reported through the tools only as whether it is set ([ADR-static-pairing-key](adr-static-pairing-key.md)). User confirmation is left entirely to Claude Code's permission mechanism, and the Broker sets up no confirmation of its own. Whether the caller's account may perform the operation is decided separately by file permissions, independent of user confirmation. The Broker is responsible for auditing.

## Consequences

A user with the corresponding file permissions can complete pairing and configuration adjustments in a session. The user can allow only the read-only tools, or authorize generating a pairing key and changing configuration separately. The Broker needs no pending-approval state and no confirmation code.

**Costs accepted:**

- When the user adds the tools for changing configuration, generating a pairing key or initiating pairing to the auto-approve list, or runs the session with permission confirmation skipped, these operations have no confirmation at all, and a model under prompt injection can loosen security settings or pair with an unknown host.
- The audit log is after-the-fact evidence: it can reveal a problem but cannot stop it; like the configuration file, it can be rewritten by a process under an account that has write permission to it, including file operations started by the model in a session.
- Within an account that has write permission, the protection strength of the Broker's security settings depends on each session's own permission settings, with no uniform lower bound.
