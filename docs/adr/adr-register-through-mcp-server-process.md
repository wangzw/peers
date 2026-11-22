# Register a session as an agent through its MCP server process

## Context

A [Broker](../glossary.md) must know which Claude Code sessions on the host take part in communication, where each session's inbox socket is, and its name and busy/idle state, and the name and state change during the session's life. Not every session should take part: a session handling sensitive work should not become visible just because this system is installed on the host.

Claude Code writes each session's name, state and inbox socket path in the host's session registry, one record per session process, rewritten in place on change and deleted when the session exits; its format has no published compatibility promise. When a session enables this system's tools, Claude Code starts an MCP server process for it at session startup, as a direct child of the session process. On Claude Code v2.1.327, it was observed that this process's environment variable `CLAUDE_CODE_MESSAGING_SOCKET` is the inbox socket path of the session it belongs to; the variable is absent when the session does not bind an inbox, and also absent from the process's environment when the inbox is bound after the MCP server process starts; Claude Code's documentation promises only to export it to hooks and the command line. When a session is renamed, the MCP server process receives no notification.

## Options Considered

### The Broker scans the session registry and registers every session

The Broker reads every record in the session registry and registers each session as an agent.

**Rejected**: whether a session takes part does not depend on the session itself, and every session on the host would be included.

### The Broker follows the MCP server process's ancestor chain to the session process

The MCP server process registers with the Broker, and the Broker starts from the process on the other end of the local socket, walks up the parent processes, and once it finds the session process reads its registry record.

**Rejected**: it requires the parent-child relationship between processes to hold under every startup method; when the MCP server process is started through a wrapper script, or is not in the same PID namespace as the Broker, the requirement does not hold. Looking up the parent process serves only to get the inbox socket path, which is already in the MCP server process's environment.

### Periodically call `claude agents --json` for the name and state

The MCP server process registers with the Broker, and the Broker runs this command periodically and takes each session's name and state from the output.

**Rejected**: it reads the very session registry, each call runs Claude Code's full startup preparation and deletes files left by exited sessions, it can only be called periodically and cannot learn of a change, its output carries no inbox socket path, and the user's settings can turn it off.

### The environment gives the socket path, and the registry record gives the name and state

The MCP server process registers with the Broker and reports the inbox socket path in its environment. The Broker finds this session's record in the session registry of the caller's account, takes the name and state from it, and watches the record for changes.

**Adopted**; see Decision.

## Decision

Only a session that has enabled this system's tools becomes an agent, registered with the Broker by that session's MCP server process. The inbox socket path comes from the MCP server process's environment; when the path is absent, registration waits and tells the session why. The session name and state come from the session's record in the session registry, and the Broker watches the record for changes.

## Consequences

Whether a session takes part is decided by whether it enables the tools. The Broker does not look up relationships between processes and does not run Claude Code commands; changes in name and state are learned from changes in the record, without periodic re-reading.

**Costs accepted:**

- A session that has not enabled the tools cannot receive this system's messages, even when the user wants it to.
- The session registry format has no compatibility promise, so registration may need changing after a Claude Code upgrade.
- The inbox socket path appearing in the MCP server process's environment is observed behavior; Claude Code's documentation promises only to export it to hooks and the command line.
- The MCP server process does not itself notice a session rename, so name updates depend on file-change notification of the session registry.
- A session with no session registry record cannot become an agent.
- When the inbox socket path is absent from the environment, registration waits until the MCP server process restarts; during that time the session cannot become an agent.
- After the Broker process exits unexpectedly, every agent recovers only when its MCP server process registers again.
