# References

| Id | Name | What it is | Source | Version |
|---|---|---|---|---|
| REF-claude-code-cross-session-messaging | Cross-session messaging | Claude Code's official documentation, covering when cross-session messages are delivered, the inbox socket, inbound controls and the size limit | <https://code.claude.com/docs/en/cross-session-messaging> | read 2026-11-18 |
| REF-claude-code-socket-transport | claude-code-socket-transport | A Go library that implements the frame format, signature wrapper and session registry read needed to write messages to a Claude Code session's inbox socket; its documentation records the inbox socket's protocol details | <https://github.com/PeterSR/claude-code-socket-transport> | commit 480bd83c0bf1, registered in third_party/UPSTREAM |
| REF-fsnotify | fsnotify | A Go library that provides cross-platform file system event watching; a Broker in a degraded run uses it to watch for configuration file changes once it is ready | <https://github.com/fsnotify/fsnotify> | v1.10.1, local changes in third_party/UPSTREAM |
| REF-mcp | Model Context Protocol | An open protocol between models and external tools, defining tool declaration, invocation and the read-only marker | <https://modelcontextprotocol.io> | — |
| REF-a2a | Agent2Agent Protocol | An open protocol for interoperability between agents, defining data structures such as Message | <https://a2a-protocol.org> | 1.0.0 |
| REF-epidemic-algorithms | Epidemic Algorithms for Replicated Database Maintenance | A paper that introduces anti-entropy reconciliation, gossip propagation and death certificates (retraction records) | <https://doi.org/10.1145/41840.41841> | 1987 |
| REF-crockford-base32 | Base32 Encoding | The Base32 encoding proposed by Douglas Crockford, which fixes the alphabet after removing easily confused letters | <https://www.crockford.com/base32.html> | — |
| REF-github-rest-api | GitHub REST API | GitHub's REST API, defining the endpoint, response and pagination for querying the latest stable release | <https://docs.github.com/en/rest/releases/releases#get-the-latest-release> | 2022-11-28 |
| REF-cobra | cobra | A Go command-line library that generates the command tree, help, usage and shell completion, as well as man page generation | <https://github.com/spf13/cobra> | v1.10.2 |
