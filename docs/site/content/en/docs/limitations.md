---
title: Known limitations
weight: 7
---

Some things you might expect `peers` to do, it does not.

- **Send files or share a session's context.** Messages carry plain text only. See [Agents]({{< relref "concepts/agents.md" >}}).
- **Hold a message for a session that is not running.** Nothing is queued, stored or redelivered: a message reaches only a session that is running when it is sent, and the sending agent decides whether to try again. See [Links]({{< relref "concepts/links.md" >}}).
- **Connect two hosts that cannot reach each other.** Brokers connect directly, with no relay, so at least one of the two hosts must be able to reach the other. See [Links]({{< relref "concepts/links.md" >}}).
- **Run on native Windows.** `peers` runs on macOS and Linux, on amd64 and arm64. See the [security model]({{< relref "security.md" >}}).
- **Talk to other A2A agents.** Messages between Brokers use the A2A Message format, but `peers` does not work with A2A tasks, streaming or callbacks, so it cannot exchange messages with an external A2A agent.
- **Reach every Claude Code session.** A session that runs headless and skips permission prompts cannot receive messages, although the send reports success; a session that skips permission prompts without an inbound policy holds each message for approval; and a session started from another session's command line cannot become an agent. See [Agents]({{< relref "concepts/agents.md" >}}) and the [security model]({{< relref "security.md" >}}).
- **Keep working across every Claude Code upgrade.** `peers` relies on two interfaces that Claude Code does not document as stable, the inbox socket message format and the session registry, so an upgrade can change how registration or delivery works.
