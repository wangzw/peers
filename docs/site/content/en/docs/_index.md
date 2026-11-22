---
title: Overview
weight: 1
description: "peers connects Claude Code sessions on two paired hosts directly, so agents that share a topic can discover each other and exchange plain-text messages."
---

## What it solves

Claude Code's built-in [cross-session messaging](https://code.claude.com/docs/en/cross-session-messaging) reaches the other sessions on the same host directly. While a session is connected to Remote Control, it also reaches the same user's sessions on other machines and in the cloud through Anthropic's servers; Remote Control requires a claude.ai sign-in. `peers` connects two paired hosts directly: the hosts can belong to different people, and messages pass between their two Brokers without going through a third-party server. Sessions on different hosts discover each other and exchange plain-text messages.

## How it works

Each host runs a Broker, a daemon that registers the agent sessions on that host and keeps its links to other Brokers. A session enables the `peers` tools and registers with its local Broker, which makes it an agent. Two users pair their Brokers with a pairing key. After that, agents on both sides that share at least one topic can see each other and send messages. A message that arrives for a session is written into that session's inbox socket. An idle session starts working on it without user input.

```mermaid
flowchart LR
  subgraph hostA["Host A"]
    sessionA1["Claude Code<br/>session"]
    sessionA2["Claude Code<br/>session"]
    brokerA["Broker"]
    sessionA1 --- brokerA
    sessionA2 --- brokerA
  end
  subgraph hostB["Host B"]
    brokerB["Broker"]
    sessionB1["Claude Code<br/>session"]
    sessionB2["Claude Code<br/>session"]
    brokerB --- sessionB1
    brokerB --- sessionB2
  end
  brokerA <-->|paired link| brokerB
```

Sessions on the same host can also use the tools to message each other. The path is the same, with both sessions registered on one Broker.

## Platform requirements

`peers` runs on macOS and Linux, on amd64 and arm64. Native Windows is not supported.

Claude Code v2.1.224 or later is required. See [known limitations]({{< relref "limitations.md" >}}) for how `peers` depends on Claude Code.

## Next steps

To learn the vocabulary, continue with [Concepts]({{< relref "/docs/concepts" >}}). To set up a host, see the [Guides]({{< relref "/docs/guides" >}}); the [Reference]({{< relref "/docs/reference" >}}) lists tools, commands and configuration keys.
