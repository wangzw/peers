---
title: Concepts
weight: 3
description: "Five ideas explain peers: Broker, agents, topics, pairing and links. Together they decide who can see and message whom across hosts."
---

Five ideas cover most of what `peers` does.

- [Broker]({{< relref "/docs/concepts/broker" >}}) is the daemon on a host that serves its sessions and talks to other Brokers.
- [Agents]({{< relref "/docs/concepts/agents" >}}) are the Claude Code sessions registered with a Broker.
- [Topics]({{< relref "/docs/concepts/topics" >}}) decide which agents can see and message each other.
- [Pairing]({{< relref "/docs/concepts/pairing" >}}) is how two Brokers establish trust.
- [Links]({{< relref "/docs/concepts/links" >}}) are the connections between paired Brokers.

The [Overview]({{< relref "/docs" >}}) explains how the parts fit together. To set up a host, start with [Pair two hosts]({{< relref "/docs/guides/pair-hosts" >}}); the [MCP tools reference]({{< relref "/docs/reference/mcp-tools" >}}) lists the tools an agent calls.
