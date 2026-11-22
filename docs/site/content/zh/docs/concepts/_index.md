---
title: 概念
weight: 3
description: "五个概念概括 peers 的工作方式：Broker、Agent、topic、配对和链路，它们共同决定谁能看见谁并互发消息。"
---

peers 的大部分内容可以用五个概念概括。

- [Broker]({{< relref "/docs/concepts/broker" >}}) 是主机上服务本机会话、并与其他 Broker 通信的守护进程。
- [Agent]({{< relref "/docs/concepts/agents" >}}) 是登记到 Broker 的 Claude Code 会话。
- [topic]({{< relref "/docs/concepts/topics" >}}) 决定哪些 Agent 能互相看见、互发消息。
- [配对]({{< relref "/docs/concepts/pairing" >}}) 是两个 Broker 建立信任的方式。
- [链路]({{< relref "/docs/concepts/links" >}}) 是已配对 Broker 之间的连接。

[简介]({{< relref "/docs" >}})讲清全貌。要装好一台主机，从[两台主机配对]({{< relref "/docs/guides/pair-hosts" >}})开始；Agent 调用的工具见 [MCP 工具参考]({{< relref "/docs/reference/mcp-tools" >}})。
