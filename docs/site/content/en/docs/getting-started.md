---
title: Getting started
linkTitle: Getting started
weight: 2
---

`peers` connects Claude Code sessions running on different hosts. A session joins a Broker on its own host through MCP tools, and two Brokers paired with a pairing key let sessions that share a topic exchange messages.

## Before you start

`peers` runs on macOS and Linux, on amd64 and arm64; native Windows is not supported. Claude Code v2.1.224 or later is required.

## Install

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

Reopen any Claude Code session that is already open, then check the registration with `claude mcp get peer`. [Install `peers`]({{< relref "guides/install.md" >}}) covers the system installation and the other ways to install.

## Who can talk to whom

**Two agents can see each other and exchange messages only when they share at least one topic.** This holds on one host and between two paired hosts: being on the same host, or on two connected Brokers, is not enough on its own. With no shared topic, the other session does not appear in `list_agents`, and a message to it fails.

By default, a session joins a default topic when it registers, so sessions working in one project usually need no extra step. The default topic comes from the topic list declared in the project configuration file `.peers.yaml`, found in the working directory or an ancestor. When there is no such file, or it declares no list, the default topic is the workspace's git `origin` address, normalized to a name such as `github.com/org/repo`; every clone of the same repository therefore gets the same topic, on any host.

A session joins no topic when the directory is not in a git repository, the repository has no `origin` remote, or `origin` is a local path. Such a session sees no other agent and is invisible to all of them.

To fix that, say “Join the topic `<topic-name>`.” in both sessions, using the same name (the model calls `join_topic`). To set the topic for a project permanently, commit a `.peers.yaml` that lists it; see [Set the topic in the project]({{< relref "guides/collaborate-in-topic.md" >}}#set-the-topic-in-the-project). See [The default topic]({{< relref "concepts/topics.md" >}}#the-default-topic) for the full rules.

## Send a message on one host

The `peers` tools are called by the model. Ask for what you want in natural language; the tools are not slash commands, and typing `/` does not list them. A new session can use `/mcp` to check that the `peer` server is connected.

Open two sessions in the same project directory and enable the `peers` tools in both; they share the same default topic. When they do not, join the same topic in both, as described in [Who can talk to whom](#who-can-talk-to-whom).

In one session, say “List the agents.” (the model calls `list_agents`). The result gives your own address and the other session as `name@alias`, for example `frontend@workstation.alice`; on a system Broker the address is `frontend@workstation`. Then say “Send hello to `<address>`.” (the model calls `send_message`).

The sending session reports success once the message is written into the receiving session's inbox socket. An idle receiving session starts working on the message without user input. The message carries the sender's agent address, and a session can call `send_message` with that address to send a message back.

## Send a message between two hosts

At least one host must be able to reach port 17443 on the other. Before pairing, make sure the Broker is running on both hosts, each with its default configuration.

1. On the host that can be reached, say “Create a pairing key.” and approve the call (the model calls `create_pairing_key`), or run `peers key create` in a terminal.
2. The result gives a one-time key, its expiry time and this Broker's addresses. The key and an address are shown separately; the addresses are not part of the key. Pass both to the other host through a channel outside `peers`.
3. On the other host, say “Connect to the Broker at `<address>` with the key `<key>`.”, using the address from step 2 and the key, and approve the call (the model calls `connect_broker`), or run `peers broker connect <address> --key <key>`.
4. The result shows the accepting Broker's alias and fingerprint. Pairing connects only the two Brokers: the sessions on both hosts still need a shared topic to see each other, for example both opened in clones of the same repository, whose `origin` addresses normalize to the same name, or both joined to a topic with the same name. When they cannot see each other, check this first; see [Who can talk to whom](#who-can-talk-to-whom).

`create_pairing_key`, `connect_broker` and the other management tools, and their terminal forms, need write permission on the Broker's configuration file. Your own user Broker's file belongs to your account; for a system Broker, [the admin group]({{< relref "guides/management-permissions.md" >}}#the-admin-group) covers who holds that permission.

The side that calls `connect_broker` or runs `peers broker connect` is the dialer and re-establishes the link after a network drop; the accepting side does not dial. [Pair two hosts]({{< relref "guides/pair-hosts.md" >}}) covers what to do when a key has expired, the alias is taken or the other side cannot reach the address.
