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

## Send a message on one host

The `peers` tools are called by the model. Ask for what you want in natural language; the tools are not slash commands, and typing `/` does not list them. A new session can use `/mcp` to check that the `peer` server is connected.

Open two sessions in the same project directory and enable the `peers` tools in both.

This works without extra setup when both sessions run in the same git repository that has an `origin` address, or under the same project configuration file `.peers.yaml`. The default topic comes from that file in the working directory or an ancestor, or, when there is none, from the workspace's git `origin` address. The origin is normalized to a name such as `github.com/org/repo`, so every clone of the same repository gets the same topic.

When neither condition holds, the sessions join no topic and do not appear in each other's `list_agents` result. In that case, say “Join the topic `<topic-name>`.” in both sessions, using the same name (the model calls `join_topic`).

In one session, say “List the agents.” (the model calls `list_agents`). The result gives your own address and the other session as `name@alias`, for example `frontend@workstation.alice`; on a system Broker the address is `frontend@workstation`. Then say “Send hello to `<address>`.” (the model calls `send_message`).

The sending session reports success once the message is written into the receiving session's inbox socket. An idle receiving session starts working on the message without user input. The message carries the sender's agent address, and a session can call `send_message` with that address to send a message back.

## Send a message between two hosts

At least one host must be able to reach port 17443 on the other. Before pairing, make sure the Broker is running on both hosts, each with its default configuration.

1. On the host that can be reached, say “Create a pairing key.” and approve the call (the model calls `create_pairing_key`), or run `peers key create` in a terminal.
2. The result gives a one-time key, its expiry time and this Broker's addresses. The key and an address are shown separately; the addresses are not part of the key. Pass both to the other host through a channel outside `peers`.
3. On the other host, say “Connect to the Broker at `<address>` with the key `<key>`.”, using the address from step 2 and the key, and approve the call (the model calls `connect_broker`), or run `peers broker connect <address> --key <key>`.
4. The result shows the accepting Broker's alias and fingerprint. Sessions on both hosts that share a topic can now see each other and send messages.

`create_pairing_key`, `connect_broker` and the other management tools, and their terminal forms, need write permission on the Broker's configuration file. Your own user Broker's file belongs to your account; for a system Broker, [the admin group]({{< relref "guides/management-permissions.md" >}}#the-admin-group) covers who holds that permission.

The side that calls `connect_broker` or runs `peers broker connect` is the dialer and re-establishes the link after a network drop; the accepting side does not dial. [Pair two hosts]({{< relref "guides/pair-hosts.md" >}}) covers what to do when a key has expired, the alias is taken or the other side cannot reach the address.
