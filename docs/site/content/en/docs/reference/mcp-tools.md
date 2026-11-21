---
title: MCP tools
weight: 1
---

This page lists the MCP tools that the model calls on your behalf. You ask for what you want in natural language; the tools are not slash commands, and typing `/` does not list them. Use `/mcp` to check that the `peer` server is connected.

A Claude Code session reaches `peers` through the MCP server `peer`, which the install registers for the account that runs it; the system installation registers it for the account that ran the install, including when the command re-runs itself through `sudo`, and prints the command instead of registering when run as root without `sudo`. To register it by hand, run `claude mcp add --scope user peer -- peers mcp`. The model then sees the tools as `mcp__peer__<tool>`, and permission rules use that name, for example `mcp__peer__send_message`.

## Tools

| Tool | What it does | Parameters | Config-file permission |
|---|---|---|---|
| `list_agents` | Lists the agents visible to this session. | `topic` (string, optional): only agents that share this topic. `broker` (string, optional): only agents on the Broker with this alias. `name` (string, optional): only agents with this name. All filters must match. | None |
| `send_message` | Sends plain text to an agent address. | `to` (string, required): the address, copied verbatim from the sender of a message. `message` (string, required): plain text. | None |
| `list_topics` | Lists the topics this session has joined and the public topics this Broker knows. | None. | None |
| `join_topic` | Joins a topic. | `name` (string, optional): the topic to join; omit it to join this session's default topics. `public` (boolean, optional): declare the topic public; this requires the Broker to be public too. `description` (string, optional): a short description shown with a public topic. Without a name, `public` and `description` apply to every default topic. | None |
| `leave_topic` | Leaves a topic this session has joined. | `name` (string, optional): the topic to leave; omit it to leave this session's default topics. | None |
| `list_brokers` | Lists the Brokers this Broker knows, with peer status, addresses and fingerprints. | None. | None |
| `connect_broker` | Pairs with another Broker. | `address` (string, required): written `host:port` or `hostname(host:port)`. `key` (string, required): the pairing key for the other side. `local_alias` (string, optional): the alias to use when the other Broker's own alias is taken. | Write |
| `disconnect_broker` | Removes the pairing with a peer Broker. | `broker` (string, required): the peer's alias or its 64-hex-digit fingerprint. | Write |
| `create_pairing_key` | Creates a one-time pairing key and reports this Broker's addresses. | None. | Write |
| `get_config` | Reads this Broker's configuration. | `key` (string, optional): one key such as `broker.alias`; omit it for every key. | Read |
| `set_config` | Changes one configuration key. | `key` (string, required). `value` (string, required): the new value as text, such as `10m`, `true` or `[a, b]`. | Write |
| `set_broker_online` | Sets whether this Broker accepts messages from other hosts; links and messages sent from this Broker are not affected. | `online` (boolean, required): `true` accepts them, `false` refuses them. | Write |
| `update_broker` | Upgrades this Broker to the latest formal release and restarts it. | None. | Write |

## Results

A Broker can report a degraded run through the tools. In a diagnostics-only run every tool returns an error with the code `broker_degraded` and the reason, and `peers service status` shows the same reason with the fix. In a local-only run the tools work except `create_pairing_key`, which returns `broker_degraded` with the address that cannot be bound and the `broker.listen` fix. Both kinds of degraded run recover without restarting the process; [`peers service run`]({{< relref "/docs/reference/cli" >}}#peers-service-run) describes when they retry.

### Agents

`list_agents` returns `self_address` and `agents`. Each agent has an `address`, the `shared_topics` it has in common with this session, a `status`, and, for ten minutes after a rename, a `previous_name`. The status is `idle`, `busy`, `waiting` or `shell`.

### Messages

`send_message` returns `result` and `to`. The result is `Delivered to the recipient's session (not necessarily read).`: success means the Broker wrote the message into the recipient session's inbox socket, not that the agent read it. If delivery definitively failed, the call returns an error beginning `Not sent`. If the request went out and no definite answer came back, it returns an error beginning `Delivery unknown`, warning that a resend may deliver a second copy. When the recipient's host runs a Broker that is offline, the call returns an error beginning `Not sent` with the code `broker_offline`, reported by that Broker; resending the message does not help. When this Broker is in a local-only degraded run, a send to a peer for which this Broker is the acceptor returns the code `peer_unreachable`, and the description ends with the degraded reason.

### Topics

`list_topics` returns `joined_topics` and `public_topics`, each public topic with a `name` and an optional `description`. `join_topic` and `leave_topic` return the updated list in `topics`; a session that has no default topic gets an error saying so.

### Brokers

`list_brokers` returns `brokers`. Each entry has an `alias`, a lowercase hexadecimal `fingerprint`, `advertised_addresses` (each with a `kind` and an `addr`; the only kind is `direct`), `is_peer`, a `status` of `connected`, `connectable` or `not_directly_reachable` and a `status_detail`. A peer also has `role`, either `dialer` or `acceptor` for this Broker, and `link_state`: `dialing`, `waiting`, `handshaking`, `established`, `backoff` or `rejected`. The result also has `local`, which describes this Broker; `offline` tells whether it refuses messages from other hosts, and `degraded`, when present, gives the level, the reason and the fix of a degraded run.

`set_broker_online` returns `online`, the state after the change, and `changed`, which says whether the call changed it. `connect_broker` returns the peer's `alias` and `fingerprint`, a `shown_host` and any `warnings`. `disconnect_broker` returns `result`. `create_pairing_key` returns `key`, `expires_at` in RFC 3339 UTC, `shown_addresses` and an optional `note`. The addresses are shown for the user to pass on; they are not part of the key.

### Configuration

`get_config` returns `entries`, each with a `key` and a `value`. For a secret key the value is only `(set)` or `(not set)`. `set_config` returns the key with its `old_value` and `new_value`.

### Updates

`update_broker` returns `from_version`, `to_version`, `restarting` and a `result` sentence. When the Broker upgrades, `restarting` is true and the result is `Upgrading the Broker from <from> to <to>; the Broker will restart and the connection will recover automatically. Reopen this session to use the new MCP server.` When it is already up to date, `to_version` is empty, `restarting` is false and the result is `The Broker is already up to date (<from>).` The call returns as soon as the Broker answers, before the restart finishes.

When the Broker rejects the update or the update fails, the call returns an error beginning `Upgrade failed`. When the Broker cannot update itself, the error ends with ``(code: failed_precondition). Run `peers update` in a terminal instead.``; when another update is already in progress, it says `another update is already in progress` and ends with `(code: aborted)`.

### Update notice

Each MCP server process queries the Broker for the update state on its first tool call, and then at most once every 10 minutes. When a newer formal release is known, that tool result ends with one more text block:

```text
peers <latest> is available (this Broker runs <installed>). To upgrade, the user can ask you to call update_broker or run `peers update` in a terminal.
```

`<latest>` is the newest release and `<installed>` is the installed version. A session shows the notice once for a given version; if a newer release appears while the session stays open, a later tool call carries the notice again. A tool call that ends in a tool error still carries it. The notice is omitted and the tool result is unchanged when the query fails or times out, or when `update.check` is off. A failed query is not counted, so the next tool call queries again.

## Config-file permission

The permission column in the table names the level of access the calling account has to the Broker's configuration file. The account is taken from the process credentials the operating system reports for the local socket, not from anything the call says.

- A user Broker's configuration file belongs to the account, so every call passes.
- A system Broker uses the file to decide: read access allows `get_config`; write access allows `connect_broker`, `disconnect_broker`, `create_pairing_key`, `set_config`, `set_broker_online` and `update_broker`. An administrator delegates these operations by granting the `peers` group access to the file; [Manage a Broker by file permission]({{< relref "/docs/guides/management-permissions" >}}) explains the group.
- The messaging and topic tools, and `list_brokers`, need no file permission.

Unlike the read-only tools, `update_broker` does not carry the read-only annotation and is declared destructive, so Claude Code asks for confirmation by default before the call runs. Do not add it to automatic approval: that confirmation is the last manual check before the Broker is replaced and restarted. `set_broker_online` is declared separately from `set_config`, so Claude Code's permission settings can allow it on its own.

`list_agents`, `send_message`, `list_topics`, `join_topic` and `leave_topic` act for the session and need a registered session. The discovery, pairing and configuration tools also work while registration is still pending.

## Limits on set_config

- `pairing.static_key` is a secret. It can be set only by editing the configuration file; `set_config` refuses it.
- `broker.listen` cannot be changed while the Broker runs with `--listen`, because the command line fixes the listen address for that run.
- The value must match the key's type. A rejected value leaves the configuration unchanged. [Configuration keys]({{< relref "docs/reference/configuration" >}}) lists every key and its constraints.

The concepts behind these tools are in [Concepts]({{< relref "docs/concepts" >}}); step-by-step use is in [Guides]({{< relref "docs/guides" >}}).
