---
title: FAQ
weight: 8
---

## How do I install `peers`?

This command installs the latest release and registers the MCP server for your account; it asks for the level when it can:

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

The release archive also contains an `install.sh`; after extracting it, run `sh ./install.sh`, which asks the same question. You can also install the binary by hand. The [Install `peers`]({{< relref "guides/install.md" >}}) guide covers the level question and every way to install, including the system installation.

If the host already has a system Broker you can use, register the MCP server only instead of installing a user Broker; [Run a system Broker]({{< relref "guides/system-broker.md" >}}) gives the command.

## How do I uninstall `peers`?

Run `peers uninstall` to remove this account's installation; the system installation is removed with `sudo peers uninstall`. The command asks whether to delete configuration, identity, audit and service logs, and keeps them unless you answer `y`. [Uninstall `peers`]({{< relref "guides/uninstall.md" >}}) covers the levels, the questions and removing only the service.

## Where is the configuration file?

A user Broker reads `~/.peers/config.yaml` and a system Broker reads `/etc/peers/config.yaml`. `peers service install` generates the file for its level when it does not exist. Without a file, every key takes its default and the Broker runs normally. See [Configuration keys]({{< relref "reference/configuration.md" >}}) for the generated file and how to change it.

## Which platforms are supported?

macOS and Linux, on amd64 and arm64. Native Windows is not supported. See [known limitations]({{< relref "limitations.md" >}}).

## Which Claude Code version do I need?

Claude Code v2.1.224 or later. See [known limitations]({{< relref "limitations.md" >}}) for how `peers` depends on Claude Code.

## Is there a central server?

No. Brokers connect directly, messages are not relayed, and there is no central registry. One of the two hosts must be able to reach the other. See [Links]({{< relref "concepts/links.md" >}}).

## Do I have to start a Broker by hand?

A session never starts a Broker. If the host already has a system Broker you can use, register the MCP server only; the session connects to it. Otherwise, start one yourself: install the user service with `peers service install`, which starts it immediately and starts it at login from then on, or run `peers service run` in a terminal outside Claude Code. The install script already installs and starts the user service, so an installation done with it needs no further step. A Broker started inside a session exits with code 6. See [Run the Broker as a service]({{< relref "guides/run-as-service.md" >}}).

## What happens when no Broker is running?

Tool calls return an error that says what to do, and the guidance depends on the case. The MCP server process does not start a Broker. Two cases get the same message: a first lookup that finds neither a user Broker nor a system Broker, and a user Broker whose socket has disappeared or refuses connections, as after a hard kill. The message says to ask the administrator to start a system Broker if the host has one, then gives the two ways to run a user Broker, `peers service install` and `peers service run` in a terminal outside Claude Code. A session that was registered on the system Broker and loses it because the Broker is not running is told to contact the administrator, or, for root, to start it with `peers service start --system`. A Broker that is running can still refuse this session, and the error then says why. When the Broker refuses this session's connection, which can happen when this account or the whole host has reached its connection limit, the call returns `the Broker is running but refused this connection; this account or the whole host may have reached its connection limit, so retry shortly.` with the code `internal`. When the Broker refuses the registration, the call relays the Broker's own sentence unchanged: an account at the registration limit of 256 gets `cannot register: account <account> already has 256 open registrations` with the code `quota_exceeded`. Neither refusal needs a manual retry: the MCP server reconnects in the background on its own, and the session gets in only after another session or connection ends and frees a slot, in this account or, when the host-wide connection limit was reached, anywhere on the host. A registration slot lasts as long as its session stays connected, so an account already at 256 sessions cannot get another one in until one of them ends. A user Broker that answers but cannot complete the connection, or whose connection is lost after registration, is reported as unavailable, with a note that the MCP server keeps retrying in the background and no instruction to start a Broker. Starting the Broker does not require reconnecting the MCP server. Terminal commands that need a Broker, such as `peers broker list`, behave the same way: they look for a running Broker and never start one. See [Command line]({{< relref "reference/cli.md" >}}).

## Can one account run more than one Broker?

No. An account runs at most one Broker, and starting another one with a different listen address fails. See [Broker]({{< relref "concepts/broker.md" >}}).

## What is a Broker's default alias?

Without `broker.alias` set, a system Broker reports the first segment of the hostname, the part before the first `.`. A user Broker reports that segment, a `.` and the account name; on a host named `workstation`, the system Broker is `workstation` and the user Broker of account alice is `workstation.alice`. The alias does not contain the listen port, so changing the port or the advertised addresses leaves it unchanged. Set `broker.alias` in the configuration file to choose the alias yourself. See [Configuration keys]({{< relref "reference/configuration.md" >}}) for the full rule.

## How do I update `peers`?

Run `peers update` in a terminal, or say “Update the Broker.” in a session and approve the call (the model calls `update_broker`). The system installation is updated without `sudo` by a member of the admin group; an account outside the group uses `sudo peers update`, or joins the group first. When the same account also runs a user Broker, `sudo peers update` reaches the system Broker. The update verifies the download and replaces the binary. A Broker that runs as a service is restarted with the new version. A Broker that is not running, or a user Broker that does not run under a service manager, picks up the new version at its next start. Sessions that are already open must reconnect with `/mcp` or be restarted. See [Update `peers`]({{< relref "guides/update.md" >}}).

## Two paired hosts stopped seeing each other after an update

Paired Brokers link only when they speak the same link protocol version, and an update can change that version. The pairing is still saved, but the link does not come up, so agents on the two hosts cannot see each other. `peers service status` gives the reason in the `DETAIL` column, and `list_brokers` gives it in `status_detail`: `link protocol version incompatible with this peer; upgrade the older Broker`; on the accepting side it continues `: peer speaks link protocol version <N>`. The dialer keeps retrying in `backoff`, and the acceptor stays in `waiting`; `peers broker list` does not show the reason. Bring the older Broker to the same version as the newer one. The pairing stays, and the dialer re-establishes the link without pairing again.

## Can I turn off update checks?

Yes. Run `peers config set update.check false`; the `Update` line and the session notice disappear immediately, and no further checks are made. See [Update `peers`]({{< relref "guides/update.md" >}}) for how checks and updates work, or the [security model]({{< relref "security.md" >}}) for what a check sends to GitHub.

## How do two hosts start trusting each other?

One host must be able to reach the other. On the host that can be reached, say “Create a pairing key.” and approve the call (the model calls `create_pairing_key`) or run `peers key create`. The result gives a one-time key, the displayed addresses and a paste-ready connect prompt that names the key and this Broker's addresses. Give the prompt to the other host's user through a channel outside the system, and do not let an agent forward the key or the prompt; [the security model]({{< relref "security.md" >}}#pairing-and-fingerprints) explains why. The other host pairs by pasting the prompt into a session and approving the call (the model calls `connect_broker`), or by running the command the prompt gives; it can also say “Connect to the Broker at `<address>` with the key `<key>`.” with an address and the key. The pairing uses that one-time key or a pre-provisioned static key configured on the accepting host. From then on each side recognizes the other by fingerprint. See [Pairing]({{< relref "concepts/pairing.md" >}}) and [Pair two hosts]({{< relref "guides/pair-hosts.md" >}}).

## Pairing says the key is invalid, but I copied it correctly

First check the one-time key: it is valid for 10 minutes and for one pairing, so a key that has expired or was already used fails even when copied correctly. A one-time key that expired less than 24 hours ago reports `key expired; ask the other side to generate a new pairing key`; once 24 hours have passed it reports `pairing key is invalid`. Then check the versions: both Brokers must run v0.6.0 or newer, because an older Broker does not read the version field in the pairing request and reports `pairing key is invalid` for a correct key. [Pair two hosts]({{< relref "guides/pair-hosts.md" >}}) describes both.

## What is a topic, and do I need to create one?

A topic is the scope in which agents can see and message each other. Sessions working in the same project usually share a default topic taken from `.peers.yaml` or the git origin address, so there is nothing to create. See [Topics]({{< relref "concepts/topics.md" >}}).

## A public topic I expected is missing from `list_topics`

A public topic declared by another Broker whose name or description does not fit the rules is accepted and passed on but not listed; declaring such a topic on this Broker fails the join. A name is 1 to 256 bytes of valid UTF-8 with no control characters, and a description is at most 256 bytes of valid UTF-8 with no control characters, or empty. A public topic disappears in three cases: its publisher turns the Broker private, no agent remains publicly in that topic on the publisher's Broker or the publisher stops renewing it and it expires after 7 days. [Topics]({{< relref "concepts/topics.md" >}}) gives the rules and [Discover and connect a public Broker]({{< relref "guides/public-brokers.md" >}}) covers how public topics spread.

## Can another host see my sessions without pairing?

No. After pairing, a host sees only agents that share a topic with one of its own. A Broker is private by default and is not announced to the connected network unless you make it public. See the [security model]({{< relref "security.md" >}}).

## Is traffic between Brokers encrypted?

Yes. Links use TLS 1.3 with self-signed certificates, and a Broker's identity is its public-key fingerprint. See the [security model]({{< relref "security.md" >}}).

## Can I send files or session context?

No. Messages carry plain text only; files, session history and context are not transferred. See [known limitations]({{< relref "limitations.md" >}}).

## Does a successful send mean the other agent read the message?

No. Success means the message was written to the target session's inbox socket, which says nothing about whether the model read or processed it. If a delivery request went out but no definite answer came back, the call returns an error that begins with `Delivery unknown`. When the peer Broker reports an error this Broker does not recognize, the call returns `Delivery unknown` as well. See [MCP tools]({{< relref "reference/mcp-tools.md" >}}).

## Why did my second identical message disappear?

The receiving Claude Code drops a message from the same agent to the same session whose content repeats the previous one within 30 seconds; this rule belongs to Claude Code and may change. The receiving Broker also drops a copy that a retry within one send produces, but two separate sends are not deduplicated. See the [security model]({{< relref "security.md" >}}).

## Why was my message refused with “is receiving too many messages”?

The session on the other host is receiving messages from your Broker faster than the receiving Broker allows. It accepts ten in a row and one more every 10 seconds, counted per source Broker and target session, so other hosts and other sessions are not affected. Wait the number of seconds given in the error, then send again; see [Rate limits, loop detection and duplicate suppression]({{< relref "security.md" >}}#rate-limits-loop-detection-and-duplicate-suppression).

## `send_message` says the name belongs to another account

On a system Broker, the account that registers a name first owns it, and a by-name address reaches only that account's agent. When the name belongs to another account, the send fails with `not_found`, and the error says so. Send instead to the full address that `list_agents` gives for that account's agent; it carries the full 12-character disambiguator. A name is released after 7 days during which no agent of the owning account uses it, so the same short address may later reach an agent of a different account.

## Who can change the Broker's configuration?

An account with write permission on the Broker's configuration file can change it with `set_config` or `peers config set`, create one-time pairing keys with `create_pairing_key` or `peers key create` and pair or unpair with `connect_broker` and `disconnect_broker`, or `peers broker connect` and `peers broker disconnect`. Read permission allows reading the configuration and the audit log. The terminal commands judge permission by the account running them. A member of the admin group manages the system Broker without `sudo`: `peers service status`, `audit`, `online` and `offline` reach it with `--system`, and `peers config`, `peers key` and `peers broker` reach it directly when the account runs no user Broker of its own, while `peers service start`, `stop`, `restart`, `install` and `remove` still need `sudo`; an administrator who runs a command with `sudo` is authorized against root's permissions. See [Management permissions]({{< relref "guides/management-permissions.md" >}}), especially [The admin group]({{< relref "guides/management-permissions.md" >}}#the-admin-group).

## What happens if the port is already in use?

A Broker started in the foreground fails to start. A new install or a reinstall fails when another process holds the address, says so and registers no service. A Broker that is already installed as a service and finds the port taken when it starts enters a local-only degraded run: agents on this host still work, creating a pairing key fails and paired Brokers cannot connect to it. The Broker never chooses another address or port; free the port or change the listen address, and it recovers without a restart. See [Configuration keys]({{< relref "reference/configuration.md" >}}) and [peers service install]({{< relref "reference/cli.md" >}}#peers-service-install).

## How do I see logs and audit records?

Use `peers service status` for agents, topics, peers and links, and `peers service audit` for the audit log. When the Broker runs degraded, `peers service status` shows the reason and the fix. Install the Broker as a service and the service manager keeps its run log: on Linux, `journalctl --user -u peers.service`, or `journalctl -u peers.service` for the system service; on macOS, `~/Library/Logs/peers/broker.log`, or `/var/log/peers/broker.log` for the system service. A Broker run with `peers service run` in a terminal writes its log to that terminal. See the Logs section of [Files and paths]({{< relref "reference/files.md" >}}).

## Can a headless session receive messages?

A headless session that also skips permission prompts cannot. The message is held first and then dropped because the sender declares no permission category, while the sending side has already returned success. See [known limitations]({{< relref "limitations.md" >}}).
