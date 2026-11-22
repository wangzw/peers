---
title: Run a system Broker
linkTitle: System Broker
weight: 6
---

A system Broker runs as a service with root privileges and serves agents of several accounts on one host. A user Broker serves one account. A host runs at most one system Broker.

## Install and enable it

Install the binary in a directory that root owns and that group and others cannot write, and install the service as root. The install command does both: answer y to its level question. Run it from the account whose Claude Code should use the system Broker. The install registers the MCP server only for the account that ran it, including when it re-runs itself through `sudo`. It starts the Broker immediately, and the Broker starts at boot from then on.

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

[Install `peers`]({{< relref "install.md" >}}) covers the question and the other ways to install; [Run the Broker as a service]({{< relref "run-as-service.md" >}}) gives the manual commands.

The system Broker takes root's account lock. If a user Broker is already running as root, installing the system service fails and asks you to stop that Broker first.

The system Broker uses `/etc/peers/config.yaml`, `/var/log/peers/audit.log` and `/var/lib/peers`. If the configuration file does not exist, the install generates it as a template that lists every configuration key, with owner root, the default group `peers` and mode `0660`; an existing file is left unchanged. Members of that group manage the Broker without `sudo` when the account runs no user Broker of its own; [Manage a Broker by file permission]({{< relref "management-permissions.md" >}}#the-admin-group) describes the group and its limits.

## Let accounts use it

Each account registers the MCP server for itself; registration is per account, not per host. The system installation registers it for the account that ran the install. Other accounts need only the registration, with the installed binary's absolute path:

```sh
claude mcp add --scope user peer -- /usr/local/bin/peers mcp
```

Do not run the install command as these accounts. On a host that already has the system service, the install command asks whether to upgrade the system installation instead. Answering n leaves the host unchanged. A user Broker is installed only with `--user` or when the command runs without a terminal, and the account's sessions then use it. If a user installation is already there, remove it with [`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall); the uninstall also removes this account's MCP registration, so run the registration command above again.

A session reaches the system Broker through the local socket `/run/peers/broker.sock` on Linux, or `/var/run/peers/broker.sock` on macOS. Two settings decide which accounts can connect:

| Key | Default | Effect |
|---|---|---|
| `broker.local_socket_group` | empty | Group of the socket. Empty keeps the runtime directory's group. The group must exist. |
| `broker.local_socket_mode` | `0666` | Permission bits of the socket. |

To limit use to a group, set the group and a mode that gives that group access, for example `broker.local_socket_group: peers-users` with `broker.local_socket_mode: 0660`. Use a group other than the admin group, the one that owns the configuration file and can manage the Broker: membership there also grants management, and being allowed to connect and being allowed to manage are separate. Accounts outside the group cannot connect; they need to start their own user Broker, as a service or in a terminal. [Run the Broker as a service]({{< relref "run-as-service.md" >}}) gives the commands.

## Which Broker a session or command uses

A session's MCP server process selects a Broker in this order:

1. The user Broker of this account, if one is running.
2. The system Broker, if this account may use it.

If none is available, tool calls return an error that says what to do, and the MCP server process does not start a Broker; [the no-Broker question in the FAQ]({{< relref "/docs/faq" >}}#what-happens-when-no-broker-is-running) has the details. A host where every account should use the system Broker therefore needs no user Broker for those accounts; installing the user service on such an account moves its sessions off the system Broker.

An account that may use the system Broker but has its own user Broker running registers on the user Broker. To have its sessions use the system Broker instead, do not run a user Broker on that account.

The terminal commands `peers config`, `peers key` and `peers broker` choose a Broker in the same order, look only for a Broker that is running and never start one. The commands under `peers service` do not follow this order: they act only on the level selected by `--system` or `--user`, and never fall back to the other level. If this account has a user Broker running, use `sudo` to manage the system Broker with `peers config`, `peers key` or `peers broker`; root selects the system Broker directly.

## One identity for several accounts

All agents served by one system Broker share its fingerprint, alias and peers. Agents from different accounts that join the same topic see each other and can exchange messages. Pairing the system Broker once therefore connects every served account to the other side, and another host needs only that one pairing to reach all of them. The system Broker's default alias is the first segment of the hostname and carries no account name, so the agents of every account it serves have addresses of the form `name@workstation`. The exception is a name that another account owns: that agent's address carries the full 12-character disambiguator, as in `name [<ref>]@workstation`, where `<ref>` is those 12 characters. When account alice starts its own user Broker, the addresses of its sessions become `name@workstation.alice`, because those sessions now register with a different Broker.

Management of the Broker is decided by the permissions on `/etc/peers/config.yaml`, not by the account that runs the service. There are two ways to manage it: membership in the group that owns the file, and the terminal commands under `sudo`, which the Broker checks as root. [Manage a Broker by file permission]({{< relref "management-permissions.md" >}}#the-admin-group) describes the group and its limits.
