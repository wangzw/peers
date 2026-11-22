---
title: Manage a Broker by file permission
linkTitle: Management permissions
weight: 7
---

The tools and the terminal commands can change a Broker only as far as the calling account could change the Broker's configuration file by hand. The Broker takes the account from the process credentials the operating system reports for the local socket; it does not rely on what the caller says.

## The rule

| Operation | Required access to the configuration file |
|---|---|
| `list_agents`, `list_topics`, `join_topic`, `leave_topic`, `send_message` | none |
| `list_brokers`, `peers broker list` | none |
| `get_config`, `peers config list`, full `peers service status` output, `peers service audit` | read |
| `set_config`, `peers config set` | write |
| `set_broker_online`, `peers service offline`, `peers service online` | write |
| `create_pairing_key`, `peers key create` | write |
| `connect_broker`, `peers broker connect` | write |
| `disconnect_broker`, `peers broker disconnect` | write |
| `update_broker`, `peers update` through the Broker | write |

`peers update` checks write permission on the binary's directory instead when it replaces the binary locally, because no Broker is running or the selected user Broker does not run as a service.

A user Broker's configuration file belongs to the account that runs it, so these checks pass for that account without any setup. A system Broker's file is `/etc/peers/config.yaml`. A new installation creates it with owner root, the default group `peers` and mode `0660`; an existing file is left unchanged. Read access also lets an account read the full status and the audit log; write access lets it change settings and manage pairing. Both levels are ordinary file permissions, not a separate list of administrators.

Members of the group that owns the configuration file manage the Broker without `sudo`. `peers service status`, `audit`, `online` and `offline` reach the system Broker with `--system`. When this account runs no user Broker, `peers config`, `peers key` and `peers broker` also reach the system Broker directly; when it runs one, these three connect to that user Broker, so run them with `sudo` to manage the system Broker. `peers service start`, `stop` and `restart` call `systemctl` or `launchctl` directly, and `peers service install` and `remove` create or delete the system service, so these still need `sudo`. An account that runs the terminal commands with `sudo` is checked as root, so an administrator can manage the Broker that way whatever the file's group is. The section [Which Broker a session or command uses]({{< relref "system-broker.md" >}}#which-broker-a-session-or-command-uses) describes the order.

The check identifies an operating-system account, not a person or a model. Any process running under the same account can make the same calls, whether through the tools or from the terminal.

## The admin group

`peers service install --system` creates the `peers` group by default when it does not exist, and uses an existing group without changing its members. Pass `--admin-group <name>` to use another group instead; the name may start only with a lowercase letter or an underscore and may contain only lowercase letters, digits, underscores and hyphens. A configuration file that the system installation creates belongs to the group with mode `0660`, so members of the group manage the Broker without `sudo`. The group is trusted as if its members could edit the configuration file by hand; it does not make them root.

An existing group that already lists members is refused when this installation generates a new configuration file, the administrator did not name the group with `--admin-group` and no earlier installation recorded the group: those members would manage the system Broker without the administrator's knowledge. The install writes no file and exits with code 1, with the error

```
peers service install: cannot use the existing group "<group>" as the administration group: its members <a, b and c> would be able to manage the system Broker; pass --admin-group <group> to accept them, or pass --admin-group with another name
```

with a single member named as `member <a>`. Naming the group with `--admin-group` accepts its members. The member list counts supplementary members and accounts whose primary group is the group, and on macOS also the accounts of nested groups; when it cannot be read, the install refuses with `cannot read the members of group "<group>": …`. A group whose group id is 0 is refused outright, named or not, because every account in it would hold the configuration file's permissions. The members are not read at all when `--admin-group` names the group or when the configuration file already exists, so a broken account database cannot block such a reinstall. A group that an earlier installation recorded in `/var/lib/peers/admin-group` is not refused, but its members are still read and listed in the summary. In the install script, a refusal under `--system` prints `install.sh: cannot install the system service` and the script exits with code 1.

The install script asks whether to add the account that ran the install to the group, and does not add it by default. `--join-admin-group` adds that account without asking, and `--no-admin-group` neither asks nor adds it. Without a terminal, the script adds nothing and prints the command to join the group. A new membership applies from the next call through the Broker, because the Broker reads the account's groups from the account database on each check. Log in again, or run `newgrp peers`, before the account's own processes use the new membership, for example to edit the configuration file directly. To add another account, run `sudo usermod -aG peers <account>` on Linux, or `sudo dseditgroup -o edit -a <account> -t user peers` on macOS; if you set `--admin-group`, use that group name instead.

For an existing installation, run, as root, the command the install output prints:

```sh
chgrp peers /etc/peers/config.yaml && chmod 0660 /etc/peers/config.yaml
```

Use `0640` instead of `0660` to grant read access only.

These limits come with the group:

- A port below 1024 in `broker.listen` takes effect only while the configuration file can be written by root alone. `chmod g-w <path>` or a port of 1024 or higher fixes it, and [Configuration keys]({{< relref "configuration.md" >}}#types-and-constraints) gives the rule.
- Installing and uninstalling the system service still needs root.

A read-only grant (`0640`) is not subject to the low-port rule, because it looks only at whether the group or others can write the file.

A change made by editing the file directly is recorded in the audit log with no account, because only changes through the local socket carry the caller's account.

Being allowed to use the Broker and being allowed to manage it are separate. The settings in [Run a system Broker]({{< relref "system-broker.md" >}}) decide which accounts can connect to the local socket; the permissions on the configuration file decide which of them can read or change the Broker.

## Other gates

- Claude Code's own permission settings decide whether a tool call needs your approval. They apply to the tools only: the terminal commands do not go through Claude Code's approval. The file check is independent of both; a tool call runs only when the file check and the approval both allow it.
- Do not add `update_broker` to Claude Code's auto-approve list. That confirmation is the last manual check before an upgrade, and for the system installation the Broker replaces a binary that runs as root.
- Neither the tools nor the terminal commands can set `pairing.static_key` or read its value. `get_config` and `peers config list` report only whether it is set, and `set_config` and `peers config set` always fail for it. Edit the configuration file to set, replace or delete it; the Broker applies the change without a restart.
- A permission error changes nothing: the configuration, the peers and the valid pairing keys stay as they were. When a terminal command lacks the required permission, the Broker's error message, which includes the path of the configuration file, is written to standard error, and the command exits with code 1.
- A configuration-file permission check runs in a separate process with a fixed number of slots. When all slots are busy, a command that needs one fails; `peers config set` prints `peers config set: quota_exceeded: Too many configuration access checks are running; try again shortly.` to standard error and exits with code 1. Retrying shortly is enough.
