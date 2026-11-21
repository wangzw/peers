---
title: Command line
weight: 2
---

The `peers` binary runs the Broker, the MCP server and the terminal commands. Run `peers -h`, `peers --help` or `peers help` to print the usage below. Every command also accepts `-h` and `--help`. Running `peers` with no arguments prints the usage to standard error and exits with code 2. A usage error, such as an unknown command, an unknown option or the wrong number of arguments, also exits with code 2.

```
peers runs an MCP server for one Claude Code session, the local Broker, and the terminal commands that manage pairings and the installation.

Usage:
  peers [flags]
  peers [command]

Pairing:
  broker      Manage the Brokers this host is paired with
  key         Manage the one-time pairing keys

Local Broker:
  config      Read and change the Broker configuration
  service     Run and manage the local Broker service

Installation:
  completion  Generate the autocompletion script for the specified shell
  uninstall   Remove the peers installation
  update      Install the newest release of peers

Flags:
  -h, --help      help for peers
      --version   print the version and exit

Use "peers [command] --help" for more information about a command.
```

## peers --version

Prints `peers <version>` and exits. A binary built without the version linker flag reports `peers dev`.

## peers broker list

```
peers broker list [flags]
```

Lists every Broker known to a running Broker, including Brokers that are not peers, as a table with the columns `ALIAS`, `FINGERPRINT`, `PEER` (`yes` or `no`), `STATUS`, `DIRECTION` and `ADDRESSES`, sorted by alias. The `ADDRESSES` column joins the addresses with commas. With no known Broker it prints `Brokers: none`. When this Broker is offline, the line `Broker: offline; messages from other hosts are refused. Run peers service online to accept them.` comes before the table. When it runs degraded, the `Broker: degraded …` and `Fix: …` lines come before the table; a diagnostics-only run writes the reason to standard error instead and exits with code 1.

The command takes no options apart from `-h`/`--help`. It requires no permission. A usage error exits with code 2; any other failure exits with code 1.

## peers broker connect

```
peers broker connect <address> [flags]
```

Pairs this Broker with another Broker and prints one line, `Paired with <alias> (<fingerprint>)`.

- `address` is written `host:port` or `hostname(host:port)`. It may come before or after the options.
- `--key <key>` is a one-time pairing key created on the other Broker, or its pre-provisioned static key. The value is required, and only the double-dash form is accepted.
- `--alias <alias>` sets the local alias. When the other Broker's own alias conflicts with an existing one and no local alias is given, the pairing fails and asks for a local alias.

Warnings in the reply are written to standard error, one per line with the prefix `warning: `, and the command still exits with code 0.

Requires write permission on the Broker's configuration file. A usage error exits with code 2; any other failure exits with code 1.

## peers broker disconnect

```
peers broker disconnect <broker> [flags]
```

Removes the pairing with a Broker and prints one line, `Disconnected <broker>`.

- `broker` is the peer's alias or its full 64-character hexadecimal fingerprint. The shell completion offers the aliases and fingerprints of the paired Brokers.

The command takes no options apart from `-h`/`--help`. Requires write permission on the Broker's configuration file. A usage error exits with code 2; any other failure exits with code 1.

## peers key create

```
peers key create [flags]
```

Creates a one-time pairing key on a running Broker and prints the key, its expiry time and this Broker's advertised addresses:

```text
Key: <key>
Expires: <RFC 3339 UTC>
Addresses:
  <address>
```

Each address is printed on its own line, indented by two spaces. When the Broker does not listen, which happens when `broker.listen` is empty and no `--listen` is given at startup, it rejects the request and the command exits with code 1.

The command takes no options apart from `-h`/`--help`. Requires write permission on the Broker's configuration file. A usage error exits with code 2; any other failure exits with code 1.

## Broker administration commands

The `config` and `broker` commands and `key create` connect to a running Broker, without starting one. They choose the Broker by the operating system account that runs them, with no command-line option to select a level: this account's running user Broker first, then the system Broker this account may use. An account that runs its own user Broker and wants to manage the system Broker must run the command with `sudo`. `peers update` uses the same account-based choice. The `service` commands that talk to a running Broker, namely `status`, `audit`, `online` and `offline`, take `--system` and `--user` instead; their sections describe the level rule. [Run a system Broker]({{< relref "/docs/guides/system-broker" >}}) explains the choice and the settings.

The Broker decides the permission of each command from the account that runs it, using the same rules as the matching tool. In the audit records left by the write commands, `account` is the account that ran the command and `actor` is empty. When an administrator runs a command with `sudo`, a system Broker authorizes it against root's permissions. A member of the system Broker's admin group can manage it without `sudo` when the account runs no user-level Broker of its own; [Manage a Broker by file permission]({{< relref "/docs/guides/management-permissions" >}}) explains the group. These commands do not pass through Claude Code's approval. When the Broker denies a command, its English error, which contains the path of the configuration file, is written to standard error and the command exits with code 1.

These commands follow the output conventions of `peers service status`: an empty value is written `-`.

## peers config list

```
peers config list [flags]
```

Prints every configuration item of a running Broker, one per line as `<key> = <value>`, sorted by `key`.

`pairing.static_key` is printed as `set` or `unset`, never by value.

The command takes no options apart from `-h`/`--help`. Requires read permission on the Broker's configuration file; on a user Broker the account has it. A usage error exits with code 2; any other failure exits with code 1.

## peers config get

```
peers config get <key> [flags]
```

Prints the named configuration item as `<key> = <value>`. Only one key is read; to print every item, use `peers config list`.

`pairing.static_key` is printed as `set` or `unset`, never by value. An unknown key fails. The shell completion offers the configuration keys.

Requires read permission on the Broker's configuration file; on a user Broker the account has it. A usage error exits with code 2; any other failure exits with code 1.

## peers config set

```
peers config set <key> <value> [flags]
```

Changes one configuration item and prints the change as `<key>: <old> -> <new>`.

- `key` and `value` are both required. The shell completion offers the configuration keys.
- `pairing.static_key` cannot be changed here; it is set in the configuration file only.

When the Broker runs with `--listen`, that option fixes `broker.listen` and `config set` cannot change it; [Configuration keys]({{< relref "docs/reference/configuration" >}}) gives the rule.

Requires write permission on the Broker's configuration file. A usage error exits with code 2; any other failure, including a key the Broker rejects, exits with code 1.

## peers service run

```
peers service run [flags]
```

Runs a Broker in the foreground until it receives `SIGINT` or `SIGTERM`. A Broker started inside a session's process tree fails with exit code 6. The service definitions start the Broker this way.

- `--system` and `--user` select the level and are mutually exclusive. Without either, root selects the system level and every other account its user level.
- `--system` runs the system Broker. It requires root.
- `--listen <address>` overrides `broker.listen` for this run only. The address uses the same syntax as the configuration key, for example `:17443` or `127.0.0.1:17443`. An empty value is rejected with a usage error; to stop listening, set `broker.listen` to an empty value in the configuration file.
- `--allow-degraded` keeps the Broker running after certain startup failures instead of exiting. An unavailable listen address, including one rejected by the low-port rule, leaves it serving local sessions in a local-only degraded run; an unavailable data directory or audit log leaves it in a diagnostics-only run. Either way it answers on the local socket and reports the failure. Without the option, a startup failure reports its cause on standard error and exits, and an unavailable listen address exits with code 4. Configuration errors, a `broker.local_socket_group` that does not exist, and failures before the account lock exit either way. The service definition always passes the option; a foreground run does not. A local-only run starts listening by itself once the address becomes available or `broker.listen` changes. A diagnostics-only run retries when the configuration file changes and once a minute, and `peers service restart` also works after the fix. Neither recovery restarts the process.

Without `--listen`, the configured `broker.listen` applies. A usage error exits with code 2. A startup failure exits with the code for its cause: 3 when another Broker of this account already runs and holds the account lock, 4 when the listen address is unavailable, 5 when the configuration is invalid, 6 when the Broker starts inside a session's process tree, 7 when `--system` is given without root, and 8 when the runtime directory is not usable. Any other failure exits with code 1.

## peers service install

```
peers service install [flags]
```

Installs a service definition, enables the service, and starts the Broker immediately. It waits up to 20 seconds for the Broker to answer; a timeout fails the install, reports where the service logs go, and includes the reason from the last `startup_failed` entry in that log. Reinstalling replaces the running Broker with the new binary. [Install `peers`]({{< relref "/docs/guides/install" >}}#installation-output) lists what the install reports.

- `--system` and `--user` select the level and are mutually exclusive. Without either, root installs the system level and every other account its user level.
- A user-level service starts the Broker when the account logs in. It must not run as root. On macOS it must run in a graphical login session. If `~/.peers/config.yaml` does not exist, it generates the file, owned by the account that runs the install with mode `0600`; an existing file is left unchanged.
- A system-level service starts the Broker at boot. It must run as root. The command creates the `peers` group when it does not exist, and uses it without changing its members when it does. If `/etc/peers/config.yaml` does not exist, it generates the file, owned by `root` with group `peers` and mode `0660`; an existing file is left unchanged. `--admin-group <name>` takes effect only in a system-level install; when the install resolves to the user level, whether through `--user` or an account other than root, it is a usage error and exits with code 2. It uses another group; the name may start only with a lowercase letter or an underscore and may contain only lowercase letters, digits, underscores and hyphens.

The generated file is a template that lists every configuration key: keys with a fixed default are written out, and the remaining keys are comments with an example value or an empty placeholder. The install summary's `config:` line ends with `(generated)` when it wrote the file and `(kept)` when the file already existed; a system-level install that kept an existing file also prints `To delegate administration, run: chgrp <group> <path> && chmod 0660 <path>`. Running that command hands an existing installation to the group, whose members then manage the Broker without `sudo`; [Manage a Broker by file permission]({{< relref "/docs/guides/management-permissions" >}}) explains the group. Do not hand the file to the group when `broker.listen` uses a port below 1024; [Configuration keys]({{< relref "docs/reference/configuration" >}}#types-and-constraints) gives the rule and the fix.

If a Broker that this service did not start is already running at that level, the install refuses and asks you to stop it first.

Before it writes or enables the service, the install checks the listen address it will use. When the address is in use, the install fails without registering a service, and the error gives the address, the configuration file and `broker.listen`. A reinstall recognizes its own running service, stops it and waits up to 20 seconds for it to release the address; when another process holds the address, an installed service is left alone. A service that a failed install wrote or enabled does not start at boot or login, and when it stopped an existing service, the error gives the command to restore it.

The service definition starts the Broker with `service run --system` or `service run --user`, and always passes `--allow-degraded`. When the Broker runs degraded after the service is registered, the install keeps the service installed and running, prints the reason and the fix command, and exits with code 3. `peers service start` and `peers service restart` exit with code 3 in the same situation.

On Linux the command uses systemd and runs `systemctl daemon-reload`, `systemctl enable` and `systemctl restart`. On macOS it writes and loads a launchd job. A `--system` install additionally requires the executable and every directory above it to be root-owned, not writable by group or others, and searchable by other accounts, and the executable to be executable by other accounts. An executable under the temporary directory is refused. On an Intel Mac with Homebrew, `/usr/local/bin` belongs to the ordinary user, so choose a root-owned directory that group and others cannot write instead.

The service files and paths are listed under [Files and paths]({{< relref "docs/reference/files" >}}); [Guides]({{< relref "docs/guides" >}}) covers installing and running the service.

The install script in the release archive installs the service with this command.

A usage error exits with code 2; a degraded run exits with code 3; any other failure exits with code 1.

## peers service remove

```
peers service remove [flags]
```

Stops and removes only the installed service. The binary and the MCP registration stay, and so do configuration, identity and audit logs; [peers uninstall](#peers-uninstall) removes the whole installation. If no service is installed, the command succeeds, prints `peers service is not installed`, and can be repeated.

- `--system` and `--user` select the level and are mutually exclusive. Without either, root selects the system level and every other account its user level.
- `--system` removes the system-level service. It must run as root.

A usage error exits with code 2; any other failure exits with code 1.

## peers service start, stop and restart

```
peers service start [flags]
peers service stop [flags]
peers service restart [flags]
```

Starts, stops or restarts the installed service.

- `--system` and `--user` select the level and are mutually exclusive. Without either, root selects the system level and every other account its user level.

On success the command prints `Broker service started (user-level)` or the matching `Broker service stopped (user-level)` or `Broker service restarted (user-level)` line, with `system-level` for the system-level service.

`stop` does not disable the service, so it starts again at the next login or boot. When that level has no installed service, the command exits with code 1 and says that no service is installed; an account that has only the system-level service installed is also told to run `sudo peers service <command> --system`.

`start` and `restart` wait for the Broker to answer and check whether it runs degraded, as an install does; [peers service install](#peers-service-install) gives the exit codes. A usage error exits with code 2.

## peers service status

```
peers service status [flags]
```

Connects to a running Broker, without starting one, and prints two tables:

- `Agents`: each registered agent's address, account, status, topics and previous name.
- `Peers`: each peer's alias, fingerprint, this Broker's role, status, link state, advertised addresses and detail.

- `--system` and `--user` select the level and are mutually exclusive. Without either, root selects the system level and every other account its user level. The command talks only to the selected level and does not fall back to the other one; an account whose Broker runs only at the system level must pass `--system`.

An empty section is printed as `Agents: none` or `Peers: none`. Without read permission on the Broker's configuration file, only the caller's own agents are listed; the Peers table is still shown. When this Broker is offline, a `Broker: offline; messages from other hosts are refused. Run peers service online to accept them.` line comes before the tables.

When the Broker runs degraded, the lines `Broker: degraded (local only): <reason>` or `Broker: degraded (diagnostics only): <reason>` and `Fix: <fix>` come before the tables. A local-only run still prints both tables. A diagnostics-only run prints only these two lines and exits with code 1.

When a newer formal release is available, `peers service status` prints one more line at the end. In that line, `<latest>` is the newest release and `<installed>` the installed version.

```text
Update: <latest> available (current <installed>); run peers update
```

No newer release means no such line. Turning off `update.check` removes the line: the Broker stops checking and stops reporting an available version. [Configuration keys]({{< relref "docs/reference/configuration" >}}) lists the two update keys.

A usage error exits with code 2; any other failure exits with code 1.

## peers service online and offline

```
peers service online [flags]
peers service offline [flags]
```

`peers service offline` makes this Broker refuse messages sent by other hosts, and `peers service online` accepts them again. The commands print `Broker is now offline` and `Broker is now online` respectively.

- `--system` and `--user` select the level and are mutually exclusive. Without either, root selects the system level and every other account its user level. The command talks only to the selected level and does not fall back to the other one.

A message that another host sends to an agent on an offline Broker is refused with the error code `broker_offline`, reported by this Broker. Agents on this host still reach each other, and an agent on this host can still send to another host, but the receiver's reply is refused; run `peers service online` before a two-way exchange. Links, topic membership, directories and new pairings keep working. A peer learns that this Broker is offline only when a send is refused; its Broker list and link state do not show it.

The state is the `broker.offline` configuration key, so it survives a restart. The commands need write permission on the configuration file, like any other configuration change; without it, they fail with `permission_denied` and an error that includes the path of the configuration file. Each change is written to the audit log with the account that ran the command. A usage error exits with code 2; any other failure exits with code 1.

## peers service audit

```
peers service audit [flags]
```

Prints the audit log from a running Broker, one line per event, in time order, then exits. It does not start a Broker and does not follow new events while it runs.

- `--system` and `--user` select the level and are mutually exclusive. Without either, root selects the system level and every other account its user level. The command talks only to the selected level and does not fall back to the other one.
- `--limit <n>` reads at most `n` events, the newest ones, still printed oldest first. `0` means every event and is the default. The maximum is `4294967295`; a value that is not a number or exceeds the maximum is a usage error.

Reading the audit log requires read permission on the Broker's configuration file. On a user Broker the account has that permission. A usage error exits with code 2; any other failure exits with code 1.

## peers update

```
peers update [flags]
```

Installs the latest formal release, either by asking the selected Broker to update itself or by replacing the binary in place.

The command installs only a newer formal release: drafts and prereleases are ignored, there is no option to choose a version, and it never downgrades.

- `--yes` (`-y`) skips the confirmation question. Without it the command asks `peers <installed> is installed. Update to the latest release? [y/N]`, where `<installed>` is the installed version, and accepts `y` or `yes`, in any case, as confirmation. When standard input is not a terminal, the command fails without `--yes` and exits with code 1. When the user answers `n`, it prints `Update canceled; nothing was changed.` and exits with code 0.
- A usage error exits with code 2. Any other failure exits with code 1. A successful update, an up-to-date result and a canceled update all exit with code 0.

The command first checks that this binary is a formal release build, then asks for confirmation unless `--yes` is given, and only then selects the Broker. It replaces the binary locally only when no Broker is running or when the selected user-level Broker does not run under a service manager; a system-level Broker that does not run under a service manager makes the command fail instead.

### Through the Broker

When the command selects a running Broker, that Broker installs the update itself. A member of the system Broker's admin group can update it without `sudo`; an account that runs its own user Broker reaches the system Broker with `sudo peers update`.

1. Sends the request to the selected Broker. The Broker decides permission from write access to its configuration file; the command does not check it first. On a denial the Broker's English error is written to standard error and the command exits with code 1, with nothing downloaded.
2. When the current version is already the latest, the command prints `peers <installed> is up to date` and exits with code 0.
3. The Broker downloads the archive for this platform and `checksums.txt`, verifies the archive's SHA-256, extracts the regular file named `peers`, and runs the new binary's `--version` to confirm that it reports the expected version. It then replaces its own binary. A symbolic link keeps pointing at the updated file.
4. The Broker answers before it restarts. The command prints `The Broker is restarting to version <latest>.` and waits until the restarted Broker reports that version, because the update is complete only when that process runs. On success it prints `The Broker is running peers <latest>.` and then the session notice. When the restarted Broker does not report the new version in time, the command fails and names where that level's service log is.
5. The session notice reads `Sessions that are already open keep the old MCP server until they are reopened; this Broker's connections recover automatically.`

A user-level Broker that was started from a terminal, rather than by a service manager, cannot replace and restart itself. It leaves the command to the local replacement below.

### By replacing the binary

With no running Broker, or after the user-level Broker above declines, the command replaces the binary in place:

1. Locates the running executable and resolves symbolic links. Only the file they point at is replaced.
2. Checks that it can write in the target's directory, before any download. Without permission it prints `cannot write to <dir>: ...`, then `run "sudo peers update" to update a system-level installation`, and exits with code 1.
3. Queries GitHub for the latest formal release. When the current version is already the latest, it prints `peers <installed> is up to date` and exits with code 0.
4. Downloads the archive for this platform and `checksums.txt`, verifies the archive's SHA-256, extracts the regular file named `peers`, and runs the new binary's `--version` to confirm that it reports the expected version. It prints `peers <installed> is installed; version <latest> is available.` and, when the release has a URL, `Release: <url>`.
5. Writes the new binary next to the target, gives it the same owner and mode as the old one, and renames it into place. The old version is not kept.
6. Starts no service and restarts none, because the service may have been stopped on purpose. It prints `The new binary takes effect the next time the Broker starts.`

The output of a successful update looks like one of these. `Release:` appears only when the release has a URL.

Through the Broker:

```text
peers <installed> is installed. Update to the latest release? [y/N] y
The Broker is restarting to version <latest>.
The Broker is running peers <latest>.
Sessions that are already open keep the old MCP server until they are reopened; this Broker's connections recover automatically.
```

By replacing the binary:

```text
peers <installed> is installed. Update to the latest release? [y/N] y
peers <installed> is installed; version <latest> is available.
Release: https://github.com/wangzw/peers/releases/tag/v<latest>
The new binary takes effect the next time the Broker starts.
```

Failures are written to standard error with the prefix `peers update: `. The messages include:

- `version "dev" is not a release build; only release builds can update themselves`
- `cannot write to <dir>: ...`, followed by `run "sudo peers update" to update a system-level installation`
- `standard input is not a terminal; pass --yes to proceed without asking`
- `checksum mismatch for <archive>`
- `downloaded binary reported "<x>", want "peers <latest>"`
- `the Broker did not report version <latest> within <duration>; check the service log with <command or path>`; when the last status query failed, its reason is in parentheses after the duration: `within <duration> (<error>)`
- `the Broker is not running under a service manager, so it cannot start the new binary again; install it as a service and run the update against that service (for example, "peers service install" or "sudo peers service install --system")`
- `another update is already in progress`

When the update goes through a Broker, a checksum or version mismatch appears after `installing <latest> failed: `, and a failure to look up the latest release appears after `looking up the latest release failed: `.

## peers uninstall

```
peers uninstall [flags]
```

Removes an installation: the service, the binary that the service starts, the MCP registration of the account that ran `sudo` (or of the account that runs the command when it is not run through `sudo`), the shell completion files and man pages the install script placed, and the runtime directory.

- Run as root to remove a system-level installation. Another account removes its own user-level installation; when the host also has a system-level installation, the command removes the user-level one and then prints `A system-level installation is still present; run: sudo peers uninstall --yes`, with `--purge` added when that option was given.
- An account whose host has only the system-level installation is asked `Remove the system-level installation through sudo? [y/N]`. Answering `y` runs the command again with `sudo`; `--yes` skips the question and runs it through `sudo` directly. Any other answer prints `Run: sudo peers uninstall --yes` and exits with code 0. Without a terminal and without `--yes`, the command fails, prints the complete `sudo peers uninstall --yes` command to run, and exits with code 1. When `--purge` is given, both of those commands carry it: `sudo peers uninstall --purge --yes`.
- Unless `--purge` is given, the command asks `Also remove configuration, identity, audit and service logs? [y/N]` and keeps them on any answer other than `y`.
- `--yes` (`-y`) skips the questions. It keeps the configuration, identity, audit and service logs unless `--purge` is also given.
- `--purge` skips the data question and removes the data too.
- Without a terminal, the command needs `--yes` to proceed, whether or not `--purge` is given; otherwise it fails before removing anything and exits with code 1.
- At the system level, answering `y` or giving `--purge` removes the admin group only when the installation created it. Otherwise, or when the group is still in use, the group is kept, and the command prints how to remove it by hand.
- The MCP registration of every other account stays; each of those accounts removes it with `claude mcp remove -s user peer`. At the system level the command removes the registration of the account that ran `sudo`; when it runs as root directly, it prints the command instead.

Before deleting a system-level binary, the command lists other accounts whose user-level Broker is running, then deletes the binary anyway.

A failure stops the command without undoing the steps that already succeeded; the error says which step failed, and running the command again continues from there. When no service is installed at either level but the MCP registration points at this binary, it uninstalls at the user level; with nothing installed, it reports that and exits with code 0.

Exit codes: 0 for a successful uninstall, for nothing to uninstall, and for declining the sudo uninstall of a system-level installation; 1 for a failure, including a missing `--yes` when there is no terminal; 2 for a usage error.

## peers completion

```
peers completion [flags]
peers completion [command]
```

Prints the autocompletion script for the named shell to standard output. The shells are `bash`, `zsh`, `fish` and `powershell`; each shell's subcommand accepts `--no-descriptions` to leave the descriptions out of the script. `peers completion` without a shell, and an unknown shell name, are usage errors and exit with code 2. A successful run exits with code 0.

The install script writes the completion files for the level it installs: bash at both levels, zsh at the system level, and fish when the host has fish. [Install `peers`]({{< relref "/docs/guides/install" >}}#shell-completion-and-man-pages) describes what the install script writes, and [Files and paths]({{< relref "/docs/reference/files" >}}#shell-completions-and-man-pages) lists the paths.

Some arguments complete dynamically: `broker disconnect` offers the aliases and fingerprints of the paired Brokers, `config get` and `config set` offer the configuration keys, and `completion` offers the shell names.

## peers mcp

```
peers mcp
```

Runs the MCP server for one Claude Code session over the process's standard input and standard output. Claude Code starts this process from the MCP registration, so there is normally no reason to run it by hand. The command is hidden from the help listing. The session uses this account's user Broker when one is running; otherwise it uses the system Broker if this account may connect to it. [Run a system Broker]({{< relref "/docs/guides/system-broker" >}}) explains the choice and the settings. A running user Broker takes precedence over the system Broker; on an account that can use the system Broker, installing a user-level service makes its sessions use the user Broker instead of the system Broker. When no Broker is available, tool calls return an error that says what to do; [the no-Broker question in the FAQ]({{< relref "/docs/faq" >}}#what-happens-when-no-broker-is-running) has the details.

A usage error exits with code 2; a failure to run the server exits with code 1.
