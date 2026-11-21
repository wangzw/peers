---
title: Update
linkTitle: Update
weight: 8
---

`peers` checks for newer releases of itself, and you install them from a terminal or from a session when you choose to. The check is automatic; an installation asks for confirmation by default unless you pass `--yes`, and a session asks by default for your approval of the `update_broker` call.

## Check for new releases

A release build of the Broker checks GitHub shortly after it starts: it waits about one minute, checks once, and then checks every 24 hours by default. The check runs in the Broker process, so one Broker covers every session that connects to it.

When a newer release exists, you see it in two places. The last line of `peers service status` is the following, where `<latest>` is the newest release and `<installed>` the installed version:

```text
Update: <latest> available (current <installed>); run peers update
```

When a newer release is available, a tool result in the session ends with:

```text
peers <latest> is available (this Broker runs <installed>). Ask the agent to call update_broker, or run `peers update` in a terminal.
```

A session shows the notice once for a given version. If a newer release appears while the session stays open, a later tool call shows the notice again. The MCP server queries the Broker for the update state on its first tool call and then at most once every 10 minutes. A build that is not a formal release, such as a binary built from source without `-ldflags`, does not check and never shows either line.

Two configuration keys control the check: `update.check` turns it on and off, and `update.check_interval` sets the period, 24 hours by default and at least one hour. [Configuration keys]({{< relref "/docs/reference/configuration" >}}) gives their values and defaults.

## Update the binary

`peers update` installs only a newer formal release; you cannot choose a version or roll back.

Run the command in a terminal:

```sh
peers update
```

It first refuses a binary that is not a release build. It then asks before changing anything; answering `y` or `yes` goes ahead, and any other answer cancels the update and leaves the binary and the service as they were. Only after you confirm does it select a Broker; what it then does depends on that Broker and on how it runs.

Add `--yes` to skip the question. When standard input is not a terminal, as in a script or a scheduled job, the command requires `--yes` and otherwise refuses with `standard input is not a terminal; pass --yes to proceed without asking`. When the installed version is already the latest, the command prints `peers 0.5.0 is up to date` and exits without changing anything.

[Command line]({{< relref "/docs/reference/cli" >}}) gives the full description of the command.

### Update a user installation

When this account's user Broker is running under the service manager, the command asks that Broker to update itself. The Broker downloads the archive for this platform and architecture, checks it against `checksums.txt`, runs the new binary to confirm that it reports the expected version, replaces its binary while keeping the original mode and owner, rewrites the user service definition in the new version's format (a systemd drop-in and the `EnvironmentVariables` in a macOS plist are kept as they are), and exits; the service manager starts the new version. If rewriting the service definition fails or times out, the Broker restores the previous binary and the service definition, does not restart, keeps running the old version, and the command reports the reason. The command returns only after the new Broker reports the new version, and the output looks like this:

```text
peers <installed> is installed. Update to the latest release? [y/N] y
The Broker is restarting to version <latest>.
The Broker is running peers <latest>.
Sessions that are already open keep the old MCP server until they are reopened; this Broker's connections recover automatically.
```

When the user Broker does not run under the service manager, the Broker declines the request and the command replaces the binary in place. The running Broker keeps the old version until you restart it, and the new binary takes effect the next time the Broker starts.

### Update the system installation

For the system installation the command asks the system Broker to update itself, and the Broker does the same work: it checks write permission on its configuration file, downloads and verifies the release, replaces its binary, and the service manager restarts it. After replacing the binary and before the restart, it rewrites the system service definition in the new version's format, and a systemd drop-in and a macOS plist's `EnvironmentVariables` are kept as they are. Members of the admin group run the command without `sudo`; [Management permissions]({{< relref "/docs/guides/management-permissions" >}}#the-admin-group) describes the group. An account outside the group is refused, because the Broker checks write permission on its configuration file, so use `sudo peers update` instead, or join the group first. When this account also runs its own user Broker, the command selects that Broker first, so `sudo peers update` is the way to reach the system Broker.

A system Broker that you started by hand in a terminal cannot update itself: the command reports an error that gives the commands for installing the Broker as a service. Whether a Broker runs under the service manager is decided by the main process PID that the service manager reports.

The command waits until the restarted Broker reports the new version. If it does not within the wait, the command reports a timeout and names where the system service writes its log. A failure before the replacement leaves the binary and the service unchanged. If rewriting the service definition fails or times out, the Broker restores the previous binary and the service definition, does not restart, keeps running the old version, and the command reports the reason.

A system update restarts only the system service. When other accounts run a user Broker from the same binary, each of them must restart its own Broker before it uses the new version.

An update the Broker applies downloads through the Broker service process, so its proxy environment variables come from that process. A replacement the command makes itself reads them from the command's environment.

A system Broker reads them from its service environment:

- Linux: run `sudo systemctl edit peers.service`, add `[Service]` and `Environment=HTTPS_PROXY=http://proxy.example:8080` (plus `HTTP_PROXY` and `NO_PROXY` as needed), then run `sudo peers service restart --system`. The drop-in survives a reinstall.
- macOS: add an `EnvironmentVariables` dictionary to `/Library/LaunchDaemons/io.github.wangzw.peers.plist` with `HTTPS_PROXY` and any other variables, then run `sudo peers service restart --system`, which reads the plist again. A reinstall rewrites the plist, so add the dictionary again afterwards; an update keeps this dictionary.

The user service works the same way: on Linux, edit the drop-in with `systemctl --user edit peers.service` and restart the service with `peers service restart`; on macOS, add the dictionary to `~/Library/LaunchAgents/io.github.wangzw.peers.plist` and run `peers service restart`, which reads the plist again. A reinstall rewrites that plist too, so add the dictionary again afterwards; an update keeps it as well.

### Update with no running Broker

When no Broker is running, the command replaces the binary itself, after checking that the directory holding the binary is writable. It does not start or restart any service, because the service may have been stopped on purpose. After you confirm and it finds a newer release, it prints the installed and available versions and the release page, replaces the binary, and ends with:

```text
The new binary takes effect the next time the Broker starts.
```

Start the Broker the way you started it before, for example with `peers service run` in a terminal, to run the new version. If the directory is not writable, the command stops before any download and tells you to run `sudo peers update`.

### Update from a session

A session can update the Broker with the `update_broker` tool, which takes no arguments. It asks the Broker registered for the session to upgrade itself and returns as soon as the Broker answers, with the version before and after and whether the Broker is about to restart. When the installed version is already the latest, it says so and changes nothing. A user or system Broker that does not run under the service manager cannot update itself, and the tool says to use the terminal command instead.

The tool needs write permission on the Broker's configuration file, which is granted separately from read permission. It needs no terminal: Claude Code asks by default for confirmation before the call runs, and that confirmation is the last manual check before the Broker's binary is replaced. Do not add `update_broker` to auto-approved tools, so the confirmation is never skipped.

After the Broker restarts, the connection recovers automatically. The MCP server in the session that made the call keeps the old version until the session is reopened. [MCP tools]({{< relref "/docs/reference/mcp-tools" >}}) lists the tool and its result fields.

## Reconnect open sessions

When the Broker applies an update, it restarts and its connections recover automatically. Sessions that are already open keep running the old MCP server, so each one must reconnect with `/mcp` or be reopened before it uses the new version. When the command replaces a binary while no Broker is running, there is no connection to recover; sessions use the new version once the Broker starts and they connect to it.

## Which command applies to your installation

| Installation | Command | What happens |
|---|---|---|
| User installation: on a host without the system service, answered n or pressed Enter at `Install for every account on this host? [y/N]`; ran without a terminal as a non-root account; ran with `--user`; or placed the binary in `~/.local/bin` yourself | `peers update` | With a service, the Broker replaces its binary and the service manager restarts it. Without one, the command replaces the binary and it takes effect at the next start. |
| System installation: the install command with `--system` or answered y at the level question, or run as root, or a manual `sudo install` into `/usr/local/bin` | `peers update` for a member of the admin group with no user Broker running; otherwise `sudo peers update`, or join the group if the account is outside it | The Broker replaces its binary and the service manager restarts it. |
| No running Broker | `peers update` | The command replaces the binary, and nothing is started or restarted. |

A binary built from source with the release version in `-ldflags` follows the row for the installation it belongs to. A binary built without `-ldflags` reports the version `dev`; it is not a release build, and the command refuses with `version "dev" is not a release build; only release builds can update themselves`. Install a release to update in place.

If the binary is a symbolic link, the command resolves the link and replaces the file it points to.

## Turn off update checks

To stop the Broker from checking:

```sh
peers config set update.check false
```

The notice disappears at once: `peers service status` no longer prints the `Update` line, and sessions no longer show the update notice. A host without access to GitHub can leave the check off; `peers update` contacts GitHub only when you run it yourself.

[The security model]({{< relref "/docs/security" >}}) describes the network access the check makes.
