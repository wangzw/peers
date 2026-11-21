---
title: Run the Broker as a service
linkTitle: Run the Broker as a service
weight: 5
---

A Claude Code session connects to a Broker but never starts one. You start a Broker either by installing it as a service or by running it in a terminal. A service starts the Broker right away, starts it at login or at boot, keeps it running and brings it back after a machine restart.

The install command already installs and starts the service, so this page covers a manual installation or installing the service after `--no-service`. The sections on stopping a Broker you started in a terminal and on uninstalling a service apply to every installation.

## Stop a Broker you started in a terminal

If you have run `peers service run` in a terminal for this account, stop it there before installing the service. The install fails while a Broker that this service did not start is answering, because that Broker holds the account lock.

## Install the user service

If the host already has a system Broker you can use, you do not need the user service.

Run the install command as the account that owns the sessions:

```sh
peers service install
```

The command writes the unit file or plist, enables the service and starts the Broker immediately. It waits up to 20 seconds for the Broker to answer; if it does not answer, the install fails and reports where the service logs go. Reinstalling replaces the running Broker with the new binary. The user installation must not run as root, because the unit belongs to one account. The install refuses a binary inside the temporary directory; this applies to both the user and the system installation.

When `~/.peers/config.yaml` does not exist, the install generates it as a template that lists every configuration key: keys with a fixed default are written out, and the remaining keys are comments with an example value or an empty placeholder. The file belongs to the account that runs the install and has mode `0600`; an existing file is left unchanged. The install summary's `config:` line ends with `(generated)` or `(kept)`. [Configuration keys]({{< relref "docs/reference/configuration" >}}) describes the generated file.

On Linux with systemd, the unit is `~/.config/systemd/user/peers.service` (or under `$XDG_CONFIG_HOME`). On macOS, the LaunchAgent is `~/Library/LaunchAgents/io.github.wangzw.peers.plist`, and the install must run in a graphical login session; a host without one uses the system service instead.

The unit or plist runs `peers service run --user --allow-degraded` as that account, so a Broker that cannot bind its listen address keeps serving this account in a local-only degraded run. It starts when the account logs in and restarts the Broker if it exits.

## Install the system service

The system installation runs as root and makes the Broker start at boot. Answering y to the install command's level question installs the binary and the service in one step, and so does running it as root; [Install `peers`]({{< relref "install.md" >}}) covers the question. The steps below do the two parts by hand. The script also asks whether to add the account that ran the install to the admin group. [The admin group]({{< relref "management-permissions.md" >}}#the-admin-group) covers the group, the answer and the options that control the question.

The binary must be in a directory that root owns and that group and others cannot write, such as `/usr/local/bin`. The service starts the binary as root, so an ordinary account that can modify the binary or one of its parent directories could take over the service:

```sh
VERSION=0.5.0
sudo install -o root -g 0 -m 0755 peers-${VERSION}/peers /usr/local/bin/peers
sudo /usr/local/bin/peers service install --system
```

On an Intel Mac with Homebrew, `/usr/local/bin` belongs to the ordinary user, so the installation is refused; choose a root-owned directory that group and others cannot write instead.

The install checks the binary and every directory above it, then starts the Broker immediately.

On Linux the command writes `/etc/systemd/system/peers.service`, enables it and starts it. On macOS it writes and loads `/Library/LaunchDaemons/io.github.wangzw.peers.plist`. A system install also generates the configuration file at `/etc/peers/config.yaml` from the system template if it does not exist, owned by root, with group `peers` and mode `0660`; an existing file is left unchanged, and the install prints the command to assign it to the group. [The admin group]({{< relref "management-permissions.md" >}}#the-admin-group) covers the group.

## Uninstall a service

Run `peers service remove` as the same account to remove the user service. For the system service, run `sudo /usr/local/bin/peers service remove --system`. The command stops the Broker and removes only the service definition; the binary and the MCP registration stay. If no service is installed, it succeeds and reports that nothing is installed, so it can be run again safely. To remove the whole installation, run [`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall), with `sudo` for the system installation.

## Start and stop the service

Run `peers service stop` and `peers service start` to stop and start the service, and `peers service restart` to restart it; for the system service, pass `--system` and run it with `sudo`. [Command line]({{< relref "/docs/reference/cli" >}}#peers-service-start-stop-and-restart) gives the exit codes.

## Check the service

Run `peers service status` in a terminal, adding `--system` for the system service. The output has an `Agents` table and a `Peers` table, or `Agents: none` and `Peers: none`. The service manager shows the Broker too: `systemctl --user status peers.service`, `systemctl status peers.service` or `launchctl print` with the label `io.github.wangzw.peers`.

Keep one Broker per account. Starting a second Broker under the same account fails with an error that an instance is already running, whichever address it would use. When another account's Broker already holds the port, an install fails; a service that was installed and only later finds the port taken enters a local-only degraded run.
