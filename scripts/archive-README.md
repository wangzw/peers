# peers

peers is a cross-host communication system for Claude Code sessions: a Broker runs on each host and the sessions on it join through the peers MCP tools, so sessions on paired hosts can find each other and exchange text messages.

## Install

### One-command installation

To install the latest release with one command:

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

The script detects the platform and architecture, downloads the matching archive, verifies it against the release's `checksums.txt`, and runs the install script from the archive. That script asks whether to install for every account on the host, or for your account only; pass `--system` or `--user` after `--` to skip the question. Run the system install from the account whose Claude Code sessions will use the Broker; the script reruns itself through sudo:

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh -s -- --system
```

`--version <version>` installs a published version instead of the latest and must come before any other option. A version that does not exist makes the command fail with `version <version> not found` and leaves the host unchanged.

Piping the script to a shell requires the same trust as downloading the archive and running it by hand: the releases are not signed. To inspect the script first, download it, read it, and run it from the directory that holds it.

### Install from the archive

Extract the archive and run the install script from the extracted directory. The script installs the binary, installs the Broker as a service and starts it, registers the MCP server with Claude Code for your account, and installs shell completions; a system install also installs the man pages. If a completion or man page cannot be written, the script prints a warning and continues. The fish completion is installed only when fish is installed.

Run it without a level option and it asks whether to install for every account on the host or for your account only. Pass `--system` or `--user` to skip the question. Run the system install from the account whose Claude Code sessions will use the Broker; the script reruns itself through sudo:

```sh
sh ./install.sh
```

`--prefix <dir>` installs the binary in another directory, `--no-service` skips the service, and `--no-mcp` skips the MCP registration. Running the script again is safe: it replaces the binary, restarts the service, and does not register the MCP server twice.

## Get started

Open a new Claude Code session, because a session that was already open does not have the new MCP server. Start two sessions in the same project directory; both join the same default topic. Ask the model to list the agents with `list_agents` and send a message with `send_message`.

## Pair two hosts

At least one of the two hosts must be able to reach port 17443 on the other. On the host that can be reached, create a one-time pairing key and note the printed key and addresses. On the other host, connect with that key. Pairing is symmetric once it succeeds.

With a user Broker:

```sh
peers key create
peers broker connect <address> --key <key>
peers broker list
```

With a system Broker, run the same commands with `sudo`:

```sh
sudo peers key create
sudo peers broker connect <address> --key <key>
sudo peers broker list
```

An administrator account that runs no user Broker can run them without `sudo`; the command then selects the system Broker. An account that also runs a user Broker uses `sudo` to reach the system Broker.

Give the key and address to the other side over a separate channel. The address takes the form `address:port` or `hostname(address:port)`.

## Administer a system Broker without root

A system install creates the `peers` group and makes the group the owner of the configuration file. The install script asks whether to add the installing account to the group, or pass `--join-admin-group` to add it without asking; log in again or run `newgrp peers` afterwards. Members of the group can create pairing keys, change the configuration and pair with another Broker without `sudo` when their account runs no user Broker; an account that also runs a user Broker uses `sudo` so the command reaches the system Broker. Group members can do only what editing the configuration file by hand allows. Installing and uninstalling the service still need root; updating it does not, because the Broker installs the release itself.

For an existing installation, make the group the owner of the configuration file with `chgrp peers /etc/peers/config.yaml` and `chmod 0660 /etc/peers/config.yaml` as root. Do not change the file's group when `broker.listen` uses a port below 1024, because such a Broker refuses to start while another account can write the file.

## Update

```sh
peers update
```

The command asks for confirmation, then downloads, checks and installs the latest official release. When it replaces a user binary locally, it leaves the service alone, because the service may have been stopped on purpose, and prints that the new binary takes effect the next time the Broker starts. It checks write permission on the binary's directory first and suggests `sudo` when that permission is missing.

For a system installation, run `peers update` as an account that administers the Broker, without `sudo`, when that account runs no user Broker; the command selects the system Broker. An account that also runs a user Broker uses `sudo peers update`, so root selects the system Broker. The Broker replaces its own binary and the service manager restarts it. The account needs write permission on the Broker's configuration file.

A session can also start the upgrade with the `update_broker` tool. Claude Code asks for confirmation before the call runs, and that confirmation is the last manual check before the upgrade.

When the update runs through the Broker, the Broker restarts and open sessions reconnect to it automatically. The MCP server inside an already open session keeps running the old binary until you reconnect it with `/mcp` or restart the session. A local replacement does not start or restart anything; the new binary takes effect the next time the Broker starts.

## Uninstall

Remove the service, the binary, the runtime directory, this account's MCP registration, and the shell completions and man pages the install script wrote:

```sh
peers uninstall
```

Add `--purge` to remove the configuration, identity, audit log and service logs as well; without it, uninstall asks before deleting them. `--yes` skips every question and keeps the data.

A system installation is removed as root:

```sh
sudo peers uninstall --yes
```

When another account runs it at a terminal, it is asked whether to run the uninstall through sudo; answering yes reruns the command. With `--yes` it reruns through sudo directly. Without a terminal and without `--yes` it prints that sudo command and exits with status 1, changing nothing.

## Documentation

- Website: <https://peers.zhanwei.wang/docs/>
- Full README and source: <https://github.com/wangzw/peers>
