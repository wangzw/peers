---
title: Install
linkTitle: Install
weight: 1
---

This guide covers the install level, installing a particular version, installing from the release archive, the install script's options, the installation output, install failures, manual installation and building from source. [Getting started]({{< relref "/docs/getting-started" >}}) gives the one-command installation; to keep an installation up to date with `peers update`, see [Update `peers`]({{< relref "update.md" >}}).

## Choose the install level

The one-command installation and the release archive's `sh ./install.sh` install either level. Run as root, the script installs the system service without asking. Any other account with a terminal and no option that names the level is asked `Install for every account on this host? [y/N]`: `y` installs the system service through `sudo`, and any other answer installs for that account. Without a terminal, a non-root account gets a user installation. When the host already has the system service, the question becomes `Upgrade the system-level installation? [Y/n]`: pressing Enter or answering y upgrades the system installation, and answering n leaves the host unchanged, exits with code 0 and suggests `--user`. `--system` and `--user` skip the question; giving both is a usage error, and `--user` as root is refused with a message to drop it. If `sudo` is not available after a `y` answer, the script fails with `a system-level install needs sudo, which was not found; install sudo, or pass --user for a user-level install`.

## Install with one command

To install a particular version, set it first and pass `--version`:

```sh
VERSION=0.5.0
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh -s -- --version "$VERSION"
```

`--version` must come before every other argument. It names a published version to install; when that version has no release archive to install, the script stops with `version <version> not found`. Every other argument is passed unchanged to the archive's `install.sh`, so the options in the next section work here as well.

The command needs macOS or Linux on amd64 or arm64; `curl` to fetch the script, while the script itself uses `curl` or `wget` to download the archive; `tar`; `sha256sum` or `shasum`; and access to github.com. The system installation needs `sudo`.

Unless `--version` names a release, the script resolves the latest stable release; it downloads this platform's archive and the same release's `checksums.txt` into a temporary directory, verifies the archive, extracts it and runs the `install.sh` it contains. Every download the script makes goes over HTTPS; with either `curl` or `wget`, it refuses a redirect to an http address. If a download fails or the checksum does not match, it stops and leaves the host unchanged, and the temporary directory is removed when the script exits or is interrupted. An interrupted download of the script itself runs nothing, because the work starts on its last line.

Piping a script into a shell means trusting it as much as downloading the archive by hand from GitHub Releases. Releases are not signed, and `checksums.txt` comes from the same release as the archive, so a matching checksum catches a damaged download but does not prove that the release is trustworthy. To inspect the script first, download it, read it and run it from the directory that holds it:

```sh
curl -fsSLO https://github.com/wangzw/peers/releases/latest/download/install.sh
sh install.sh
```

The script does the same work as the `install.sh` in the archive, described in the next section.

## Install from the release archive

`peers` is distributed as a `tar.gz` archive on [GitHub Releases](https://github.com/wangzw/peers/releases/latest). Download the archive for your platform and architecture together with `checksums.txt` from the same release:

```sh
VERSION=0.5.0
curl -LO https://github.com/wangzw/peers/releases/download/v${VERSION}/peers_${VERSION}_<os>_<arch>.tar.gz
curl -LO https://github.com/wangzw/peers/releases/download/v${VERSION}/checksums.txt
```

Replace `<os>_<arch>` with `darwin_amd64`, `darwin_arm64`, `linux_amd64` or `linux_arm64`. The checksum and extraction examples below use the arm64 archive; replace `arm64` with `amd64` on an amd64 host.

Check the archive against `checksums.txt` before using it. On Linux:

```sh
VERSION=0.5.0
grep peers_${VERSION}_linux_arm64.tar.gz checksums.txt | sha256sum -c -
```

On macOS, use `shasum` instead:

```sh
VERSION=0.5.0
grep peers_${VERSION}_darwin_arm64.tar.gz checksums.txt | shasum -a 256 -c -
```

Create a directory and extract the archive into it: the archive has no top-level directory, so without one its contents, including the `peers` binary, `LICENSE`, `README.md`, `install.sh`, `third_party/`, `THIRD_PARTY_LICENSES/` and `man/`, land directly in the current directory:

```sh
VERSION=0.5.0
mkdir peers-${VERSION}
tar -xzf peers_${VERSION}_darwin_arm64.tar.gz -C peers-${VERSION}
```

Replace `darwin` with `linux` on Linux.

Enter the extracted directory and run the script:

```sh
VERSION=0.5.0
cd peers-${VERSION}
sh ./install.sh
```

The script asks for the level as the online command does; [Choose the install level](#choose-the-install-level) covers the question.

The user installation puts the binary into `~/.local/bin`, installs and starts the user service and registers the MCP server for your account. The system installation puts the binary into `/usr/local/bin`, installs and starts the system service and registers the MCP server for the account that ran the install; it also creates the `peers` group and asks whether to add that account to it. [The admin group]({{< relref "management-permissions.md" >}}#the-admin-group) covers the group, the question and its effect.

The script accepts these options:

| Option | Effect |
|---|---|
| `--system` | Install the system service without the level question; the script re-runs itself through `sudo` when it is not root. |
| `--user` | Install for your account without the level question. Cannot be combined with `--system`. |
| `--prefix <dir>` | Install the binary into `<dir>` instead of the default, `~/.local/bin` for the user installation or `/usr/local/bin` for the system installation. |
| `--no-service` | Do not install the Broker service. [Start the Broker](#start-the-broker) yourself. |
| `--no-mcp` | Do not register the MCP server. |
| `--admin-group <name>` | Manage the system Broker through the group `<name>` instead of the default `peers` group; the name may start only with a lowercase letter or an underscore and may contain only lowercase letters, digits, underscores and hyphens. Valid only with `--system`. |
| `--join-admin-group` | Add the account that ran the install to the admin group without asking. Valid only with `--system`. |
| `--no-admin-group` | Do not ask whether to add the account that ran the install to the admin group. Valid only with `--system`. |

Giving both `--join-admin-group` and `--no-admin-group` is a usage error.

The script is safe to run again: it replaces the binary, restarts the service with it and does not register the MCP server twice. If the server is already registered with a different command, the script keeps the existing registration; to change it, run `claude mcp remove -s user peer` and run the script again. It never accesses the network; everything it installs comes from the extracted archive.

With `--no-mcp`, the script skips the registration, and you run the registration command in the manual section yourself. When it cannot find `claude`, or runs as root without `sudo` and has no account to register for, it prints the `claude mcp add` command to run.

If the configuration file for the level does not exist, `service install` creates it with every configuration key listed: `~/.peers/config.yaml` for the user service, and `/etc/peers/config.yaml` for the system service. [Configuration keys]({{< relref "/docs/reference/configuration" >}}) describes the file.

On macOS, the script clears the quarantine attribute from the binary itself, so a browser download needs no extra step. If the user installation directory is not on `PATH`, the script says so; add it to `PATH` to run `peers` by name. The user service on macOS needs a graphical login session; on a host without one, install the system service or use `--no-service`.

The system installation requires the binary's directory and every directory above it to be owned by root and not writable by group or others. On an Intel Mac with Homebrew, `/usr/local/bin` belongs to your account, so the installation is refused; create a root-owned directory with `sudo mkdir -p <dir>` and pass it with `--prefix` instead.

## Shell completions and man pages

The install script writes shell completions for `peers` from the installed binary. The system installation includes the bash, zsh and fish completions; the user installation includes the bash and fish ones and prints the command that enables the zsh completion by hand. The fish completion is installed only when `fish` is on the host, in either installation.

To generate a completion script yourself, run `peers completion <shell>` and write its output where your shell reads completions; `bash`, `zsh`, `fish` and `powershell` are supported.

The system installation also writes the man pages into `/usr/local/share/man/man1`, so `man peers` works. The pages are in the archive's `man/` directory.

## Installation output

A successful installation's summary begins with `peers <version> installed (<level>, starts at <login|boot>)`, and an upgrade prints `upgraded from <version>` before it. With `--no-service`, the summary begins with `peers <version> installed (no service)`. The summary reports the level, the binary path, the version, the configuration file path, the location of the run log, the audit log path and the commands to start, stop, restart and uninstall the Broker. The start, stop and restart commands use `sudo` for the system installation. [Files and paths]({{< relref "/docs/reference/files" >}}) lists the configuration, run log, audit log and runtime paths.

A failed installation names the cause: an address that is already in use is reported with the address, the configuration file and `broker.listen`; a service manager error gives the command and its output; missing permissions give the path and the missing permission; and a Broker that exits during startup gives the reason from its run log. Only when the service is running and the Broker has entered a degraded run does the install keep the service, give a fix command and exit with code 3; a service that a failed install wrote or enabled does not start at boot or login. [peers service install]({{< relref "/docs/reference/cli" >}}#peers-service-install) and [the port question in the FAQ]({{< relref "/docs/faq" >}}#what-happens-if-the-port-is-already-in-use) give the details.

To remove an installation, see [peers uninstall]({{< relref "/docs/reference/cli" >}}#peers-uninstall); to start, stop or restart its service, see [service start, stop and restart]({{< relref "/docs/reference/cli" >}}#peers-service-start-stop-and-restart).

## Install manually

Download, verify and extract the archive as described in the previous section, then install the binary by hand. The commands below run from the directory that contains the extracted `peers-${VERSION}` directory, so run `cd ..` first if you entered it in the previous section.

On macOS, a browser download carries the quarantine attribute, and because the binaries are not signed or notarized, macOS blocks the binary until the attribute is removed. Clear it from the extracted binary if a browser downloaded the archive:

```sh
VERSION=0.5.0
xattr -d com.apple.quarantine peers-${VERSION}/peers
```

A command-line download with `curl` or `wget` does not set the attribute.

If you install the system service, use the `sudo install` step in [Run the Broker as a service]({{< relref "run-as-service.md" >}}) instead of the `mv` below.

Move the binary into a directory on your `PATH`:

```sh
VERSION=0.5.0
mkdir -p ~/.local/bin
mv peers-${VERSION}/peers ~/.local/bin/
peers --version
```

The printed version matches the version you downloaded.

### Register the MCP server

Register the server once for your account:

```sh
claude mcp add --scope user peer -- peers mcp
```

The server name must be `peer`. Claude Code shows the tools as `mcp__peer__<tool>`, and you write permission rules with that name. If `peers` is not on your `PATH`, use its absolute path.

Check the registration:

```sh
claude mcp get peer
```

The output shows `Scope: User config`. A session started afterwards lists `peer` in `/mcp`.

### Start the Broker

[Installing with one command]({{< relref "/docs/getting-started" >}}) or with the archive script starts the service for you. If you installed with `--no-service` or by hand, start the Broker yourself.

Sessions connect to a Broker but never start one. If the host already has a system Broker you can use, skip this step; the session connects to it.

Otherwise, install the user service; the install starts it right away, and from then on it starts at login:

```sh
peers service install
```

The user installation must not run as root, and on macOS it needs a graphical login session; on a host without one, install the system service with `peers service install --system`, as root, or ask an administrator to run it; see [Run a system Broker]({{< relref "system-broker.md" >}}). Alternatively, run the Broker in a terminal outside Claude Code and leave it running:

```sh
peers service run
```

A Broker started inside a session exits with code 6. Starting the Broker does not require reconnecting the MCP server. [Run the Broker as a service]({{< relref "run-as-service.md" >}}) covers both service forms and uninstalling it.

## Build from source

To build from source you need Go 1.27. Clone the repository and build:

```sh
VERSION=0.5.0
git clone https://github.com/wangzw/peers
cd peers
go build -trimpath -ldflags "-X main.version=$VERSION" -o peers ./cmd/peers
```

Without `-ldflags`, `peers --version` prints `peers dev`, and `peers update` does not update such a binary. Installing with `go install …@version` is not supported, because `go.mod` uses a `replace` directive that points at a vendored dependency.

The build leaves `peers` in the current directory; follow the manual installation steps above to put it on `PATH`, register the MCP server and start the Broker.
