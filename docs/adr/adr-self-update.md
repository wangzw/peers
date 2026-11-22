# Install self-updates from this project's GitHub releases

## Context

The user requires peers to discover and install a new version of itself: the Broker periodically checks whether a new official release exists, and the user runs the update command after seeing a hint in the terminal to complete the installation (`REQ-update-001` to `REQ-update-006`). An update can also be started from a session: the MCP tool `update_broker` makes the Broker perform the upgrade through the local interface, and Claude Code's confirmation of that tool call is the human confirmation for this upgrade (`REQ-update-008`).

A system installation runs as a service, and day-to-day management is carried out by an ordinary account in the admin group without gaining root (`REQ-broker-config-043`). The updater downloads and runs a piece of new code, the most privileged external input in the system. The project publishes on GitHub, and the release artifacts are per-platform `tar.gz` installers and a `checksums.txt`, with no release signature at present.

The update must be decided by the user and must not silently replace a running process. Both the installer and the checksum come from outside, so the source, the naming, the checksum and the size limit are part of the decision. Versions are compared only between official releases; pre-releases and drafts do not take part.

## Options Considered

### Silent install

After the Broker finds a new version it downloads, verifies and replaces the binary directly, with no user involvement.

**Rejected**: it conflicts with the user confirmation that `REQ-update-004` requires; when replacing the running process fails, the user has no chance to keep the status quo, and when the download source is compromised there is no human judgment in the loop.

### Show only the version and let the user download and install by hand

The Broker and the terminal command show only the new version number, and installation is still done by hand following the README steps.

**Rejected**: `REQ-update-004` requires the command to download, verify and replace the binary automatically, and showing only the version does not satisfy it.

### Distribute and upgrade through a system package manager

Install and upgrade through Homebrew, apt or another package manager, with peers not downloading by itself.

**Not adopted**: it is compatible with this decision, but it requires maintaining packaging and release processes for each package manager, which the present scope does not need. It can be added without replacing this decision.

### Release signing and verification

Sign at release time with a key held by the project, and have the updater verify the signature before installing.

**Not adopted**: it requires a process for keeping and publishing a signing key, and the current release has no signature. It is compatible with this decision and can be added without replacing it; the observable condition for adding it is in Invalidation Conditions.

### Allow updates to be started only in the terminal

A session shows only an update hint, and the upgrade must be run in the terminal.

**Rejected**: the user requires driving the upgrade from a session; a tool call still goes through Claude Code's human confirmation, and the terminal is not the only place to obtain confirmation.

### Download from this project's official GitHub releases and verify SHA-256

Access only the hard-coded repository `wangzw/peers`, take the installer and `checksums.txt` from the latest official release's assets, and replace the binary after comparing with SHA-256.

**Adopted**; see Decision.

## Decision

The only source of self-updates is the official GitHub releases of this project's repository `wangzw/peers`, and access always uses HTTPS.

Version information comes from the GitHub REST API's `GET /repos/wangzw/peers/releases/latest` ([REF-github-rest-api](../references.md)), which returns only the latest official release and no pre-release or draft. Checking for a new version uses `releases/latest`; installing fetches the tag the user confirmed through `GET /repos/wangzw/peers/releases/tags/<tag>`, so that a change of latest after the user's confirmation does not install a different version. The installer and `checksums.txt` are taken from that release's assets and downloaded through `browser_download_url`. The installer's name must be exactly `peers_<version>_<GOOS>_<GOARCH>.tar.gz`.

Downloaded content is compared by SHA-256 against the matching line in `checksums.txt`, and a mismatch fails. `checksums.txt` and the installer each have a download size limit, the `checksums.txt` limit smaller and scanned by line; these limits are written as named constants. Extraction takes only the regular file named `peers` and rejects path traversal and symbolic links. The new binary is first run with `--version` in a temporary location, and the output must be exactly `peers <version>`, only after which the target is replaced. Replacement writes a temporary file in the target's directory, keeps the original binary's mode and owner, `fsync`s and `rename`s, and keeps no old version.

Releases have no signature yet, so the updater provides the same level of trust as a manual download by the user. Release signing and verification are outside this decision's scope.

### Delegated upgrade

A system or user Broker running as a service performs the upgrade itself: the Broker takes this project's latest official release version, downloads, verifies and replaces its own binary, replies to the caller and exits normally, and the service manager restarts it with the new binary. The terminal command asks the Broker to perform it on a system installation and no longer replaces the binary locally.

Starting an upgrade requires write permission on the Broker's configuration file, at the same level as changing configuration. A member of the admin group can therefore make a Broker running as root replace its own binary without gaining root, with the same level of trust as root running `peers update` itself; this path gives them no permission beyond editing the configuration file. The upgrade takes only this project's latest official release version: the caller cannot specify the source or version, or request a downgrade.

Releases are not signed yet, and this path's trust depends on GitHub's HTTPS and the checksums in `checksums.txt`.

When an upgrade is started from a session through the MCP tool `update_broker`, Claude Code's confirmation of that tool call is the last human confirmation before the upgrade. The tool is not allowed by default and is not added to the auto-approve list: once added, the model could make a Broker running as root install a new version without human confirmation.

## Consequences

Updating relies only on this project's own release channel and introduces no third-party distribution or package manager. No new key has to be kept, and the release process is unchanged. A wrongly named installer, a missing checksum, an oversized download or an archive with an abnormal path all fail before replacement, and the target binary is unchanged (`REQ-update-005`).

**Costs accepted:**

- When GitHub is unavailable or the account or release assets are compromised, SHA-256 cannot provide trust independent of that account; the level of trust is the same as a manual download by the user. The same applies when a third-party action or cache used by the release job is compromised, either of which can replace a binary installed as root; the project narrows this path by pinning actions to commits and not using a cache in the release job.
- Only one installer naming is recognized; changing the naming of release artifacts would prevent the old version's updater from installing a new version.
- The installer's download limit refuses an installer larger than that, even when it is otherwise legitimate.
- The replacement requires the target directory to be writable; a terminal update of a user installation therefore requires the account running it to have write permission on that directory (`REQ-update-006`).
- A member of the admin group can make a Broker running as root replace its own binary, with the same level of trust as root running the update itself. Before release signing exists, compromising this project's GitHub account or release pipeline is enough to replace a binary installed as root through this path.
- An agent in a session can start an upgrade; whether it runs depends on the user's confirmation of the tool call, and adding it to the auto-approve list would cancel that confirmation.
- After the update completes, the Broker restarts, and open sessions automatically recover their connection to it; the MCP server process in a session is still the old version, and the new version is only used after reconnecting the MCP server or reopening the session.
- Non-release builds, such as the `dev` and `e2e` versions, do not take part in checking and updating.

## Invalidation Conditions

When releases start carrying a signature, this decision changes to require verifying the signature before installing. When the project's releases move to a channel other than GitHub Releases, or no longer name artifacts `peers_<version>_<GOOS>_<GOARCH>.tar.gz`, the hard-coded repository, interface and naming premises no longer hold, and this decision has to be re-evaluated. When Claude Code no longer requires confirmation by default for tools carrying `destructiveHint`, a session upgrade loses its last human confirmation, and delegated upgrade has to be re-evaluated.
