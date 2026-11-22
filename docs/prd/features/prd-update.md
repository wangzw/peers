# Automatic updates

## Problem Statement

After a user installs peers, new versions are published only on GitHub and the user does not go looking for them. A running Broker may therefore stay on an old version for a long time and miss fixes and improvements. The user needs a way to discover and install a new version without leaving the terminal, and at the same time installing a new version amounts to replacing a running process with new code, which the user must decide explicitly and never happen silently.

## Scope

This document covers the Broker's periodic check for official releases, the update hint in the terminal status output and in a session, and the prompt, download, verification, replacement and service restart that the terminal update command and the session's upgrade tool perform for an official release. For a system installation, the Broker and the service manager perform the replacement and restart. After replacing the binary, the new binary rewrites the service definition in its own format and has the service manager reload it, so that the startup method the new version requires takes effect.

### Out of Scope

- Prereleases and drafts. The check and the update target official releases only.
- Signing of release artifacts and signature verification.
- Automatic updates for non-release builds, such as a binary running with a `dev` or `e2e` version.
- Automatic installation without confirmation. Installation must be confirmed by the user: answering the prompt in the terminal, explicitly passing a parameter that skips the prompt, or approving the upgrade tool's call in a session.
- Distribution and upgrades through a system package manager.

## Actors

| Actor | Authority |
|---|---|
| User | Views status in the terminal and runs the update command, deciding whether to install a new version; for a system installation, does so as an account that has write permission on the configuration file, without needing root. |
| agent | Receives the update hint in a tool result; may call the upgrade tool to start an installation, and the user confirms whether it runs. |
| Administrator | Runs the system Broker and grants write permission on the configuration file to the accounts that need to manage it; these accounts can update the system installation without root. |

## Functional Requirements

### REQ-update-001

A Broker MUST check, at the configured interval, whether this project has an official release on GitHub newer than the current version, excluding prereleases and drafts. A failed check MUST NOT affect other functions. The user MUST be able to turn the check off through configuration and to change the check interval.

- **Acceptance**: A Broker runs a version below the latest official release and learns of the new version within the check interval after startup; after the check is turned off and the Broker restarted, it sends no more check requests; with the check request made to fail, listing agents and sending messages work as usual.
- **Status**: Implemented
- **E2E test**: `TestREQUpdate001_PeriodicCheck`

### REQ-update-002

When a new version is available, a user viewing status in the terminal MUST see the current version, the available version and the method to update.

- **Acceptance**: When a new version is available, running the status command produces output that contains both version numbers and the update command; when there is no new version, the update hint does not appear in the output.
- **Status**: Implemented
- **E2E test**: `TestREQUpdate002_StatusShowsAvailableVersion`

### REQ-update-003

When a new version is available, in every session with the tools enabled the result of the first tool call MUST carry an update hint stating the available version and the method to update; in one session the same version is hinted only once.

- **Acceptance**: When a new version is available, calling a tool twice in a row in one session produces a hint only on the first result; opening another session and calling again produces the hint once more.
- **Status**: Implemented
- **E2E test**: `TestREQUpdate003_SessionHintOncePerSession`

### REQ-update-004

The user MUST be able to run `peers update [--yes]` in the terminal. The command shows the current and available versions and asks whether to update; after the user confirms, it automatically downloads this platform's release package, verifies it and then replaces the current binary; if the service is installed, it restarts the service. `--yes` skips the prompt; with no terminal and no `--yes`, the command reports an error and exits with status 1.

- **Acceptance**: On a host where the service is installed as a user service, running the update command and confirming leads, when the command finishes, to `peers --version` printing the new version and the Broker running the new version; if the user declines, neither the binary nor the service changes; run with `--yes`, the command completes the update without asking.
- **Status**: Implemented
- **E2E test**: `TestREQUpdate004_ConfirmedUpdateRestartsUserService`, `TestREQUpdate004_RefusedUpdateChangesNothing`, `TestREQUpdate004_YesSkipsPrompt`

### REQ-update-005

The update MUST fail, and the existing binary and the service MUST NOT change, when the downloaded release package's checksum does not match, extraction fails, or the new binary cannot run with the expected version.

- **Acceptance**: Induce a release package checksum mismatch, a corrupted release package and a new binary reporting a different version number; in all three cases `peers update` reports an error, `peers --version` still prints the original version and the service keeps running.
- **Status**: Implemented
- **E2E test**: `TestREQUpdate005_FailedUpdateKeepsBinaryAndService`

### REQ-update-006

For a user installation, which replaces the binary locally, when the account running the update has no write permission on the directory containing the current binary, the update MUST fail before downloading and state that it must be run as an identity that has write permission on the directory. This check does not apply to updating a system installation, which the Broker performs (`REQ-update-007`).

- **Acceptance**: Running the user update command as an account without write permission on the directory containing the current binary makes the command report insufficient permission before it sends any download request, stating the required identity.
- **Status**: Implemented
- **E2E test**: `TestREQUpdate006_UpdateRequiresWritePermission`

### REQ-update-007

An account that has write permission on the system Broker's configuration file MUST be able to run the update without gaining root: the Broker downloads, verifies and replaces the binary itself, and the service manager restarts it with the new binary; the update command reports success only after the Broker runs the new version. When the Broker does not run under a service manager, this kind of update MUST be refused, with the reason stated.

- **Acceptance**: On a host where the service is installed as a system service and the binary is in a root-owned directory, run the update command and confirm as an account that has write permission on the configuration file but is not root. When the command finishes, the Broker runs the new version, the binary's owner and permissions are unchanged, the audit records this account and the old and new versions, registered sessions do not reconnect the MCP service, and the next agent listing succeeds. Run as an account without write permission, it returns insufficient permission before any download. Run on a system Broker started by hand in a terminal, the command reports an error stating that a service manager is needed, and the binary is unchanged.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/self_update_test.go` `TestREQUpdate007_SystemUpdateByAdminGroup`, `TestREQUpdate007_SystemUpdateRefusedOutsideAdminGroup`, `TestREQUpdate007_CLIWithoutUserBrokerUsesSystemBroker`, `TestREQUpdate007_CLIWithUserBrokerUsesUserBroker`, `TestREQUpdate007_RootUpdateUsesSystemBroker`, `TestREQUpdate007_SudoWithUserBrokerUsesSystemBroker`, `TestREQUpdate007_TerminalSystemBrokerNeedsServiceManager`, `TestREQUpdate007_TerminalBrokerWithInheritedServiceEnvironmentIsRefused`

### REQ-update-008

An upgrade MUST be startable from a session: an agent can call a tool that takes no parameters to have the Broker the current session is connected to download, verify and install this project's latest official release, after which the service manager restarts that Broker with the new version. The tool returns as soon as it receives the Broker's response and does not wait for the restart to finish. When the Broker does not run under a service manager, the tool MUST fail and state that the update command can be run in the terminal.

- **Acceptance**: On user and system Brokers installed as services, a session calls this tool: it returns the version before the upgrade and the target version, the Broker then runs the new version, the session does not need to reconnect the MCP service, and the next agent listing succeeds; when it is already the latest version, it returns an explanation and the Broker does not restart; when an account without write permission on the configuration file calls it, it returns insufficient permission and gives the configuration file path, and sends no download request; when the Broker does not run under a service manager, it returns an error and gives the terminal alternative; when an upgrade is already in progress, it returns the corresponding error. Check the tool declaration: it does not carry the read-only annotation and is declared separately from the other tools, so Claude Code's permission settings can allow or deny it on its own.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/self_update_test.go` `TestREQUpdate008_UpdateBrokerUnderUserService`, `TestREQUpdate008_UpdateBrokerUnderSystemService`, `TestREQUpdate008_UpdateBrokerAlreadyLatest`, `TestREQUpdate008_UpdateBrokerRefusedOutsideAdminGroup`; returning `aborted` when an upgrade is already in progress is covered by unit tests (`TestApplyUpdateConcurrentIsAborted` in `internal/broker`, and the aborted subcase of `TestUpdateBroker` in `internal/mcpserver`)

### REQ-update-009

After replacing the binary and before restarting, a Broker MUST rewrite the service definition for its level in the new version's format using the new binary, and have the service manager load it.

- **Acceptance**: On a system installation, self-update from the current version to a next version with a different startup method. After the update, the definition the service manager actually loaded carries a setting only the new version writes, the environment settings the user added earlier such as a proxy remain in effect, and the Broker runs normally with the new version. Verify on Linux and macOS separately. On Linux the reload completes before the update command receives the response, and the update command reports failure when the service manager did not load the new definition. On macOS the reload is performed by a detached helper process after the response, and a failure does not affect the success response already returned but is reported only in the audit record and `post-update.log`.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/update_post_install_test.go` `TestREQUpdate009_UpdateRewritesServiceUnitInNewFormat`; `test/e2e/darwinservice/update_test.go` `TestREQUpdate007_LaunchDaemonReplacedByApplyUpdate`

### REQ-update-010

When the new binary does not provide a post-update step, the Broker MUST treat it as needing no post-update step and continue to complete the update and restart.

- **Acceptance**: Run an update with a new version that implements no post-update step; the update succeeds and the Broker runs the new version.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/update_post_install_test.go` `TestREQUpdate010_UpdateWithoutPostUpdateStep`; the unit test `TestInstallPostUpdateNotNeeded` in `internal/broker/update` covers the same contract

### REQ-update-011

When the post-update step returns another nonzero exit code or exceeds the time limit, the Broker MUST roll the binary back to the old version, not restart, and report an error to the caller stating the reason.

- **Acceptance**: Run the update in the two cases where the new version's post-update step fails or times out; both updates fail, the binary and the running Broker stay on the old version, and the command output contains the reason.
- **Status**: Implemented
- **E2E test**: `test/e2e/system/update_post_install_test.go` `TestREQUpdate011_FailedPostUpdateRollsBack`; the timeout is covered by `TestInstallPostUpdateTimeoutRollsBack` in `internal/broker/update`, and the nonzero exit by `TestInstallPostUpdateFailureRollsBack` in the same directory

## Assumptions and Dependencies

- Release artifacts are hosted on GitHub Releases, and each official release carries an installation package for this platform and architecture and a `checksums.txt`.
- The host where the user runs the update command can reach GitHub; when it cannot, both the check and the update fail, and other functions are unaffected.
- A version number is a semantic version, and the current version must be an official release for automatic update.
