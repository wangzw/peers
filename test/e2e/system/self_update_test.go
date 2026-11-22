//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// selfUpdateArchiveDir is where the inner test assembles a release-shaped
	// archive around the old release binary.
	selfUpdateArchiveDir = "/var/lib/peers-self-update-archive"
	// selfUpdateBinary is the system binary the install script writes.
	selfUpdateBinary = "/usr/local/bin/peers"
	// selfUpdateConfigPath is the system Broker's configuration file.
	selfUpdateConfigPath = "/etc/peers/config.yaml"
	// selfUpdateSystemSocket is the system Broker's local socket.
	selfUpdateSystemSocket = "/run/peers/broker.sock"
	// selfUpdateAdminGroup is the group the admin-group unit will grant. Until
	// that unit is merged, the test performs the same chgrp and chmod itself.
	selfUpdateAdminGroup = "peers-admins"
	// selfUpdateUserListen is the user Broker's address when the system
	// Broker is also running. The configuration template would otherwise give
	// both the same port and the second Broker would refuse to start.
	selfUpdateUserListen = "127.0.0.1:17463"

	// selfUpdateRestartBound covers systemd's RestartSec=5, the Broker's startup
	// and the readiness wait the update path performs before it reports
	// restarting. The same bound covers the MCP server's reconnect window after
	// the Broker goes away.
	selfUpdateRestartBound = 30 * time.Second
	// selfUpdateAuditBound is the fallback for the wait for a local audit
	// event; the Broker writes it right after applying the update.
	selfUpdateAuditBound = harness.EventTimeout
	// selfUpdateStatusRPCTimeout bounds one UpdateStatus call on a local
	// socket. The Broker answers in milliseconds; the bound covers a loaded
	// container without hiding a hung call.
	selfUpdateStatusRPCTimeout = 5 * time.Second
	// selfUpdateListToolsTimeout bounds one MCP tools/list call.
	selfUpdateListToolsTimeout = 10 * time.Second
	// selfUpdateNoRestartWindow is how long the already-latest case watches a
	// Broker that must not restart. The Broker schedules a restart 500 ms after
	// answering ApplyUpdate, so the window covers that timer with slack.
	selfUpdateNoRestartWindow = 2 * time.Second
	// selfUpdateInheritedInvocationID is a value a shell started by systemd
	// carries into every child process.
	selfUpdateInheritedInvocationID = "3f0a6f0b1c2d4e5f8a9b0c1d2e3f4a5b"
)

// TestREQUpdate007_SystemUpdateByAdminGroup covers the first acceptance of
// REQ-update-007: an account that may write the system Broker's configuration
// file runs "peers update --yes" without root. The Broker downloads and
// replaces the binary, the service manager restarts it on the new version, the
// binary keeps its owner and mode, the audit log records update_applied, and a
// session registered before the restart lists Agents again without being
// reconnected.
func TestREQUpdate007_SystemUpdateByAdminGroup(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)

	beforeUID, beforeGID, beforeMode := fileOwnerAndMode(t, selfUpdateBinary)
	beforeVersion := updateBinaryVersion(t, selfUpdateBinary)
	if beforeVersion != "peers "+updateOldVersion {
		t.Fatalf("the install script installed %q, want %q", beforeVersion, "peers "+updateOldVersion)
	}

	// Register a session on the old Broker. It must keep working after the
	// restart without being closed and reopened.
	session := syscontainer.NewSession(t, "peer", "selfupdate-registered")
	selfUpdateWaitSession(t, session)

	res := syscontainer.RunAs(t, installAccount, selfUpdateAPIEnv(fake), "update", "--yes")
	combined := res.Stdout + res.Stderr
	if res.ExitCode != 0 {
		t.Fatalf("peers update --yes as %s exited %d\nstdout:\n%s\nstderr:\n%s",
			installAccount, res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(combined, updateNewVersion) {
		t.Fatalf("the update output does not name version %s:\n%s", updateNewVersion, combined)
	}

	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, updateNewVersion)

	afterUID, afterGID, afterMode := fileOwnerAndMode(t, selfUpdateBinary)
	if afterUID != beforeUID || afterGID != beforeGID || afterMode != beforeMode {
		t.Errorf("%s owner:mode changed from %d:%d %#o to %d:%d %#o across the update",
			selfUpdateBinary, beforeUID, beforeGID, beforeMode, afterUID, afterGID, afterMode)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateNewVersion {
		t.Fatalf("the installed binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
	selfUpdateWaitAuditEvent(t, installAccount)
	selfUpdateWaitSession(t, session)
}

// TestREQUpdate007_SystemUpdateRefusedOutsideAdminGroup covers the denial half
// of REQ-update-007: an account that may not write the configuration file gets
// permission_denied, nothing is downloaded, and the binary is left alone.
func TestREQUpdate007_SystemUpdateRefusedOutsideAdminGroup(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)
	downloadsBefore := fake.Downloads()

	res := syscontainer.RunAs(t, "peer", selfUpdateAPIEnv(fake), "update", "--yes")
	combined := res.Stdout + res.Stderr
	if res.ExitCode == 0 {
		t.Fatalf("peers update --yes as peer outside the admin group succeeded\nstdout:\n%s\nstderr:\n%s",
			res.Stdout, res.Stderr)
	}
	if !strings.Contains(combined, "permission_denied") {
		t.Fatalf("the refused update does not report permission_denied:\n%s", combined)
	}
	if got := fake.Downloads(); got != downloadsBefore {
		t.Fatalf("the refused update downloaded %d assets, want none", got-downloadsBefore)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateOldVersion {
		t.Fatalf("the binary reports %q after the refusal, want %q", got, "peers "+updateOldVersion)
	}
}

// TestREQUpdate007_CLIWithoutUserBrokerUsesSystemBroker covers the routing rule
// for an ordinary account that runs no user Broker: the selection order
// reaches the system Broker, so "peers update" delegates to it.
func TestREQUpdate007_CLIWithoutUserBrokerUsesSystemBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)

	if _, err := os.Stat(syscontainer.UserSocketPath(t, installAccount)); err == nil {
		t.Fatalf("a user Broker socket exists, so the case would not test the fallback to the system Broker")
	}

	res := syscontainer.RunAs(t, installAccount, selfUpdateAPIEnv(fake), "update", "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("peers update --yes as %s without a user Broker exited %d\nstdout:\n%s\nstderr:\n%s",
			installAccount, res.ExitCode, res.Stdout, res.Stderr)
	}
	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, updateNewVersion)
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateNewVersion {
		t.Fatalf("the system binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
	selfUpdateWaitAuditEvent(t, installAccount)
}

// TestREQUpdate007_CLIWithUserBrokerUsesUserBroker covers the other half of the
// selection order: the same admin account also runs a user Broker, so
// "peers update" must reach that Broker and leave the system Broker
// and its binary untouched.
func TestREQUpdate007_CLIWithUserBrokerUsesUserBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)
	userTarget, userEnv := selfUpdateSetupUserServiceFor(t, installAccount)
	selfUpdateSetUserUnitEnv(t, installAccount, userEnv, fake.URL())
	selfUpdateWaitUserBrokerVersion(t, installAccount, updateOldVersion)

	beforePID := selfUpdateUserServiceMainPID(t, installAccount, userEnv)
	userBeforeHash := updateFileHash(t, userTarget)
	systemBeforeHash := updateFileHash(t, selfUpdateBinary)
	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, updateOldVersion)

	// The command is the system-installed binary, so only the Broker selection
	// can send the update to the user install.
	res := syscontainer.RunAs(t, installAccount, selfUpdateAPIEnv(fake), "update", "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("peers update --yes as %s with a user Broker exited %d\nstdout:\n%s\nstderr:\n%s",
			installAccount, res.ExitCode, res.Stdout, res.Stderr)
	}

	selfUpdateWaitUserBrokerVersion(t, installAccount, updateNewVersion)
	if after := updateFileHash(t, userTarget); after == userBeforeHash {
		t.Errorf("the user binary at %s was not replaced", userTarget)
	}
	if got := updateBinaryVersion(t, userTarget); got != "peers "+updateNewVersion {
		t.Fatalf("the user binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
	if afterPID := selfUpdateUserServiceMainPID(t, installAccount, userEnv); afterPID == beforePID {
		t.Errorf("the user Broker kept MainPID %d, want it restarted on the new version", beforePID)
	}

	if after := updateFileHash(t, selfUpdateBinary); after != systemBeforeHash {
		t.Errorf("the system binary at %s changed during a user update", selfUpdateBinary)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateOldVersion {
		t.Fatalf("the system binary reports %q after a user update, want %q",
			got, "peers "+updateOldVersion)
	}
	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, updateOldVersion)
}

// TestREQUpdate007_RootUpdateUsesSystemBroker covers the root route: a root
// caller has no separate user Broker, so "peers update" reaches the
// system Broker.
func TestREQUpdate007_RootUpdateUsesSystemBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)

	beforeUID, beforeGID, beforeMode := fileOwnerAndMode(t, selfUpdateBinary)

	res := syscontainer.RunAs(t, "root", selfUpdateAPIEnv(fake), "update", "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("peers update --yes as root exited %d\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}

	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, updateNewVersion)
	afterUID, afterGID, afterMode := fileOwnerAndMode(t, selfUpdateBinary)
	if afterUID != beforeUID || afterGID != beforeGID || afterMode != beforeMode {
		t.Errorf("%s owner:mode changed from %d:%d %#o to %d:%d %#o across the update",
			selfUpdateBinary, beforeUID, beforeGID, beforeMode, afterUID, afterGID, afterMode)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateNewVersion {
		t.Fatalf("the system binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
	selfUpdateWaitAuditEvent(t, "root")
}

// TestREQUpdate007_SudoWithUserBrokerUsesSystemBroker covers the routing rule
// that keeps an administrator's user Broker out of the way: an admin-group
// member who also runs a user Broker executes "sudo peers update",
// and the system Broker is the one upgraded.
func TestREQUpdate007_SudoWithUserBrokerUsesSystemBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)
	userTarget, userEnv := selfUpdateSetupUserServiceFor(t, installAccount)
	selfUpdateSetUserUnitEnv(t, installAccount, userEnv, fake.URL())
	selfUpdateWaitUserBrokerVersion(t, installAccount, updateOldVersion)

	beforePID := selfUpdateUserServiceMainPID(t, installAccount, userEnv)
	userBeforeHash := updateFileHash(t, userTarget)
	beforeUID, beforeGID, beforeMode := fileOwnerAndMode(t, selfUpdateBinary)

	res := selfUpdateRunSudo(t, installAccount, fake.URL(), "update", "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("sudo peers update --yes as %s exited %d\nstdout:\n%s\nstderr:\n%s",
			installAccount, res.ExitCode, res.Stdout, res.Stderr)
	}

	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, updateNewVersion)

	// The user Broker and the binary it runs must be exactly as they were.
	if afterPID := selfUpdateUserServiceMainPID(t, installAccount, userEnv); afterPID != beforePID {
		t.Errorf("the user Broker MainPID changed from %d to %d; sudo updated the wrong Broker",
			beforePID, afterPID)
	}
	if after := updateFileHash(t, userTarget); after != userBeforeHash {
		t.Errorf("the user binary at %s changed during sudo update", userTarget)
	}
	if got := updateBinaryVersion(t, userTarget); got != "peers "+updateOldVersion {
		t.Fatalf("the user binary reports %q, want %q", got, "peers "+updateOldVersion)
	}
	selfUpdateWaitUserBrokerVersion(t, installAccount, updateOldVersion)

	afterUID, afterGID, afterMode := fileOwnerAndMode(t, selfUpdateBinary)
	if afterUID != beforeUID || afterGID != beforeGID || afterMode != beforeMode {
		t.Errorf("%s owner:mode changed from %d:%d %#o to %d:%d %#o across the update",
			selfUpdateBinary, beforeUID, beforeGID, beforeMode, afterUID, afterGID, afterMode)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateNewVersion {
		t.Fatalf("the system binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
	selfUpdateWaitAuditEvent(t, "root")
}

// TestREQUpdate007_TerminalSystemBrokerNeedsServiceManager covers the third
// acceptance of REQ-update-007: a system Broker started by hand is not under a
// service manager, so the update is refused with an explanation and the binary
// is unchanged.
func TestREQUpdate007_TerminalSystemBrokerNeedsServiceManager(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, false)
		syscontainer.Run(t, syscontainer.Plain)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))

	// The manually started Broker runs this binary directly; no systemd unit
	// sets INVOCATION_ID.
	syscontainer.CopyReleaseArtifact(t, updateOldArtifact, selfUpdateBinary, 0o755)
	selfUpdateRunRoot(t, "chown", "0:0", selfUpdateBinary)
	selfUpdateRunRoot(t, "chmod", "0755", selfUpdateBinary)
	beforeHash := updateFileHash(t, selfUpdateBinary)

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")
	downloadsBefore := fake.Downloads()

	res := syscontainer.RunAs(t, "root", selfUpdateAPIEnv(fake), "update", "--yes")
	combined := res.Stdout + res.Stderr
	if res.ExitCode == 0 {
		t.Fatalf("the update succeeded on a manually started system Broker\nstdout:\n%s\nstderr:\n%s",
			res.Stdout, res.Stderr)
	}
	lower := strings.ToLower(combined)
	if !strings.Contains(lower, "service manager") && !strings.Contains(lower, "service install") {
		t.Fatalf("the refusal does not say that a service manager is required:\n%s", combined)
	}
	if after := updateFileHash(t, selfUpdateBinary); after != beforeHash {
		t.Fatalf("the binary at %s changed after the refused update", selfUpdateBinary)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateOldVersion {
		t.Fatalf("the binary reports %q after the refusal, want %q", got, "peers "+updateOldVersion)
	}
	if got := fake.Downloads(); got != downloadsBefore {
		t.Fatalf("the refused update downloaded %d assets, want none", got-downloadsBefore)
	}
}

// TestREQUpdate007_TerminalBrokerWithInheritedServiceEnvironmentIsRefused
// covers the service-manager test without the environment shortcut: the Broker
// is started by hand, but its shell carries INVOCATION_ID the way a terminal
// started by systemd does. The Broker is still not the systemd unit's main
// process, so both the CLI and update_broker refuse before any download and
// leave the binary alone. update_broker names the terminal replacement.
func TestREQUpdate007_TerminalBrokerWithInheritedServiceEnvironmentIsRefused(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, false)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))

	syscontainer.CopyReleaseArtifact(t, updateOldArtifact, selfUpdateBinary, 0o755)
	selfUpdateRunRoot(t, "chown", "0:0", selfUpdateBinary)
	selfUpdateRunRoot(t, "chmod", "0755", selfUpdateBinary)
	beforeHash := updateFileHash(t, selfUpdateBinary)

	syscontainer.StartSystemBrokerEnv(t, "broker:\n  alias: sys\n", []string{
		"INVOCATION_ID=" + selfUpdateInheritedInvocationID,
		"PEERS_TEST_UPDATE_API=" + fake.URL(),
	})
	downloadsBefore := fake.Downloads()

	res := syscontainer.RunAs(t, "root", selfUpdateAPIEnv(fake), "update", "--yes")
	combined := res.Stdout + res.Stderr
	if res.ExitCode == 0 {
		t.Fatalf("the update succeeded on a manually started system Broker whose shell carries INVOCATION_ID\nstdout:\n%s\nstderr:\n%s",
			res.Stdout, res.Stderr)
	}
	lower := strings.ToLower(combined)
	if !strings.Contains(lower, "service manager") && !strings.Contains(lower, "service install") {
		t.Fatalf("the refusal does not say that a service manager is required:\n%s", combined)
	}
	if after := updateFileHash(t, selfUpdateBinary); after != beforeHash {
		t.Fatalf("the binary at %s changed after the refused update", selfUpdateBinary)
	}

	// The same Broker refuses the tool and names the terminal command.
	session := syscontainer.NewSession(t, "root", "terminal-env-update")
	selfUpdateRequireUpdateTool(t, session)
	selfUpdateWaitConnected(t, session)
	text, isError := session.Call(t, "update_broker", map[string]any{})
	if !isError {
		t.Fatalf("update_broker succeeded on a manually started Broker: %s", text)
	}
	if !strings.Contains(text, "failed_precondition") {
		t.Fatalf("update_broker = %q, want failed_precondition", text)
	}
	if !strings.Contains(text, "Run `peers update` in a terminal instead.") {
		t.Fatalf("update_broker = %q, want the terminal alternative", text)
	}

	if got := fake.Downloads(); got != downloadsBefore {
		t.Fatalf("the refused updates downloaded %d assets, want none", got-downloadsBefore)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateOldVersion {
		t.Fatalf("the binary reports %q after the refusal, want %q", got, "peers "+updateOldVersion)
	}
}

// TestREQUpdate008_UpdateBrokerAlreadyLatest covers an up-to-date Broker: the
// tool answers with an empty target version and the already-current sentence,
// nothing is downloaded, the Broker keeps its pid, and the session lists Agents
// without reconnecting.
func TestREQUpdate008_UpdateBrokerAlreadyLatest(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, false)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	svc := updateSetupUserService(t)
	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateOldVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateSetUserUnitEnv(t, "peer", svc.userEnv, fake.URL())
	selfUpdateWaitUserBrokerVersion(t, "peer", updateOldVersion)

	beforePID := selfUpdateUserServiceMainPID(t, "peer", svc.userEnv)
	beforeHash := updateFileHash(t, svc.target)
	downloadsBefore := fake.Downloads()

	session := syscontainer.NewSession(t, "peer", "mcp-update-current")
	selfUpdateRequireUpdateTool(t, session)
	selfUpdateWaitConnected(t, session)

	text, isError := session.Call(t, "update_broker", map[string]any{})
	if isError {
		t.Fatalf("update_broker on an up-to-date Broker reported an error: %s", text)
	}
	var out struct {
		FromVersion string `json:"from_version"`
		ToVersion   string `json:"to_version"`
		Restarting  bool   `json:"restarting"`
		Result      string `json:"result"`
	}
	if err := json.Unmarshal([]byte(selfUpdateJSONLine(t, text)), &out); err != nil {
		t.Fatalf("decoding the update_broker result %q: %v", text, err)
	}
	if out.FromVersion != updateOldVersion {
		t.Errorf("from_version = %q, want %q", out.FromVersion, updateOldVersion)
	}
	if out.ToVersion != "" {
		t.Errorf("to_version = %q, want empty for an up-to-date Broker", out.ToVersion)
	}
	if out.Restarting {
		t.Error("restarting = true for an up-to-date Broker")
	}
	if want := "The Broker is already up to date (" + updateOldVersion + ")."; out.Result != want {
		t.Errorf("result = %q, want %q", out.Result, want)
	}

	// The session keeps working without being reopened.
	selfUpdateWaitSession(t, session)

	// Nothing may schedule a restart after the answer.
	time.Sleep(selfUpdateNoRestartWindow)
	if after := selfUpdateUserServiceMainPID(t, "peer", svc.userEnv); after != beforePID {
		t.Errorf("the Broker MainPID changed from %d to %d without an update", beforePID, after)
	}
	if after := updateFileHash(t, svc.target); after != beforeHash {
		t.Errorf("the binary at %s changed without an update", svc.target)
	}
	if got := fake.Downloads(); got != downloadsBefore {
		t.Fatalf("the up-to-date call downloaded %d assets, want none", got-downloadsBefore)
	}
	if got := updateBinaryVersion(t, svc.target); got != "peers "+updateOldVersion {
		t.Fatalf("the binary reports %q after the up-to-date call, want %q", got, "peers "+updateOldVersion)
	}
}

// TestREQUpdate008_UpdateBrokerUnderUserService covers the user half of
// REQ-update-008: a real "peers mcp" session calls update_broker, the user
// service's Broker replaces the binary and is restarted by the user manager,
// the tool result names both versions and restarting, and the session reaches
// the restarted Broker again without being reopened. The result also says the
// session must be reopened to run the new MCP binary.
func TestREQUpdate008_UpdateBrokerUnderUserService(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, false)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	svc := updateSetupUserService(t)
	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateSetUserUnitEnv(t, "peer", svc.userEnv, fake.URL())
	selfUpdateWaitUserBrokerVersion(t, "peer", updateOldVersion)

	session := syscontainer.NewSession(t, "peer", "mcp-update-user")
	selfUpdateRequireUpdateTool(t, session)
	selfUpdateWaitConnected(t, session)
	text := selfUpdateCallBrokerUpdate(t, session, updateOldVersion, updateNewVersion)

	selfUpdateWaitUserBrokerVersion(t, "peer", updateNewVersion)
	selfUpdateWaitSession(t, session)
	selfUpdateRequireReopenHint(t, text)

	if got := updateBinaryVersion(t, svc.target); got != "peers "+updateNewVersion {
		t.Fatalf("the user service binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
}

// TestREQUpdate008_UpdateBrokerUnderSystemService covers the system half
// of REQ-update-008: a real "peers mcp" session of an admin-group account
// calls update_broker, the system Broker replaces the binary and systemd
// restarts it, the binary keeps its owner and mode, the audit log records the
// update, and the session reaches the new Broker without being reopened.
func TestREQUpdate008_UpdateBrokerUnderSystemService(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)

	beforeUID, beforeGID, beforeMode := fileOwnerAndMode(t, selfUpdateBinary)

	session := syscontainer.NewSession(t, installAccount, "mcp-update-system")
	selfUpdateRequireUpdateTool(t, session)
	selfUpdateWaitConnected(t, session)
	text := selfUpdateCallBrokerUpdate(t, session, updateOldVersion, updateNewVersion)

	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, updateNewVersion)

	afterUID, afterGID, afterMode := fileOwnerAndMode(t, selfUpdateBinary)
	if afterUID != beforeUID || afterGID != beforeGID || afterMode != beforeMode {
		t.Errorf("%s owner:mode changed from %d:%d %#o to %d:%d %#o across the update",
			selfUpdateBinary, beforeUID, beforeGID, beforeMode, afterUID, afterGID, afterMode)
	}
	selfUpdateWaitAuditEvent(t, installAccount)
	selfUpdateWaitSession(t, session)
	selfUpdateRequireReopenHint(t, text)
}

// TestREQUpdate008_UpdateBrokerRefusedOutsideAdminGroup covers the denial half
// of REQ-update-008: a session of an account outside the admin group gets
// permission_denied from update_broker and the fake API serves no download.
func TestREQUpdate008_UpdateBrokerRefusedOutsideAdminGroup(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, true)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	selfUpdateInstallSystemService(t, fake)
	downloadsBefore := fake.Downloads()

	session := syscontainer.NewSession(t, "peer", "mcp-update-denied")
	selfUpdateRequireUpdateTool(t, session)
	selfUpdateWaitConnected(t, session)

	text, isError := session.Call(t, "update_broker", map[string]any{})
	if !isError {
		t.Fatalf("update_broker as peer outside the admin group succeeded: %s", text)
	}
	if !strings.Contains(text, "(code: permission_denied)") {
		t.Fatalf("update_broker as peer outside the admin group = %q, want permission_denied", text)
	}
	if got := fake.Downloads(); got != downloadsBefore {
		t.Fatalf("the refused update downloaded %d assets, want none", got-downloadsBefore)
	}
	if got := updateBinaryVersion(t, selfUpdateBinary); got != "peers "+updateOldVersion {
		t.Fatalf("the binary reports %q after the refusal, want %q", got, "peers "+updateOldVersion)
	}
}

// selfUpdateStageHostAssets builds the two release versions and, when
// withInstallScript is set, stages the install script the system
// scenarios run. It runs on the host.
func selfUpdateStageHostAssets(t *testing.T, withInstallScript bool) {
	t.Helper()

	updateStageAssets(t)
	if withInstallScript {
		stageInstallArchive(t)
	}
}

// selfUpdateAPIEnv returns the environment the old release reads the fake
// GitHub API base from.
func selfUpdateAPIEnv(fake *harness.FakeGitHub) []string {
	return []string{"PEERS_TEST_UPDATE_API=" + fake.URL()}
}

// selfUpdatePrepareArchive assembles a release-shaped directory around the
// staged binary named by artifact, with the install script and the two archive
// files. It returns the directory.
func selfUpdatePrepareArchive(t *testing.T, artifact string) string {
	t.Helper()

	if err := os.RemoveAll(selfUpdateArchiveDir); err != nil {
		t.Fatalf("clearing the archive directory %s: %v", selfUpdateArchiveDir, err)
	}
	if err := os.MkdirAll(selfUpdateArchiveDir, 0o755); err != nil {
		t.Fatalf("creating the archive directory %s: %v", selfUpdateArchiveDir, err)
	}

	syscontainer.CopyReleaseArtifact(t, artifact, filepath.Join(selfUpdateArchiveDir, "peers"), 0o755)
	staged := syscontainer.ArtifactPath(stagedArchiveRel(t))
	for _, file := range []struct {
		name string
		mode os.FileMode
	}{
		{"install.sh", 0o755},
		{"LICENSE", 0o644},
		{"README.md", 0o644},
	} {
		if err := copyInstallFile(filepath.Join(staged, file.name), filepath.Join(selfUpdateArchiveDir, file.name), file.mode); err != nil {
			t.Fatalf("building the release archive: %v", err)
		}
	}
	return selfUpdateArchiveDir
}

// selfUpdateInstallSystemService installs the old release through the install
// script as the system service, grants the extrausers account write access to
// the configuration file, and points the service at the fake API. It also
// verifies that the service is running the old release.
//
// When the install script knows --join-admin-group, the test lets the script do
// the group grant. Until the admin-group unit is merged into main, the script
// rejects the flag, so the test performs the same chgrp and chmod itself.
func selfUpdateInstallSystemService(t *testing.T, fake *harness.FakeGitHub) {
	t.Helper()
	selfUpdateInstallSystemServiceFrom(t, fake, updateOldArtifact, updateOldVersion)
}

// selfUpdateInstallSystemServiceFrom is selfUpdateInstallSystemService for a
// caller that installs another built binary and expects it to run as
// oldVersion.
func selfUpdateInstallSystemServiceFrom(t *testing.T, fake *harness.FakeGitHub, artifact, oldVersion string) {
	t.Helper()

	configureInstallSudo(t)
	archive := selfUpdatePrepareArchive(t, artifact)

	args := []string{"-n", "sh", "./install.sh", "--system", "--no-mcp"}
	joinAdminGroup := selfUpdateInstallScriptJoinsAdminGroup(t)
	if joinAdminGroup {
		args = append(args, "--join-admin-group")
	}
	res := runInstallScriptAs(t, installAccount, installUserEnv(), archive, "sudo", args...)
	if res.ExitCode != 0 {
		t.Fatalf("sudo sh ./install.sh --system as %s: exit %d\nstdout: %s\nstderr: %s",
			installAccount, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	if !joinAdminGroup {
		selfUpdateRunRoot(t, "usermod", "-aG", selfUpdateAdminGroup, installAccount)
		selfUpdateRunRoot(t, "chgrp", selfUpdateAdminGroup, selfUpdateConfigPath)
		selfUpdateRunRoot(t, "chmod", "0640", selfUpdateConfigPath)
	}

	selfUpdateSetSystemUnitEnv(t, fake.URL())
	harness.Eventually(t, selfUpdateRestartBound, "the system service to answer after the restart", func() (bool, string) {
		if state := selfUpdateUnitState(t, "root", nil, "peers.service"); state != "active" {
			return false, "peers.service is " + state
		}
		if _, err := os.Stat(selfUpdateSystemSocket); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, oldVersion)
}

// selfUpdateInstallScriptJoinsAdminGroup reports whether the staged install
// script accepts --join-admin-group.
func selfUpdateInstallScriptJoinsAdminGroup(t *testing.T) bool {
	t.Helper()

	path := filepath.Join(syscontainer.ArtifactPath(stagedArchiveRel(t)), "install.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the staged install script %s: %v", path, err)
	}
	return strings.Contains(string(data), "--join-admin-group")
}

// selfUpdateSetSystemUnitEnv writes a systemd drop-in that hands the fake API
// base to the system Broker and restarts the unit so it takes effect. The
// drop-in stays in place across the restart the update itself performs.
func selfUpdateSetSystemUnitEnv(t *testing.T, apiURL string) {
	t.Helper()

	dir := filepath.Join("/etc/systemd/system", "peers.service.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	content := fmt.Sprintf("[Service]\nEnvironment=PEERS_TEST_UPDATE_API=%s\n", apiURL)
	path := filepath.Join(dir, "e2e-update-api.conf")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	mustRun(t, "root", nil, "systemctl", "daemon-reload")
	mustRun(t, "root", nil, "systemctl", "restart", "peers.service")
}

// selfUpdateSetUserUnitEnv writes a systemd user drop-in that hands the fake API
// base to the account's user Broker and restarts it. The drop-in stays in place
// across the restart the update itself performs.
func selfUpdateSetUserUnitEnv(t *testing.T, account string, userEnv []string, apiURL string) {
	t.Helper()

	uid, gid, home := selfUpdatePasswd(t, account)
	dir := filepath.Join(home, ".config", "systemd", "user", "peers.service.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	if err := os.Chown(dir, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", dir, err)
	}
	content := fmt.Sprintf("[Service]\nEnvironment=PEERS_TEST_UPDATE_API=%s\n", apiURL)
	path := filepath.Join(dir, "e2e-update-api.conf")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := os.Chown(path, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", path, err)
	}

	mustRun(t, account, userEnv, "systemctl", "--user", "daemon-reload")
	mustRun(t, account, userEnv, "systemctl", "--user", "restart", "peers.service")
	harness.Eventually(t, selfUpdateRestartBound, "the user service to answer after the restart", func() (bool, string) {
		if state := selfUpdateUnitState(t, account, userEnv, "peers.service", "--user"); state != "active" {
			return false, "peers.service is " + state
		}
		if _, err := os.Stat(syscontainer.UserSocketPath(t, account)); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
}

// selfUpdateSetupUserServiceFor installs the old release as the account's
// user service. It returns the target binary and the environment its user
// manager commands need.
func selfUpdateSetupUserServiceFor(t *testing.T, account string) (string, []string) {
	t.Helper()
	return selfUpdateSetupUserServiceFrom(t, account, updateOldArtifact)
}

// selfUpdateSetupUserServiceFrom is selfUpdateSetupUserServiceFor for a caller
// that installs another staged artifact as the account's user service.
func selfUpdateSetupUserServiceFrom(t *testing.T, account, artifact string) (string, []string) {
	t.Helper()

	uid, gid, home := selfUpdatePasswd(t, account)
	target := filepath.Join(home, "bin", "peers")
	syscontainer.CopyReleaseArtifact(t, artifact, target, 0o755)
	dir := filepath.Dir(target)
	if err := os.Chown(dir, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", dir, err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	if err := os.Chown(target, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", target, err)
	}
	selfUpdateWriteAccountConfig(t, filepath.Join(home, ".peers", "config.yaml"), uid, gid,
		fmt.Sprintf("broker:\n  listen: %q\nupdate:\n  check: false\n", selfUpdateUserListen))

	uidString := strconv.FormatUint(uint64(uid), 10)
	userEnv := []string{
		"XDG_RUNTIME_DIR=/run/user/" + uidString,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + uidString + "/bus",
	}
	mustRun(t, "root", nil, "loginctl", "enable-linger", account)
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uidString+".service")
	waitUserManager(t, uid)

	install := runWithEnv(t, account, userEnv, target, "service", "install")
	if install.ExitCode != 0 {
		t.Fatalf("peers service install as %s: exit %d\nstdout: %s\nstderr: %s\njournal:\n%s",
			account, install.ExitCode, strings.TrimSpace(install.Stdout), strings.TrimSpace(install.Stderr),
			selfUpdateUserJournal(t, account, userEnv))
	}
	harness.Eventually(t, updateServiceBound, "the user service socket of "+account, func() (bool, string) {
		if _, err := os.Stat(syscontainer.UserSocketPath(t, account)); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
	if pid := selfUpdateUserServiceMainPID(t, account, userEnv); pid == 0 {
		t.Fatalf("the user service of %s reports MainPID 0 after install\njournal:\n%s",
			account, selfUpdateUserJournal(t, account, userEnv))
	}
	return target, userEnv
}

// selfUpdateUserJournal returns the tail of the account's user unit log, for a
// setup failure inside the container.
func selfUpdateUserJournal(t *testing.T, account string, userEnv []string) string {
	t.Helper()

	res := runWithEnv(t, account, userEnv, "journalctl", "--user", "-u", "peers.service", "-n", "50", "--no-pager")
	return strings.TrimSpace(res.Stdout + res.Stderr)
}

// selfUpdateUserServiceMainPID returns the MainPID systemd reports for the
// account's user unit.
func selfUpdateUserServiceMainPID(t *testing.T, account string, userEnv []string) int {
	t.Helper()

	res := runWithEnv(t, account, userEnv, "systemctl", "--user", "show", "--property", "MainPID", "--value", "peers.service")
	if res.ExitCode != 0 {
		t.Fatalf("systemctl --user show MainPID as %s: exit %d\nstderr: %s",
			account, res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	pid, err := strconv.Atoi(strings.TrimSpace(res.Stdout))
	if err != nil {
		t.Fatalf("MainPID %q of %s is not a number: %v", strings.TrimSpace(res.Stdout), account, err)
	}
	return pid
}

// selfUpdatePasswd reads the uid, gid and home directory of account through
// getent, so it also sees accounts that exist only in the extrausers source.
func selfUpdatePasswd(t *testing.T, account string) (uint32, uint32, string) {
	t.Helper()

	res := runWithEnv(t, "root", nil, "getent", "passwd", account)
	if res.ExitCode != 0 {
		t.Fatalf("getent passwd %s: exit %d\nstderr: %s", account, res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	fields := strings.Split(strings.TrimSpace(res.Stdout), ":")
	if len(fields) != 7 || fields[0] != account {
		t.Fatalf("getent passwd %s = %q, want seven fields", account, res.Stdout)
	}
	uid, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		t.Fatalf("account %q has a non-numeric uid %q: %v", account, fields[2], err)
	}
	gid, err := strconv.ParseUint(fields[3], 10, 32)
	if err != nil {
		t.Fatalf("account %q has a non-numeric gid %q: %v", account, fields[3], err)
	}
	return uint32(uid), uint32(gid), fields[5]
}

// selfUpdateWriteAccountConfig writes a configuration file owned by uid:gid in
// a private directory.
func selfUpdateWriteAccountConfig(t *testing.T, path string, uid, gid uint32, content string) {
	t.Helper()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	if err := os.Chown(dir, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := os.Chown(path, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

// selfUpdateUnitState returns systemctl's is-active answer for the unit.
func selfUpdateUnitState(t *testing.T, account string, env []string, unit string, scopeArgs ...string) string {
	t.Helper()

	args := append(append([]string{}, scopeArgs...), "is-active", unit)
	res := runWithEnv(t, account, env, "systemctl", args...)
	return strings.TrimSpace(res.Stdout)
}

// selfUpdateRunSudo runs the system-installed peers through sudo as
// account, with the fake API base set in the root command's environment.
func selfUpdateRunSudo(t *testing.T, account, apiURL string, args ...string) syscontainer.Result {
	t.Helper()

	argv := append([]string{"-n", "env", "PEERS_TEST_UPDATE_API=" + apiURL, selfUpdateBinary}, args...)
	return runWithEnv(t, account, nil, "sudo", argv...)
}

// selfUpdateWaitBrokerVersion waits until the Broker behind socket reports want
// as its running version. The test process is root, so the system socket's
// permission check is satisfied.
func selfUpdateWaitBrokerVersion(t *testing.T, socket, want string) {
	t.Helper()

	harness.Eventually(t, selfUpdateRestartBound, "the Broker to report version "+want, func() (bool, string) {
		oracle, err := node.DialOracle(socket)
		if err != nil {
			return false, err.Error()
		}
		defer func() { _ = oracle.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), selfUpdateStatusRPCTimeout)
		defer cancel()
		resp, err := oracle.UpdateStatus(ctx)
		if err != nil {
			return false, err.Error()
		}
		got := resp.GetCurrentVersion()
		return got == want, fmt.Sprintf("the Broker reports %q", got)
	})
}

// selfUpdateWaitUserBrokerVersion waits until the account's user Broker reports
// want. The Broker admits only that account's peer credentials, so the read
// runs as the account.
func selfUpdateWaitUserBrokerVersion(t *testing.T, account, want string) {
	t.Helper()

	harness.Eventually(t, selfUpdateRestartBound, "the user Broker to report version "+want, func() (bool, string) {
		got, err := updateBrokerVersion(t, account)
		if err != nil {
			return false, err.Error()
		}
		return got == want, fmt.Sprintf("the user Broker reports %q", got)
	})
}

// selfUpdateWaitSession waits until a tool call on the existing session
// succeeds. It proves the MCP server reconnected to the restarted Broker
// without the session being reopened.
func selfUpdateWaitSession(t *testing.T, session *syscontainer.Session) {
	t.Helper()

	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the session of "+session.Account+" to reach its Broker", func(deadline time.Time) (bool, string) {
		text, isError, err := session.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, text
	})
}

// selfUpdateWaitConnected waits until the session's MCP server has connected
// to its Broker. A tool call made in the first moments after the server starts
// answers with "still being set up", which is not what these tests exercise;
// list_brokers needs no Agent identity, so it also works before registration.
func selfUpdateWaitConnected(t *testing.T, session *syscontainer.Session) {
	t.Helper()

	harness.EventuallyPoll(t, selfUpdateRestartBound, "the session of "+session.Account+" to reach the Broker", func(deadline time.Time) (bool, string) {
		text, isError, err := session.PollCall(t, deadline, "list_brokers", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, text
	})
}

// selfUpdateRequireUpdateTool checks that the MCP server exposes update_broker
// with the annotations the design requires: it is not read-only and is
// destructive, so a client can authorize it separately.
func selfUpdateRequireUpdateTool(t *testing.T, session *syscontainer.Session) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), selfUpdateListToolsTimeout)
	defer cancel()
	result, err := session.MCP.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("listing the MCP tools as %s: %v", session.Account, err)
	}
	var names []string
	for _, tool := range result.Tools {
		names = append(names, tool.Name)
		if tool.Name != "update_broker" {
			continue
		}
		if tool.Annotations == nil {
			t.Fatalf("update_broker has no annotations; want readOnlyHint=false and destructiveHint=true")
		}
		if tool.Annotations.ReadOnlyHint {
			t.Errorf("update_broker readOnlyHint = true, want false")
		}
		if tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint {
			t.Errorf("update_broker destructiveHint = %v, want true", tool.Annotations.DestructiveHint)
		}
		return
	}
	t.Fatalf("the MCP server does not expose update_broker; tools: %s", strings.Join(names, ", "))
}

// selfUpdateCallBrokerUpdate calls update_broker and checks the from, to and
// restarting values. It returns the whole tool result text, which also carries
// the note about reopening the session.
func selfUpdateCallBrokerUpdate(t *testing.T, session *syscontainer.Session, from, to string) string {
	t.Helper()

	text, isError := session.Call(t, "update_broker", map[string]any{})
	if isError {
		t.Fatalf("update_broker as %s reported an error: %s", session.Account, text)
	}

	var out struct {
		FromVersion string `json:"from_version"`
		ToVersion   string `json:"to_version"`
		From        string `json:"from"`
		To          string `json:"to"`
		Restarting  bool   `json:"restarting"`
	}
	line := selfUpdateJSONLine(t, text)
	if err := json.Unmarshal([]byte(line), &out); err != nil {
		t.Fatalf("decoding the update_broker result %q: %v", text, err)
	}
	gotFrom, gotTo := out.FromVersion, out.ToVersion
	if gotFrom == "" {
		gotFrom = out.From
	}
	if gotTo == "" {
		gotTo = out.To
	}
	if gotFrom != from {
		t.Fatalf("update_broker from = %q, want %q\n%s", gotFrom, from, text)
	}
	if gotTo != to {
		t.Fatalf("update_broker to = %q, want %q\n%s", gotTo, to, text)
	}
	if !out.Restarting {
		t.Fatalf("update_broker restarting = false, want true\n%s", text)
	}
	return text
}

// selfUpdateJSONLine returns the first non-empty line of text that starts with
// an object. The typed tool result is one JSON object; an update hint may be
// appended after it as another text block.
func selfUpdateJSONLine(t *testing.T, text string) string {
	t.Helper()

	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "{") {
			return strings.TrimSpace(line)
		}
	}
	t.Fatalf("the tool result has no JSON object:\n%s", text)
	return ""
}

// selfUpdateRequireReopenHint checks that the tool result tells the caller the
// open session must be reopened to run the new MCP binary.
func selfUpdateRequireReopenHint(t *testing.T, text string) {
	t.Helper()

	lower := strings.ToLower(text)
	reopen := strings.Contains(lower, "reopen") || strings.Contains(lower, "restart")
	if !strings.Contains(lower, "session") || !reopen {
		t.Fatalf("the update_broker result does not say the open session must be reopened:\n%s", text)
	}
}

// selfUpdateWaitAuditEvent waits until the Broker's audit log records
// update_applied by account with both versions.
func selfUpdateWaitAuditEvent(t *testing.T, account string) {
	t.Helper()
	selfUpdateWaitAuditEventFor(t, account, updateOldVersion, updateNewVersion)
}

// selfUpdateWaitAuditEventFor waits for update_applied by account between the
// two named versions.
func selfUpdateWaitAuditEventFor(t *testing.T, account, from, to string) {
	t.Helper()

	harness.Eventually(t, selfUpdateAuditBound, "the audit log to record update_applied by "+account, func() (bool, string) {
		res := syscontainer.RunAs(t, account, nil, "service", "audit", "--system")
		if res.ExitCode != 0 {
			return false, fmt.Sprintf("peers service audit exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
		}
		for _, line := range strings.Split(res.Stdout, "\n") {
			if strings.Contains(line, "update_applied") &&
				strings.Contains(line, account) &&
				strings.Contains(line, from) &&
				strings.Contains(line, to) {
				return true, line
			}
		}
		return false, res.Stdout
	})
}

// selfUpdateRunRoot runs a command as root and fails the test when it does not
// succeed.
func selfUpdateRunRoot(t *testing.T, argv ...string) {
	t.Helper()

	cmd := syscontainer.Command("root", nil, argv[0], argv[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(argv, " "), err, strings.TrimSpace(string(out)))
	}
}
