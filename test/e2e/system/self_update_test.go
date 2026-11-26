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
	selfUpdateArchiveDir   = "/var/lib/peers-self-update-archive"
	selfUpdateBinary       = "/usr/local/bin/peers"
	selfUpdateConfigPath   = "/etc/peers/config.yaml"
	selfUpdateSystemSocket = "/run/peers/broker.sock"
	selfUpdateAdminGroup   = "peers-admins"
	// selfUpdateUserListen is the user Broker's address when the system Broker also
	// runs; the template would otherwise give both the same port.
	selfUpdateUserListen = "127.0.0.1:17463"

	// selfUpdateRestartBound covers systemd's RestartSec=5, the Broker's startup and
	// readiness, and the MCP server's reconnect window.
	selfUpdateRestartBound = 30 * time.Second
	selfUpdateAuditBound   = harness.EventTimeout
	// selfUpdateStatusRPCTimeout bounds one UpdateStatus call on a local socket;
	// the Broker answers in milliseconds, so the bound covers a loaded container
	// without hiding a hung call.
	selfUpdateStatusRPCTimeout = 5 * time.Second
	selfUpdateListToolsTimeout = 10 * time.Second
	// selfUpdateNoRestartWindow is how long the already-latest case watches a Broker
	// that must not restart; the Broker schedules a restart 500 ms after ApplyUpdate.
	selfUpdateNoRestartWindow       = 2 * time.Second
	selfUpdateInheritedInvocationID = "3f0a6f0b1c2d4e5f8a9b0c1d2e3f4a5b"
)

// TestREQUpdate007_SystemUpdateByAdminGroup checks that an account that may
// write the system configuration updates the Broker without root, keeping the
// binary's owner and mode and recording the update in the audit log.
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

// TestREQUpdate007_SystemUpdateRefusedOutsideAdminGroup checks that an account
// that may not write the configuration gets permission_denied, with nothing
// downloaded and the binary untouched.
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

// TestREQUpdate007_CLIWithoutUserBrokerUsesSystemBroker checks that an account
// with no user Broker has its update delegated to the system Broker.
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

// TestREQUpdate007_CLIWithUserBrokerUsesUserBroker checks that an account
// running a user Broker has its update reach that Broker, leaving the system
// Broker and binary untouched.
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

	// The command is the system-installed binary, so only the Broker selection can
	// send the update to the user install.
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

// TestREQUpdate007_RootUpdateUsesSystemBroker checks that a root caller's
// update reaches the system Broker.
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

// TestREQUpdate007_SudoWithUserBrokerUsesSystemBroker checks that sudo peers
// update upgrades the system Broker even when the admin also runs a user
// Broker.
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

// TestREQUpdate007_TerminalSystemBrokerNeedsServiceManager checks that a system
// Broker started by hand, outside a service manager, refuses the update and
// leaves the binary unchanged.
func TestREQUpdate007_TerminalSystemBrokerNeedsServiceManager(t *testing.T) {
	if !syscontainer.InContainer() {
		selfUpdateStageHostAssets(t, false)
		syscontainer.Run(t, syscontainer.Plain)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))

	// The manually started Broker runs this binary directly; no systemd unit sets
	// INVOCATION_ID.
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
// checks that a hand-started Broker whose shell carries INVOCATION_ID still
// refuses both the CLI and update_broker before any download.
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

// TestREQUpdate008_UpdateBrokerAlreadyLatest checks that update_broker on an
// up-to-date Broker reports no target version, downloads nothing and restarts
// nothing.
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

	selfUpdateWaitSession(t, session)

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

// TestREQUpdate008_UpdateBrokerUnderUserService checks that update_broker
// restarts a user service on the new version, reaches it again without
// reopening, and tells the caller to reopen the session.
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

// TestREQUpdate008_UpdateBrokerUnderSystemService checks that update_broker lets
// the system Broker replace the binary and restart under systemd, keeping the
// binary's owner and mode and recording the update.
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

// TestREQUpdate008_UpdateBrokerRefusedOutsideAdminGroup checks that
// update_broker from outside the admin group returns permission_denied and
// serves no download.
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

func selfUpdateStageHostAssets(t *testing.T, withInstallScript bool) {
	t.Helper()

	updateStageAssets(t)
	if withInstallScript {
		stageInstallArchive(t)
	}
}

func selfUpdateAPIEnv(fake *harness.FakeGitHub) []string {
	return []string{"PEERS_TEST_UPDATE_API=" + fake.URL()}
}

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

// selfUpdateInstallSystemService installs the old release as the system service
// and, when the install script rejects --join-admin-group, performs the group
// grant itself.
func selfUpdateInstallSystemService(t *testing.T, fake *harness.FakeGitHub) {
	t.Helper()
	selfUpdateInstallSystemServiceFrom(t, fake, updateOldArtifact, updateOldVersion)
}

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
// base to the system Broker; it stays in place across the update's restart.
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

// selfUpdateSetUserUnitEnv writes a user drop-in that hands the fake API base to
// the account's user Broker; it stays in place across the update's restart.
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

func selfUpdateSetupUserServiceFor(t *testing.T, account string) (string, []string) {
	t.Helper()
	return selfUpdateSetupUserServiceFrom(t, account, updateOldArtifact)
}

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

func selfUpdateUserJournal(t *testing.T, account string, userEnv []string) string {
	t.Helper()

	res := runWithEnv(t, account, userEnv, "journalctl", "--user", "-u", "peers.service", "-n", "50", "--no-pager")
	return strings.TrimSpace(res.Stdout + res.Stderr)
}

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

// selfUpdatePasswd reads uid, gid and home through getent, so it also sees
// accounts that exist only in the extrausers source.
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

func selfUpdateUnitState(t *testing.T, account string, env []string, unit string, scopeArgs ...string) string {
	t.Helper()

	args := append(append([]string{}, scopeArgs...), "is-active", unit)
	res := runWithEnv(t, account, env, "systemctl", args...)
	return strings.TrimSpace(res.Stdout)
}

func selfUpdateRunSudo(t *testing.T, account, apiURL string, args ...string) syscontainer.Result {
	t.Helper()

	argv := append([]string{"-n", "env", "PEERS_TEST_UPDATE_API=" + apiURL, selfUpdateBinary}, args...)
	return runWithEnv(t, account, nil, "sudo", argv...)
}

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

// selfUpdateWaitUserBrokerVersion reads as account, because a user Broker admits
// only that account's peer credentials.
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

// selfUpdateWaitConnected polls list_brokers, which needs no Agent identity,
// because a tool call in the first moments answers that the session is still
// being set up.
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

// selfUpdateJSONLine returns the first line that is a JSON object; an update hint
// may be appended after it as another text block.
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

func selfUpdateRequireReopenHint(t *testing.T, text string) {
	t.Helper()

	lower := strings.ToLower(text)
	reopen := strings.Contains(lower, "reopen") || strings.Contains(lower, "restart")
	if !strings.Contains(lower, "session") || !reopen {
		t.Fatalf("the update_broker result does not say the open session must be reopened:\n%s", text)
	}
}

func selfUpdateWaitAuditEvent(t *testing.T, account string) {
	t.Helper()
	selfUpdateWaitAuditEventFor(t, account, updateOldVersion, updateNewVersion)
}

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

func selfUpdateRunRoot(t *testing.T, argv ...string) {
	t.Helper()

	cmd := syscontainer.Command("root", nil, argv[0], argv[1:]...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(argv, " "), err, strings.TrimSpace(string(out)))
	}
}
