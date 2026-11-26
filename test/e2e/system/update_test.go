//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const updateFrameBound = harness.EventTimeout

// TestREQUpdate001_PeriodicCheck checks that an enabled Broker learns a newer
// release, a disabled one sends no request, and a failing check leaves
// messaging working.
func TestREQUpdate001_PeriodicCheck(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Plain)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	exe := updateStagedOldBinary()

	broker := updateStartPeerBroker(t, fake, true)
	updateWaitStatusLine(t, exe, updateStatusLine(updateNewVersion, updateOldVersion))
	broker.Stop(t)

	fake.ResetRequests()
	broker = updateStartPeerBroker(t, fake, false)
	harness.Consistently(t, updateFirstCheckBound, "no update request while checking is disabled", func() (bool, string) {
		if n := fake.Requests(); n != 0 {
			return false, fmt.Sprintf("the fake API received %d requests", n)
		}
		return true, ""
	})
	if res := updateStatus(t, exe); res.ExitCode != 0 {
		t.Fatalf("status after the disabled-check window exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	broker.Stop(t)

	fake.SetFailure(true)
	fake.ResetRequests()
	updateStartPeerBroker(t, fake, true)
	harness.Eventually(t, updateCheckArrivalBound, "the Broker's failed update check", func() (bool, string) {
		return fake.Requests() > 0, fmt.Sprintf("requests: %d", fake.Requests())
	})

	alpha := syscontainer.NewSession(t, "peer", "alpha")
	beta := syscontainer.NewSession(t, "peer", "beta")
	e2eSysWaitRegistered(t, alpha)
	e2eSysWaitRegistered(t, beta)

	const topic = "e2e/update-check-failure"
	e2eSysJoinTopic(t, alpha, topic)
	e2eSysJoinTopic(t, beta, topic)
	alphaSelf := e2eSysSelfAddress(t, alpha)
	betaSelf := e2eSysSelfAddress(t, beta)
	e2eSysWaitVisible(t, alpha, topic, betaSelf)
	e2eSysWaitVisible(t, beta, topic, alphaSelf)

	if text, isError := alpha.Call(t, "send_message", map[string]any{"to": betaSelf, "message": "still-working"}); isError {
		t.Fatalf("send_message after a failed update check reported an error: %s", text)
	}
	if frame, ok := beta.Fake.WaitFrame(updateFrameBound); !ok {
		t.Fatalf("the message did not reach beta within %s", updateFrameBound)
	} else if !strings.Contains(frame.Content, "still-working") {
		t.Fatalf("beta received %q, want the message sent after the failed check", frame.Content)
	}
}

// TestREQUpdate002_StatusShowsAvailableVersion checks that status carries both
// versions and the update command when a newer release exists and no update
// line when it does not.
func TestREQUpdate002_StatusShowsAvailableVersion(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Plain)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	exe := updateStagedOldBinary()

	broker := updateStartPeerBroker(t, fake, true)
	updateWaitStatusLine(t, exe, updateStatusLine(updateNewVersion, updateOldVersion))
	broker.Stop(t)

	fake.SetRelease(updateLoadRelease(t, updateOldVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	fake.ResetRequests()
	updateStartPeerBroker(t, fake, true)
	harness.Eventually(t, updateCheckArrivalBound, "the Broker's check against an up-to-date release", func() (bool, string) {
		return fake.Requests() > 0, fmt.Sprintf("requests: %d", fake.Requests())
	})
	harness.Consistently(t, updateCheckSettleWindow, "status without an update hint", func() (bool, string) {
		res := updateStatus(t, exe)
		if res.ExitCode != 0 {
			return false, fmt.Sprintf("status exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
		}
		if strings.Contains(res.Stdout, "Update:") {
			return false, res.Stdout
		}
		return true, ""
	})
}

// TestREQUpdate003_SessionHintOncePerSession checks that only the first tool
// call of a session carries the update hint and that a new session gets it
// again.
func TestREQUpdate003_SessionHintOncePerSession(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Plain)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	exe := updateStagedOldBinary()

	// Wait for the Broker to learn the release before starting a session, so the
	// first call always sees the update hint.
	updateStartPeerBroker(t, fake, true)
	updateWaitStatusLine(t, exe, updateStatusLine(updateNewVersion, updateOldVersion))

	hint := updateHint(updateNewVersion, updateOldVersion)

	a := syscontainer.NewSession(t, "peer", "hint-a")
	updateWaitAgentListed(t, "hint-a", exe)
	first, isError := a.Call(t, "list_agents", nil)
	if isError {
		t.Fatalf("the first list_agents of session hint-a failed: %s", first)
	}
	if !strings.Contains(first, hint) {
		t.Fatalf("the first tool result does not carry the update hint %q:\n%s", hint, first)
	}
	second, isError := a.Call(t, "list_agents", nil)
	if isError {
		t.Fatalf("the second list_agents of session hint-a failed: %s", second)
	}
	if strings.Contains(second, hint) {
		t.Fatalf("the second tool result of session hint-a repeats the update hint:\n%s", second)
	}

	b := syscontainer.NewSession(t, "peer", "hint-b")
	updateWaitAgentListed(t, "hint-b", exe)
	firstB, isError := b.Call(t, "list_agents", nil)
	if isError {
		t.Fatalf("the first list_agents of session hint-b failed: %s", firstB)
	}
	if !strings.Contains(firstB, hint) {
		t.Fatalf("the first tool result of a new session does not carry the update hint %q:\n%s", hint, firstB)
	}
}

// TestREQUpdate004_ConfirmedUpdateRestartsUserService checks that a confirmed
// update replaces the binary and restarts the user service on the new version.
func TestREQUpdate004_ConfirmedUpdateRestartsUserService(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	svc := updateSetupUserService(t)
	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	// The Broker performs the update, so its own service environment must reach
	// the fake API; the command's environment only covers a local fallback.
	selfUpdateSetUserUnitEnv(t, "peer", svc.userEnv, fake.URL())
	env := append(append([]string(nil), svc.userEnv...), "PEERS_TEST_UPDATE_API="+fake.URL())

	beforeHash := updateFileHash(t, svc.target)
	res := updateRunInteractive(t, "peer", env, svc.target, "y\n", "update")
	combined := res.Stdout + res.Stderr
	if res.ExitCode != 0 {
		t.Fatalf("the confirmed update exited %d\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	for _, want := range []string{updateOldVersion, updateNewVersion} {
		if !strings.Contains(combined, want) {
			t.Fatalf("the update output does not name version %s:\n%s", want, combined)
		}
	}
	if fake.Downloads() == 0 {
		t.Errorf("the update reported success without downloading the release")
	}
	if after := updateFileHash(t, svc.target); after == beforeHash {
		t.Errorf("the binary at %s was not replaced", svc.target)
	}
	if got := updateBinaryVersion(t, svc.target); got != "peers "+updateNewVersion {
		t.Fatalf("the installed binary reports %q, want %q", got, "peers "+updateNewVersion)
	}

	harness.Eventually(t, updateServiceBound, "the user Broker to report the new version", func() (bool, string) {
		version, err := updateBrokerVersion(t, "peer")
		if err != nil {
			return false, fmt.Sprintf("querying the Broker's version: %v", err)
		}
		return version == updateNewVersion, fmt.Sprintf("the Broker reports version %q, want %q", version, updateNewVersion)
	})
	if state := updateServiceState(t, svc.userEnv); state != "active" {
		t.Fatalf("the user service is %q after the update, want active", state)
	}
}

// TestREQUpdate004_RefusedUpdateChangesNothing checks that declining the update
// leaves the binary and the running service untouched.
func TestREQUpdate004_RefusedUpdateChangesNothing(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	svc := updateSetupUserService(t)
	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	env := append(append([]string(nil), svc.userEnv...), "PEERS_TEST_UPDATE_API="+fake.URL())

	beforeHash := updateFileHash(t, svc.target)
	res := updateRunInteractive(t, "peer", env, svc.target, "n\n", "update")
	combined := res.Stdout + res.Stderr
	// The command asks before it contacts the Broker, so a refusal names only
	// the installed version.
	for _, want := range []string{updateOldVersion, "Update canceled"} {
		if !strings.Contains(combined, want) {
			t.Fatalf("the update output does not contain %q:\n%s", want, combined)
		}
	}

	if after := updateFileHash(t, svc.target); after != beforeHash {
		t.Fatalf("the binary at %s changed after the update was refused", svc.target)
	}
	if pid := userServiceMainPID(t, svc.userEnv); pid != svc.beforePID {
		t.Fatalf("the user service MainPID changed from %d to %d after the refusal", svc.beforePID, pid)
	}
	if state := updateServiceState(t, svc.userEnv); state != "active" {
		t.Fatalf("the user service is %q after the refusal, want active", state)
	}
	if got := updateBinaryVersion(t, svc.target); got != "peers "+updateOldVersion {
		t.Fatalf("the binary reports %q after the refusal, want %q", got, "peers "+updateOldVersion)
	}
}

// TestREQUpdate004_YesSkipsPrompt checks that --yes completes the update
// without asking, with non-terminal standard input.
func TestREQUpdate004_YesSkipsPrompt(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	svc := updateSetupUserService(t)
	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	// The Broker performs the update, so its own service environment must reach
	// the fake API; the command's environment only covers a local fallback.
	selfUpdateSetUserUnitEnv(t, "peer", svc.userEnv, fake.URL())
	env := append(append([]string(nil), svc.userEnv...), "PEERS_TEST_UPDATE_API="+fake.URL())

	beforeHash := updateFileHash(t, svc.target)
	res := runWithEnv(t, "peer", env, svc.target, "update", "--yes")
	combined := res.Stdout + res.Stderr
	if res.ExitCode != 0 {
		t.Fatalf("peers update --yes exited %d with non-terminal input\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(combined, updateNewVersion) {
		t.Fatalf("update --yes output does not name version %s:\n%s", updateNewVersion, combined)
	}
	if after := updateFileHash(t, svc.target); after == beforeHash {
		t.Errorf("the binary at %s was not replaced", svc.target)
	}
	if got := updateBinaryVersion(t, svc.target); got != "peers "+updateNewVersion {
		t.Fatalf("the installed binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
	harness.Eventually(t, updateServiceBound, "the user Broker to report the new version", func() (bool, string) {
		version, err := updateBrokerVersion(t, "peer")
		if err != nil {
			return false, fmt.Sprintf("querying the Broker's version: %v", err)
		}
		return version == updateNewVersion, fmt.Sprintf("the Broker reports version %q, want %q", version, updateNewVersion)
	})
}

// TestREQUpdate005_FailedUpdateKeepsBinaryAndService checks that a checksum
// mismatch, a corrupt package and a lying binary each fail the update leaving
// the binary and service unchanged.
func TestREQUpdate005_FailedUpdateKeepsBinaryAndService(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	svc := updateSetupUserService(t)
	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))
	// The Broker performs the update, so its service environment must reach the
	// fake API; otherwise every case would fail at the lookup instead of at the
	// artifact check under test.
	selfUpdateSetUserUnitEnv(t, "peer", svc.userEnv, fake.URL())

	cases := []struct {
		name      string
		archive   string
		checksums string
	}{
		{"checksum mismatch", updateGoodReleaseArtifact, updateMismatchChecksumsFile},
		{"corrupt package", updateCorruptReleaseArtifact, updateCorruptChecksumsFile},
		{"binary reports another version", updateWrongReleaseArtifact, updateWrongChecksumsFile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake.SetRelease(updateLoadRelease(t, updateNewVersion, tc.archive, tc.checksums))
			env := append(append([]string(nil), svc.userEnv...), "PEERS_TEST_UPDATE_API="+fake.URL())

			beforeHash := updateFileHash(t, svc.target)
			beforePID := userServiceMainPID(t, svc.userEnv)

			res := runWithEnv(t, "peer", env, svc.target, "update", "--yes")
			if res.ExitCode == 0 {
				t.Fatalf("the update succeeded with %s\nstdout:\n%s\nstderr:\n%s", tc.name, res.Stdout, res.Stderr)
			}
			if after := updateFileHash(t, svc.target); after != beforeHash {
				t.Fatalf("the binary at %s changed after %s", svc.target, tc.name)
			}
			if pid := userServiceMainPID(t, svc.userEnv); pid != beforePID {
				t.Fatalf("the user service MainPID changed from %d to %d after %s", beforePID, pid, tc.name)
			}
			if state := updateServiceState(t, svc.userEnv); state != "active" {
				t.Fatalf("the user service is %q after %s, want active", state, tc.name)
			}
			if got := updateBinaryVersion(t, svc.target); got != "peers "+updateOldVersion {
				t.Fatalf("the binary reports %q after %s, want %q", got, tc.name, "peers "+updateOldVersion)
			}
		})
	}
}

// TestREQUpdate006_UpdateRequiresWritePermission checks that an ordinary account
// updating a root-owned binary fails before downloading and names sudo.
func TestREQUpdate006_UpdateRequiresWritePermission(t *testing.T) {
	if !syscontainer.InContainer() {
		updateStageAssets(t)
		syscontainer.Run(t, syscontainer.Plain)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, updateLoadRelease(t, updateNewVersion, updateGoodReleaseArtifact, updateGoodChecksumsArtifact))

	rootDir := "/opt/root-owned"
	target := filepath.Join(rootDir, "peers")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", rootDir, err)
	}
	syscontainer.CopyReleaseArtifact(t, updateOldArtifact, target, 0o755)
	for _, path := range []string{rootDir, target} {
		if err := os.Chown(path, 0, 0); err != nil {
			t.Fatalf("chown %s: %v", path, err)
		}
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatalf("chmod %s: %v", path, err)
		}
	}

	beforeHash := updateFileHash(t, target)
	env := []string{"PEERS_TEST_UPDATE_API=" + fake.URL()}
	res := runWithEnv(t, "peer", env, target, "update", "--yes")
	combined := res.Stdout + res.Stderr
	if res.ExitCode == 0 {
		t.Fatalf("the update as peer of a root-owned binary succeeded\nstdout:\n%s\nstderr:\n%s", res.Stdout, res.Stderr)
	}
	if !strings.Contains(combined, "sudo") {
		t.Fatalf("the permission error does not suggest sudo:\n%s", combined)
	}
	if n := fake.Requests(); n != 0 {
		t.Fatalf("the refused update sent %d requests to the API before failing; it must fail before downloading", n)
	}
	if after := updateFileHash(t, target); after != beforeHash {
		t.Fatalf("the binary at %s changed after the refused update", target)
	}
	if got := updateBinaryVersion(t, target); got != "peers "+updateOldVersion {
		t.Fatalf("the binary reports %q, want %q", got, "peers "+updateOldVersion)
	}
}
