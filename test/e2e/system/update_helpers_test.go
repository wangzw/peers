//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// The release versions and artifact names the update scenarios build. The old
// version is the running release; the new one is what the fake GitHub API
// offers; the other version is inside a package whose binary lies about itself.
const (
	updateOldVersion   = "0.1.0"
	updateNewVersion   = "0.1.2"
	updateOtherVersion = "0.9.9"
)

// Artifact names under the directory compose mounts for the test binary. Names
// are unique per content, so parallel tests stage the same files harmlessly.
const (
	updateOldArtifact            = "update-old-" + updateOldVersion
	updateGoodReleaseArtifact    = "update-release-good.tar.gz"
	updateGoodChecksumsArtifact  = "update-checksums-good.txt"
	updateMismatchChecksumsFile  = "update-checksums-mismatch.txt"
	updateCorruptReleaseArtifact = "update-release-corrupt.tar.gz"
	updateCorruptChecksumsFile   = "update-checksums-corrupt.txt"
	updateWrongReleaseArtifact   = "update-release-wrong.tar.gz"
	updateWrongChecksumsFile     = "update-checksums-wrong.txt"
)

const (
	// updateBrokerListen is the user Broker's listen address in the
	// update scenarios. One Broker runs per container, so no other listener
	// competes for it.
	updateBrokerListen = "127.0.0.1:17461"

	// updateInitialCheckDelay is the first-check delay the staged old release
	// is started with through the test-build hook. Only a build carrying the
	// peers_testenv tag reads the variable; a release build waits one minute.
	updateInitialCheckDelay = 3 * time.Second

	// updateFirstCheckBound is the quiet window in which a Broker with checking
	// disabled must send no request. An enabled check fires one
	// updateInitialCheckDelay after the Broker starts its timer, so four times
	// the delay still leaves nine seconds for startup before the timer starts.
	// The window proves the first check after the check is disabled sends no
	// request.
	updateFirstCheckBound = 4 * updateInitialCheckDelay

	// updateCheckArrivalBound bounds an enabled Broker's first check: the
	// shortened delay plus slack for container startup, the loopback request
	// and the status read. It replaces the product's one-minute delay that the
	// earlier two-minute bound had to cover.
	updateCheckArrivalBound = 30 * time.Second

	// updateCheckSettleWindow is how long a Broker whose check found no newer
	// release must keep status free of an update line after its request was
	// observed. The fake API answers on loopback, so a check result is stored
	// well inside this window.
	updateCheckSettleWindow = 2 * time.Second

	// updateServiceBound covers systemd starting the restarted user service.
	// The unit has Restart=always and RestartSec=5; the update command waits
	// for readiness itself, so this bound only absorbs the status read.
	updateServiceBound = 15 * time.Second
)

// updateStagedOldBinary returns the old release binary visible to the
// container.
func updateStagedOldBinary() string {
	return syscontainer.ReleaseArtifactPath(updateOldArtifact)
}

// updateStageAssets builds every binary and package the update scenarios serve
// and stages them next to the test binary. It runs on the host: the container
// has no Go toolchain, and compose mounts the test binary's directory read-only
// at syscontainer.ReleaseArtifactPath.
func updateStageAssets(t *testing.T) {
	t.Helper()

	old := harness.BuildBinaryVersion(t, updateOldVersion, "linux", runtime.GOARCH, "peers_testenv")
	good := harness.BuildBinaryVersion(t, updateNewVersion, "linux", runtime.GOARCH)
	other := harness.BuildBinaryVersion(t, updateOtherVersion, "linux", runtime.GOARCH)

	syscontainer.StageReleaseArtifact(t, updateOldArtifact, updateReadFile(t, old), 0o755)

	assetName := harness.ReleaseAssetName(updateNewVersion, "linux", runtime.GOARCH)
	goodArchive := harness.ReleaseTarGz(t, good)
	syscontainer.StageReleaseArtifact(t, updateGoodReleaseArtifact, goodArchive, 0o644)
	syscontainer.StageReleaseArtifact(t, updateGoodChecksumsArtifact, harness.ChecksumLine(assetName, goodArchive), 0o644)

	// The mismatch case serves the good package with a checksum line that
	// cannot match it. The corrupt case serves a checksum that matches the
	// broken bytes, so the failure happens at extraction instead.
	syscontainer.StageReleaseArtifact(t, updateMismatchChecksumsFile, []byte(strings.Repeat("0", 64)+"  "+assetName+"\n"), 0o644)
	corrupt := []byte("this package is not a gzip stream\n")
	syscontainer.StageReleaseArtifact(t, updateCorruptReleaseArtifact, corrupt, 0o644)
	syscontainer.StageReleaseArtifact(t, updateCorruptChecksumsFile, harness.ChecksumLine(assetName, corrupt), 0o644)

	// The wrong-version case serves a well-formed package whose binary reports
	// a different version than the release names.
	wrongArchive := harness.ReleaseTarGz(t, other)
	syscontainer.StageReleaseArtifact(t, updateWrongReleaseArtifact, wrongArchive, 0o644)
	syscontainer.StageReleaseArtifact(t, updateWrongChecksumsFile, harness.ChecksumLine(assetName, wrongArchive), 0o644)
}

// updateReadFile reads path and fails the test when it cannot.
func updateReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return data
}

// updateReadStaged reads an artifact the host staged.
func updateReadStaged(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(syscontainer.ReleaseArtifactPath(name))
	if err != nil {
		t.Fatalf("reading staged artifact %s from %s: %v", name, syscontainer.ReleaseArtifactPath(name), err)
	}
	return data
}

// updateLoadRelease reads a staged package and checksum file into a release the
// shared fake GitHub API serves.
func updateLoadRelease(t *testing.T, version, archiveArtifact, checksumsArtifact string) harness.Release {
	t.Helper()
	return harness.Release{
		Version:     version,
		ArchiveName: harness.ReleaseAssetName(version, "linux", runtime.GOARCH),
		Archive:     updateReadStaged(t, archiveArtifact),
		Checksums:   updateReadStaged(t, checksumsArtifact),
	}
}

// updateBrokerConfig is a user Broker configuration with the given update
// check setting. The interval is the smallest the design permits; the first
// check follows the startup delay, not the interval.
func updateBrokerConfig(check bool) string {
	return fmt.Sprintf("broker:\n  listen: %q\nupdate:\n  check: %t\n  check_interval: 1h\n",
		updateBrokerListen, check)
}

// updateStartPeerBroker writes the configuration and starts the old release as
// peer's user Broker. The fake API base is handed to it through the
// test-build hook.
func updateStartPeerBroker(t *testing.T, fake *harness.FakeGitHub, check bool) *syscontainer.Proc {
	t.Helper()
	writeAccountFile(t, "peer", "/home/peer/.peers/config.yaml", updateBrokerConfig(check))
	env := []string{
		"PEERS_TEST_UPDATE_API=" + fake.URL(),
		// The staged old release is built with peers_testenv, so it reads this
		// hook and checks after updateInitialCheckDelay instead of the
		// product's one minute. A release build ignores the variable.
		"PEERS_TEST_UPDATE_INITIAL_DELAY=" + updateInitialCheckDelay.String(),
	}
	return syscontainer.StartUserBrokerPath(t, updateStagedOldBinary(), "peer", env,
		"--listen", updateBrokerListen)
}

// updatePeerEnv returns the user-manager environment of an account.
func updatePeerEnv(t *testing.T, account string) []string {
	t.Helper()
	uid := syscontainer.AccountUID(t, account)
	id := strconv.FormatUint(uint64(uid), 10)
	return []string{
		"XDG_RUNTIME_DIR=/run/user/" + id,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + id + "/bus",
	}
}

// updateStatus runs the terminal status command against peer's user Broker.
func updateStatus(t *testing.T, exe string) syscontainer.Result {
	t.Helper()
	env := append(updatePeerEnv(t, "peer"), "PEERS_TEST_SOCKET="+syscontainer.UserSocketPath(t, "peer"))
	return runWithEnv(t, "peer", env, exe, "service", "status")
}

// updateStatusClientRole is the TestMain role that reads the Broker's running
// version as the account that owns it.
const updateStatusClientRole = "update-status-client"

// updateBrokerVersion runs the test binary as account in the update-status
// client role and returns the version the Broker reports. A user Broker
// admits only its own account's peer credentials, so the read runs as that
// account instead of the root test process.
func updateBrokerVersion(t *testing.T, account string) (string, error) {
	t.Helper()
	base := syscontainer.Command(account, []string{
		registerClientRoleEnv + "=" + updateStatusClientRole,
		"PEERS_TEST_SOCKET=" + syscontainer.UserSocketPath(t, account),
	}, copyTestBinary(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, base.Path, base.Args[1:]...)
	cmd.Env = base.Env
	cmd.Dir = base.Dir
	cmd.SysProcAttr = base.SysProcAttr
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

// runUpdateStatusClientRole is the body of the test binary when TestMain
// selects the update-status role. It prints the version the Broker runs.
func runUpdateStatusClientRole() int {
	oracle, err := node.DialOracle(os.Getenv("PEERS_TEST_SOCKET"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "update-status-client:", err)
		return 1
	}
	defer func() { _ = oracle.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := oracle.Hello(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "update-status-client:", err)
		return 1
	}
	resp, err := oracle.UpdateStatus(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "update-status-client:", err)
		return 1
	}
	fmt.Println(resp.GetCurrentVersion())
	return 0
}

// updateWaitStatusLine waits until status prints want, which is the observable
// point at which the Broker has learned the check's result.
func updateWaitStatusLine(t *testing.T, exe, want string) {
	t.Helper()
	harness.Eventually(t, updateCheckArrivalBound, "status to report the available version", func() (bool, string) {
		res := updateStatus(t, exe)
		if res.ExitCode != 0 {
			return false, fmt.Sprintf("status exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
		}
		return strings.Contains(res.Stdout, want), res.Stdout
	})
}

// updateWaitAgentListed waits until the terminal status lists the session,
// which proves the MCP server registered without the test spending a tool
// call. A tool call would consume the session's first-call update hint.
func updateWaitAgentListed(t *testing.T, name, exe string) {
	t.Helper()
	harness.Eventually(t, updateCheckArrivalBound, "the session "+name+" to register", func() (bool, string) {
		res := updateStatus(t, exe)
		if res.ExitCode != 0 {
			return false, fmt.Sprintf("status exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
		}
		return strings.Contains(res.Stdout, name+"@"), res.Stdout
	})
}

// updateRequireFeature fails fast when the build lacks the update command.
// Without the gate, a test that waits for a Broker's first check would sit in
// its bound when the whole feature is absent.
func updateRequireFeature(t *testing.T) {
	t.Helper()
	res := syscontainer.RunAs(t, "root", nil, "--help")
	if res.ExitCode != 0 || !strings.Contains(res.Stdout, "update") {
		t.Fatalf("this build does not provide the update command, so the update acceptance cannot run: exit %d\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
}

// updateHint is the sentence a session's first tool result carries when a
// newer release is available.
func updateHint(available, current string) string {
	return fmt.Sprintf("peers %s is available (this Broker runs %s). "+
		"To upgrade, the user can ask you to call update_broker or run `peers update` in a terminal.",
		available, current)
}

// updateStatusLine is the line the status output carries when a newer release
// is available.
func updateStatusLine(available, current string) string {
	return fmt.Sprintf("Update: %s available (current %s); run peers update", available, current)
}

// updatePeerIDs returns the peer account and its numeric ids.
func updatePeerIDs(t *testing.T) (*user.User, int, int) {
	t.Helper()
	u, err := user.Lookup("peer")
	if err != nil {
		t.Fatalf("looking up account peer: %v", err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		t.Fatalf("account peer has a non-numeric uid %q: %v", u.Uid, err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		t.Fatalf("account peer has a non-numeric gid %q: %v", u.Gid, err)
	}
	return u, uid, gid
}

// updateUserService is one installed user service running the old
// release from a peer-owned directory.
type updateUserService struct {
	target    string
	userEnv   []string
	beforePID int
}

// updateSetupUserService copies the old release to /home/peer/bin, installs it
// as peer's user service through the product's subcommand, and returns the
// running service's state.
func updateSetupUserService(t *testing.T) updateUserService {
	t.Helper()

	u, uid, gid := updatePeerIDs(t)
	target := filepath.Join(u.HomeDir, "bin", "peers")
	syscontainer.CopyReleaseArtifact(t, updateOldArtifact, target, 0o755)

	// The update writes a temporary file next to the target before renaming it,
	// so the directory must belong to peer as well.
	dir := filepath.Dir(target)
	for _, path := range []string{dir, target} {
		if err := os.Chown(path, uid, gid); err != nil {
			t.Fatalf("chown %s to peer: %v", path, err)
		}
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}

	// The restarted service must not call GitHub through the container's
	// network, which has no egress.
	writeAccountFile(t, "peer", filepath.Join(u.HomeDir, ".peers", "config.yaml"), "update:\n  check: false\n")

	peerUID := syscontainer.AccountUID(t, "peer")
	uidString := strconv.FormatUint(uint64(peerUID), 10)
	userEnv := updatePeerEnv(t, "peer")
	mustRun(t, "root", nil, "loginctl", "enable-linger", "peer")
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uidString+".service")
	waitUserManager(t, peerUID)

	install := runWithEnv(t, "peer", userEnv, target, "service", "install")
	if install.ExitCode != 0 {
		t.Fatalf("peers service install as peer: exit %d\nstdout: %s\nstderr: %s",
			install.ExitCode, strings.TrimSpace(install.Stdout), strings.TrimSpace(install.Stderr))
	}
	syscontainer.WaitFor(t, "the user service socket "+syscontainer.UserSocketPath(t, "peer"), func() bool {
		_, err := os.Stat(syscontainer.UserSocketPath(t, "peer"))
		return err == nil
	})
	beforePID := userServiceMainPID(t, userEnv)
	if beforePID == 0 {
		t.Fatalf("the user service reports MainPID 0 after install")
	}
	return updateUserService{target: target, userEnv: userEnv, beforePID: beforePID}
}

// updateRunInteractive runs the update command under a pseudo-terminal and
// feeds answer to its prompt. Without a terminal the command would refuse
// before asking, so the confirmation and refusal cases need one.
func updateRunInteractive(t *testing.T, account string, env []string, exe, answer string, args ...string) syscontainer.Result {
	t.Helper()

	command := exe
	if len(args) > 0 {
		command += " " + strings.Join(args, " ")
	}
	cmd := syscontainer.Command(account, env, "script", "-q", "-e", "-c", command, "/dev/null")
	cmd.Stdin = strings.NewReader(answer)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	res := syscontainer.Result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s update under a pseudo-terminal: %v", exe, err)
	}
	res.ExitCode = exitErr.ExitCode()
	return res
}

// updateFileHash returns the SHA-256 of a file.
func updateFileHash(t *testing.T, path string) string {
	t.Helper()
	sum := sha256.Sum256(updateReadFile(t, path))
	return hex.EncodeToString(sum[:])
}

// updateBinaryVersion runs --version and returns its trimmed output.
func updateBinaryVersion(t *testing.T, exe string) string {
	t.Helper()
	res := runWithEnv(t, "peer", nil, exe, "--version")
	if res.ExitCode != 0 {
		t.Fatalf("%s --version: exit %d\nstderr: %s", exe, res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	return strings.TrimSpace(res.Stdout)
}

// updateServiceState returns systemd's is-active answer for the user unit.
func updateServiceState(t *testing.T, userEnv []string) string {
	t.Helper()
	res := runWithEnv(t, "peer", userEnv, "systemctl", "--user", "is-active", "peers.service")
	return strings.TrimSpace(res.Stdout)
}
