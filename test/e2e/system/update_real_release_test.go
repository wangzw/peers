//go:build e2e

package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// realReleaseOldTag and realReleaseOldVersion name the released tree; the test
	// builds the tag itself so the old binary has the real 0.4.0 command tree, not a
	// current-tree build stamped with the old version.
	realReleaseOldTag     = "v0.4.0"
	realReleaseOldVersion = "0.4.0"
	// realReleaseNewVersion is the branch build the old release updates to; only
	// its version has to be newer, the source is the tree under test.
	realReleaseNewVersion = "0.5.0"

	realReleaseOldArtifact          = "update-real-old-" + realReleaseOldVersion
	realReleaseNewReleaseArtifact   = "update-real-release-" + realReleaseNewVersion + ".tar.gz"
	realReleaseNewChecksumsArtifact = "update-real-checksums-" + realReleaseNewVersion + ".txt"

	// realReleaseOld/NewSystemCommand and realReleaseOld/NewUserCommand are the
	// ExecStart suffixes of the two command trees; the unit assertion names the
	// binary because both releases install it at the same path.
	realReleaseOldSystemCommand = "broker --system --allow-degraded"
	realReleaseNewSystemCommand = "service run --system --allow-degraded"
	realReleaseOldUserCommand   = "broker --allow-degraded"
	realReleaseNewUserCommand   = "service run --user --allow-degraded"
)

// TestREQUpdate009_UpdatesARealV040InstallToTheNewCommandTree checks that a real
// v0.4.0 install updates across the release boundary, moving both its system and
// its user unit to the new command tree.
func TestREQUpdate009_UpdatesARealV040InstallToTheNewCommandTree(t *testing.T) {
	if !syscontainer.InContainer() {
		realReleaseStageHostAssets(t)
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}
	updateRequireFeature(t)

	fake := harness.StartFakeGitHub(t, harness.Release{
		Version:     realReleaseNewVersion,
		ArchiveName: harness.ReleaseAssetName(realReleaseNewVersion, "linux", runtime.GOARCH),
		Archive:     updateReadStaged(t, realReleaseNewReleaseArtifact),
		Checksums:   updateReadStaged(t, realReleaseNewChecksumsArtifact),
	})

	realReleaseInstallSystemService(t, fake)
	beforeUnit := postUpdateReadFile(t, postUpdateUnitPath)
	realReleaseAssertUnit(t, "the installed system unit", beforeUnit, selfUpdateBinary, realReleaseOldSystemCommand)
	if strings.Contains(beforeUnit, "service run") {
		t.Fatalf("the installed system unit already uses the new command tree, so the update would not cross the release boundary:\n%s", beforeUnit)
	}
	if got := realReleaseBinaryVersion(t, "root", selfUpdateBinary); got != "peers "+realReleaseOldVersion {
		t.Fatalf("the installed system binary reports %q, want %q", got, "peers "+realReleaseOldVersion)
	}

	res := syscontainer.RunAs(t, installAccount, selfUpdateAPIEnv(fake), "update", "--yes")
	combined := res.Stdout + res.Stderr
	if res.ExitCode != 0 {
		t.Fatalf("peers update --yes as %s exited %d\nstdout:\n%s\nstderr:\n%s",
			installAccount, res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(combined, realReleaseNewVersion) {
		t.Fatalf("the update output does not name version %s:\n%s", realReleaseNewVersion, combined)
	}

	afterUnit := postUpdateReadFile(t, postUpdateUnitPath)
	realReleaseAssertUnit(t, "the rewritten system unit", afterUnit, selfUpdateBinary, realReleaseNewSystemCommand)
	if got := realReleaseBinaryVersion(t, "root", selfUpdateBinary); got != "peers "+realReleaseNewVersion {
		t.Fatalf("the installed system binary reports %q, want %q", got, "peers "+realReleaseNewVersion)
	}
	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, realReleaseNewVersion)
	realReleaseRequireStatus(t, installAccount, nil, "--system")
	selfUpdateWaitAuditEventFor(t, installAccount, realReleaseOldVersion, realReleaseNewVersion)

	// The system binary now belongs to the new release, so the update selects the
	// account's user Broker, which is still the 0.4.0 build.
	userTarget, userEnv := selfUpdateSetupUserServiceFrom(t, installAccount, realReleaseOldArtifact)
	selfUpdateSetUserUnitEnv(t, installAccount, userEnv, fake.URL())
	selfUpdateWaitUserBrokerVersion(t, installAccount, realReleaseOldVersion)

	_, _, home := selfUpdatePasswd(t, installAccount)
	userUnitPath := filepath.Join(home, ".config", "systemd", "user", "peers.service")
	beforeUserUnit := postUpdateReadFile(t, userUnitPath)
	realReleaseAssertUnit(t, "the installed user unit", beforeUserUnit, userTarget, realReleaseOldUserCommand)
	if got := realReleaseBinaryVersion(t, installAccount, userTarget); got != "peers "+realReleaseOldVersion {
		t.Fatalf("the installed user binary reports %q, want %q", got, "peers "+realReleaseOldVersion)
	}

	res = syscontainer.RunAs(t, installAccount, selfUpdateAPIEnv(fake), "update", "--yes")
	combined = res.Stdout + res.Stderr
	if res.ExitCode != 0 {
		t.Fatalf("peers update --yes as %s for its user Broker exited %d\nstdout:\n%s\nstderr:\n%s",
			installAccount, res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(combined, realReleaseNewVersion) {
		t.Fatalf("the user update output does not name version %s:\n%s", realReleaseNewVersion, combined)
	}

	afterUserUnit := postUpdateReadFile(t, userUnitPath)
	realReleaseAssertUnit(t, "the rewritten user unit", afterUserUnit, userTarget, realReleaseNewUserCommand)
	if got := realReleaseBinaryVersion(t, installAccount, userTarget); got != "peers "+realReleaseNewVersion {
		t.Fatalf("the installed user binary reports %q, want %q", got, "peers "+realReleaseNewVersion)
	}
	selfUpdateWaitUserBrokerVersion(t, installAccount, realReleaseNewVersion)
	realReleaseRequireStatus(t, installAccount, userEnv)
}

func realReleaseStageHostAssets(t *testing.T) {
	t.Helper()

	old := realReleaseBuildTag(t, realReleaseOldTag, realReleaseOldVersion)
	syscontainer.StageReleaseArtifact(t, realReleaseOldArtifact, updateReadFile(t, old), 0o755)

	newBinary := harness.BuildBinaryVersion(t, realReleaseNewVersion, "linux", runtime.GOARCH)
	postUpdateStageNext(t, realReleaseNewVersion, realReleaseNewReleaseArtifact,
		realReleaseNewChecksumsArtifact, newBinary)
}

// realReleaseBuildTag builds cmd/peers from tag with the peers_testenv tag, so
// it reads the fake GitHub API base; nothing is downloaded.
func realReleaseBuildTag(t *testing.T, tag, version string) string {
	t.Helper()

	root := harness.ModuleRoot(t)
	work := t.TempDir()
	archivePath := filepath.Join(work, "source.tar")
	if out, err := exec.Command("git", "-C", root, "archive", "--format=tar", "-o", archivePath, tag).CombinedOutput(); err != nil {
		t.Fatalf("git archive %s: %v; the end-to-end checkout needs the tag\n%s",
			tag, err, strings.TrimSpace(string(out)))
	}

	src := filepath.Join(work, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("creating %s: %v", src, err)
	}
	if out, err := exec.Command("tar", "-x", "-f", archivePath, "-C", src).CombinedOutput(); err != nil {
		t.Fatalf("extracting %s: %v\n%s", archivePath, err, strings.TrimSpace(string(out)))
	}

	binary := filepath.Join(work, "peers-"+version)
	build := exec.Command("go", "build", "-trimpath", "-ldflags", "-X main.version="+version,
		"-tags", "peers_testenv", "-o", binary, "./cmd/peers")
	build.Dir = src
	build.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+runtime.GOARCH,
	)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building %s from tag %s: %v\n%s", version, tag, err, strings.TrimSpace(string(out)))
	}
	return binary
}

func realReleaseInstallSystemService(t *testing.T, fake *harness.FakeGitHub) {
	t.Helper()

	syscontainer.CopyReleaseArtifact(t, realReleaseOldArtifact, selfUpdateBinary, 0o755)
	selfUpdateRunRoot(t, "chown", "0:0", selfUpdateBinary)
	selfUpdateRunRoot(t, "chmod", "0755", selfUpdateBinary)

	res := runWithEnv(t, "root", nil, selfUpdateBinary, "service", "install", "--system",
		"--admin-group", selfUpdateAdminGroup)
	if res.ExitCode != 0 {
		t.Fatalf("peers service install --system as root: exit %d\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	selfUpdateRunRoot(t, "usermod", "-aG", selfUpdateAdminGroup, installAccount)

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
	selfUpdateWaitBrokerVersion(t, selfUpdateSystemSocket, realReleaseOldVersion)
}

func realReleaseAssertUnit(t *testing.T, name, unit, binary, command string) {
	t.Helper()

	if want := "ExecStart=" + binary + " " + command; !strings.Contains(unit, want) {
		t.Fatalf("%s does not contain %q:\n%s", name, want, unit)
	}
}

func realReleaseBinaryVersion(t *testing.T, account, exe string) string {
	t.Helper()

	res := runWithEnv(t, account, nil, exe, "--version")
	if res.ExitCode != 0 {
		t.Fatalf("%s --version as %s: exit %d\nstderr: %s",
			exe, account, res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	return strings.TrimSpace(res.Stdout)
}

func realReleaseRequireStatus(t *testing.T, account string, env []string, args ...string) {
	t.Helper()

	argv := append([]string{"service", "status"}, args...)
	res := runWithEnv(t, account, env, selfUpdateBinary, argv...)
	if res.ExitCode != 0 {
		t.Fatalf("peers %s as %s: exit %d\nstdout:\n%s\nstderr:\n%s",
			strings.Join(argv, " "), account, res.ExitCode, res.Stdout, res.Stderr)
	}
}
