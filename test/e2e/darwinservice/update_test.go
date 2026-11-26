//go:build e2e_darwin

package darwinservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	updateOldVersion = "0.1.0"
	updateNewVersion = "0.1.2"
	updateAPIEnv     = "PEERS_TEST_UPDATE_API"

	// updateRestartBound follows launchd's default 10-second ThrottleInterval
	// plus the Broker's startup and readiness, matching restartBound.
	updateRestartBound = 30 * time.Second

	// updateCommandBound is the update command's own 60-second wait
	// (updateRestartWait in cmd/peers) plus the restart margin, so a stuck
	// relaunch fails with output.
	updateCommandBound = 90 * time.Second

	// daemonNextEnvName is an environment entry only the next-version plist
	// template carries; with the old and new ProgramArguments identical,
	// reading it back is the only proof that launchd loaded the rewritten plist.
	daemonNextEnvName  = "PEERS_E2E_POST_UPDATE"
	daemonNextEnvValue = "next-format"
)

// TestREQUpdate007_LaunchDaemonReplacedByApplyUpdate checks that a system
// LaunchDaemon replaces its own binary through ApplyUpdate and keeps its root
// ownership while the restarted Broker still serves.
func TestREQUpdate007_LaunchDaemonReplacedByApplyUpdate(t *testing.T) {
	resetSystemService(t)
	preserveInstalledBinary(t)
	t.Cleanup(func() { resetSystemService(t) })
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		if data, err := exec.Command("sudo", "-n", "cat", "/var/log/peers/broker.log").Output(); err == nil {
			t.Logf("system Broker log:\n%s", data)
		}
	})

	old := harness.BuildBinaryVersion(t, updateOldVersion, "darwin", runtime.GOARCH, "peers_testenv")
	installRootBinary(t, old, installedBinary)

	newBinary := buildBinaryWithDaemonEnvironment(t, updateNewVersion)
	fake := harness.StartFakeGitHub(t, harness.NewRelease(t, updateNewVersion, "darwin", runtime.GOARCH, newBinary))

	res := run(t, "sudo", "-n", installedBinary, "service", "install", "--system")
	if res.exitCode != 0 {
		t.Fatalf("service install --system: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	pid := waitForPID(t, "system", serviceStartBound)
	t.Logf("launchd started the old release with pid %d", pid)

	beforeUID, beforeGID, beforeMode, err := binaryOwner(installedBinary)
	if err != nil {
		t.Fatalf("reading the owner of %s before the update: %v", installedBinary, err)
	}
	if beforeUID != 0 {
		t.Fatalf("%s is owned by uid %d before the update, want root", installedBinary, beforeUID)
	}

	addDaemonEnvironment(t, updateAPIEnv, fake.URL())
	bootout("system")
	waitForGone(t, "system", serviceStopBound)
	if err := sudo("launchctl", "bootstrap", "system", daemonPlist); err != nil {
		t.Fatalf("launchctl bootstrap system %s: %v", daemonPlist, err)
	}
	restarted := waitForNewPID(t, "system", pid, updateRestartBound)
	t.Logf("launchd restarted the old release with the test environment, pid %d", restarted)
	waitForBrokerVersion(t, updateOldVersion)

	res = runWithin(t, updateCommandBound, "sudo", "-n", installedBinary, "update", "--yes")
	combined := res.stdout + res.stderr
	if res.exitCode != 0 {
		t.Fatalf("peers update --yes: exit %d\nstdout:\n%s\nstderr:\n%s",
			res.exitCode, res.stdout, res.stderr)
	}
	for _, want := range []string{
		"The Broker is restarting with version " + updateNewVersion,
		"The Broker is running peers " + updateNewVersion,
	} {
		if !strings.Contains(combined, want) {
			t.Fatalf("the update output does not contain %q:\n%s", want, combined)
		}
	}
	if fake.Downloads() == 0 {
		t.Errorf("the update succeeded without downloading the release from the fake API")
	}

	if got := binaryVersion(t, installedBinary); got != "peers "+updateNewVersion {
		t.Fatalf("the installed binary reports %q, want %q", got, "peers "+updateNewVersion)
	}
	afterUID, afterGID, afterMode, err := binaryOwner(installedBinary)
	if err != nil {
		t.Fatalf("reading the owner of %s after the update: %v", installedBinary, err)
	}
	if afterUID != beforeUID || afterGID != beforeGID || afterMode != beforeMode {
		t.Errorf("the binary owner or mode changed by the update: before uid=%d gid=%d mode=%#o, after uid=%d gid=%d mode=%#o",
			beforeUID, beforeGID, beforeMode, afterUID, afterGID, afterMode)
	}

	waitForBrokerVersion(t, updateNewVersion)

	waitForLaunchdArguments(t, "system", []string{installedBinary, "service", "run", "--system", "--allow-degraded"})
	waitForLaunchdEnvironment(t, "system", daemonNextEnvName, daemonNextEnvValue)
	waitForLaunchdEnvironment(t, "system", updateAPIEnv, fake.URL())
	if status := run(t, "sudo", "-n", installedBinary, "service", "status"); status.exitCode != 0 {
		t.Fatalf("peers service status after the update: exit %d\nstdout: %s\nstderr: %s",
			status.exitCode, status.stdout, status.stderr)
	}

	audit := run(t, "sudo", "-n", installedBinary, "service", "audit", "--limit", "50")
	if audit.exitCode != 0 {
		t.Fatalf("peers service audit after the update: exit %d\nstdout: %s\nstderr: %s",
			audit.exitCode, audit.stdout, audit.stderr)
	}
	if !strings.Contains(audit.stdout, "update_applied") {
		t.Errorf("the audit log carries no update_applied event:\n%s", audit.stdout)
	}
	for _, want := range []string{updateOldVersion, updateNewVersion} {
		if !strings.Contains(audit.stdout, want) {
			t.Errorf("the audit log does not name version %s:\n%s", want, audit.stdout)
		}
	}
}

func preserveInstalledBinary(t *testing.T) {
	t.Helper()
	data, err := os.ReadFile(installedBinary)
	if err != nil {
		t.Fatalf("reading %s: %v", installedBinary, err)
	}
	backup, err := os.CreateTemp("", "peers-test-binary-*")
	if err != nil {
		t.Fatalf("creating a backup of %s: %v", installedBinary, err)
	}
	if _, err := backup.Write(data); err != nil {
		_ = backup.Close()
		_ = os.Remove(backup.Name())
		t.Fatalf("writing the backup of %s: %v", installedBinary, err)
	}
	if err := backup.Close(); err != nil {
		_ = os.Remove(backup.Name())
		t.Fatalf("closing the backup of %s: %v", installedBinary, err)
	}
	t.Cleanup(func() {
		if err := sudo("install", "-o", "root", "-g", "wheel", "-m", "0755", backup.Name(), installedBinary); err != nil {
			t.Errorf("restoring %s: %v", installedBinary, err)
		}
		_ = os.Remove(backup.Name())
	})
}

func installRootBinary(t *testing.T, src, dst string) {
	t.Helper()
	if err := sudo("install", "-o", "root", "-g", "wheel", "-m", "0755", src, dst); err != nil {
		t.Fatalf("installing %s as %s: %v", src, dst, err)
	}
}

func binaryOwner(path string) (uid, gid uint32, mode os.FileMode, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, fmt.Errorf("no owner information for %s", path)
	}
	return st.Uid, st.Gid, info.Mode().Perm(), nil
}

func binaryVersion(t *testing.T, path string) string {
	t.Helper()
	res := run(t, path, "--version")
	if res.exitCode != 0 {
		t.Fatalf("%s --version: exit %d\nstderr: %s", path, res.exitCode, res.stderr)
	}
	return strings.TrimSpace(res.stdout)
}

func addDaemonEnvironment(t *testing.T, key, value string) {
	t.Helper()
	const plistBuddy = "/usr/libexec/PlistBuddy"
	if err := sudo(plistBuddy, "-c", "Add :EnvironmentVariables dict", daemonPlist); err != nil {
		t.Fatalf("adding EnvironmentVariables to %s: %v", daemonPlist, err)
	}
	if err := sudo(plistBuddy, "-c", "Add :EnvironmentVariables:"+key+" string "+value, daemonPlist); err != nil {
		t.Fatalf("adding %s to %s: %v", key, daemonPlist, err)
	}
}

func waitForBrokerVersion(t *testing.T, want string) {
	t.Helper()
	waitFor(t, updateRestartBound, "the system Broker to report version "+want, func() (bool, string) {
		got, err := systemBrokerVersion()
		if err != nil {
			return false, err.Error()
		}
		return got == want, "the Broker reports " + got
	})
}

func systemBrokerVersion() (string, error) {
	binary, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locating the test binary: %w", err)
	}
	cmd := exec.Command("sudo", "-n", "env", roleEnv+"="+systemUpdateStatusRole, binary)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("system-update-status: %w: %s", err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", fmt.Errorf("system-update-status: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func runWithin(t *testing.T, bound time.Duration, name string, args ...string) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	res := result{stdout: stdout.String(), stderr: stderr.String()}
	if err == nil {
		return res
	}
	if ctx.Err() != nil {
		t.Fatalf("running %s %v did not finish within %s\nstdout:\n%s\nstderr:\n%s",
			name, args, bound, res.stdout, res.stderr)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s %v: %v", name, args, err)
	}
	res.exitCode = exitErr.ExitCode()
	return res
}

func buildBinaryWithDaemonEnvironment(t *testing.T, version string) string {
	t.Helper()

	root := harness.ModuleRoot(t)
	unitsPath := filepath.Join(root, "internal", "broker", "service", "units.go")
	data, err := os.ReadFile(unitsPath)
	if err != nil {
		t.Fatalf("reading %s: %v", unitsPath, err)
	}

	// The daemon template is the only one with Umask 18; the agent template
	// uses 63.
	const original = "\t<key>Umask</key>\n\t<integer>18</integer>\n"
	replacement := original +
		"\t<key>EnvironmentVariables</key>\n\t<dict>\n" +
		"\t\t<key>" + daemonNextEnvName + "</key>\n" +
		"\t\t<string>" + daemonNextEnvValue + "</string>\n" +
		"\t</dict>\n"
	if got := strings.Count(string(data), original); got != 1 {
		t.Fatalf("%s has %d matches of the daemon Umask entry, want exactly 1; the next-version build cannot change the plist format", unitsPath, got)
	}
	modified := strings.Replace(string(data), original, replacement, 1)

	dir := t.TempDir()
	modifiedPath := filepath.Join(dir, "units.go")
	if err := os.WriteFile(modifiedPath, []byte(modified), 0o644); err != nil {
		t.Fatalf("writing the overlaid units.go: %v", err)
	}
	overlayPath := filepath.Join(dir, "overlay.json")
	overlay, err := json.Marshal(struct {
		Replace map[string]string `json:"Replace"`
	}{Replace: map[string]string{unitsPath: modifiedPath}})
	if err != nil {
		t.Fatalf("encoding the build overlay: %v", err)
	}
	if err := os.WriteFile(overlayPath, overlay, 0o644); err != nil {
		t.Fatalf("writing the build overlay: %v", err)
	}

	return harness.BuildBinaryVersionOverlay(t, version, "darwin", runtime.GOARCH, overlayPath)
}

var launchdArgumentsPattern = regexp.MustCompile(`(?ms)^\s*arguments = \{\n(.*?)^\s*\}`)

// launchdEnvironmentPattern captures the environment block of launchctl print.
// The line must start with "environment", so the "inherited environment" and
// "default environment" blocks above it do not match.
var launchdEnvironmentPattern = regexp.MustCompile(`(?ms)^\s*environment = \{\n(.*?)^\s*\}`)

// waitForLaunchdArguments polls within the restart bound because the reload is
// asynchronous: the detached helper runs after the old Broker exits.
func waitForLaunchdArguments(t *testing.T, domain string, want []string) {
	t.Helper()
	waitFor(t, updateRestartBound, "launchd to load the rewritten ProgramArguments", func() (bool, string) {
		out, loaded := launchctlPrint(domain)
		if !loaded {
			return false, "the job is not loaded"
		}
		got := launchdArguments(out)
		return slices.Equal(got, want), fmt.Sprintf("arguments = %q", got)
	})
}

func launchdArguments(out string) []string {
	match := launchdArgumentsPattern.FindStringSubmatch(out)
	if match == nil {
		return nil
	}
	var args []string
	for _, line := range strings.Split(match[1], "\n") {
		if line = strings.TrimSpace(line); line != "" {
			args = append(args, line)
		}
	}
	return args
}

func launchdEnvironment(out string) map[string]string {
	match := launchdEnvironmentPattern.FindStringSubmatch(out)
	if match == nil {
		return nil
	}
	env := map[string]string{}
	for _, line := range strings.Split(match[1], "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), " => ")
		if ok {
			env[name] = value
		}
	}
	return env
}

// waitForLaunchdEnvironment polls within the restart bound because the reload
// is asynchronous.
func waitForLaunchdEnvironment(t *testing.T, domain, name, want string) {
	t.Helper()
	waitFor(t, updateRestartBound, "launchd to load "+name+"="+want, func() (bool, string) {
		out, loaded := launchctlPrint(domain)
		if !loaded {
			return false, "the job is not loaded"
		}
		env := launchdEnvironment(out)
		return env[name] == want, fmt.Sprintf("%s = %q", name, env[name])
	})
}
