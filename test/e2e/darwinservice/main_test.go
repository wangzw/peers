//go:build e2e_darwin

// Package darwinservice exercises the launchd installation on macOS: the
// install step starts the Broker, launchd restarts it after a crash, and
// uninstall stops it and removes the plist.
package darwinservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/node"
)

const (
	// installedBinary is the root-owned copy of the product the tests run: the
	// system installation refuses a binary outside a root-owned directory.
	installedBinary  = "/usr/local/libexec/peers-test"
	e2eBinary        = "/usr/local/libexec/peers-e2e"
	serviceLabel     = "io.github.wangzw.peers"
	daemonPlist      = "/Library/LaunchDaemons/" + serviceLabel + ".plist"
	systemSocketPath = "/var/run/peers/broker.sock"

	// roleEnv selects a helper role of the test binary; TestMain dispatches it
	// before the build so a sudo re-execution does not build again.
	roleEnv                = "PEERS_DARWIN_E2E_ROLE"
	systemPairKeyRole      = "system-pair-key"
	systemUpdateStatusRole = "system-update-status"

	// serviceStartBound covers launchd's RunAtLoad start after install, with
	// room for a loaded runner.
	serviceStartBound = 30 * time.Second
	// restartBound is set by launchd's default 10-second ThrottleInterval, not
	// by the unit.
	restartBound = 30 * time.Second
	// serviceStopBound is launchd's default 5-second exit timeout plus the
	// Broker's SIGTERM shutdown, with margin for a loaded runner.
	serviceStopBound = 20 * time.Second
	pollInterval     = 250 * time.Millisecond
	eventTimeout     = 30 * time.Second
)

var pidPattern = regexp.MustCompile(`(?m)^\s*pid = (\d+)$`)

// TestMain dispatches the helper roles, refuses service resets outside CI,
// installs the test binaries and runs the tests.
func TestMain(m *testing.M) {
	if runtime.GOOS != "darwin" {
		fmt.Fprintln(os.Stderr, "darwinservice: these tests run on macOS only")
		os.Exit(1)
	}
	if os.Getenv(roleEnv) == systemPairKeyRole {
		os.Exit(runSystemPairKeyRole())
	}
	switch os.Getenv(roleEnv) {
	case accessCheckRole:
		os.Exit(runAccessCheckRole())
	case getConfigRole:
		os.Exit(runGetConfigRole())
	case helperRegisterRole:
		os.Exit(runHelperRegisterRole())
	case helperRegisterWatchRole:
		os.Exit(runHelperRegisterWatchRole())
	case systemUpdateStatusRole:
		os.Exit(runSystemUpdateStatusRole())
	}
	if os.Getenv("CI") == "" && os.Getenv("PEERS_E2E_ALLOW_SERVICE_RESET") != "1" {
		fmt.Fprintln(os.Stderr, "darwinservice: refusing to uninstall a possibly real service outside CI; set PEERS_E2E_ALLOW_SERVICE_RESET=1 to allow it")
		os.Exit(1)
	}
	if err := buildAndInstallBinary(); err != nil {
		fmt.Fprintf(os.Stderr, "darwinservice: install the test binary: %v\n", err)
		os.Exit(1)
	}
	if err := installE2EBinary(); err != nil {
		fmt.Fprintf(os.Stderr, "darwinservice: install the e2e binary: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	removeInstalledBinary()
	os.Exit(code)
}

func runSystemPairKeyRole() int {
	oracle, err := node.DialOracle(systemSocketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "system-pair-key: %v\n", err)
		return 1
	}
	defer func() { _ = oracle.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	resp, err := oracle.CreatePairingKey(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "system-pair-key: %v\n", err)
		return 1
	}
	fmt.Println(resp.GetKey())
	return 0
}

func runSystemUpdateStatusRole() int {
	oracle, err := node.DialOracle(systemSocketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "system-update-status: %v\n", err)
		return 1
	}
	defer func() { _ = oracle.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	resp, err := oracle.UpdateStatus(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "system-update-status: %v\n", err)
		return 1
	}
	fmt.Println(resp.GetCurrentVersion())
	return 0
}

func buildAndInstallBinary() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	tmp, err := os.MkdirTemp("", "peers-darwin-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	built := filepath.Join(tmp, "peers")
	build := exec.Command("go", "build", "-o", built, "./cmd/peers")
	build.Dir = root
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("go build ./cmd/peers: %w", err)
	}
	if err := sudo("mkdir", "-p", filepath.Dir(installedBinary)); err != nil {
		return err
	}
	return sudo("install", "-o", "root", "-g", "wheel", "-m", "0755", built, installedBinary)
}

func installE2EBinary() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if err := sudo("mkdir", "-p", filepath.Dir(e2eBinary)); err != nil {
		return err
	}
	return sudo("install", "-o", "root", "-g", "wheel", "-m", "0755", exe, e2eBinary)
}

func removeInstalledBinary() {
	_ = sudo("rm", "-f", installedBinary)
	_ = sudo("rm", "-f", e2eBinary)
}

func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot locate the source file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..")), nil
}

func sudo(args ...string) error {
	cmd := exec.Command("sudo", append([]string{"-n"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sudo %v: %w: %s", args, err, string(out))
	}
	return nil
}

type result struct {
	stdout   string
	stderr   string
	exitCode int
}

func run(t *testing.T, name string, args ...string) result {
	t.Helper()
	cmd := exec.Command(name, args...)
	out, err := cmd.Output()
	res := result{stdout: string(out)}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s %v: %v", name, args, err)
	}
	res.exitCode = exitErr.ExitCode()
	res.stderr = string(exitErr.Stderr)
	return res
}

func launchctlPrint(domain string) (string, bool) {
	target := domain + "/" + serviceLabel
	var cmd *exec.Cmd
	if domain == "system" {
		cmd = exec.Command("sudo", "-n", "launchctl", "print", target)
	} else {
		cmd = exec.Command("launchctl", "print", target)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err == nil
}

func launchdPID(domain string) int {
	out, loaded := launchctlPrint(domain)
	if !loaded {
		return 0
	}
	match := pidPattern.FindStringSubmatch(out)
	if match == nil {
		return 0
	}
	pid, err := strconv.Atoi(match[1])
	if err != nil {
		return 0
	}
	return pid
}

func waitForPID(t *testing.T, domain string, bound time.Duration) int {
	t.Helper()
	var pid int
	waitFor(t, bound, "launchd to start "+serviceLabel+" in "+domain, func() (bool, string) {
		pid = launchdPID(domain)
		return pid != 0, fmt.Sprintf("pid = %d", pid)
	})
	return pid
}

func waitForNewPID(t *testing.T, domain string, old int, bound time.Duration) int {
	t.Helper()
	var pid int
	waitFor(t, bound, fmt.Sprintf("launchd to replace pid %d in %s", old, domain), func() (bool, string) {
		pid = launchdPID(domain)
		return pid != 0 && pid != old, fmt.Sprintf("pid = %d, want a pid different from %d", pid, old)
	})
	return pid
}

func waitForGone(t *testing.T, domain string, bound time.Duration) {
	t.Helper()
	waitFor(t, bound, "the service to disappear from "+domain, func() (bool, string) {
		pid := launchdPID(domain)
		return pid == 0, fmt.Sprintf("pid = %d", pid)
	})
}

func waitFor(t *testing.T, bound time.Duration, what string, check func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		ok, detail := check()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s: %s", bound, what, detail)
		}
		time.Sleep(pollInterval)
	}
}

func processAlive(pid int) bool {
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "pid=").Output()
	return err == nil && len(out) > 0
}

func killPID(t *testing.T, pid int, root bool) {
	t.Helper()
	var err error
	if root {
		err = sudo("kill", "-9", strconv.Itoa(pid))
	} else {
		err = exec.Command("kill", "-9", strconv.Itoa(pid)).Run()
	}
	// The process may have exited between sampling the pid and killing it.
	if err != nil && processAlive(pid) {
		t.Fatalf("killing pid %d: %v", pid, err)
	}
}

func bootout(domain string) {
	target := domain + "/" + serviceLabel
	if domain == "system" {
		_ = sudo("launchctl", "bootout", target)
		return
	}
	_ = exec.Command("launchctl", "bootout", target).Run()
}
