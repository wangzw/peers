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
	// installedBinary is the root-owned copy of the product the tests run.
	// The system installation refuses binaries outside a root-owned
	// directory, and a user installation accepts the same copy.
	installedBinary = "/usr/local/libexec/peers-test"
	// e2eBinary is a root-owned copy of this test binary. The multi-group test
	// runs a role as another account, which can execute this copy but may not
	// be able to traverse the go test temporary directory.
	e2eBinary = "/usr/local/libexec/peers-e2e"
	// serviceLabel is the launchd label of both service levels.
	serviceLabel = "io.github.wangzw.peers"
	// daemonPlist is where the system installation writes its plist.
	daemonPlist = "/Library/LaunchDaemons/" + serviceLabel + ".plist"
	// systemSocketPath is the system Broker's local socket.
	systemSocketPath = "/var/run/peers/broker.sock"

	// roleEnv selects an internal helper role of the test binary. The role runs
	// before the build step, so it also works when sudo re-executes the binary.
	roleEnv = "PEERS_DARWIN_E2E_ROLE"
	// systemPairKeyRole creates a one-time pairing key on the system Broker and
	// prints it. It runs as root, where the system configuration file grants
	// write permission without changing the file mode.
	systemPairKeyRole = "system-pair-key"
	// systemUpdateStatusRole reads the version the system Broker runs and prints
	// it. It runs as root, where the system socket admits the caller.
	systemUpdateStatusRole = "system-update-status"

	// serviceStartBound bounds the wait for launchd to start the Broker after
	// install. RunAtLoad starts it immediately; the bound leaves room for a
	// loaded runner.
	serviceStartBound = 30 * time.Second
	// restartBound bounds the wait for launchd to relaunch a SIGKILLed Broker.
	// launchd applies its default ThrottleInterval of 10 seconds before it
	// relaunches a process that exited too quickly, so this bound is set by
	// that throttle, not by the unit.
	restartBound = 30 * time.Second
	// serviceStopBound bounds the wait for the Broker to disappear after
	// uninstall. It covers launchd's default 5-second exit timeout for the job
	// plus the Broker's SIGTERM shutdown, with margin for a loaded runner.
	serviceStopBound = 20 * time.Second
	// pollInterval separates two probes of launchd state.
	pollInterval = 250 * time.Millisecond
	// eventTimeout is the package fallback for an event the tests cannot force:
	// a local-interface call answering, the uninstall prompt appearing, or a
	// role subprocess printing its result. It only decides how long a wait that
	// never completes takes to fail; the success path returns on the event, so
	// a larger value costs nothing.
	eventTimeout = 30 * time.Second
)

// pidPattern matches the "pid = <n>" line of launchctl print.
var pidPattern = regexp.MustCompile(`(?m)^\s*pid = (\d+)$`)

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
	// These tests uninstall the real io.github.wangzw.peers services. A
	// developer machine may have them installed for everyday use, so refuse to
	// run outside CI unless the caller opts in explicitly.
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

// runSystemPairKeyRole creates a one-time pairing key on the system Broker and
// prints it. The test runs this binary through sudo so the system Broker's
// configuration-file write permission is satisfied by root.
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

// runSystemUpdateStatusRole prints the version the system Broker reports. The
// test runs this binary as root so the system socket admits the caller.
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

// buildAndInstallBinary builds cmd/peers as the current user and installs
// it into the root-owned directory the system test needs.
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

// installE2EBinary copies this test binary to a root-owned path so another
// account can execute a role from it.
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

// removeInstalledBinary removes the copies installed by TestMain.
func removeInstalledBinary() {
	_ = sudo("rm", "-f", installedBinary)
	_ = sudo("rm", "-f", e2eBinary)
}

// repoRoot returns the repository root from this file's location.
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot locate the source file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..")), nil
}

// sudo runs one command through sudo without a password prompt.
func sudo(args ...string) error {
	cmd := exec.Command("sudo", append([]string{"-n"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sudo %v: %w: %s", args, err, string(out))
	}
	return nil
}

// result is the outcome of one command run to completion.
type result struct {
	stdout   string
	stderr   string
	exitCode int
}

// run runs name with args and returns its output and exit code. A process that
// cannot be started fails the test.
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

// launchctlPrint returns the output of launchctl print for domain/label and
// whether the service is loaded.
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

// launchdPID returns the running pid of the service in domain, or 0.
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

// waitForPID waits until launchd reports a running pid for domain.
func waitForPID(t *testing.T, domain string, bound time.Duration) int {
	t.Helper()
	var pid int
	waitFor(t, bound, "launchd to start "+serviceLabel+" in "+domain, func() (bool, string) {
		pid = launchdPID(domain)
		return pid != 0, fmt.Sprintf("pid = %d", pid)
	})
	return pid
}

// waitForNewPID waits until launchd reports a running pid other than old.
func waitForNewPID(t *testing.T, domain string, old int, bound time.Duration) int {
	t.Helper()
	var pid int
	waitFor(t, bound, fmt.Sprintf("launchd to replace pid %d in %s", old, domain), func() (bool, string) {
		pid = launchdPID(domain)
		return pid != 0 && pid != old, fmt.Sprintf("pid = %d, want a pid different from %d", pid, old)
	})
	return pid
}

// waitForGone waits until launchd no longer reports a pid for domain.
func waitForGone(t *testing.T, domain string, bound time.Duration) {
	t.Helper()
	waitFor(t, bound, "the service to disappear from "+domain, func() (bool, string) {
		pid := launchdPID(domain)
		return pid == 0, fmt.Sprintf("pid = %d", pid)
	})
}

// waitFor calls check until it reports true or bound elapses.
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

// processAlive reports whether pid is still running.
func processAlive(pid int) bool {
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "pid=").Output()
	return err == nil && len(out) > 0
}

// killPID kills pid with SIGKILL, through sudo for a root-owned process.
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

// bootout unloads the service from domain, ignoring the error launchd reports
// when it is not loaded.
func bootout(domain string) {
	target := domain + "/" + serviceLabel
	if domain == "system" {
		_ = sudo("launchctl", "bootout", target)
		return
	}
	_ = exec.Command("launchctl", "bootout", target).Run()
}
