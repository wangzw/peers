//go:build e2e_darwin

package darwinservice

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const uninstallPromptBound = eventTimeout

type pollBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *pollBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *pollBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func uninstallUserPlist() string {
	return filepath.Join(os.Getenv("HOME"), "Library", "LaunchAgents", serviceLabel+".plist")
}

func uninstallUserDataDir() string {
	return filepath.Join(os.Getenv("HOME"), ".peers")
}

func uninstallUserLogDir() string {
	return filepath.Join(os.Getenv("HOME"), "Library", "Logs", "peers")
}

func uninstallRuntimeDir() string {
	return fmt.Sprintf("/tmp/peers-%d", os.Getuid())
}

// copyUninstallBinary copies the installed product to a path the test account
// owns. The copy is named peers because uninstall removes only a binary with
// that name.
func copyUninstallBinary(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(os.Getenv("HOME"), "uninstall-test")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	dst := filepath.Join(dir, "peers")
	data, err := os.ReadFile(installedBinary)
	if err != nil {
		t.Fatalf("reading %s: %v", installedBinary, err)
	}
	if err := os.WriteFile(dst, data, 0o755); err != nil {
		t.Fatalf("writing %s: %v", dst, err)
	}
	if err := os.Chmod(dst, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", dst, err)
	}
	return dst
}

// runUninstallWithAnswer runs the product in a pseudo-terminal and answers its
// first question with answer. The product asks on /dev/tty and script does not
// reliably pass the exit status, so this runs it under script and reads the
// status from a file.
func runUninstallWithAnswer(t *testing.T, answer, name string, args ...string) result {
	t.Helper()
	statusFile := filepath.Join(t.TempDir(), "status")
	argv := append([]string{name}, args...)
	quoted := make([]string, len(argv))
	for i, arg := range argv {
		quoted[i] = shellQuote(arg)
	}
	inner := strings.Join(quoted, " ") + "; printf '%s' $? > " + shellQuote(statusFile)

	cmd := exec.Command("/usr/bin/script", "-q", "/dev/null", "/bin/sh", "-c", inner)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("opening script's stdin: %v", err)
	}
	output := new(pollBuffer)
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", name, err)
	}

	deadline := time.Now().Add(uninstallPromptBound)
	for !strings.Contains(output.String(), "[y/N]") && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := io.WriteString(stdin, answer); err != nil {
		t.Logf("writing the answer to %s: %v", name, err)
	}
	_ = stdin.Close()
	_ = cmd.Wait()

	data, err := os.ReadFile(statusFile)
	if err != nil {
		t.Fatalf("reading the exit status of %s: %v\n%s", name, err, output.String())
	}
	code, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("exit status %q of %s is not a number: %v", strings.TrimSpace(string(data)), name, err)
	}
	return result{stdout: output.String(), exitCode: code}
}

func shellQuote(arg string) string {
	if arg != "" && !strings.ContainsAny(arg, " \t\n'\"\\$`;&|<>()*?[]{}!~#") {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

func waitUninstallSocket(t *testing.T) {
	t.Helper()
	socket := filepath.Join(uninstallRuntimeDir(), "broker.sock")
	waitFor(t, serviceStartBound, "the user Broker socket "+socket, func() (bool, string) {
		_, err := os.Stat(socket)
		return err == nil, fmt.Sprint(err)
	})
}

func requireUninstallMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s still exists: %v", path, err)
	}
}

func requireUninstallPresent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("%s is missing: %v", path, err)
	}
}

func assertPeerMCPNotRegistered(t *testing.T) {
	t.Helper()
	claude, err := exec.LookPath("claude")
	if err != nil {
		t.Log("claude is not installed; skipping the MCP registration check")
		return
	}
	if out, err := exec.Command(claude, "mcp", "get", "peer").CombinedOutput(); err == nil {
		t.Errorf("claude mcp get peer still reports a registration:\n%s", out)
	}
}

// TestUninstallREQBrokerConfig051UserLevel checks that a user uninstall keeps
// data by default, removes it with --purge, and leaves no launchd job.
func TestUninstallREQBrokerConfig051UserLevel(t *testing.T) {
	resetUserService(t)
	t.Cleanup(func() { resetUserService(t) })

	t.Run("REQ-broker-config-051: a user uninstall keeps data by default and --purge removes it", func(t *testing.T) {
		domain := userDomain(t)
		binary := copyUninstallBinary(t)

		res := run(t, binary, "service", "install")
		if res.exitCode != 0 {
			t.Fatalf("service install: exit %d\nstdout: %s\nstderr: %s", res.exitCode, res.stdout, res.stderr)
		}
		if _, loaded := launchctlPrint(domain); !loaded {
			t.Fatalf("launchd does not have %s loaded after install", serviceLabel)
		}
		waitUninstallSocket(t)

		if err := os.MkdirAll(uninstallUserDataDir(), 0o700); err != nil {
			t.Fatalf("creating %s: %v", uninstallUserDataDir(), err)
		}
		sentinel := filepath.Join(uninstallUserDataDir(), "uninstall-keep-sentinel")
		if err := os.WriteFile(sentinel, []byte("kept\n"), 0o600); err != nil {
			t.Fatalf("writing %s: %v", sentinel, err)
		}

		kept := runUninstallWithAnswer(t, "\n", binary, "uninstall")
		if kept.exitCode != 0 {
			t.Fatalf("uninstall: exit %d\n%s", kept.exitCode, kept.stdout)
		}
		if !strings.Contains(kept.stdout, "Also remove configuration, identity, audit and service logs? [y/N]") {
			t.Errorf("output does not contain the data question:\n%s", kept.stdout)
		}
		if !strings.Contains(kept.stdout, "kept configuration, data and logs") {
			t.Errorf("output does not say the data was kept:\n%s", kept.stdout)
		}
		if _, loaded := launchctlPrint(domain); loaded {
			t.Errorf("launchd still has %s loaded after uninstall", serviceLabel)
		}
		requireUninstallMissing(t, uninstallUserPlist())
		requireUninstallMissing(t, uninstallRuntimeDir())
		requireUninstallMissing(t, binary)
		requireUninstallPresent(t, sentinel)
		requireUninstallPresent(t, uninstallUserLogDir())
		assertPeerMCPNotRegistered(t)

		binary = copyUninstallBinary(t)
		res = run(t, binary, "service", "install")
		if res.exitCode != 0 {
			t.Fatalf("second service install: exit %d\nstdout: %s\nstderr: %s", res.exitCode, res.stdout, res.stderr)
		}
		waitUninstallSocket(t)

		purged := run(t, binary, "uninstall", "--purge", "--yes")
		if purged.exitCode != 0 {
			t.Fatalf("uninstall --purge: exit %d\nstdout: %s\nstderr: %s", purged.exitCode, purged.stdout, purged.stderr)
		}
		if strings.Contains(purged.stdout, "Also remove configuration") {
			t.Errorf("--purge asked the data question:\n%s", purged.stdout)
		}
		if _, loaded := launchctlPrint(domain); loaded {
			t.Errorf("launchd still has %s loaded after --purge", serviceLabel)
		}
		for _, gone := range []string{
			uninstallUserPlist(),
			uninstallRuntimeDir(),
			uninstallUserDataDir(),
			uninstallUserLogDir(),
			binary,
		} {
			requireUninstallMissing(t, gone)
		}
		assertPeerMCPNotRegistered(t)

		again := run(t, installedBinary, "uninstall")
		if again.exitCode != 0 {
			t.Fatalf("second uninstall: exit %d\nstdout: %s\nstderr: %s", again.exitCode, again.stdout, again.stderr)
		}
		if !strings.Contains(again.stdout, "not installed") {
			t.Errorf("second uninstall output = %q, want it to report no installation", again.stdout)
		}
	})
}
