//go:build e2e

package e2e

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// uninstallSystemCopy is named peers because uninstall only removes a binary
	// with that name.
	uninstallSystemCopy = "/usr/local/libexec/peers"
	uninstallPeerBinary = "/home/peer/bin/peers"

	uninstallSystemUnit      = "/etc/systemd/system/peers.service"
	uninstallSystemDropIn    = "/etc/systemd/system/peers.service.d"
	uninstallSystemConfigDir = "/etc/peers"
	uninstallSystemDataDir   = "/var/lib/peers"
	uninstallSystemLogDir    = "/var/log/peers"
	uninstallSystemSocket    = "/run/peers/broker.sock"

	uninstallPeerHome    = "/home/peer"
	uninstallUserUnit    = uninstallPeerHome + "/.config/systemd/user/peers.service"
	uninstallUserDropIn  = uninstallPeerHome + "/.config/systemd/user/peers.service.d"
	uninstallUserDataDir = uninstallPeerHome + "/.peers"

	uninstallFakeClaudePath = "/usr/local/bin/claude"
	// uninstallFakeClaudeState is world-writable because the registration is
	// read and removed by both root and the ordinary account.
	uninstallFakeClaudeState = "/var/lib/peers-uninstall-state"
	uninstallSudoersPath     = "/etc/sudoers.d/peers-uninstall"
	// uninstallPath includes /usr/local/bin, where the fake claude lives.
	uninstallPath        = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
	uninstallPromptBound = harness.EventTimeout
)

// uninstallFakeClaudeScript prints mcp get output in the real command's shape:
// the command path on its own line, without the arguments.
const uninstallFakeClaudeScript = `#!/bin/sh
state=/var/lib/peers-uninstall-state
registered="$state/registered-peer"
mkdir -p "$state" 2>/dev/null || true

if [ "$1" = "mcp" ] && [ "$2" = "get" ] && [ "$3" = "peer" ]; then
	if [ -f "$registered" ]; then
		printf 'peer:\n  Scope: user\n  Command: %s\n  Args: mcp\n' "$(cat "$registered")"
		exit 0
	fi
	printf 'No MCP server found with name: peer\n' >&2
	exit 1
fi

if [ "$1" = "mcp" ] && [ "$2" = "remove" ] && [ "$3" = "-s" ] && [ "$4" = "user" ] && [ "$5" = "peer" ]; then
	rm -f "$registered"
	exit 0
fi

exit 0
`

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// runWithAnswer runs name in a pseudo-terminal because the program asks on /dev/tty,
// and writes answer once the question appears.
func runWithAnswer(t *testing.T, account string, env []string, answer, name string, args ...string) syscontainer.Result {
	t.Helper()

	cmdline := shellCommand(append([]string{name}, args...)...)
	cmd := syscontainer.Command(account, env, "script", "-qec", cmdline, "/dev/null")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("opening script's stdin: %v", err)
	}
	output := new(lockedBuffer)
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s as %s: %v", name, account, err)
	}

	// A program that fails before asking is waited for instead of blocking the
	// test.
	deadline := time.Now().Add(uninstallPromptBound)
	for !strings.Contains(output.String(), "[y/N]") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := io.WriteString(stdin, answer); err != nil {
		t.Logf("writing the answer to %s: %v", name, err)
	}
	_ = stdin.Close()

	err = cmd.Wait()
	res := syscontainer.Result{Stdout: output.String()}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s as %s: %v", name, account, err)
	}
	res.ExitCode = exitErr.ExitCode()
	return res
}

func shellCommand(args ...string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = shellQuote(arg)
	}
	return strings.Join(quoted, " ")
}

func shellQuote(arg string) string {
	if arg != "" && !strings.ContainsAny(arg, " \t\n'\"\\$`;&|<>()*?[]{}!~#") {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

func uninstallAccountIDs(t *testing.T, account string) (uint32, uint32) {
	t.Helper()
	u, err := user.Lookup(account)
	if err != nil {
		t.Fatalf("looking up account %q: %v", account, err)
	}
	uid, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		t.Fatalf("account %q has a non-numeric uid %q", account, u.Uid)
	}
	gid, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		t.Fatalf("account %q has a non-numeric gid %q", account, u.Gid)
	}
	return uint32(uid), uint32(gid)
}

func installUninstallBinary(t *testing.T, src, dst string, uid, gid uint32) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(dst), err)
	}
	if err := os.Chmod(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("chmod %s: %v", filepath.Dir(dst), err)
	}
	if err := os.Chown(filepath.Dir(dst), int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", filepath.Dir(dst), err)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("reading %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o755); err != nil {
		t.Fatalf("writing %s: %v", dst, err)
	}
	if err := os.Chmod(dst, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", dst, err)
	}
	if err := os.Chown(dst, int(uid), int(gid)); err != nil {
		t.Fatalf("chown %s: %v", dst, err)
	}
}

func installUninstallFakeClaude(t *testing.T, binary string) {
	t.Helper()
	if err := os.RemoveAll(uninstallFakeClaudeState); err != nil {
		t.Fatalf("clearing %s: %v", uninstallFakeClaudeState, err)
	}
	if err := os.MkdirAll(uninstallFakeClaudeState, 0o777); err != nil {
		t.Fatalf("creating %s: %v", uninstallFakeClaudeState, err)
	}
	if err := os.Chmod(uninstallFakeClaudeState, 0o777); err != nil {
		t.Fatalf("chmod %s: %v", uninstallFakeClaudeState, err)
	}
	if err := os.WriteFile(uninstallFakeClaudePath, []byte(uninstallFakeClaudeScript), 0o755); err != nil {
		t.Fatalf("writing the fake claude: %v", err)
	}
	if err := os.Chmod(uninstallFakeClaudePath, 0o755); err != nil {
		t.Fatalf("chmod the fake claude: %v", err)
	}
	if binary != "" {
		setUninstallMCPRegistration(t, binary)
	}
}

func setUninstallMCPRegistration(t *testing.T, binary string) {
	t.Helper()
	path := filepath.Join(uninstallFakeClaudeState, "registered-peer")
	if err := os.WriteFile(path, []byte(binary+"\n"), 0o666); err != nil {
		t.Fatalf("writing the MCP registration: %v", err)
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatalf("chmod the MCP registration: %v", err)
	}
}

func uninstallMCPRegistered(t *testing.T) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(uninstallFakeClaudeState, "registered-peer"))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	t.Fatalf("inspecting the MCP registration: %v", err)
	return false
}

func uninstallUserEnv(uid uint32) []string {
	id := strconv.FormatUint(uint64(uid), 10)
	return []string{
		"PATH=" + uninstallPath,
		"XDG_RUNTIME_DIR=/run/user/" + id,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + id + "/bus",
	}
}

func startUninstallUserManager(t *testing.T, peerUID uint32) {
	t.Helper()
	mustRun(t, "root", nil, "loginctl", "enable-linger", "peer")
	mustRun(t, "root", nil, "systemctl", "start", "user@"+strconv.FormatUint(uint64(peerUID), 10)+".service")
	waitUserManager(t, peerUID)
}

func installUninstallUserService(t *testing.T, env []string, binary string) {
	t.Helper()
	res := runWithEnv(t, "peer", env, binary, "service", "install")
	if res.ExitCode != 0 {
		t.Fatalf("service install as peer: exit %d\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	syscontainer.WaitFor(t, "the peer Broker socket", func() bool {
		_, err := os.Stat(filepath.Join("/tmp", "peers-"+strconv.FormatUint(uint64(syscontainer.AccountUID(t, "peer")), 10), "broker.sock"))
		return err == nil
	})
}

func installUninstallSystemService(t *testing.T, binary string) {
	t.Helper()
	res := runWithEnv(t, "root", nil, binary, "service", "install", "--system")
	if res.ExitCode != 0 {
		t.Fatalf("service install --system: exit %d\nstdout: %s\nstderr: %s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	syscontainer.WaitFor(t, "the system Broker socket "+uninstallSystemSocket, func() bool {
		_, err := os.Stat(uninstallSystemSocket)
		return err == nil
	})
}

func requireMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s still exists: %v", path, err)
	}
}

func requirePresent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("%s is missing: %v", path, err)
	}
}

func uninstallIdentity(t *testing.T, dataDir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "identity.crt"))
	if err != nil {
		t.Fatalf("reading the identity in %s: %v", dataDir, err)
	}
	return data
}

func uninstallGroupExists(t *testing.T) bool {
	t.Helper()
	res := runWithEnv(t, "root", nil, "getent", "group", systemAdminGroup)
	return res.ExitCode == 0
}

func configureUninstallSudo(t *testing.T) {
	t.Helper()
	entry := "peer ALL=(ALL) NOPASSWD: ALL\nDefaults:peer !requiretty\n"
	if err := os.WriteFile(uninstallSudoersPath, []byte(entry), 0o440); err != nil {
		t.Fatalf("writing %s: %v", uninstallSudoersPath, err)
	}
	if err := os.Chmod(uninstallSudoersPath, 0o440); err != nil {
		t.Fatalf("chmod %s: %v", uninstallSudoersPath, err)
	}
}

// TestUninstallREQBrokerConfig051UserLevel checks that a user uninstall keeps the
// data by default, that a reinstall keeps the identity, and that --purge removes
// everything without asking.
func TestUninstallREQBrokerConfig051UserLevel(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("REQ-broker-config-051: a user uninstall keeps data by default and --purge removes it", func(t *testing.T) {
		peerUID := syscontainer.AccountUID(t, "peer")
		_, peerGID := uninstallAccountIDs(t, "peer")
		env := uninstallUserEnv(peerUID)
		startUninstallUserManager(t, peerUID)
		installUninstallFakeClaude(t, uninstallPeerBinary)
		installUninstallBinary(t, releaseBinary, uninstallPeerBinary, peerUID, peerGID)

		installUninstallUserService(t, env, uninstallPeerBinary)
		identity := uninstallIdentity(t, uninstallUserDataDir)

		res := runWithAnswer(t, "peer", env, "\n", uninstallPeerBinary, "uninstall")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall as peer: exit %d\n%s", res.ExitCode, res.Stdout)
		}
		for _, want := range []string{
			"Also remove configuration, identity, audit and service logs? [y/N]",
			"kept configuration, data and logs",
			"removed the binary " + uninstallPeerBinary,
		} {
			if !strings.Contains(res.Stdout, want) {
				t.Errorf("output does not contain %q:\n%s", want, res.Stdout)
			}
		}
		requireMissing(t, uninstallUserUnit)
		requireMissing(t, filepath.Join("/tmp", "peers-"+strconv.FormatUint(uint64(peerUID), 10)))
		requireMissing(t, uninstallPeerBinary)
		if uninstallMCPRegistered(t) {
			t.Error("the peer MCP registration was not removed")
		}
		for _, kept := range []string{
			filepath.Join(uninstallUserDataDir, "config.yaml"),
			filepath.Join(uninstallUserDataDir, "identity.crt"),
			filepath.Join(uninstallUserDataDir, "audit.log"),
		} {
			requirePresent(t, kept)
		}
		if procs := peerBrokers(peerUID); len(procs) != 0 {
			t.Errorf("peer Broker still runs after uninstall: %v", procs)
		}

		installUninstallBinary(t, releaseBinary, uninstallPeerBinary, peerUID, peerGID)
		installUninstallUserService(t, env, uninstallPeerBinary)
		if got := uninstallIdentity(t, uninstallUserDataDir); !bytes.Equal(got, identity) {
			t.Error("the Broker identity changed across uninstall and reinstall")
		}

		res = runWithEnv(t, "peer", env, uninstallPeerBinary, "uninstall", "--purge", "--yes")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall --purge as peer: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		if strings.Contains(res.Stdout, "Also remove configuration") {
			t.Errorf("--purge asked the data question:\n%s", res.Stdout)
		}
		for _, gone := range []string{
			uninstallUserUnit,
			uninstallUserDropIn,
			uninstallUserDataDir,
			uninstallPeerBinary,
		} {
			requireMissing(t, gone)
		}

		again := runWithEnv(t, "peer", env, releaseBinary, "uninstall")
		if again.ExitCode != 0 {
			t.Fatalf("second uninstall: exit %d\nstdout: %s\nstderr: %s",
				again.ExitCode, again.Stdout, again.Stderr)
		}
		if !strings.Contains(again.Stdout, "not installed") {
			t.Errorf("second uninstall output = %q, want it to report no installation", again.Stdout)
		}
	})
}

// TestUninstallREQBrokerConfig051SystemLevel checks the same uninstall behavior for
// a system installation.
func TestUninstallREQBrokerConfig051SystemLevel(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("REQ-broker-config-051: a system uninstall keeps data by default and --purge removes it", func(t *testing.T) {
		installUninstallBinary(t, releaseBinary, uninstallSystemCopy, 0, 0)
		installUninstallFakeClaude(t, "")
		installUninstallSystemService(t, uninstallSystemCopy)
		identity := uninstallIdentity(t, uninstallSystemDataDir)
		if !uninstallGroupExists(t) {
			t.Fatal("the admin group does not exist after the installation")
		}

		res := runWithAnswer(t, "root", nil, "\n", uninstallSystemCopy, "uninstall")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall as root: exit %d\n%s", res.ExitCode, res.Stdout)
		}
		for _, want := range []string{
			"Also remove configuration, identity, audit and service logs? [y/N]",
			"kept configuration, data and logs: " + uninstallSystemConfigDir,
		} {
			if !strings.Contains(res.Stdout, want) {
				t.Errorf("output does not contain %q:\n%s", want, res.Stdout)
			}
		}
		for _, kept := range []string{
			filepath.Join(uninstallSystemConfigDir, "config.yaml"),
			filepath.Join(uninstallSystemDataDir, "identity.crt"),
			filepath.Join(uninstallSystemLogDir, "audit.log"),
		} {
			requirePresent(t, kept)
		}
		requireMissing(t, uninstallSystemUnit)
		requireMissing(t, uninstallSystemSocket)
		requireMissing(t, uninstallSystemCopy)
		if !uninstallGroupExists(t) {
			t.Error("the admin group was removed although the data was kept")
		}

		installUninstallBinary(t, releaseBinary, uninstallSystemCopy, 0, 0)
		installUninstallSystemService(t, uninstallSystemCopy)
		if got := uninstallIdentity(t, uninstallSystemDataDir); !bytes.Equal(got, identity) {
			t.Error("the Broker identity changed across uninstall and reinstall")
		}

		res = runWithEnv(t, "root", nil, uninstallSystemCopy, "uninstall", "--purge", "--yes")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall --purge as root: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		if strings.Contains(res.Stdout, "Also remove configuration") {
			t.Errorf("--purge asked the data question:\n%s", res.Stdout)
		}
		for _, gone := range []string{
			uninstallSystemUnit,
			uninstallSystemDropIn,
			uninstallSystemConfigDir,
			uninstallSystemDataDir,
			uninstallSystemLogDir,
			uninstallSystemCopy,
		} {
			requireMissing(t, gone)
		}
		if uninstallGroupExists(t) {
			t.Error("the admin group still exists after --purge")
		}

		again := runWithEnv(t, "root", nil, releaseBinary, "uninstall")
		if again.ExitCode != 0 {
			t.Fatalf("second uninstall: exit %d\nstdout: %s\nstderr: %s",
				again.ExitCode, again.Stdout, again.Stderr)
		}
		if !strings.Contains(again.Stdout, "not installed") {
			t.Errorf("second uninstall output = %q, want it to report no installation", again.Stdout)
		}
	})
}

// TestUninstallREQBrokerConfig051UnrecordedGroup checks that --purge keeps an admin
// group the installation did not record creating, says why, and prints the manual
// removal command.
func TestUninstallREQBrokerConfig051UnrecordedGroup(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("REQ-broker-config-051: --purge keeps an unrecorded peers group", func(t *testing.T) {
		installUninstallBinary(t, releaseBinary, uninstallSystemCopy, 0, 0)
		installUninstallSystemService(t, uninstallSystemCopy)
		if !uninstallGroupExists(t) {
			t.Fatal("the admin group does not exist after the installation")
		}
		record := filepath.Join(uninstallSystemDataDir, "admin-group")
		if err := os.Remove(record); err != nil {
			t.Fatalf("removing the group record %s: %v", record, err)
		}

		res := runWithEnv(t, "root", nil, uninstallSystemCopy, "uninstall", "--purge", "--yes")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall --purge as root: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		if !uninstallGroupExists(t) {
			t.Error("the unrecorded peers group was removed after --purge")
		}
		deleteCommand := "groupdel " + systemAdminGroup
		if runtime.GOOS == "darwin" {
			deleteCommand = "dseditgroup -o delete " + systemAdminGroup
		}
		for _, want := range []string{
			"kept group " + systemAdminGroup + ": the installation did not record creating it",
			"remove it manually with: " + deleteCommand,
		} {
			if !strings.Contains(res.Stdout, want) {
				t.Errorf("output does not contain %q:\n%s", want, res.Stdout)
			}
		}
		requireMissing(t, uninstallSystemUnit)
		requireMissing(t, uninstallSystemCopy)
	})
}

// TestUninstallREQBrokerConfig051NoService checks uninstall for an installation made
// with --no-service, where only the MCP registration and the binary exist.
func TestUninstallREQBrokerConfig051NoService(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("REQ-broker-config-051: an installation without a service is uninstalled through its MCP registration", func(t *testing.T) {
		peerUID := syscontainer.AccountUID(t, "peer")
		_, peerGID := uninstallAccountIDs(t, "peer")
		env := uninstallUserEnv(peerUID)
		// Without a unit, uninstall treats the running executable as the
		// installation only when it sits at the default prefix and the MCP
		// registration points to it.
		noServiceBinary := "/home/peer/.local/bin/peers"
		installUninstallBinary(t, releaseBinary, noServiceBinary, peerUID, peerGID)
		installUninstallFakeClaude(t, noServiceBinary)
		if err := os.MkdirAll(uninstallUserDataDir, 0o700); err != nil {
			t.Fatalf("creating %s: %v", uninstallUserDataDir, err)
		}
		if err := os.Chown(uninstallUserDataDir, int(peerUID), int(peerGID)); err != nil {
			t.Fatalf("chown %s: %v", uninstallUserDataDir, err)
		}
		configPath := filepath.Join(uninstallUserDataDir, "config.yaml")
		if err := os.WriteFile(configPath, []byte("broker:\n  alias: kept\n"), 0o600); err != nil {
			t.Fatalf("writing %s: %v", configPath, err)
		}
		if err := os.Chown(configPath, int(peerUID), int(peerGID)); err != nil {
			t.Fatalf("chown %s: %v", configPath, err)
		}

		res := runWithAnswer(t, "peer", env, "\n", noServiceBinary, "uninstall")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall as peer: exit %d\n%s", res.ExitCode, res.Stdout)
		}
		for _, want := range []string{
			"the service was not installed",
			"Also remove configuration, identity, audit and service logs? [y/N]",
			"kept configuration, data and logs",
		} {
			if !strings.Contains(res.Stdout, want) {
				t.Errorf("output does not contain %q:\n%s", want, res.Stdout)
			}
		}
		requireMissing(t, noServiceBinary)
		requirePresent(t, configPath)
		if uninstallMCPRegistered(t) {
			t.Error("the peer MCP registration was not removed")
		}
	})
}

// TestUninstallREQBrokerConfig052NonRootSystemOnly checks that a non-root account
// whose host has only the system installation is refused with the complete sudo
// command, and that the command then removes it.
func TestUninstallREQBrokerConfig052NonRootSystemOnly(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("REQ-broker-config-052: a non-root account is refused and the printed sudo command succeeds", func(t *testing.T) {
		// The service is installed from the binary the peer will run, so uninstall
		// can prove it starts the same file before removing it.
		installUninstallFakeClaude(t, releaseBinary)
		configureUninstallSudo(t)
		installUninstallSystemService(t, releaseBinary)

		refused := runInstallScriptAs(t, "peer", nil, "/", releaseBinary, "uninstall", "--purge")
		if refused.ExitCode != 1 {
			t.Fatalf("refused uninstall: exit %d, want 1\nstdout:\n%s\nstderr:\n%s",
				refused.ExitCode, refused.Stdout, refused.Stderr)
		}
		for _, want := range []string{
			"only a system installation is present",
			"sudo peers uninstall --purge --yes",
		} {
			if !strings.Contains(refused.Stdout+refused.Stderr, want) {
				t.Errorf("output does not contain %q:\n%s\n%s", want, refused.Stdout, refused.Stderr)
			}
		}
		requirePresent(t, uninstallSystemUnit)
		requirePresent(t, releaseBinary)
		requirePresent(t, filepath.Join(uninstallSystemConfigDir, "config.yaml"))
		if !uninstallMCPRegistered(t) {
			t.Error("a refused uninstall removed the MCP registration")
		}

		accepted := runInstallScriptAs(t, "peer", nil, "/", "sudo", "-n", releaseBinary, "uninstall", "--purge", "--yes")
		if accepted.ExitCode != 0 {
			t.Fatalf("sudo uninstall: exit %d\nstdout:\n%s\nstderr:\n%s",
				accepted.ExitCode, accepted.Stdout, accepted.Stderr)
		}
		requireMissing(t, uninstallSystemUnit)
		requireMissing(t, releaseBinary)
		requireMissing(t, uninstallSystemConfigDir)
		if uninstallGroupExists(t) {
			t.Error("the admin group still exists after the system --purge")
		}
		if uninstallMCPRegistered(t) {
			t.Error("the peer MCP registration was not removed through sudo")
		}
	})
}

// TestUninstallREQBrokerConfig052TwoLevels checks that a non-root uninstall removes
// only the user installation and names the system command.
func TestUninstallREQBrokerConfig052TwoLevels(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("REQ-broker-config-052: a non-root uninstall removes only the user installation", func(t *testing.T) {
		peerUID := syscontainer.AccountUID(t, "peer")
		_, peerGID := uninstallAccountIDs(t, "peer")
		env := uninstallUserEnv(peerUID)

		installUninstallBinary(t, releaseBinary, uninstallSystemCopy, 0, 0)
		installUninstallSystemService(t, uninstallSystemCopy)
		startUninstallUserManager(t, peerUID)
		// The system Broker already listens on the default port, so the user
		// Broker needs its own.
		writeAccountFile(t, "peer", filepath.Join(uninstallUserDataDir, "config.yaml"), "broker:\n  listen: \":17445\"\n")
		installUninstallFakeClaude(t, "")
		installUninstallBinary(t, releaseBinary, uninstallPeerBinary, peerUID, peerGID)
		installUninstallUserService(t, env, uninstallPeerBinary)

		res := runWithAnswer(t, "peer", env, "\n", uninstallPeerBinary, "uninstall")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall as peer: exit %d\n%s", res.ExitCode, res.Stdout)
		}
		if !strings.Contains(res.Stdout, "sudo peers uninstall") {
			t.Errorf("output does not name the system command:\n%s", res.Stdout)
		}
		requireMissing(t, uninstallUserUnit)
		requireMissing(t, uninstallPeerBinary)
		requirePresent(t, uninstallSystemUnit)
		requirePresent(t, uninstallSystemSocket)
		requirePresent(t, filepath.Join(uninstallSystemConfigDir, "config.yaml"))
	})
}

// TestUninstallREQBrokerConfig054OtherAccountBroker checks that a system uninstall
// names another account's user Broker before it removes the shared binary.
func TestUninstallREQBrokerConfig054OtherAccountBroker(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	t.Run("REQ-broker-config-054: a system --purge lists another account's Broker", func(t *testing.T) {
		installUninstallBinary(t, releaseBinary, uninstallSystemCopy, 0, 0)
		installUninstallSystemService(t, uninstallSystemCopy)
		other := syscontainer.StartUserBroker(t, "other", "--listen", ":17444")

		res := runWithEnv(t, "root", nil, uninstallSystemCopy, "uninstall", "--purge", "--yes")
		if res.ExitCode != 0 {
			t.Fatalf("uninstall --purge as root: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, res.Stdout, res.Stderr)
		}
		for _, want := range []string{
			"Other accounts run a user Broker: other.",
			"will fail to start after " + uninstallSystemCopy + " is removed",
		} {
			if !strings.Contains(res.Stdout, want) {
				t.Errorf("output does not contain %q:\n%s", want, res.Stdout)
			}
		}
		requireMissing(t, uninstallSystemUnit)
		requireMissing(t, uninstallSystemCopy)
		other.Stop(t)
	})
}
