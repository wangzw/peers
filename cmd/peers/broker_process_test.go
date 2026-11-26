package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker"
)

func dialBroker(t *testing.T, addr string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, processTimeout)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = conn.Close()
}

// waitBrokerStarted polls until the child writes its broker_started record,
// which is emitted only after the account lock and both listeners are in
// place, so the record means the Broker serves this account.
func waitBrokerStarted(t *testing.T, p *brokerProc) bool {
	t.Helper()

	pid := p.cmd.Process.Pid
	deadline := time.Now().Add(processTimeout)
	for {
		select {
		case <-p.exited:
			return false
		default:
		}
		if hasBrokerStarted(p, pid) {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		poll := remaining
		if poll > 10*time.Millisecond {
			poll = 10 * time.Millisecond
		}
		<-time.After(poll)
	}
}

func hasBrokerStarted(p *brokerProc, pid int) bool {
	for _, line := range strings.Split(p.stderr.String(), "\n") {
		var rec struct {
			Event string `json:"event"`
			PID   int    `json:"pid"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec.Event == broker.EvBrokerStarted && rec.PID == pid {
			return true
		}
	}
	return false
}

func startBrokerTerminal(t *testing.T, sb *sandbox, args ...string) *brokerProc {
	t.Helper()

	command := []string{"service", "run"}
	if !namesLevel(args) {
		command = append(command, "--user")
	}
	cmd := exec.Command(testBinary, append(command, args...)...)
	cmd.Env = sb.env()
	cmd.WaitDelay = waitDelay
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}

	p := &brokerProc{t: t, cmd: cmd, stderr: stderr, exited: make(chan struct{})}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(p.stop)
	return p
}

func namesLevel(args []string) bool {
	for _, arg := range args {
		if arg == "--system" || arg == "--user" ||
			strings.HasPrefix(arg, "--system=") || strings.HasPrefix(arg, "--user=") {
			return true
		}
	}
	return false
}

func startBrokerTerminalOnFreePort(t *testing.T, sb *sandbox, args ...string) (*brokerProc, int) {
	t.Helper()

	const attempts = 2
	for attempt := 1; ; attempt++ {
		port := freePort(t)
		addr := listenAddr(port)
		p := startBrokerTerminal(t, sb, append([]string{"--listen", addr}, args...)...)
		if waitBrokerStarted(t, p) {
			return p, port
		}
		select {
		case <-p.exited:
		default:
			t.Fatalf("broker did not report %s on %s within %s while still running (stderr: %s)", broker.EvBrokerStarted, addr, processTimeout, p.stderr.String())
		}
		code := p.cmd.ProcessState.ExitCode()
		if code != exitListenUnavailable || attempt == attempts {
			t.Fatalf("broker start on %s: exit code %d (stderr: %s)", addr, code, p.stderr.String())
		}
		p.stop()
	}
}

// TestBrokerListenFlagOverridesConfig checks that --listen overrides the
// configured address and leaves the configuration file untouched.
func TestBrokerListenFlagOverridesConfig(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	configPort, _ := occupiedPort(t)
	sb.writeConfig(fmt.Sprintf("broker:\n  listen: %q\n", listenAddr(configPort)))
	before, err := os.ReadFile(sb.configPath())
	if err != nil {
		t.Fatalf("read config before start: %v", err)
	}

	p, listenPort := startBrokerTerminalOnFreePort(t, sb)
	dialBroker(t, listenAddr(listenPort))

	after, err := os.ReadFile(sb.configPath())
	if err != nil {
		t.Fatalf("read config after start: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("config file changed:\nbefore %q\nafter  %q", before, after)
	}

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal broker: %v", err)
	}
	if code := p.wait(processTimeout); code != exitOK {
		t.Fatalf("exit code after SIGTERM = %d, want %d (stderr: %s)", code, exitOK, p.stderr.String())
	}

	afterExit, err := os.ReadFile(sb.configPath())
	if err != nil {
		t.Fatalf("read config after exit: %v", err)
	}
	if !bytes.Equal(before, afterExit) {
		t.Fatalf("config file changed after exit:\nbefore %q\nafter  %q", before, afterExit)
	}
}

// TestBrokerSecondInstanceFailsWithAnotherListenAddress checks that a second
// instance fails on the account lock whatever address it asks for, and the
// first keeps serving.
func TestBrokerSecondInstanceFailsWithAnotherListenAddress(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	first, firstPort := startBrokerTerminalOnFreePort(t, sb)

	// Keep the address held with occupiedPort rather than freePort, so a second
	// instance that bound before taking the lock exits with exitListenUnavailable
	// instead of exitAlreadyRunning.
	secondPort, _ := occupiedPort(t)

	second := startBrokerTerminal(t, sb, "--listen", listenAddr(secondPort))
	if code := second.wait(processTimeout); code != exitAlreadyRunning {
		t.Fatalf("second exit code = %d, want %d (stderr: %s)", code, exitAlreadyRunning, second.stderr.String())
	}
	reason := second.lastStderrLine("peers service run: ")
	if !strings.Contains(reason, "already running") {
		t.Fatalf("second stderr line = %q, want it to contain %q", reason, "already running")
	}

	if err := first.cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("first broker is not running after the second failed: %v", err)
	}
	dialBroker(t, listenAddr(firstPort))

	if err := first.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal first broker: %v", err)
	}
	if code := first.wait(processTimeout); code != exitOK {
		t.Fatalf("first exit code after SIGTERM = %d, want %d (stderr: %s)", code, exitOK, first.stderr.String())
	}

	startBrokerTerminalOnFreePort(t, sb)
}

// TestBrokerListenAddressUnavailable checks that an unavailable address fails
// the start instead of falling back to another one.
func TestBrokerListenAddressUnavailable(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	port, _ := occupiedPort(t)
	addr := listenAddr(port)

	p := startBrokerTerminal(t, sb, "--listen", addr)
	if code := p.wait(processTimeout); code != exitListenUnavailable {
		t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitListenUnavailable, p.stderr.String())
	}
	reason := p.lastStderrLine("peers service run: ")
	if !strings.Contains(reason, addr) {
		t.Fatalf("stderr line = %q, want it to contain %q", reason, addr)
	}
}

// TestBrokerInvalidConfig checks that a malformed or unreadable configuration
// file fails the start with exitInvalidConfig.
func TestBrokerInvalidConfig(t *testing.T) {
	t.Parallel()

	t.Run("malformed YAML", func(t *testing.T) {
		t.Parallel()

		sb := newSandbox(t)
		sb.writeConfig("broker: [\n")

		p := startBrokerTerminal(t, sb)
		if code := p.wait(processTimeout); code != exitInvalidConfig {
			t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitInvalidConfig, p.stderr.String())
		}
		reason := p.lastStderrLine("peers service run: ")
		if !strings.Contains(reason, "config file parse error") {
			t.Fatalf("stderr line = %q, want it to contain %q", reason, "config file parse error")
		}
	})

	t.Run("unreadable config", func(t *testing.T) {
		t.Parallel()

		sb := newSandbox(t)
		if err := os.MkdirAll(sb.configPath(), 0o700); err != nil {
			t.Fatalf("create config path as a directory: %v", err)
		}

		p := startBrokerTerminal(t, sb)
		if code := p.wait(processTimeout); code != exitInvalidConfig {
			t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitInvalidConfig, p.stderr.String())
		}
		reason := p.lastStderrLine("peers service run: ")
		if !strings.Contains(reason, sb.configPath()) {
			t.Fatalf("stderr line = %q, want it to name the config file %q", reason, sb.configPath())
		}
	})
}

// TestBrokerInsideSession checks that a Broker started inside a Claude Code
// session exits with exitInSession.
func TestBrokerInsideSession(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	sb.extraEnv = append(sb.extraEnv, "CLAUDE_CODE_MESSAGING_SOCKET=/tmp/x.sock")

	p := startBrokerTerminal(t, sb)
	if code := p.wait(processTimeout); code != exitInSession {
		t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitInSession, p.stderr.String())
	}
	if reason := p.lastStderrLine("peers service run: "); reason == "" {
		t.Fatal("standard error carries no failure line, want the session failure reason")
	}
}

// TestBrokerUsesTestHome checks that the brokers write their identity into the
// test home and leave the developer's real data directory untouched.
func TestBrokerUsesTestHome(t *testing.T) {
	t.Parallel()

	account, err := user.Current()
	if err != nil {
		t.Fatalf("find the developer account: %v", err)
	}
	realData := filepath.Join(account.HomeDir, ".peers")
	before, beforeErr := os.Stat(realData)

	first, second := newSandbox(t), newSandbox(t)
	first.writeConfig("broker:\n  alias: host-a\n")
	second.writeConfig("broker:\n  alias: host-b\n")

	startBrokerTerminalOnFreePort(t, first)
	startBrokerTerminalOnFreePort(t, second)

	for _, sb := range []*sandbox{first, second} {
		for _, name := range []string{"identity.key", "identity.crt"} {
			path := filepath.Join(sb.home, ".peers", name)
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("identity file %s: %v", path, err)
			}
		}
	}

	after, afterErr := os.Stat(realData)
	switch {
	case beforeErr == nil:
		if afterErr != nil {
			t.Fatalf("real %s disappeared during the test: %v", realData, afterErr)
		}
		if !after.ModTime().Equal(before.ModTime()) {
			t.Fatalf("real %s was modified: mtime %v -> %v", realData, before.ModTime(), after.ModTime())
		}
	case os.IsNotExist(beforeErr):
		if !os.IsNotExist(afterErr) {
			t.Fatalf("real %s appeared during the test (%v)", realData, afterErr)
		}
	default:
		t.Fatalf("stat real %s before the test: %v", realData, beforeErr)
	}
}

// TestBrokerRuntimeDirBadPermissions checks that a world-writable runtime
// directory fails the start with exitRuntimeDir.
func TestBrokerRuntimeDirBadPermissions(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	dir := sb.runtimeDir()
	if err := os.Mkdir(dir, 0o777); err != nil {
		t.Fatalf("create runtime directory: %v", err)
	}
	// Chmod after Mkdir so the umask cannot narrow the mode the check rejects.
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod runtime directory: %v", err)
	}

	p := startBrokerTerminal(t, sb)
	if code := p.wait(processTimeout); code != exitRuntimeDir {
		t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitRuntimeDir, p.stderr.String())
	}
	reason := p.lastStderrLine("peers service run: ")
	if !strings.Contains(reason, dir) {
		t.Fatalf("stderr line = %q, want it to name the runtime directory %q", reason, dir)
	}
}

// TestBrokerInvalidListenValue checks that a malformed --listen value is a
// usage error.
func TestBrokerInvalidListenValue(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	p := startBrokerTerminal(t, sb, "--listen", "not-an-address")
	if code := p.wait(processTimeout); code != exitUsage {
		t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitUsage, p.stderr.String())
	}
	if stderr := p.stderr.String(); !strings.Contains(stderr, "--listen") {
		t.Fatalf("stderr = %q, want it to mention %q", stderr, "--listen")
	}
	if stderr := p.stderr.String(); !strings.Contains(stderr, "peers service run") {
		t.Fatalf("stderr = %q, want the broker usage", stderr)
	}
}

// TestBrokerSystemRequiresRoot checks that --system without root exits with
// exitNotRoot and names the reason.
func TestBrokerSystemRequiresRoot(t *testing.T) {
	t.Parallel()

	if euid := os.Geteuid(); euid == 0 {
		t.Skip("the test process runs as root: --system is expected to proceed and serve the system Broker")
	}

	sb := newSandbox(t)
	p := startBrokerTerminal(t, sb, "--system")
	if code := p.wait(processTimeout); code != exitNotRoot {
		t.Fatalf("exit code = %d, want %d (stderr: %s)", code, exitNotRoot, p.stderr.String())
	}
	if stderr := p.stderr.String(); !strings.Contains(stderr, "must run as root") {
		t.Fatalf("stderr = %q, want it to name the root requirement", stderr)
	}
	if reason := p.lastStderrLine("peers service run: "); !strings.Contains(reason, "must run as root") {
		t.Fatalf("stderr line = %q, want it to name the root requirement", reason)
	}
}

// TestBrokerShutsDownOnSIGINT checks that a terminal Broker exits cleanly on
// SIGINT.
func TestBrokerShutsDownOnSIGINT(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	p, _ := startBrokerTerminalOnFreePort(t, sb)

	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal broker: %v", err)
	}
	if code := p.wait(processTimeout); code != exitOK {
		t.Fatalf("exit code after SIGINT = %d, want %d (stderr: %s)", code, exitOK, p.stderr.String())
	}
}
