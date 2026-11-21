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

// dialBroker fails the test when the address is not accepting connections.
func dialBroker(t *testing.T, addr string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, processTimeout)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = conn.Close()
}

// waitBrokerStarted polls until the child writes its broker_started record.
// The record is written in Step 10 of Run in run.go, after the account lock
// and both listeners are in place, so it means the Broker is serving this
// account. It reports false when the process exits or the deadline passes
// first.
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

// hasBrokerStarted reports whether standard error already holds this process's
// broker_started record. Every line is a structured log record; the event name
// comes from internal/broker so the test does not spell it out, and the pid
// must match the child, so a record from another process cannot satisfy it.
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

// startBrokerTerminal runs "peers service run" the way a terminal launch does.
// The returned process has no reader, so readiness is observed in standard
// error. Unless args names a level, the command acts on the user installation so the
// test does not depend on the account that runs it.
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

// namesLevel reports whether args already selects --system or --user.
func namesLevel(args []string) bool {
	for _, arg := range args {
		if arg == "--system" || arg == "--user" ||
			strings.HasPrefix(arg, "--system=") || strings.HasPrefix(arg, "--user=") {
			return true
		}
	}
	return false
}

// startBrokerTerminalOnFreePort starts a terminal Broker with --listen on a
// port that freePort released and waits until the child reports that it
// started. The port can be taken between freePort's close and the Broker's
// bind, so a Broker that exits because the address is unavailable is retried
// once on a fresh port.
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
		// Another process took the port between freePort's close and the
		// Broker's bind. The Broker has exited; retry on a fresh port.
		p.stop()
	}
}

// TestBrokerListenFlagOverridesConfig covers REQ-broker-config-025 and
// REQ-broker-config-029: --listen replaces the configured address for this
// run, and the configuration file is left untouched.
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

	// The Broker also owns the configuration file during shutdown; compare once
	// more after it has exited, not only after it became ready.
	afterExit, err := os.ReadFile(sb.configPath())
	if err != nil {
		t.Fatalf("read config after exit: %v", err)
	}
	if !bytes.Equal(before, afterExit) {
		t.Fatalf("config file changed after exit:\nbefore %q\nafter  %q", before, afterExit)
	}
}

// TestBrokerSecondInstanceFailsWithAnotherListenAddress covers
// REQ-broker-config-013: a second instance fails whatever address it asks for,
// and the first one keeps serving.
func TestBrokerSecondInstanceFailsWithAnotherListenAddress(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	first, firstPort := startBrokerTerminalOnFreePort(t, sb)

	// Ask for a different address and hold it for the rest of the test, so it
	// is provably not the first Broker's and not bindable by anyone else. The
	// first Broker is ready only after its broker_started record, which it
	// writes once the account lock is held, so the second instance must fail on
	// the lock before it binds. The held address makes that observable: a
	// Broker that bound before taking the lock would exit with
	// exitListenUnavailable, while this one exits with exitAlreadyRunning.
	//
	// A freePort released here would instead be free for another parallel test
	// to take before the assertion, which is what made this test flaky.
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

	// The lock is free again, so a Broker can start. The helper retries on a
	// fresh port if this one was taken after freePort released it.
	startBrokerTerminalOnFreePort(t, sb)
}

// TestBrokerListenAddressUnavailable covers REQ-broker-config-024: an
// unavailable address fails the start instead of falling back to another one.
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
		// A directory in place of the file makes the read fail with its path.
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

// TestBrokerSystemRequiresRoot checks that --system without root ends with the
// dedicated exit code and names the reason. As root the command is supposed to
// proceed and serve the system Broker, so the case is skipped instead.
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

// TestBrokerShutsDownOnSIGINT checks the shutdown path of a Broker started
// from a terminal.
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
