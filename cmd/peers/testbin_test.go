package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var testBinary string

// waitDelay bounds how long Wait waits for the child's I/O pipes after it
// exits: an os/exec backstop, not a wait for the process itself.
const waitDelay = 3 * time.Second

// processTimeout bounds every wait on an event from a child process; it only
// decides how long a wait that never completes takes to fail.
const processTimeout = 30 * time.Second

// maxRuntimeRootBytes leaves room for <euid>/broker.sock under the runtime root
// inside the Unix socket path limit.
const maxRuntimeRootBytes = 60

// maxSocketPathBytes is the shorter sun_path of the supported platforms (macOS
// 104 bytes including the NUL), so a path may use at most 103 bytes.
const maxSocketPathBytes = 103

// TestMain builds the peers test binary and runs the package's tests.
func TestMain(m *testing.M) {
	// A child re-executed to reach the real main must not run the tests again.
	if os.Getenv(mainEntryEnv) != "" {
		runMainEntryForTest()
	}

	dir, err := os.MkdirTemp("", "peers-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create binary directory: %v\n", err)
		os.Exit(1)
	}
	testBinary = filepath.Join(dir, "peers")

	build := exec.Command("go", "build",
		"-tags", "peers_testenv",
		"-ldflags", "-X main.version=0.0.0-cmdtest",
		"-o", testBinary,
		".")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build test binary: %v\n%s", err, out)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// sandbox is one isolated account environment for a subprocess test; its
// runtime root comes from os.MkdirTemp because t.TempDir's path can exceed the
// Unix socket limit on macOS.
type sandbox struct {
	t        *testing.T
	root     string
	home     string
	extraEnv []string
}

func newSandbox(t *testing.T) *sandbox {
	t.Helper()

	root, err := sandboxRoot()
	if err != nil {
		t.Fatalf("create runtime root: %v", err)
	}
	// Chmod after MkdirTemp so a umask cannot narrow 0700.
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		t.Fatalf("chmod runtime root: %v", err)
	}

	sb := &sandbox{t: t, root: root, home: t.TempDir()}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return sb
}

// sandboxRoot returns a runtime root short enough for the Unix socket paths
// under it, falling back to /tmp when TMPDIR is too long.
func sandboxRoot() (string, error) {
	root, err := os.MkdirTemp("", "ap-")
	if err != nil {
		return "", err
	}
	if len(root) <= maxRuntimeRootBytes {
		return root, nil
	}
	_ = os.RemoveAll(root)

	root, err = os.MkdirTemp("/tmp", "ap-")
	if err != nil {
		return "", err
	}
	if len(root) > maxRuntimeRootBytes {
		_ = os.RemoveAll(root)
		return "", fmt.Errorf("temporary runtime root %q is longer than %d bytes", root, maxRuntimeRootBytes)
	}
	return root, nil
}

// env is the child environment, built from nothing so the runner's session
// variables cannot leak into the process under test. Because the runner is
// itself inside a Claude Code session, PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK
// turns off only the parent-chain walk; a child that sets the session variable
// itself is still refused.
func (s *sandbox) env() []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + s.home,
		"PEERS_TEST_RUNTIME_ROOT=" + s.root,
		"PEERS_TEST_HOME=" + s.home,
		"PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK=1",
	}
	return append(env, s.extraEnv...)
}

func (s *sandbox) runtimeDir() string {
	if euid := os.Geteuid(); euid == 0 {
		return filepath.Join(s.root, "root")
	}
	return filepath.Join(s.root, strconv.Itoa(os.Geteuid()))
}

func (s *sandbox) configPath() string {
	return filepath.Join(s.home, ".peers", "config.yaml")
}

// writeConfig writes the account configuration through PEERS_TEST_HOME, not
// HOME.
func (s *sandbox) writeConfig(yaml string) {
	s.t.Helper()

	dir := filepath.Dir(s.configPath())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		s.t.Fatalf("create config directory: %v", err)
	}
	if err := os.WriteFile(s.configPath(), []byte(yaml), 0o600); err != nil {
		s.t.Fatalf("write config: %v", err)
	}
}

func occupiedPort(t *testing.T) (int, net.Listener) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on an ephemeral port: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().(*net.TCPAddr).Port, l
}

func listenAddr(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

// freePort returns a released port; because the close leaves an unavoidable
// race, prefer occupiedPort when binding must fail.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on an ephemeral port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close ephemeral listener: %v", err)
	}
	return port
}

// lockedBuffer is a concurrency-safe child stderr sink: os/exec writes from
// another goroutine while tests read.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type brokerProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stderr *lockedBuffer

	exited  chan struct{}
	waitErr error
}

func (p *brokerProc) wait(timeout time.Duration) int {
	p.t.Helper()

	select {
	case <-p.exited:
	case <-time.After(timeout):
		p.t.Fatalf("broker did not exit within %s (stderr: %s)", timeout, p.stderr.String())
	}
	return p.cmd.ProcessState.ExitCode()
}

func (p *brokerProc) lastStderrLine(prefix string) string {
	text := strings.TrimRight(p.stderr.String(), "\n")
	if text == "" {
		return ""
	}
	lines := strings.Split(text, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(lines[i], prefix) {
			return lines[i]
		}
	}
	return ""
}

// stop terminates a running Broker with SIGTERM and falls back to SIGKILL after
// processTimeout.
func (p *brokerProc) stop() {
	select {
	case <-p.exited:
		return
	default:
	}

	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
		return
	case <-time.After(processTimeout):
	}
	_ = p.cmd.Process.Kill()
	// WaitDelay closes the pipes, so the exit is waited for with processTimeout
	// rather than waitDelay; racing the same instant would turn a delayed reap
	// into a false failure.
	select {
	case <-p.exited:
	case <-time.After(processTimeout):
		p.t.Errorf("broker process %d did not exit after SIGKILL", p.cmd.Process.Pid)
	}
}
