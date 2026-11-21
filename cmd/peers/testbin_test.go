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

// testBinary is the peers binary built once by TestMain. Subprocess tests
// run it as a child process instead of calling the package's functions, so the
// exit codes, the standard streams and the signal handling are the real ones.
var testBinary string

// waitDelay bounds how long Wait waits for the child's I/O pipes after the
// process has exited, so a grandchild that inherited one of them cannot hang
// the test harness. It is also the backstop after the SIGKILL fallback in stop.
const waitDelay = 3 * time.Second

// processTimeout is the package-wide fallback for a wait on an event produced
// by a child process: the process exiting, one line on its standard output or
// standard error, the Register stream being opened or closed, or its exit after
// SIGTERM or the SIGKILL fallback. It only decides how long a wait that will
// never complete takes to fail: the success path selects on the event itself
// and never spends this time, so a larger value costs nothing. Thirty seconds
// is far above the worst normal latency under load, including process startup
// and os/exec's pipe-copy goroutines; it is not a claim that a child should
// take that long.
const processTimeout = 30 * time.Second

// maxRuntimeRootBytes leaves room for the account directory and broker.sock
// under the runtime root inside the Unix sun_path limit.
const maxRuntimeRootBytes = 60

// maxSocketPathBytes is the shortest sockaddr_un.sun_path across the supported
// platforms. macOS sizes sun_path at 104 bytes and Linux at 108, both including
// the trailing NUL, so a path may use at most 103 bytes.
const maxSocketPathBytes = 103

// TestMain builds the binary every subprocess test runs. The build uses
// os.Environ because go build is a tool here, not the process under test; the
// process under test gets a clean environment from sandbox.env.
func TestMain(m *testing.M) {
	// A child started by TestMainEntryTurnsAPanicIntoExitFailure re-executes
	// the test binary to reach the real main; it must not run the tests again.
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

// sandbox is one isolated account environment for a subprocess test. The
// runtime root comes from os.MkdirTemp rather than t.TempDir: t.TempDir embeds
// the test name and, on macOS, lives under /var/folders, so "<root>/<euid>/
// broker.sock" would exceed the 104-byte Unix socket path limit.
type sandbox struct {
	t    *testing.T
	root string // value of PEERS_TEST_RUNTIME_ROOT
	home string // value of PEERS_TEST_HOME and HOME

	// extraEnv is appended to env for every child started from this sandbox.
	// Tests use it for the variables the child needs beyond the fixed four,
	// such as PEERS_TEST_SOCKET or CLAUDE_CODE_MESSAGING_SOCKET.
	extraEnv []string
}

func newSandbox(t *testing.T) *sandbox {
	t.Helper()

	root, err := sandboxRoot()
	if err != nil {
		t.Fatalf("create runtime root: %v", err)
	}
	// os.MkdirTemp already creates the directory with 0700; set it again so a
	// restrictive umask or a platform default cannot widen it.
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		t.Fatalf("chmod runtime root: %v", err)
	}

	sb := &sandbox{t: t, root: root, home: t.TempDir()}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return sb
}

// sandboxRoot returns a runtime root short enough for the Unix socket paths
// under it. A long TMPDIR, which macOS uses by default, leaves too few bytes
// for the account directory and broker.sock, so a too-long root falls back to
// a fresh directory under /tmp.
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

// env is the child environment. It starts from nothing rather than
// os.Environ so the session variables of the test runner (for example
// CLAUDE_CODE_MESSAGING_SOCKET or PEERS_TEST_SOCKET) cannot leak into the
// process under test.
func (s *sandbox) env() []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + s.home,
		"PEERS_TEST_RUNTIME_ROOT=" + s.root,
		"PEERS_TEST_HOME=" + s.home,
	}
	return append(env, s.extraEnv...)
}

// runtimeDir mirrors the config and mcpserver test hooks: the superuser's
// directory is root/root, everyone else's is root/<euid>.
func (s *sandbox) runtimeDir() string {
	if euid := os.Geteuid(); euid == 0 {
		return filepath.Join(s.root, "root")
	}
	return filepath.Join(s.root, strconv.Itoa(os.Geteuid()))
}

func (s *sandbox) configPath() string {
	return filepath.Join(s.home, ".peers", "config.yaml")
}

// writeConfig writes the account configuration. The account home is selected
// through PEERS_TEST_HOME, not HOME, so this does not depend on the child
// honoring HOME.
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

// occupiedPort returns a port that stays bound for the lifetime of the
// listener, so the Broker under test cannot bind it.
func occupiedPort(t *testing.T) (int, net.Listener) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on an ephemeral port: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().(*net.TCPAddr).Port, l
}

// listenAddr formats a loopback address for the Broker to bind or the test to
// dial.
func listenAddr(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

// freePort returns a port that was bound and released, so the Broker under
// test can take it. There is an unavoidable gap between the close and the
// Broker's bind, which is why tests prefer occupiedPort when the point is that
// binding must fail.
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

// lockedBuffer is the standard error sink for a child process. os/exec copies
// the child's output from another goroutine, and tests read the buffer while
// the child may still write, so every access is guarded.
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

// brokerProc is one Broker child process started by a test helper.
type brokerProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stderr *lockedBuffer

	exited  chan struct{}
	waitErr error
}

// wait waits for the process to exit and returns its exit code.
func (p *brokerProc) wait(timeout time.Duration) int {
	p.t.Helper()

	select {
	case <-p.exited:
	case <-time.After(timeout):
		p.t.Fatalf("broker did not exit within %s (stderr: %s)", timeout, p.stderr.String())
	}
	return p.cmd.ProcessState.ExitCode()
}

// lastStderrLine returns the last line of the Broker's standard error that
// starts with prefix, or "" when there is none. The Broker also logs JSON
// runtime records to standard error, so tests select the failure line by
// prefix.
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

// stop is the cleanup registered by the start helpers: terminate a still
// running Broker with SIGTERM and fall back to SIGKILL after three seconds.
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
	case <-time.After(3 * time.Second):
	}
	_ = p.cmd.Process.Kill()
	// WaitDelay bounds the wait for the child's pipes; a process that still
	// refuses to exit after SIGKILL is reported instead of being signalled
	// again by pid.
	select {
	case <-p.exited:
	case <-time.After(waitDelay):
		p.t.Errorf("broker process %d did not exit after SIGKILL", p.cmd.Process.Pid)
	}
}
