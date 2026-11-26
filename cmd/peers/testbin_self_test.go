package main

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// TestSandboxEnvFromEmpty checks that the child environment holds only the
// fixed variables and does not leak the runner's session variables.
func TestSandboxEnvFromEmpty(t *testing.T) {
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", "/x")
	t.Setenv("PEERS_TEST_SOCKET", "/y")

	sb := newSandbox(t)
	env := sb.env()

	got := make(map[string]string, len(env))
	for _, kv := range env {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("environment entry %q has no =", kv)
		}
		if _, dup := got[name]; dup {
			t.Fatalf("duplicate environment variable %q", name)
		}
		got[name] = value
	}

	want := []string{"HOME", "PATH", "PEERS_TEST_HOME", "PEERS_TEST_RUNTIME_ROOT", "PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK"}
	if len(got) != len(want) {
		t.Fatalf("env has %d variables, want exactly %d: %q", len(got), len(want), env)
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Fatalf("env is missing %s: %q", name, env)
		}
	}
	if got["PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK"] != "1" {
		t.Fatalf("PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK = %q, want 1", got["PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK"])
	}
	for _, name := range []string{"CLAUDE_CODE_MESSAGING_SOCKET", "PEERS_TEST_SOCKET"} {
		if _, ok := got[name]; ok {
			t.Fatalf("env leaked %s from the test process: %q", name, env)
		}
	}

	root := got["PEERS_TEST_RUNTIME_ROOT"]
	if !filepath.IsAbs(root) {
		t.Fatalf("runtime root %q is not absolute", root)
	}
	if len(root) > 60 {
		t.Fatalf("runtime root %q is %d bytes, want at most 60", root, len(root))
	}
	if root != sb.root {
		t.Fatalf("runtime root %q is not the sandbox root %q", root, sb.root)
	}
	if got["HOME"] != got["PEERS_TEST_HOME"] {
		t.Fatalf("HOME %q and PEERS_TEST_HOME %q differ", got["HOME"], got["PEERS_TEST_HOME"])
	}

	wantRuntime := filepath.Join(sb.root, strconv.Itoa(os.Geteuid()))
	if os.Geteuid() == 0 {
		wantRuntime = filepath.Join(sb.root, "root")
	}
	if dir := sb.runtimeDir(); dir != wantRuntime {
		t.Fatalf("runtimeDir() = %q, want %q", dir, wantRuntime)
	}
}

// TestOccupiedPortRefusesBind checks that an occupied port refuses a second
// bind and freePort yields a usable one.
func TestOccupiedPortRefusesBind(t *testing.T) {
	t.Parallel()

	port, held := occupiedPort(t)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	if l, err := net.Listen("tcp", addr); err == nil {
		_ = l.Close()
		t.Fatalf("port %d was not occupied", port)
	}

	if err := held.Close(); err != nil {
		t.Fatalf("close held listener: %v", err)
	}

	var listenErr error
	for attempt := 1; attempt <= 5; attempt++ {
		free := freePort(t)
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(free)))
		if err == nil {
			_ = l.Close()
			listenErr = nil
			break
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("listen on freePort %d: %v", free, err)
		}
		listenErr = err
	}
	if listenErr != nil {
		t.Fatalf("freePort yielded no bindable port after 5 attempts: %v", listenErr)
	}
}

// TestSandboxShortRootUnderLongTMPDIR checks that a long TMPDIR falls back to a
// root short enough for the socket paths.
func TestSandboxShortRootUnderLongTMPDIR(t *testing.T) {
	base := t.TempDir()
	long := filepath.Join(base, strings.Repeat("very-long-temp-root-", 4))
	if err := os.MkdirAll(long, 0o700); err != nil {
		t.Fatalf("create long TMPDIR: %v", err)
	}
	t.Setenv("TMPDIR", long)

	sb := newSandbox(t)

	if len(sb.root) > maxRuntimeRootBytes {
		t.Fatalf("runtime root %q is %d bytes, want at most %d", sb.root, len(sb.root), maxRuntimeRootBytes)
	}
	if strings.HasPrefix(sb.root, long) {
		t.Fatalf("runtime root %q still sits under the long TMPDIR %q", sb.root, long)
	}
	for _, path := range []string{
		filepath.Join(sb.root, strconv.Itoa(os.Geteuid()), "broker.sock"),
		filepath.Join(sb.root, "root", "broker.sock"),
	} {
		if len(path) > maxSocketPathBytes {
			t.Fatalf("socket path %q is %d bytes, want at most %d", path, len(path), maxSocketPathBytes)
		}
	}
}

// TestBinaryBuilt checks that TestMain produced a runnable binary.
func TestBinaryBuilt(t *testing.T) {
	t.Parallel()

	info, err := os.Stat(testBinary)
	if err != nil {
		t.Fatalf("test binary: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("test binary %s is not executable (mode %v)", testBinary, info.Mode())
	}

	out, err := exec.Command(testBinary, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("--version: %v\n%s", err, out)
	}
}
