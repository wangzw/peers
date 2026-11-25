package broker

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// maxUnixSocketPath is the longest Unix socket path macOS can bind, whose
// sun_path is shorter than Linux's 107 bytes.
const maxUnixSocketPath = 103

func shortSocketDir(t *testing.T, filename string) string {
	t.Helper()
	return socketPathIn(t, t.TempDir(), filename)
}

func socketPathIn(t *testing.T, base, filename string) string {
	t.Helper()
	path := filepath.Join(base, filename)
	if len(path) <= maxUnixSocketPath {
		return path
	}
	dir, err := os.MkdirTemp("/tmp", "ap-sock-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path = filepath.Join(dir, filename)
	if len(path) > maxUnixSocketPath {
		t.Fatalf("socket path %q is %d bytes, over the %d-byte limit", path, len(path), maxUnixSocketPath)
	}
	return path
}

// TestShortSocketDirFallsBackForLongBase checks that a base directory too long
// for a Unix socket path falls back under /tmp.
func TestShortSocketDirFallsBackForLongBase(t *testing.T) {
	base := filepath.Join(t.TempDir(), strings.Repeat("x", maxUnixSocketPath))
	path := socketPathIn(t, base, "broker.sock")
	if !strings.HasPrefix(path, "/tmp/") {
		t.Fatalf("socket path = %q, want a /tmp fallback", path)
	}
	if len(path) > maxUnixSocketPath {
		t.Fatalf("socket path %q is %d bytes, want at most %d", path, len(path), maxUnixSocketPath)
	}
	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("fallback directory: %v", err)
	}
}

// TestBindLocalSocketReplacesStaleSocket checks that a leftover socket is
// replaced and the new one accepts connections.
func TestBindLocalSocketReplacesStaleSocket(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("ListenUnix stale: %v", err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("Close stale listener: %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket was not left in place: %v", err)
	}

	sock, err := BindLocalSocket(path, 0o600, nil)
	if err != nil {
		t.Fatalf("BindLocalSocket: %v", err)
	}
	defer func() { _ = sock.Close(true) }()

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial bound socket: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close dialed connection: %v", err)
	}
}

// TestBindLocalSocketRefusesRegularFile checks that a regular file at the path
// is left unchanged and reported as an error.
func TestBindLocalSocketRefusesRegularFile(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	content := []byte("do not replace")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := BindLocalSocket(path, 0o600, nil); err == nil {
		t.Fatal("BindLocalSocket succeeded over a regular file")
	} else if !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("BindLocalSocket error = %q, want it to say not a socket", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("file content changed to %q, want %q", got, content)
	}
	if info, err := os.Lstat(path); err != nil {
		t.Fatalf("Lstat: %v", err)
	} else if !info.Mode().IsRegular() {
		t.Fatalf("path is no longer a regular file: %s", info.Mode())
	}
}

// TestBindLocalSocketReportsRetryLimit checks that a socket removed on every
// bind attempt is reported after the retry budget and each retry is logged.
func TestBindLocalSocketReportsRetryLimit(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	var logs bytes.Buffer
	var level slog.LevelVar
	logger := NewLogger(&logs, &level)
	defer swapAfterListen(func() { _ = os.Remove(path) })()

	_, err := BindLocalSocket(path, 0o600, logger)
	if err == nil {
		t.Fatal("BindLocalSocket succeeded although every attempt lost its socket")
	}
	if want := fmt.Sprintf("after %d attempts", bindAttempts); !strings.Contains(err.Error(), want) {
		t.Fatalf("BindLocalSocket error = %q, want it to say %q", err, want)
	}
	retries := 0
	for _, rec := range decodeLogLines(t, logs.String()) {
		switch rec["event"] {
		case EvLocalSocketBindRetry:
			retries++
			if rec["socket"] != path {
				t.Errorf("retry %d socket = %v, want %s", retries, rec["socket"], path)
			}
			if got, ok := rec["attempt"].(float64); !ok || int(got) != retries {
				t.Errorf("retry %d attempt = %v, want %d", retries, rec["attempt"], retries)
			}
		case EvLocalSocketRebound:
			t.Errorf("a bind that never succeeded logged %s", EvLocalSocketRebound)
		}
	}
	if retries != bindAttempts-1 {
		t.Errorf("retry records = %d, want %d", retries, bindAttempts-1)
	}
}

// TestBindLocalSocketPermission checks that the bound socket has the requested
// file mode.
func TestBindLocalSocketPermission(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	sock, err := BindLocalSocket(path, 0o600, nil)
	if err != nil {
		t.Fatalf("BindLocalSocket: %v", err)
	}
	defer func() { _ = sock.Close(true) }()

	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket mode = %o, want 600", got)
	}
}

// TestLocalSocketCloseDoesNotRemoveReplacement checks that closing one
// LocalSocket leaves a replacement socket at the same path in place.
func TestLocalSocketCloseDoesNotRemoveReplacement(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	a, err := BindLocalSocket(path, 0o600, nil)
	if err != nil {
		t.Fatalf("BindLocalSocket A: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove A's socket: %v", err)
	}
	b, err := BindLocalSocket(path, 0o600, nil)
	if err != nil {
		t.Fatalf("BindLocalSocket B: %v", err)
	}
	defer func() { _ = b.Close(true) }()

	if err := a.Close(true); err != nil {
		t.Fatalf("Close A: %v", err)
	}
	if !b.Present() {
		t.Fatal("B's socket disappeared after A was closed")
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("Dial after closing A: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close dialed connection: %v", err)
	}
}

// TestLocalSocketPresent checks that Present reports true only while the path
// names the bound socket.
func TestLocalSocketPresent(t *testing.T) {
	path := shortSocketDir(t, "broker.sock")
	sock, err := BindLocalSocket(path, 0o600, nil)
	if err != nil {
		t.Fatalf("BindLocalSocket: %v", err)
	}
	if !sock.Present() {
		t.Fatal("Present() = false right after binding")
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove socket: %v", err)
	}
	if sock.Present() {
		t.Fatal("Present() = true after the socket file was removed")
	}
	if err := sock.Close(false); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sock.Close(true); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path still exists after Close(true) with no socket present: %v", err)
	}
}
