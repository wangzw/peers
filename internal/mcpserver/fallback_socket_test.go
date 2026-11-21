package mcpserver

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// shortTempDir returns a temporary directory at a short path. A Unix socket
// path is limited to 104 bytes on macOS, and t.TempDir() there lands under
// /var/folders/<long account directory>/T, which leaves too little room for the
// socket below it. /tmp keeps the whole path short on every platform these
// tests run on.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "peers-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// listenUnix starts a real Unix socket at path and keeps it open for the test.
func listenUnix(t *testing.T, path string) {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen %s: %v", path, err)
	}
	t.Cleanup(func() { _ = ln.Close() })
}

// holdAccountLock takes the account lock at path and keeps it for the test,
// standing in for a Broker that is running.
func holdAccountLock(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open account lock %s: %v", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		t.Fatalf("lock %s: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })
}

// TestFindUsesFallbackSocketAfterTakenDirDisappears covers the occupant that
// deletes the taken /tmp directory: the location rule alone would send the MCP
// server back to the preferred path, but the Broker is still running from the
// fallback directory. Selection probes both locations and uses the fallback
// socket.
func TestFindUsesFallbackSocketAfterTakenDirDisappears(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	base := shortTempDir(t)
	preferred := filepath.Join(base, "preferred")
	fallback := filepath.Join(base, "fallback")
	target := filepath.Join(base, "target")
	mkdir(t, target, 0o700)
	symlink(t, target, preferred)
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, preferred, fallback)

	runDir := filepath.Join(fallback, "peers")
	mkdir(t, runDir, 0o700)
	sock := filepath.Join(runDir, "broker.sock")
	listenUnix(t, sock)
	holdAccountLock(t, filepath.Join(runDir, "broker.lock"))

	// The occupant removes the taken directory. Resolving the location alone now
	// names the preferred path again.
	if err := os.Remove(preferred); err != nil {
		t.Fatalf("remove taken directory: %v", err)
	}

	got, err := productionSelector().Find(context.Background())
	if err != nil {
		t.Fatalf("Find after the taken directory disappeared: %v", err)
	}
	if got.Path != sock {
		t.Errorf("Find selected %q, want the running fallback socket %q", got.Path, sock)
	}
}

// TestFindPrefersTheSocketWithTheLiveBroker covers both locations answering the
// probe: the account lock says which one is the running Broker.
func TestFindPrefersTheSocketWithTheLiveBroker(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	base := shortTempDir(t)
	preferred := filepath.Join(base, "preferred")
	fallback := filepath.Join(base, "fallback")
	mkdir(t, preferred, 0o700)
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, preferred, fallback)

	listenUnix(t, filepath.Join(preferred, "broker.sock"))
	runDir := filepath.Join(fallback, "peers")
	mkdir(t, runDir, 0o700)
	fallbackSocket := filepath.Join(runDir, "broker.sock")
	listenUnix(t, fallbackSocket)
	holdAccountLock(t, filepath.Join(runDir, "broker.lock"))

	got, err := productionSelector().Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Path != fallbackSocket {
		t.Errorf("Find selected %q, want the socket whose Broker holds the account lock %q", got.Path, fallbackSocket)
	}
}
