//go:build e2e

package e2e

import (
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// runtimeFallbackPeersBinary is the peers build the system image provides.
	runtimeFallbackPeersBinary = "/usr/local/bin/peers"
	// runtimeFallbackListen is a port no other system test binds.
	runtimeFallbackListen = ":17446"
	// runtimeFallbackStartBound bounds the start of a user Broker after another
	// account has taken its preferred runtime directory.
	runtimeFallbackStartBound = 20 * time.Second
)

// TestRuntimeDirectoryFallbackWhenAnotherAccountTakesThePreferredPath covers the
// real shape of the runtime-directory fallback: another account has created
// /tmp/peers-<uid> before the account's user Broker starts. The Broker must use
// the per-account fallback directory, a session started as the account must
// register through it, and nothing may be served from the taken directory.
func TestRuntimeDirectoryFallbackWhenAnotherAccountTakesThePreferredPath(t *testing.T) {
	if !syscontainer.InContainer() {
		// The container and the runtime directories inside it go away with the
		// project. Anything the run leaves behind on the host is collected by the
		// same cleanup the rest of the suite uses.
		t.Cleanup(func() { harness.CleanResidue(t.Logf) })
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	peerUID, peerGID := accountIDs(t, "peer")
	otherUID, otherGID := accountIDs(t, "other")
	uid := strconv.Itoa(peerUID)

	preferred := "/tmp/peers-" + uid
	fallbackBase := "/run/user/" + uid
	fallbackDir := filepath.Join(fallbackBase, "peers")
	socket := filepath.Join(fallbackDir, "broker.sock")

	// The fallback base exists only when the account has a logind session. The
	// rule accepts it only when this account owns it with no group or other
	// access, so create it with those properties.
	if err := os.MkdirAll(fallbackBase, 0o700); err != nil {
		t.Fatalf("creating %s: %v", fallbackBase, err)
	}
	if err := os.Chown(fallbackBase, peerUID, peerGID); err != nil {
		t.Fatalf("chown %s: %v", fallbackBase, err)
	}
	if err := os.Chmod(fallbackBase, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", fallbackBase, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(fallbackDir) })

	// other takes the preferred path before peer's Broker starts. The directory
	// is a plain directory, so only the owner comparison can tell it is taken.
	if err := os.RemoveAll(preferred); err != nil {
		t.Fatalf("removing %s: %v", preferred, err)
	}
	if err := os.Mkdir(preferred, 0o700); err != nil {
		t.Fatalf("creating %s: %v", preferred, err)
	}
	if err := os.Chown(preferred, otherUID, otherGID); err != nil {
		t.Fatalf("chown %s: %v", preferred, err)
	}
	if err := os.Chmod(preferred, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", preferred, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(preferred) })

	stderr := &lockedBuffer{}
	startRuntimeFallbackBroker(t, stderr, socket)

	// The taken directory must stay untouched: no socket and no lock of peer's
	// Broker may appear there.
	for _, name := range []string{"broker.sock", "broker.lock"} {
		if _, err := os.Lstat(filepath.Join(preferred, name)); err == nil {
			t.Errorf("the taken directory %s contains %s, want nothing of peer's Broker", preferred, name)
		}
	}
	info, err := os.Lstat(preferred)
	if err != nil {
		t.Fatal(err)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || int(st.Uid) != int(otherUID) {
		t.Errorf("%s is no longer owned by other", preferred)
	}

	// The Broker reports where it went, so an operator can find it.
	if logs := stderr.String(); !strings.Contains(logs, "runtime_dir_fallback") {
		t.Errorf("the Broker log does not record runtime_dir_fallback:\n%s", logs)
	} else if !strings.Contains(logs, preferred) {
		t.Errorf("the Broker log does not name the taken path %s:\n%s", preferred, logs)
	}

	// A session started as the account must find the Broker through the
	// fallback socket and register.
	session := syscontainer.NewSession(t, "peer", "fallback")
	waitRegistered(t, session)
}

// accountIDs returns the uid and gid of account.
func accountIDs(t *testing.T, account string) (int, int) {
	t.Helper()
	u, err := user.Lookup(account)
	if err != nil {
		t.Fatalf("looking up account %q: %v", account, err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		t.Fatalf("account %q has a non-numeric uid %q", account, u.Uid)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		t.Fatalf("account %q has a non-numeric gid %q", account, u.Gid)
	}
	return uid, gid
}

// startRuntimeFallbackBroker runs "peers service run --user" as peer and waits
// for socket. The process is stopped when the test ends. A process that exits
// before the socket exists fails the test with its output.
func startRuntimeFallbackBroker(t *testing.T, stderr *lockedBuffer, socket string) {
	t.Helper()
	cmd := syscontainer.Command("peer", nil, runtimeFallbackPeersBinary,
		"service", "run", "--user", "--listen", runtimeFallbackListen)
	cmd.Stdout = &lockedBuffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the peer user Broker: %v", err)
	}

	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
			return
		case <-time.After(harness.EventTimeout):
		}
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Logf("the peer user Broker did not exit after SIGKILL")
		}
	})

	harness.Eventually(t, runtimeFallbackStartBound, "the peer user Broker socket "+socket, func() (bool, string) {
		if _, err := os.Stat(socket); err == nil {
			return true, ""
		}
		select {
		case <-done:
			t.Fatalf("the peer user Broker exited before creating %s: %v\n%s", socket, waitErr, stderr.String())
		default:
		}
		return false, stderr.String()
	})
}
