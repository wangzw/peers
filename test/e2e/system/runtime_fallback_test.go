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
	runtimeFallbackPeersBinary = "/usr/local/bin/peers"
	runtimeFallbackListen      = ":17446"
	runtimeFallbackStartBound  = 20 * time.Second
)

// TestRuntimeDirectoryFallbackWhenAnotherAccountTakesThePreferredPath checks that
// a user Broker whose preferred runtime directory belongs to another account
// uses the per-account fallback and still serves a session.
func TestRuntimeDirectoryFallbackWhenAnotherAccountTakesThePreferredPath(t *testing.T) {
	if !syscontainer.InContainer() {
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

	// The fallback base exists only with a logind session and is accepted only when
	// this account owns it with no group or other access.
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

	// other takes the preferred path before peer's Broker starts; a plain directory
	// is detected as taken only by its owner.
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

	if logs := stderr.String(); !strings.Contains(logs, "runtime_dir_fallback") {
		t.Errorf("the Broker log does not record runtime_dir_fallback:\n%s", logs)
	} else if !strings.Contains(logs, preferred) {
		t.Errorf("the Broker log does not name the taken path %s:\n%s", preferred, logs)
	}

	session := syscontainer.NewSession(t, "peer", "fallback")
	waitRegistered(t, session)
}

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
