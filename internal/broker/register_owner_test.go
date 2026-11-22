package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// TestRegisterSocketSymlinkNotOwned checks that the socket owner check does not
// follow a symlink. The record names a symlink whose target is a socket owned
// by the caller's account; following the link would accept it, so the
// registration must be rejected as socket_not_owned instead.
func TestRegisterSocketSymlinkNotOwned(t *testing.T) {
	tb := newTestBroker(t)
	target := fakesession.New(t, fakesession.Options{Name: "symlink-target"})

	linkPath := filepath.Join(t.TempDir(), "inbox.sock")
	if err := os.Symlink(target.SocketPath(), linkPath); err != nil {
		t.Fatalf("symlink %s -> %s: %v", linkPath, target.SocketPath(), err)
	}
	fake := fakesession.New(t, fakesession.Options{
		NoBind:     true,
		SocketPath: linkPath,
		Name:       "symlinked",
	})
	rs := startRegistration(t, tb, fake)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	tb.clock.Advance(10 * time.Second)
	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED {
		t.Fatalf("reason = %v, want socket_not_owned", reason)
	}
}

// TestRegisterRegularFileNotOwned checks that a path owned by the caller is
// still rejected when it is not a socket. The record names a regular file; an
// owner check without the socket type check would accept it.
func TestRegisterRegularFileNotOwned(t *testing.T) {
	tb := newTestBroker(t)
	path := filepath.Join(t.TempDir(), "inbox.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	fake := fakesession.New(t, fakesession.Options{
		NoBind:     true,
		SocketPath: path,
		Name:       "regular-file",
	})
	rs := startRegistration(t, tb, fake)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	tb.clock.Advance(10 * time.Second)
	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED {
		t.Fatalf("reason = %v, want socket_not_owned", reason)
	}
}
