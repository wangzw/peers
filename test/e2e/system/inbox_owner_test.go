//go:build e2e

package e2e

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// inboxWriteBound bounds the wait for the account helper's connection to the
// inbox socket and for the credential read on it. No product timer governs
// either event, so it uses the tree's fallback.
const inboxWriteBound = harness.EventTimeout

// TestREQBrokerConfig018_WriteRunsAsTargetAccount checks that the system
// Broker reaches another account's inbox through a process running as that
// account. The test binds the inbox socket, hands it to other, and reads
// SO_PEERCRED from the accepted connection: the uid must be other's, not
// root's. A root write would let the Broker's own identity stand in for every
// served account.
func TestREQBrokerConfig018_WriteRunsAsTargetAccount(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")

	const socketPath = "/tmp/ap-other-rec.sock"
	_ = os.Remove(socketPath)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("listening on %s: %v", socketPath, err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
		_ = os.Remove(socketPath)
	})
	otherUID := syscontainer.AccountUID(t, "other")
	if err := os.Lchown(socketPath, int(otherUID), -1); err != nil {
		t.Fatalf("lchown %s to other: %v", socketPath, err)
	}

	other := syscontainer.NewSession(t, "other", "recv", syscontainer.WithSocket(socketPath))
	e2eSysWaitRegistered(t, other)
	sender := syscontainer.NewSession(t, "peer", "send")
	e2eSysWaitRegistered(t, sender)
	const topic = "e2e/cred"
	for _, session := range []*syscontainer.Session{other, sender} {
		if text, isError := session.Call(t, "join_topic", map[string]any{"name": topic}); isError {
			t.Fatalf("join_topic as %s: %s", session.Account, text)
		}
	}

	type credentials struct {
		uid uint32
		err error
	}
	got := make(chan credentials, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			got <- credentials{err: err}
			return
		}
		defer func() { _ = conn.Close() }()
		uid, err := unixPeerUID(conn)
		if err == nil {
			// Read the frame so the writer's wait for the receiver's close
			// completes.
			_ = conn.SetReadDeadline(time.Now().Add(inboxWriteBound))
			_, _ = bufio.NewReader(conn).ReadString('\n')
		}
		got <- credentials{uid: uid, err: err}
	}()

	to := e2eSysSelfAddress(t, other)
	if text, isError := sender.Call(t, "send_message",
		map[string]any{"to": to, "message": "credential probe"}); isError {
		t.Fatalf("send_message to %s: %s", to, text)
	}

	select {
	case res := <-got:
		if res.err != nil {
			t.Fatalf("reading the inbox connection's credentials: %v", res.err)
		}
		t.Logf("the inbox write ran as uid %d (other is %d)", res.uid, otherUID)
		if res.uid != otherUID {
			t.Errorf("the inbox write ran as uid %d, want %d (other)", res.uid, otherUID)
		}
	case <-time.After(inboxWriteBound):
		t.Fatalf("no connection reached the inbox socket %s within %s", socketPath, inboxWriteBound)
	}
}
