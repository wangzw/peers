package service

import (
	"errors"
	"net"
	"os"
	"testing"
)

// connectedUnixPeer returns one end of a connected Unix stream socket pair. The
// other end stays open so the peer credentials the kernel reports exist.
func connectedUnixPeer(t *testing.T) *net.UnixConn {
	t.Helper()
	sock := shortProbeSocket(t)
	lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		t.Fatalf("listen on %s: %v", sock, err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		t.Fatalf("dial %s: %v", sock, err)
	}
	t.Cleanup(func() { _ = client.Close() })

	peer, err := lis.AcceptUnix()
	if err != nil {
		t.Fatalf("accept on %s: %v", sock, err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	return peer
}

// TestSocketPeerPIDReportsThePeerProcess pins the pid the kernel reports for
// the other end of a connected socket.
func TestSocketPeerPIDReportsThePeerProcess(t *testing.T) {
	conn := connectedUnixPeer(t)

	pid, err := socketPeerPID(conn)
	if err != nil {
		t.Fatalf("socketPeerPID: %v", err)
	}
	if want := os.Getpid(); pid != want {
		t.Errorf("socketPeerPID = %d, want the peer process's pid %d", pid, want)
	}
	if pid <= 1 {
		t.Errorf("socketPeerPID = %d, want a pid above 1", pid)
	}
}

// TestSocketPeerPIDReportsAClosedSocket checks that a socket whose descriptor
// is gone yields the raw connection error, not a pid.
func TestSocketPeerPIDReportsAClosedSocket(t *testing.T) {
	sock := shortProbeSocket(t)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen on %s: %v", sock, err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial %s: %v", sock, err)
	}
	uc := conn.(*net.UnixConn)
	if err := uc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	pid, err := socketPeerPID(uc)
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("socketPeerPID error = %v, want net.ErrClosed", err)
	}
	if pid != 0 {
		t.Errorf("socketPeerPID = %d, want 0 when the socket is closed", pid)
	}
}
