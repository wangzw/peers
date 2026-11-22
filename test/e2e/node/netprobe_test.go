package node

import (
	"errors"
	"net"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"
)

func TestHoldPortAndReachable(t *testing.T) {
	listener, err := HoldPort("127.0.0.1:0")
	if err != nil {
		t.Fatalf("node: HoldPort: %v", err)
	}
	addr := listener.Addr().String()

	if !TCPReachable(addr, 2*time.Second) {
		t.Fatalf("node: %s is not reachable while the port is held", addr)
	}
	conn, err := OpenConn(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("node: OpenConn: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("node: closing connection: %v", err)
	}

	tcp, ok := listener.(*net.TCPListener)
	if !ok {
		t.Fatalf("node: HoldPort returned %T, want a TCP listener", listener)
	}
	// Accept after Close is the direct proof that the socket stopped
	// accepting. Dialing the released address instead would also succeed
	// whenever another process had taken the port in the meantime, so it
	// cannot tell a listener that failed to close from a busy host. The
	// deadline makes a Close that does nothing fail the test rather than
	// block in Accept.
	if err := tcp.SetDeadline(time.Now().Add(eventTimeout)); err != nil {
		t.Fatalf("node: setting the accept deadline: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("node: closing listener: %v", err)
	}
	if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("node: Accept after the listener closed = %v, want net.ErrClosed", err)
	}

	// TCPReachable reports false when the connection cannot be made. Port zero
	// is never assigned to a listener, so this address is guaranteed
	// unreachable and does not depend on a released ephemeral port staying
	// free.
	if TCPReachable("127.0.0.1:0", 2*time.Second) {
		t.Fatal("node: 127.0.0.1:0 is reported reachable")
	}
}

func TestListeningAddrs(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("node: ListeningAddrs is implemented on Linux only")
	}

	listener, err := HoldPort("127.0.0.1:0")
	if err != nil {
		t.Fatalf("node: HoldPort: %v", err)
	}
	defer func() { _ = listener.Close() }()
	want := listener.Addr().String()

	addrs, err := ListeningAddrs(os.Getpid())
	if err != nil {
		t.Fatalf("node: ListeningAddrs: %v", err)
	}
	if !slices.Contains(addrs, want) {
		t.Fatalf("node: listening addresses %v do not contain %s", addrs, want)
	}

	if _, err := ListeningAddrs(-1); err == nil {
		t.Fatal("node: ListeningAddrs(-1) returned no error")
	}
}
