package link

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// listenNode is a Link with fake dependencies, a recording subscription and a
// started notifier.
type listenNode struct {
	t        *testing.T
	l        *Link
	peers    *linktest.FakePeers
	settings *linktest.FakeSettings
	clock    *linktest.FakeClock

	mu      sync.Mutex
	ups     []Fingerprint
	downs   []Fingerprint
	handled int
}

func newListenNode(t *testing.T) *listenNode {
	t.Helper()
	n := &listenNode{
		t:        t,
		peers:    linktest.NewFakePeers(t),
		settings: linktest.NewFakeSettings("self"),
		clock:    linktest.NewFakeClock(time.Unix(1700000000, 0)),
	}
	l, err := New(Options{Peers: n.peers, Settings: n.settings, Clock: n.clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n.l = l
	l.Subscribe(n.onUp, n.onDown)
	l.notifier.start()
	t.Cleanup(l.Close)
	return n
}

func (n *listenNode) onUp(peer Fingerprint, _ []byte) {
	n.mu.Lock()
	n.ups = append(n.ups, peer)
	n.mu.Unlock()
}

func (n *listenNode) onDown(peer Fingerprint) {
	n.mu.Lock()
	n.downs = append(n.downs, peer)
	n.mu.Unlock()
}

func (n *listenNode) fingerprint() Fingerprint { return n.peers.Fingerprint() }

// waitUps blocks until n.up callbacks were delivered.
func (n *listenNode) waitUps(t *testing.T, want int) []Fingerprint {
	t.Helper()
	waitFor(t, func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return len(n.ups) >= want
	})
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Fingerprint(nil), n.ups...)
}

func (n *listenNode) waitHandled(t *testing.T, want int) {
	t.Helper()
	waitFor(t, func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.handled >= want
	})
}

func (n *listenNode) counts() (ups, downs, handled int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.ups), len(n.downs), n.handled
}

// listenNodeAddr binds n to a loopback port and returns the address.
func listenNodeAddr(t *testing.T, n *listenNode) string {
	t.Helper()
	if err := n.l.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := n.l.Addr()
	if addr == nil {
		t.Fatal("Addr returned nil after Listen")
	}
	return addr.String()
}

// currentListener returns the listener n accepts on now.
func (n *listenNode) currentListener(t *testing.T) net.Listener {
	t.Helper()
	n.l.mu.Lock()
	lis := n.l.lis
	n.l.mu.Unlock()
	if lis == nil {
		t.Fatal("the Link is not listening")
	}
	return lis
}

// requireStopped checks that ln no longer accepts connections. Accept is the
// direct evidence: a closed listener reports net.ErrClosed, while one the
// product left open times out at the deadline. Dialing the address the
// listener used to hold is not a substitute: another process can bind that
// released port and make the dial succeed for a reason unrelated to the
// listener the product was supposed to close.
func requireStopped(t *testing.T, ln net.Listener) {
	t.Helper()
	base := ln
	if wrapper, ok := ln.(*trackingListener); ok {
		base = wrapper.Listener
	}
	tcp, ok := base.(*net.TCPListener)
	if !ok {
		t.Fatalf("listener is a %T, want a TCP listener", base)
	}
	err := tcp.SetDeadline(time.Now().Add(2 * time.Second))
	if errors.Is(err, net.ErrClosed) {
		return
	}
	if err != nil {
		t.Fatalf("setting the accept deadline on %s: %v", ln.Addr(), err)
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("the old listener on %s still accepts connections: Accept = %v", ln.Addr(), err)
	}
}

// waitFirstFrameTimer blocks until the registry holds the accepted connection
// and its first-frame timer is armed. Unlike clock.WaitPending, which accepts
// any pending timer, this names the timer the test is about to fire.
func (n *listenNode) waitFirstFrameTimer(t *testing.T) {
	t.Helper()
	waitFor(t, func() bool {
		reg := n.l.unauth
		reg.mu.Lock()
		defer reg.mu.Unlock()
		if reg.conns.Len() != 1 {
			return false
		}
		tc, ok := reg.conns.Front().Value.(*trackedConn)
		return ok && tc.timer != nil
	})
}

// dialOnce dials addr from from, checks the accepting peer's certificate
// fingerprint and completes the Hello exchange. The accepting side is expected
// to install the link; the dialing side keeps the raw connection only.
func dialOnce(t *testing.T, from *Link, addr string) *conn {
	t.Helper()
	tc, id, err := from.dialTLS(context.Background(), addr)
	if err != nil {
		t.Fatalf("dialTLS %s: %v", addr, err)
	}
	c, err := from.openClientStream(tc, id, addr)
	if err != nil {
		_ = tc.Close()
		t.Fatalf("openClientStream %s: %v", addr, err)
	}
	c.role = pairing.RoleDialer
	if _, failure, _, err := from.dialerHandshake(c); err != nil || failure != FailNone {
		c.closeAsync(FailConnectionLost)
		t.Fatalf("dialer handshake with %s: failure=%v err=%v", addr, failure, err)
	}
	t.Cleanup(func() { c.closeAsync(FailConnectionLost) })
	return c
}

// TestREQBrokerConfig028_ListenAllAddressesDualStack checks that an empty host
// part binds every local address, so both IPv4 and IPv6 loopback reach it.
func TestREQBrokerConfig028_ListenAllAddressesDualStack(t *testing.T) {
	acceptor := newListenNode(t)
	dialer := newListenNode(t)
	linktest.Introduce(dialer.peers, acceptor.peers, "dialer", "acceptor")

	if err := acceptor.l.Listen(":0"); err != nil {
		t.Fatalf("Listen(:0): %v", err)
	}
	addr := acceptor.l.Addr()
	if addr == nil {
		t.Fatal("Addr returned nil after Listen(:0)")
	}
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatalf("split listen address %q: %v", addr, err)
	}

	t.Run("IPv4 loopback", func(t *testing.T) {
		dialOnce(t, dialer.l, net.JoinHostPort("127.0.0.1", port))
		acceptor.waitUps(t, 1)
	})

	// The accepting side keeps only one link per peer; forget the first one
	// before a second inbound connection arrives.
	acceptor.l.Drop(dialer.fingerprint())
	waitFor(t, func() bool {
		return acceptor.l.State(dialer.fingerprint()).Phase == PhaseNone
	})

	t.Run("IPv6 loopback", func(t *testing.T) {
		probe, err := net.Listen("tcp6", "[::1]:0")
		if err != nil {
			t.Skipf("IPv6 loopback is unavailable: %v", err)
		}
		_ = probe.Close()

		dialOnce(t, dialer.l, net.JoinHostPort("::1", port))
		acceptor.waitUps(t, 2)
	})
}

// TestREQBrokerConfig028_EmptyListenStopsAccepting checks that an empty address
// stops accepting while dialing still works.
func TestREQBrokerConfig028_EmptyListenStopsAccepting(t *testing.T) {
	acceptor := newListenNode(t)
	dialer := newListenNode(t)
	other := newListenNode(t)
	linktest.Introduce(dialer.peers, acceptor.peers, "dialer", "acceptor")

	addr := listenNodeAddr(t, acceptor)
	old := acceptor.currentListener(t)
	dialOnce(t, dialer.l, addr)
	acceptor.waitUps(t, 1)

	if err := acceptor.l.Listen(""); err != nil {
		t.Fatalf("Listen(\"\"): %v", err)
	}
	if got := acceptor.l.Addr(); got != nil {
		t.Fatalf("Addr after Listen(\"\") = %v, want nil", got)
	}
	requireStopped(t, old)

	// The same instance still dials out to another listener.
	linktest.Introduce(acceptor.peers, other.peers, "acceptor", "other")
	otherAddr := listenNodeAddr(t, other)
	dialOnce(t, acceptor.l, otherAddr)
	other.waitUps(t, 1)
}

// TestListenFailureKeepsOldListener checks that a failed bind leaves the old
// listener serving and the old address reported.
func TestListenFailureKeepsOldListener(t *testing.T) {
	acceptor := newListenNode(t)
	dialer := newListenNode(t)
	linktest.Introduce(dialer.peers, acceptor.peers, "dialer", "acceptor")

	addr := listenNodeAddr(t, acceptor)

	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	_, port, err := net.SplitHostPort(blocker.Addr().String())
	if err != nil {
		t.Fatalf("split blocker address: %v", err)
	}

	if err := acceptor.l.Listen(net.JoinHostPort("127.0.0.1", port)); err == nil {
		t.Fatal("Listen on an occupied port succeeded")
	}
	if got := acceptor.l.Addr(); got == nil || got.String() != addr {
		t.Fatalf("Addr after a failed Listen = %v, want %s", got, addr)
	}

	dialOnce(t, dialer.l, addr)
	acceptor.waitUps(t, 1)
}

// TestListenSwapKeepsEstablishedLinks checks that rebinding keeps the links
// already accepted on the old listener.
func TestListenSwapKeepsEstablishedLinks(t *testing.T) {
	acceptor := newListenNode(t)
	dialer := newListenNode(t)
	linktest.Introduce(dialer.peers, acceptor.peers, "dialer", "acceptor")

	oldAddr := listenNodeAddr(t, acceptor)
	old := acceptor.currentListener(t)
	dialOnce(t, dialer.l, oldAddr)
	acceptor.waitUps(t, 1)

	if err := acceptor.l.Send(dialer.fingerprint(), Frame{Kind: 200, Payload: []byte("still-up")}); err != nil {
		t.Fatalf("Send on the established link: %v", err)
	}

	newAddr := listenNodeAddr(t, acceptor)
	if newAddr == oldAddr {
		t.Fatalf("rebinding reused the old address %s", oldAddr)
	}
	requireStopped(t, old)

	dialOnce(t, dialer.l, newAddr)
	acceptor.waitUps(t, 2)
}

// TestSilentConnectionClosedAtFirstFrameDeadline checks that a TLS connection
// that opens no stream is closed once the first-frame deadline passes.
func TestSilentConnectionClosedAtFirstFrameDeadline(t *testing.T) {
	acceptor := newListenNode(t)
	client := newListenNode(t)
	addr := listenNodeAddr(t, acceptor)

	conn, err := tls.Dial("tcp", addr, client.peers.Identity().TLSConfig())
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Wait for this connection specifically: the registry must still hold it
	// and its first-frame timer must be armed.
	acceptor.waitFirstFrameTimer(t)

	// The gRPC server flushes its HTTP/2 SETTINGS as soon as the TLS handshake
	// completes, before any stream exists. Take the first byte so the close is
	// observed with data already buffered, the ordering that made a single Read
	// misreport the connection as open.
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if n, err := conn.Read(make([]byte, 1)); err != nil || n != 1 {
		t.Fatalf("read the server's first flush: n=%d err=%v", n, err)
	}

	// The connection must still be tracked before the deadline fires.
	if n := acceptor.l.unauth.count(); n != 1 {
		t.Fatalf("unauthenticated connections before the deadline = %d, want 1", n)
	}

	acceptor.clock.Advance(acceptor.l.firstFrameTimeout)

	// Drain everything the server flushed before the close. A single Read
	// returns that buffered data with a nil error, which says nothing about
	// whether the connection is still open. Only EOF, a reset or a locally
	// closed connection prove the deadline closed it; a read timeout proves it
	// did not.
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 4096)
	for {
		_, err := conn.Read(buf)
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) {
			return
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("the silent connection stayed open past the first-frame deadline: read timed out")
		}
		t.Fatalf("read after the first-frame deadline: %v", err)
	}
}

// TestUnauthenticatedCapAppliesToListener checks the cap is enforced on the
// real listener: the oldest unauthenticated connection is closed, and a
// legitimate peer can still establish a link afterwards.
func TestUnauthenticatedCapAppliesToListener(t *testing.T) {
	acceptor := newListenNode(t)
	dialer := newListenNode(t)
	linktest.Introduce(dialer.peers, acceptor.peers, "dialer", "acceptor")

	// SetTestKnobs changes the fields only; the registry is built in New, so the
	// listener cap is replaced here before the first Listen.
	acceptor.l.unauth = newUnauthRegistry(2, 8, acceptor.l.firstFrameTimeout, acceptor.l.logger, acceptor.clock.AfterFunc)
	addr := listenNodeAddr(t, acceptor)

	var raw []net.Conn
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial unauthenticated connection %d: %v", i, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		raw = append(raw, c)
	}

	waitFor(t, func() bool { return acceptor.l.unauth.count() == 2 })

	if err := raw[0].SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := raw[0].Read(make([]byte, 1)); err == nil {
		t.Fatal("the oldest unauthenticated connection was not closed")
	}
	if n := acceptor.l.unauth.count(); n != 2 {
		t.Fatalf("unauthenticated count = %d, want 2", n)
	}

	dialOnce(t, dialer.l, addr)
	acceptor.waitUps(t, 1)
}

// TestCloseStopsCallbacksAndGoroutines checks that Close returns, tears the
// links down and leaves no callback or handler running afterwards.
func TestCloseStopsCallbacksAndGoroutines(t *testing.T) {
	acceptor := newListenNode(t)
	dialerB := newListenNode(t)
	dialerC := newListenNode(t)
	linktest.Introduce(dialerB.peers, acceptor.peers, "b", "acceptor")
	linktest.Introduce(dialerC.peers, acceptor.peers, "c", "acceptor")

	acceptor.l.HandleState(200, func(Fingerprint, Frame) {
		acceptor.mu.Lock()
		acceptor.handled++
		acceptor.mu.Unlock()
	})

	addr := listenNodeAddr(t, acceptor)
	connB := dialOnce(t, dialerB.l, addr)
	acceptor.waitUps(t, 1)
	connC := dialOnce(t, dialerC.l, addr)
	acceptor.waitUps(t, 2)

	// Prove the state handler runs before Close.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := connB.sendDirect(ctx, &linkpb.Frame{Kind: 200})
	cancel()
	if err != nil {
		t.Fatalf("send a state frame from the peer: %v", err)
	}
	acceptor.waitHandled(t, 1)

	closed := make(chan struct{})
	go func() {
		acceptor.l.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return within 3s")
	}

	ups, downs, handled := acceptor.counts()

	connB.closeAsync(FailConnectionLost)
	connC.closeAsync(FailConnectionLost)
	acceptor.clock.Advance(time.Hour)
	// Teardown of the two accepted connections is what could post a late
	// notification. Once both records are back to waiting, any further callback
	// would have to come from the stopped notifier, which cannot deliver.
	for _, fp := range []Fingerprint{dialerB.fingerprint(), dialerC.fingerprint()} {
		waitFor(t, func() bool { return acceptor.l.State(fp).Phase == PhaseWaiting })
	}

	ups2, downs2, handled2 := acceptor.counts()
	if ups2 != ups || downs2 != downs || handled2 != handled {
		t.Fatalf("callbacks continued after Close: before=(%d,%d,%d) after=(%d,%d,%d)",
			ups, downs, handled, ups2, downs2, handled2)
	}

	if err := acceptor.l.Send(dialerB.fingerprint(), Frame{Kind: 200}); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("Send after Close = %v, want ErrNotEstablished", err)
	}
	if err := acceptor.l.Listen("127.0.0.1:0"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Listen after Close = %v, want ErrClosed", err)
	}
	if got := acceptor.l.Addr(); got != nil {
		t.Fatalf("Addr after Close = %v, want nil", got)
	}

	acceptor.l.Close()
}
