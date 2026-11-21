package link

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
)

// TestIdentityFromStateRejectsNonEd25519 checks that a peer certificate with a
// key other than Ed25519 is refused: the fingerprint must always name an
// Ed25519 key taken from the SubjectPublicKeyInfo.
func TestIdentityFromStateRejectsNonEd25519(t *testing.T) {
	st := tls.ConnectionState{
		Version:          tls.VersionTLS13,
		PeerCertificates: []*x509.Certificate{{PublicKey: &rsa.PublicKey{}}},
	}
	_, err := identityFromState(st, "127.0.0.1:1")
	if err == nil {
		t.Fatal("identityFromState accepted a non-Ed25519 certificate key")
	}
	if !strings.Contains(err.Error(), "not Ed25519") {
		t.Fatalf("error = %v, want it to name the Ed25519 requirement", err)
	}
}

// closeTrackingListener wraps a TCP listener and records whether each accepted
// connection was closed. The unauthenticated registry closes the raw connection
// it tracks when it evicts it, so a closed record proves the connection was
// evicted rather than authenticated.
type closeTrackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []*closeTrackingConn
}

func listenCloseTracking(t *testing.T) *closeTrackingListener {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return &closeTrackingListener{Listener: raw}
}

func (l *closeTrackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	cc := &closeTrackingConn{Conn: c}
	l.mu.Lock()
	l.conns = append(l.conns, cc)
	l.mu.Unlock()
	return cc, nil
}

func (l *closeTrackingListener) accepted() []*closeTrackingConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*closeTrackingConn(nil), l.conns...)
}

// closeTrackingConn records Close so a test can tell that the registry evicted
// the connection.
type closeTrackingConn struct {
	net.Conn
	mu     sync.Mutex
	closed bool
}

func (c *closeTrackingConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.Conn.Close()
}

func (c *closeTrackingConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// startTransportServerOn serves l's gRPC endpoint on ln and tracks accepted
// connections with l's unauth registry, as Listen does.
func startTransportServerOn(t *testing.T, l *Link, onStream func(*conn, *trackedConn), ln net.Listener) {
	t.Helper()
	tl := &trackingListener{Listener: ln, reg: l.unauth}
	srv := l.newGRPCServer(onStream)
	go func() { _ = srv.Serve(tl) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = tl.Close()
	})
}

// pendingPairingFlood opens one inbound connection that completed TLS, holds it
// unauthenticated inside a blocked pairing Accept, then registers 100
// connections from 100 distinct stranger sources. It returns whether the
// inbound connection's raw socket was closed by the flood. prepare runs before
// the connection arrives and decides what the server's peer table holds for the
// dialer's fingerprint.
func pendingPairingFlood(t *testing.T, prepare func(server, client *transportTestLink)) bool {
	t.Helper()

	server := newTransportTestLink(t)
	client := newTransportTestLink(t)
	if prepare != nil {
		prepare(server, client)
	}

	accepted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server.peers.SetAccept(func(pairing.PairConn) error {
		once.Do(func() { close(accepted) })
		<-release
		return nil
	})
	defer close(release)

	lt := listenCloseTracking(t)
	startTransportServerOn(t, server.Link, server.acceptStream, lt)

	tc, id := dialTestConnection(t, client.Link, lt.Addr().String())
	cc, err := client.newGRPCClientConn(tc, id, lt.Addr().String())
	if err != nil {
		t.Fatalf("newGRPCClientConn: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), transportTestTimeout)
	defer cancel()
	stream, err := linkpb.NewLinkClient(cc).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := stream.Send(&linkpb.Frame{Kind: pairingpb.KindPairRequest}); err != nil {
		t.Fatalf("send PairRequest: %v", err)
	}

	select {
	case <-accepted:
	case <-time.After(transportTestTimeout):
		t.Fatal("pairing Accept was not called")
	}

	// The connection has completed TLS and is held unauthenticated inside
	// Accept. Its fingerprint decides which pool holds it: a known peer stays
	// in the known-peer pool, an unknown one in the stranger-source pool the
	// flood below fills.
	for i := 0; i < 100; i++ {
		server.unauth.track(&fakeSourceConn{remote: fmt.Sprintf("198.51.0.%d:4000", i)})
	}

	conns := lt.accepted()
	if len(conns) != 1 {
		t.Fatalf("accepted connections = %d, want 1", len(conns))
	}
	return conns[0].isClosed()
}

// TestKnownPeerSurvivesStrangerFlood reproduces the denial of service where a
// connection that completed TLS with a fingerprint in the peer table still
// counted against the stranger-source global limit. One hundred distinct
// stranger sources then arrive; before the fix the global-limit eviction closes
// the known peer's connection because it was the oldest in the shared pool.
func TestKnownPeerSurvivesStrangerFlood(t *testing.T) {
	if pendingPairingFlood(t, func(server, client *transportTestLink) {
		server.peers.Add(client.peers.Fingerprint(), pairing.RoleAcceptor, "client")
	}) {
		t.Fatal("connection from a known peer was closed when 100 distinct stranger sources connected")
	}
}

// TestUnknownPeerStaysInStrangerPool checks the other side of the rule: a
// fingerprint absent from the peer table stays in the stranger-source pool and
// is still evicted by the global limit when the flood arrives.
func TestUnknownPeerStaysInStrangerPool(t *testing.T) {
	if !pendingPairingFlood(t, nil) {
		t.Fatal("connection from an unknown fingerprint was not closed by the stranger-source limits")
	}
}

// TestUnpairedFingerprintStaysInStrangerPool checks a fingerprint that was a
// peer until it was just unpaired: it must fall back to the stranger-source
// pool, so the same flood that cannot evict a known peer does evict it.
func TestUnpairedFingerprintStaysInStrangerPool(t *testing.T) {
	if !pendingPairingFlood(t, func(server, client *transportTestLink) {
		fp := client.peers.Fingerprint()
		server.peers.Add(fp, pairing.RoleAcceptor, "client")
		server.peers.Remove(fp)
		if role := server.peers.Role(fp); role != pairing.RoleUnknown {
			t.Fatalf("unpaired fingerprint role = %v, want unknown", role)
		}
	}) {
		t.Fatal("connection from a just-unpaired fingerprint was not closed by the stranger-source limits")
	}
}
