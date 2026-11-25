package link_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// TestLinkVersionAcceptorNewer checks that a newer accepting side leaves the
// dialer backing off with version incompatible and records the version on the
// waiting side.
func TestLinkVersionAcceptorNewer(t *testing.T) {
	acceptor := newNode(t, "b", func(o *link.Options) { o.ProtocolVersion = linkpb.ProtocolVersion + 1 })
	dialer := newNode(t, "a")
	addr := listenAt(t, acceptor)
	linktest.Introduce(dialer.Peers, acceptor.Peers, "a", "b", addr)
	startAcceptorThenDialer(acceptor, dialer)

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()

	waitState(t, dialer.Link, fpAcceptor, "dialer to back off with version_incompatible", func(st link.LinkState) bool {
		return st.Phase == link.PhaseBackoff && st.LastFailure == link.FailVersionIncompatible
	})

	waitState(t, acceptor.Link, fpDialer, "acceptor to record the dialer's version", func(st link.LinkState) bool {
		return st.Phase == link.PhaseWaiting && st.LastFailure == link.FailVersionIncompatible && st.PeerVersion == linkpb.ProtocolVersion
	})
	if got := acceptor.upCount(fpDialer); got != 0 {
		t.Fatalf("acceptor reported the link up %d times, want never", got)
	}
	if got := dialer.upCount(fpAcceptor); got != 0 {
		t.Fatalf("dialer reported the link up %d times, want never", got)
	}
}

// TestLinkVersionDialerNewer checks that the accepting side rejects a newer
// dialer's Hello and waits with the dialer's recorded version while the dialer
// backs off as version incompatible and the link never comes up.
func TestLinkVersionDialerNewer(t *testing.T) {
	dialer := newNode(t, "b", func(o *link.Options) { o.ProtocolVersion = linkpb.ProtocolVersion + 1 })
	acceptor := newNode(t, "a")
	addr := listenAt(t, acceptor)
	linktest.Introduce(dialer.Peers, acceptor.Peers, "b", "a", addr)
	startAcceptorThenDialer(acceptor, dialer)

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()

	waitState(t, acceptor.Link, fpDialer, "acceptor to record the dialer's version", func(st link.LinkState) bool {
		return st.Phase == link.PhaseWaiting && st.LastFailure == link.FailVersionIncompatible && st.PeerVersion == linkpb.ProtocolVersion+1
	})
	waitState(t, dialer.Link, fpAcceptor, "dialer to back off after the reject", func(st link.LinkState) bool {
		return st.Phase == link.PhaseBackoff && st.LastFailure == link.FailVersionIncompatible
	})
	if got := acceptor.upCount(fpDialer); got != 0 {
		t.Fatalf("acceptor reported the link up %d times, want never", got)
	}
	if got := dialer.upCount(fpAcceptor); got != 0 {
		t.Fatalf("dialer reported the link up %d times, want never", got)
	}
}

// stubServerCreds performs the TLS handshake itself because the real dialer
// offers no ALPN protocol, which grpc's standard TLS credentials refuse.
type stubServerCreds struct {
	id linktest.Identity
}

func (c stubServerCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	tc := tls.Server(raw, c.id.TLSConfig())
	if err := tc.Handshake(); err != nil {
		_ = tc.Close()
		return nil, nil, err
	}
	return tc, stubAuthInfo{}, nil
}

func (stubServerCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("stub: server credentials cannot dial")
}

func (stubServerCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c stubServerCreds) Clone() credentials.TransportCredentials { return c }

func (stubServerCreds) OverrideServerName(string) error { return nil }

type stubAuthInfo struct{}

func (stubAuthInfo) AuthType() string { return "stub" }

// stubAcceptor answers the dialer's Hello with a newer protocol version and
// records the frames the dialer sends after it.
type stubAcceptor struct {
	linkpb.UnimplementedLinkServer

	version uint32
	frames  chan *linkpb.Frame
	ended   chan error
}

func (s *stubAcceptor) Connect(stream grpc.BidiStreamingServer[linkpb.Frame, linkpb.Frame]) error {
	first, err := stream.Recv()
	if err != nil {
		s.ended <- fmt.Errorf("read the dialer's Hello: %w", err)
		return nil
	}
	if first.GetKind() != linkpb.KindHello {
		s.ended <- fmt.Errorf("first frame kind %d, want Hello", first.GetKind())
		return nil
	}
	hello := new(linkpb.Hello)
	if err := proto.Unmarshal(first.GetPayload(), hello); err != nil {
		s.ended <- fmt.Errorf("decode the dialer's Hello: %w", err)
		return nil
	}
	if got := hello.GetProtocolVersion(); got != linkpb.ProtocolVersion {
		s.ended <- fmt.Errorf("dialer version %d, want %d", got, linkpb.ProtocolVersion)
		return nil
	}

	payload, err := proto.Marshal(&linkpb.Hello{
		ProtocolVersion: s.version,
		Alias:           "stub",
		Nonce:           make([]byte, 32),
	})
	if err != nil {
		s.ended <- fmt.Errorf("encode the stub Hello: %w", err)
		return nil
	}
	if err := stream.Send(&linkpb.Frame{Kind: linkpb.KindHello, Payload: payload}); err != nil {
		s.ended <- fmt.Errorf("send the stub Hello: %w", err)
		return nil
	}

	for {
		f, err := stream.Recv()
		if err != nil {
			s.ended <- err
			return nil
		}
		s.frames <- f
	}
}

// TestDialerRejectsAcceptorHello checks that a dialer rejects a Hello with a
// newer version, sends Reject, ends the stream and never reports the link up.
func TestDialerRejectsAcceptorHello(t *testing.T) {
	id := linktest.NewIdentity(t)
	stub := &stubAcceptor{version: linkpb.ProtocolVersion + 1, frames: make(chan *linkpb.Frame, 4), ended: make(chan error, 1)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(stubServerCreds{id: id}))
	linkpb.RegisterLinkServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	dialer := newNode(t, "dialer")
	dialer.Peers.Add(id.Fingerprint, pairing.RoleDialer, "stub", ln.Addr().String())
	dialer.Start()

	waitState(t, dialer.Link, id.Fingerprint, "dialer to back off with version_incompatible after the newer Hello", func(st link.LinkState) bool {
		return st.Phase == link.PhaseBackoff && st.LastFailure == link.FailVersionIncompatible && st.PeerVersion == linkpb.ProtocolVersion+1
	})

	select {
	case f := <-stub.frames:
		if f.GetKind() != linkpb.KindReject {
			t.Fatalf("the dialer sent frame kind %d after the Hello, want Reject", f.GetKind())
		}
		reject := new(linkpb.Reject)
		if err := proto.Unmarshal(f.GetPayload(), reject); err != nil {
			t.Fatalf("decode the dialer's Reject: %v", err)
		}
		if got := reject.GetReason(); got != linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE {
			t.Fatalf("reject reason = %v, want PROTOCOL_VERSION_INCOMPATIBLE", got)
		}
	case <-time.After(eventTimeout):
		select {
		case err := <-stub.ended:
			t.Fatalf("the dialer sent no Reject after the version mismatch (stream ended: %v)", err)
		default:
			t.Fatal("the dialer sent no Reject after the version mismatch")
		}
	}
	select {
	case f := <-stub.frames:
		t.Fatalf("the dialer sent an extra frame after the Reject: kind %d", f.GetKind())
	case err := <-stub.ended:
		t.Logf("the dialer ended the stream after the Reject: %v", err)
	case <-time.After(eventTimeout):
		t.Fatalf("the dialer did not end the stream within %v", eventTimeout)
	}
	if got := dialer.upCount(id.Fingerprint); got != 0 {
		t.Fatalf("dialer reported the link up %d times, want never", got)
	}
}

// TestLinkVersionRecoversAfterUpgrade checks that the dialer establishes the
// link once the accepting side runs the same version.
func TestLinkVersionRecoversAfterUpgrade(t *testing.T) {
	acceptor := newNode(t, "b", func(o *link.Options) { o.ProtocolVersion = linkpb.ProtocolVersion + 1 })
	dialer := newNode(t, "a")
	addr := listenAt(t, acceptor)
	linktest.Introduce(dialer.Peers, acceptor.Peers, "a", "b", addr)
	dialer.Start()
	acceptor.Start()

	fpAcceptor := acceptor.Peers.Fingerprint()
	waitState(t, dialer.Link, fpAcceptor, "dialer to back off with version_incompatible", func(st link.LinkState) bool {
		return st.Phase == link.PhaseBackoff && st.LastFailure == link.FailVersionIncompatible
	})

	acceptor.Close()
	upgraded := newNodeFrom(t, acceptor.Peers, acceptor.Settings, linktest.NewFakeClock(time.Unix(1_700_000_000, 0)))
	if err := upgraded.Listen(addr); err != nil {
		t.Fatalf("restarted Listen(%s): %v", addr, err)
	}
	upgraded.Start()
	dialer.Kick(fpAcceptor)

	dialer.waitUp(t, fpAcceptor, 1)
	st := dialer.State(fpAcceptor)
	if st.LastFailure != link.FailNone {
		t.Fatalf("last failure after the upgrade = %v, want none", st.LastFailure)
	}
	if st.PeerVersion != 0 {
		t.Fatalf("peer version after the upgrade = %d, want 0", st.PeerVersion)
	}
}
