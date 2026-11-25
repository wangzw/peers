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

// lingerStubServerCreds performs the TLS handshake itself because the real
// dialer offers no ALPN protocol, which grpc's standard TLS credentials refuse.
type lingerStubServerCreds struct {
	id linktest.Identity
}

func (c lingerStubServerCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	tc := tls.Server(raw, c.id.TLSConfig())
	if err := tc.Handshake(); err != nil {
		_ = tc.Close()
		return nil, nil, err
	}
	return tc, lingerStubAuthInfo{}, nil
}

func (lingerStubServerCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("stub: server credentials cannot dial")
}

func (lingerStubServerCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c lingerStubServerCreds) Clone() credentials.TransportCredentials { return c }

func (lingerStubServerCreds) OverrideServerName(string) error { return nil }

type lingerStubAuthInfo struct{}

func (lingerStubAuthInfo) AuthType() string { return "stub" }

// lingerStubAcceptor answers the dialer's Hello with a newer protocol version
// and then pauses before reading, so a dialer that closes early loses the
// Reject.
type lingerStubAcceptor struct {
	linkpb.UnimplementedLinkServer

	version   uint32
	readDelay time.Duration
	frames    chan *linkpb.Frame
	ended     chan error
}

func (s *lingerStubAcceptor) Connect(stream grpc.BidiStreamingServer[linkpb.Frame, linkpb.Frame]) error {
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

	time.Sleep(s.readDelay)

	for {
		f, err := stream.Recv()
		if err != nil {
			s.ended <- err
			return nil
		}
		s.frames <- f
	}
}

// TestDialerRejectReachesAcceptorBeforeClose checks that the dialer's Reject for
// a version mismatch is written to the stream before the dialer closes it.
func TestDialerRejectReachesAcceptorBeforeClose(t *testing.T) {
	id := linktest.NewIdentity(t)
	stub := &lingerStubAcceptor{
		version:   linkpb.ProtocolVersion + 1,
		readDelay: 250 * time.Millisecond,
		frames:    make(chan *linkpb.Frame, 4),
		ended:     make(chan error, 1),
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(lingerStubServerCreds{id: id}))
	linkpb.RegisterLinkServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	dialer := newNode(t, "dialer")
	dialer.Peers.Add(id.Fingerprint, pairing.RoleDialer, "stub", ln.Addr().String())
	dialer.Start()

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
	case err := <-stub.ended:
		t.Fatalf("the accepting side never received the dialer's Reject: %v", err)
	case <-time.After(eventTimeout):
		t.Fatal("the accepting side never received the dialer's Reject")
	}

	waitState(t, dialer.Link, id.Fingerprint, "dialer to back off with version_incompatible", func(st link.LinkState) bool {
		return st.Phase == link.PhaseBackoff && st.LastFailure == link.FailVersionIncompatible && st.PeerVersion == linkpb.ProtocolVersion+1
	})
	if got := dialer.upCount(id.Fingerprint); got != 0 {
		t.Fatalf("dialer reported the link up %d times, want never", got)
	}
}
