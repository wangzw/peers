package node

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/link/linkpb"
)

type linkTestServer struct {
	linkpb.UnimplementedLinkServer

	echo         bool
	rejectReason linkpb.RejectReason

	mu           sync.Mutex
	fingerprints [][sha256.Size]byte
	hello        *linkpb.Hello
}

func (s *linkTestServer) Connect(stream grpc.BidiStreamingServer[linkpb.Frame, linkpb.Frame]) error {
	s.recordPeer(stream.Context())

	if s.echo {
		for {
			frame, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			if err := stream.Send(frame); err != nil {
				return err
			}
		}
	}

	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := &linkpb.Hello{}
	if err := proto.Unmarshal(frame.GetPayload(), hello); err != nil {
		return err
	}
	s.mu.Lock()
	s.hello = hello
	s.mu.Unlock()

	payload, err := proto.Marshal(&linkpb.Reject{Reason: s.rejectReason})
	if err != nil {
		return err
	}
	return stream.Send(&linkpb.Frame{Kind: linkpb.KindReject, Payload: payload})
}

func (s *linkTestServer) recordPeer(ctx context.Context) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return
	}
	fp := sha256.Sum256(info.State.PeerCertificates[0].Raw)
	s.mu.Lock()
	s.fingerprints = append(s.fingerprints, fp)
	s.mu.Unlock()
}

func (s *linkTestServer) fingerprintsCopy() [][sha256.Size]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][sha256.Size]byte(nil), s.fingerprints...)
}

func (s *linkTestServer) helloCopy() *linkpb.Hello {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hello
}

func startLinkTestServer(t *testing.T, srv linkpb.LinkServer) string {
	t.Helper()
	cert, err := NewSelfSignedCert()
	if err != nil {
		t.Fatalf("node: NewSelfSignedCert: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("node: listening for the Link stand-in: %v", err)
	}
	server := grpc.NewServer(grpc.Creds(probeServerCreds{cfg: &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
		// The relay's upstream client goes through gRPC's own TLS credentials,
		// which require h2 when the server offers ALPN; the probe offers none.
		NextProtos: []string{"h2"},
	}}))
	linkpb.RegisterLinkServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// probeServerCreds completes the TLS handshake itself and requires no ALPN
// protocol, which gRPC's own server credentials would insist on.
type probeServerCreds struct{ cfg *tls.Config }

func (c probeServerCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	tc := tls.Server(raw, c.cfg)
	if err := tc.HandshakeContext(context.Background()); err != nil {
		_ = tc.Close()
		return nil, nil, err
	}
	return tc, credentials.TLSInfo{
		State:          tc.ConnectionState(),
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
	}, nil
}

func (probeServerCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("node: probe server credentials cannot dial")
}

func (probeServerCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c probeServerCreds) Clone() credentials.TransportCredentials { return c }

func (probeServerCreds) OverrideServerName(string) error { return nil }

// TestProbeHelloGetsReject checks that ProbeHello returns the Broker's Reject
// reason after sending a complete Hello.
func TestProbeHelloGetsReject(t *testing.T) {
	srv := &linkTestServer{rejectReason: linkpb.RejectReason_REJECT_REASON_NOT_A_PEER}
	addr := startLinkTestServer(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reason, err := ProbeHello(ctx, addr)
	if err != nil {
		t.Fatalf("ProbeHello: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_NOT_A_PEER {
		t.Fatalf("reason = %v, want REJECT_REASON_NOT_A_PEER", reason)
	}

	hello := srv.helloCopy()
	if hello == nil {
		t.Fatal("the stand-in received no Hello")
	}
	if hello.GetProtocolVersion() != linkpb.ProtocolVersion {
		t.Fatalf("protocol_version = %d, want %d", hello.GetProtocolVersion(), linkpb.ProtocolVersion)
	}
	if hello.GetAlias() != "probe" {
		t.Fatalf("alias = %q, want %q", hello.GetAlias(), "probe")
	}
	if len(hello.GetNonce()) != 32 {
		t.Fatalf("nonce length = %d, want 32", len(hello.GetNonce()))
	}
}

// TestProbeHelloUsesFreshCertificate checks that every ProbeHello presents a
// different client certificate.
func TestProbeHelloUsesFreshCertificate(t *testing.T) {
	srv := &linkTestServer{rejectReason: linkpb.RejectReason_REJECT_REASON_NOT_A_PEER}
	addr := startLinkTestServer(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if _, err := ProbeHello(ctx, addr); err != nil {
			t.Fatalf("ProbeHello call %d: %v", i+1, err)
		}
	}

	fingerprints := srv.fingerprintsCopy()
	if len(fingerprints) != 2 {
		t.Fatalf("the stand-in saw %d certificates, want 2", len(fingerprints))
	}
	if fingerprints[0] == fingerprints[1] {
		t.Fatalf("both probes presented the same certificate fingerprint %x, want a fresh one per probe", fingerprints[0])
	}
}
