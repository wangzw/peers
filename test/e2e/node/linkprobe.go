package node

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// ProbeHello dials addr over TLS with a fresh certificate, sends a Hello as the
// first frame and returns the Reject reason. It is how a test asks a Broker
// what it does with a client that is not a paired peer.
//
// gRPC's own TLS credentials insist on the h2 ALPN protocol, which the Broker's
// Link server does not negotiate, so the handshake is done here and the
// connection is then handed to gRPC.
//
// A stream that ends, or a ctx that expires, before a Reject arrives is an
// error.
func ProbeHello(ctx context.Context, addr string) (linkpb.RejectReason, error) {
	cert, err := NewSelfSignedCert()
	if err != nil {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, err
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: generate nonce: %w", err)
	}
	payload, err := proto.Marshal(&linkpb.Hello{
		ProtocolVersion: linkpb.ProtocolVersion,
		Alias:           "probe",
		Nonce:           nonce,
	})
	if err != nil {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: encode hello: %w", err)
	}

	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: dial %s: %w", addr, err)
	}
	tc := tls.Client(raw, &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // A peer is identified by its pairing key, not by a trusted chain.
		Certificates:       []tls.Certificate{cert},
	})
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: TLS handshake with %s: %w", addr, err)
	}

	var used atomic.Bool
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			if !used.CompareAndSwap(false, true) {
				return nil, errors.New("node: probe hello: transport already used")
			}
			return tc, nil
		}),
		grpc.WithTransportCredentials(probeCreds{}),
		grpc.WithNoProxy(),
	)
	if err != nil {
		_ = tc.Close()
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: dial %s: %w", addr, err)
	}
	defer func() {
		_ = conn.Close()
		_ = tc.Close()
	}()

	stream, err := linkpb.NewLinkClient(conn).Connect(ctx)
	if err != nil {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: open stream: %w", err)
	}
	if err := stream.Send(&linkpb.Frame{Kind: linkpb.KindHello, Payload: payload}); err != nil {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: send hello: %w", err)
	}

	for {
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: stream ended before a reject: %w", err)
			}
			return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: no reject: %w", err)
		}
		if frame.GetKind() != linkpb.KindReject {
			continue
		}
		var reject linkpb.Reject
		if err := proto.Unmarshal(frame.GetPayload(), &reject); err != nil {
			return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("node: probe hello: decode reject: %w", err)
		}
		return reject.GetReason(), nil
	}
}

type probeCreds struct{}

func (probeCreds) ClientHandshake(_ context.Context, _ string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return raw, probeAuthInfo{}, nil
}

func (probeCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("node: probe credentials cannot accept")
}

func (probeCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (probeCreds) Clone() credentials.TransportCredentials { return probeCreds{} }

func (probeCreds) OverrideServerName(string) error { return nil }

type probeAuthInfo struct{}

func (probeAuthInfo) AuthType() string { return "peers-tls" }

func (probeAuthInfo) GetCommonAuthInfo() credentials.CommonAuthInfo {
	return credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}
}
