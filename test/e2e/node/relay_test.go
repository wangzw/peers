package node

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/link/linkpb"
)

func openRelayClient(t *testing.T, addr string) (grpc.BidiStreamingClient[linkpb.Frame, linkpb.Frame], [sha256.Size]byte) {
	t.Helper()
	cert, err := NewSelfSignedCert()
	if err != nil {
		t.Fatalf("node: NewSelfSignedCert: %v", err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(clientTLS(cert)))
	if err != nil {
		t.Fatalf("node: dialing the relay at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	stream, err := linkpb.NewLinkClient(conn).Connect(context.Background())
	if err != nil {
		t.Fatalf("node: opening a stream through the relay: %v", err)
	}
	return stream, sha256.Sum256(cert.Certificate[0])
}

func roundTrip(t *testing.T, stream grpc.BidiStreamingClient[linkpb.Frame, linkpb.Frame], frames []*linkpb.Frame) {
	t.Helper()
	for _, frame := range frames {
		if err := stream.Send(frame); err != nil {
			t.Fatalf("node: sending a frame through the relay: %v", err)
		}
	}
	for i, want := range frames {
		got, err := stream.Recv()
		if err != nil {
			t.Fatalf("node: receiving frame %d through the relay: %v", i, err)
		}
		if !proto.Equal(got, want) {
			t.Fatalf("frame %d came back as %v, want %v", i, got, want)
		}
	}
}

// The counters advance after the copy writes to the peer, so a client can see
// its frame before the relay has counted it.
func waitRelayStats(t *testing.T, r *Relay, streams, up, down int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		gotStreams, gotUp, gotDown := r.Stats()
		if gotStreams == streams && gotUp == up && gotDown == down {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Relay.Stats() = (%d, %d, %d), want (%d, %d, %d)",
				gotStreams, gotUp, gotDown, streams, up, down)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRelayCopiesFramesBothWays checks that the relay copies frames in both
// directions and presents its own certificate to upstream, never a client's.
func TestRelayCopiesFramesBothWays(t *testing.T) {
	echo := &linkTestServer{echo: true}
	upstreamAddr := startLinkTestServer(t, echo)

	relay, err := StartRelay("127.0.0.1:0", upstreamAddr)
	if err != nil {
		t.Fatalf("StartRelay: %v", err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	relayAddr := relay.listener.Addr().String()

	first := []*linkpb.Frame{
		{Kind: linkpb.KindPeerInfo, RequestId: 1, Payload: []byte("one")},
		{Kind: linkpb.KindPeerInfo, RequestId: 2, IsReply: true, Payload: []byte("two")},
		{Kind: linkpb.KindPeerInfo, RequestId: 3, Payload: []byte("three")},
	}
	stream, firstFingerprint := openRelayClient(t, relayAddr)
	roundTrip(t, stream, first)
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("node: closing the first stream's send side: %v", err)
	}
	waitRelayStats(t, relay, 1, 3, 3)

	// A second client checks that upstream always sees the relay's own
	// certificate; passing the client's through would make it an ordinary proxy.
	second := []*linkpb.Frame{{Kind: linkpb.KindPeerInfo, RequestId: 7, Payload: []byte("second")}}
	stream, secondFingerprint := openRelayClient(t, relayAddr)
	roundTrip(t, stream, second)
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("node: closing the second stream's send side: %v", err)
	}
	waitRelayStats(t, relay, 2, 4, 4)

	fingerprints := echo.fingerprintsCopy()
	if len(fingerprints) != 2 {
		t.Fatalf("the upstream stand-in saw %d client certificates, want 2", len(fingerprints))
	}
	if fingerprints[0] != fingerprints[1] {
		t.Fatalf("upstream certificate fingerprints %x and %x differ, want the relay's fixed certificate",
			fingerprints[0], fingerprints[1])
	}
	if fingerprints[0] == firstFingerprint || fingerprints[0] == secondFingerprint {
		t.Fatalf("upstream saw a client certificate fingerprint %x, want the relay's own", fingerprints[0])
	}
	if firstFingerprint == secondFingerprint {
		t.Fatal("the two relay clients presented the same certificate")
	}
}
