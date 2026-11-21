//go:build e2e

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/testkit/linktest"
	"github.com/wangzw/peers/test/e2e/fakepeer/fakerec"
)

// stubWait bounds every wait in these tests. The events it waits for cross a
// local gRPC stream and a pipe, and no product timer governs them, so it uses
// the same five-second fallback as harness.EventTimeout. This is a standalone
// main package and does not import the harness.
const stubWait = 5 * time.Second

// stubLinkServer records the frames of one stream. After stopAfter frames it
// optionally sends a Reject and ends the stream with endErr.
type stubLinkServer struct {
	linkpb.UnimplementedLinkServer

	frames     chan *linkpb.Frame
	stopAfter  int
	sendReject bool
	endErr     error
}

func (s *stubLinkServer) Connect(stream grpc.BidiStreamingServer[linkpb.Frame, linkpb.Frame]) error {
	for n := 1; ; n++ {
		f, err := stream.Recv()
		if err != nil {
			return nil
		}
		s.frames <- f
		if n < s.stopAfter {
			continue
		}
		if s.sendReject {
			payload, err := proto.Marshal(&linkpb.Reject{
				Reason: linkpb.RejectReason_REJECT_REASON_NOT_A_PEER,
			})
			if err != nil {
				return err
			}
			if err := stream.Send(&linkpb.Frame{Kind: linkpb.KindReject, Payload: payload}); err != nil {
				return nil
			}
		}
		return s.endErr
	}
}

// startStubServer runs stub behind TLS on the loopback interface and returns
// its address. The identity comes from the link test kit; no Broker runs.
func startStubServer(t *testing.T, stub *stubLinkServer) string {
	t.Helper()
	id := linktest.NewIdentity(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(id.TLSConfig())))
	linkpb.RegisterLinkServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

// dialStub returns the open function dialUnpaired uses and a cleanup for the
// client connection. The server certificate is not verified because the stub
// has no fingerprint the client could compare against.
func dialStub(t *testing.T, address string) (func(context.Context) (linkpb.Link_ConnectClient, error), func()) {
	t.Helper()
	var conn *grpc.ClientConn
	open := func(ctx context.Context) (linkpb.Link_ConnectClient, error) {
		c, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS13,
		})))
		if err != nil {
			return nil, err
		}
		conn = c
		return linkpb.NewLinkClient(c).Connect(ctx)
	}
	return open, func() {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func waitFrame(t *testing.T, stub *stubLinkServer) *linkpb.Frame {
	t.Helper()
	select {
	case f := <-stub.frames:
		return f
	case <-time.After(stubWait):
		t.Fatal("the stub did not receive the next frame in time")
		return nil
	}
}

func TestDialUnpairedSendsHelloThenRecords(t *testing.T) {
	stub := &stubLinkServer{
		frames:     make(chan *linkpb.Frame, 8),
		stopAfter:  3,
		sendReject: true,
	}
	open, closeConn := dialStub(t, startStubServer(t, stub))
	t.Cleanup(closeConn)

	rec, err := fakerec.Build(fakerec.Spec{
		Seed:      "unpaired-card",
		Kind:      "card",
		Alias:     "intruder",
		Addresses: []fakerec.Addr{{Kind: "direct", Addr: "127.0.0.1:17443"}},
		Version:   1,
		ExpiresIn: 24 * time.Hour,
	}, testNow)
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	set := &recordSet{}
	set.set([]*directorypb.Record{rec})

	ctx, cancel := context.WithTimeout(context.Background(), stubWait)
	defer cancel()
	reason := dialUnpaired(ctx, open, set)
	if reason != "REJECT_REASON_NOT_A_PEER" {
		t.Fatalf("dialUnpaired returned %q, want REJECT_REASON_NOT_A_PEER", reason)
	}

	frames := []*linkpb.Frame{waitFrame(t, stub), waitFrame(t, stub), waitFrame(t, stub)}
	wantKinds := []uint32{linkpb.KindHello, directorypb.KindRecords, directorypb.KindRecordList}
	for i, want := range wantKinds {
		if got := frames[i].GetKind(); got != want {
			t.Fatalf("frame %d kind = %d, want %d", i, got, want)
		}
	}

	hello := new(linkpb.Hello)
	if err := proto.Unmarshal(frames[0].GetPayload(), hello); err != nil {
		t.Fatalf("decode Hello: %v", err)
	}
	if got := hello.GetProtocolVersion(); got != linkpb.ProtocolVersion {
		t.Errorf("Hello protocol version = %d, want %d", got, linkpb.ProtocolVersion)
	}
	if got := hello.GetAlias(); got != "fakepeer" {
		t.Errorf("Hello alias = %q, want %q", got, "fakepeer")
	}
	if got := len(hello.GetNonce()); got != 32 {
		t.Errorf("Hello nonce length = %d, want 32", got)
	}

	records := new(directorypb.Records)
	if err := proto.Unmarshal(frames[1].GetPayload(), records); err != nil {
		t.Fatalf("decode Records: %v", err)
	}
	if n := len(records.GetRecords()); n != 1 {
		t.Fatalf("Records carries %d records, want 1", n)
	}
	if !bytes.Equal(records.GetRecords()[0].GetRaw(), rec.GetRaw()) {
		t.Errorf("Records changed the raw bytes")
	}

	list := new(directorypb.RecordList)
	if err := proto.Unmarshal(frames[2].GetPayload(), list); err != nil {
		t.Fatalf("decode RecordList: %v", err)
	}
	if n := len(list.GetRecords()); n != 1 {
		t.Fatalf("RecordList carries %d records, want 1", n)
	}
}

func TestDialUnpairedReportsStreamError(t *testing.T) {
	stub := &stubLinkServer{
		frames:    make(chan *linkpb.Frame, 8),
		stopAfter: 1,
		endErr:    errors.New("stub ended"),
	}
	open, closeConn := dialStub(t, startStubServer(t, stub))
	t.Cleanup(closeConn)

	ctx, cancel := context.WithTimeout(context.Background(), stubWait)
	defer cancel()
	if reason := dialUnpaired(ctx, open, &recordSet{}); reason == "" {
		t.Fatal("dialUnpaired returned an empty reason")
	}
}
