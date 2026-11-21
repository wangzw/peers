package link

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestUnauthenticatedReadBudgetClosesLargeFirstFrame reproduces the unbounded
// pre-authentication read: a connection that has not authenticated sends one
// PairRequest larger than the 64 KiB read budget. serveInbound must never
// receive that frame, and the close must be logged with cause byte_budget.
func TestUnauthenticatedReadBudgetClosesLargeFirstFrame(t *testing.T) {
	var logs bytes.Buffer
	server := newTransportTestLinkWithLogger(t, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))

	frameRead := make(chan struct{}, 1)
	server.peers.SetAccept(func(pairing.PairConn) error {
		frameRead <- struct{}{}
		return errors.New("test: pairing is not exercised")
	})

	handlerDone := make(chan struct{})
	addr := startTransportServer(t, server.Link, func(c *conn, tr *trackedConn) {
		defer close(handlerDone)
		_, _ = server.serveInbound(c, tr)
	})

	client := newTransportTestLink(t)
	c, _ := openTestStream(t, client.Link, addr)

	payload, err := proto.Marshal(&pairingpb.PairRequest{
		LookupTag: make([]byte, 1<<20),
	})
	if err != nil {
		t.Fatalf("marshal PairRequest: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	_ = c.sendDirect(ctx, &linkpb.Frame{Kind: pairingpb.KindPairRequest, Payload: payload})

	select {
	case <-handlerDone:
	case <-time.After(eventTimeout):
		t.Fatal("serveInbound did not return after the oversized first frame")
	}
	select {
	case <-frameRead:
		t.Fatal("serveInbound received a first frame above the 64 KiB pre-authentication read budget")
	default:
	}
	if !strings.Contains(logs.String(), `"cause":"byte_budget"`) {
		t.Fatalf("the read-budget close did not log cause byte_budget:\n%s", logs.String())
	}
}

// TestHeaderListLimitRejectsOversizedMetadata reproduces the unbounded header
// list: an unauthenticated stream carries 32 KiB of metadata, above the 16 KiB
// HTTP/2 header list limit. The stream must be rejected before the Connect
// handler runs, and the client must see ResourceExhausted.
func TestHeaderListLimitRejectsOversizedMetadata(t *testing.T) {
	server := newTransportTestLink(t)
	started := make(chan struct{}, 1)
	addr := startTransportServer(t, server.Link, func(c *conn, _ *trackedConn) {
		started <- struct{}{}
		<-c.done()
	})

	client := newTransportTestLink(t)
	tc, id := dialTestConnection(t, client.Link, addr)
	cc, err := client.newGRPCClientConn(tc, id, addr)
	if err != nil {
		t.Fatalf("newGRPCClientConn: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	md := metadata.Pairs("x-link-test-padding", strings.Repeat("m", 32<<10))
	stream, err := linkpb.NewLinkClient(cc).Connect(metadata.NewOutgoingContext(ctx, md))

	recvErr := make(chan error, 1)
	if err != nil {
		recvErr <- err
	} else {
		go func() {
			_, recv := stream.Recv()
			recvErr <- recv
		}()
	}

	select {
	case err := <-recvErr:
		if err == nil {
			t.Fatal("a stream carrying 32 KiB of metadata was accepted")
		}
		// The client refuses to send the header list after learning the server's
		// limit, or the server rejects it on the wire; both are the rejection
		// under test and leave the Connect handler unrun.
		if code := status.Code(err); code != codes.ResourceExhausted &&
			!strings.Contains(err.Error(), "header list size") {
			t.Fatalf("oversized metadata error = %v, want a header-list rejection", err)
		}
	case <-started:
		t.Fatal("the server accepted a pre-authentication stream carrying 32 KiB of metadata")
	case <-time.After(eventTimeout):
		t.Fatal("the stream carrying 32 KiB of metadata was neither rejected nor accepted")
	}
}
