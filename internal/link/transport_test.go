package link

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/testkit/linktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// eventTimeout is the package-wide fallback for the real-time waits in these
// tests that move a control-sized frame. The large-payload round trip uses
// largePayloadTimeout instead. It only decides how long a wait that will never
// complete takes to fail: the success path selects on the event itself and
// never spends this time, so a larger value costs nothing. It equals the
// product's default link.keepalive_interval (15s) plus link.keepalive_timeout
// (10s), the same values linktest.FakeSettings returns: a link the product
// still considers alive must not be declared dead by the test before the
// product's own keepalive policy would.
const eventTimeout = 15*time.Second + 10*time.Second

// largePayloadTimeout is the guard for one wait on the large-payload round
// trip. It is derived from the product's own liveness bound rather than picked
// for the test: a Link keeps a silent connection alive until one keepalive
// interval has passed and the keepalive that interval prompts goes unanswered
// for a keepalive timeout (settings.go). A test guard shorter than that would
// fail a transfer the product still considers alive, which is how the fixed
// three-second bound fired on a 1.5 MiB frame that was merely slow to be
// delivered under CI load. The success path is event-driven, so the guard only
// turns a frame that never arrives into a failure instead of a hang.
func largePayloadTimeout(settings *linktest.FakeSettings) time.Duration {
	return settings.KeepaliveInterval() + settings.KeepaliveTimeout()
}

// authReadyTimeout is the fallback bound for the wait on the server finishing
// authentication in TestLargePayloadRoundTrip. It only turns a wait that would
// never complete into a failure instead of a hang: the success path selects on
// the authentication event itself and never spends this time, so a larger
// value costs nothing. It is 30 seconds because the test packages use 30
// seconds as the uniform fallback for event waits, not because it is derived
// from a product timer: authentication readiness has no product timer of its
// own to follow.
const authReadyTimeout = 30 * time.Second

// transportTestLink bundles a Link with the fakes behind it.
type transportTestLink struct {
	*Link
	peers    *linktest.FakePeers
	settings *linktest.FakeSettings
	clock    *linktest.FakeClock
}

func newTransportTestLink(t *testing.T) *transportTestLink {
	t.Helper()
	return newTransportTestLinkWithLogger(t, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// newTransportTestLinkWithLogger is newTransportTestLink with a caller-chosen
// logger, so a test can assert on a Debug record.
func newTransportTestLinkWithLogger(t *testing.T, logger *slog.Logger) *transportTestLink {
	t.Helper()
	peers := linktest.NewFakePeers(t)
	settings := linktest.NewFakeSettings("test")
	clock := linktest.NewFakeClock(time.Unix(1_700_000_000, 0))
	l, err := New(Options{
		Peers:    peers,
		Settings: settings,
		Clock:    clock,
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &transportTestLink{Link: l, peers: peers, settings: settings, clock: clock}
}

// startTransportServer serves l's gRPC endpoint on a loopback port and returns
// its address. The listener tracks inbound connections with l's unauth
// registry, as Listen does.
func startTransportServer(t *testing.T, l *Link, onStream func(*conn, *trackedConn)) string {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ln := &trackingListener{Listener: raw, reg: l.unauth}
	srv := l.newGRPCServer(onStream)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
	})
	return raw.Addr().String()
}

// dialTestConnection dials addr and returns the authenticated TLS connection.
func dialTestConnection(t *testing.T, l *Link, addr string) (*tls.Conn, peerIdentity) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	tc, id, err := l.dialTLS(ctx, addr)
	if err != nil {
		t.Fatalf("dialTLS: %v", err)
	}
	t.Cleanup(func() { _ = tc.Close() })
	return tc, id
}

// openTestStream dials addr and opens the link stream over it.
func openTestStream(t *testing.T, l *Link, addr string) (*conn, peerIdentity) {
	t.Helper()
	tc, id := dialTestConnection(t, l, addr)
	c, err := l.openClientStream(tc, id, addr)
	if err != nil {
		t.Fatalf("openClientStream: %v", err)
	}
	t.Cleanup(func() { c.closeAsync(FailConnectionLost) })
	return c, id
}

func TestDialTLSReturnsServerIdentity(t *testing.T) {
	server := newTransportTestLink(t)
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {})
	client := newTransportTestLink(t)

	tc, id := dialTestConnection(t, client.Link, addr)
	if want := server.peers.Fingerprint(); id.fp != want {
		t.Fatalf("dialTLS fingerprint = %s, want server identity %s", id.fp, want)
	}
	if got := tc.ConnectionState().Version; got != tls.VersionTLS13 {
		t.Fatalf("negotiated TLS version = %#x, want TLS 1.3", got)
	}
	if len(id.pub) != 32 {
		t.Fatalf("identity public key has %d bytes, want 32", len(id.pub))
	}
}

func TestServerSeesClientIdentity(t *testing.T) {
	server := newTransportTestLink(t)
	seen := make(chan peerIdentity, 1)
	addr := startTransportServer(t, server.Link, func(c *conn, _ *trackedConn) {
		seen <- c.id
	})
	client := newTransportTestLink(t)

	openTestStream(t, client.Link, addr)

	select {
	case id := <-seen:
		if want := client.peers.Fingerprint(); id.fp != want {
			t.Fatalf("server saw %s, want client identity %s", id.fp, want)
		}
	case <-time.After(eventTimeout):
		t.Fatal("server did not accept the stream")
	}
}

func TestServerRejectsClientWithoutCertificate(t *testing.T) {
	server := newTransportTestLink(t)
	called := make(chan struct{}, 1)
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {
		called <- struct{}{}
	})

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	tc := tls.Client(raw, &tls.Config{
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true,
	})
	// The TLS layer only requests a client certificate, so the handshake itself
	// may succeed; the server must then reject the connection for having no
	// peer certificate.
	_ = tc.Handshake()
	_ = tc.SetReadDeadline(time.Now().Add(eventTimeout))
	if _, err := tc.Read(make([]byte, 1)); err == nil {
		t.Fatal("server kept a certificate-less connection open")
	}

	// The TLS handshake already ended, so onStream cannot run after this point.
	select {
	case <-called:
		t.Fatal("onStream ran for a certificate-less client")
	default:
	}
}

func TestServerRejectsTLS12(t *testing.T) {
	server := newTransportTestLink(t)
	called := make(chan struct{}, 1)
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {
		called <- struct{}{}
	})
	client := newTransportTestLink(t)

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	tc := tls.Client(raw, &tls.Config{
		Certificates:       []tls.Certificate{client.peers.Identity().Cert},
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	})
	if err := tc.Handshake(); err == nil {
		t.Fatal("TLS 1.2 handshake succeeded against a TLS 1.3 server")
	}

	// The TLS handshake already failed, so onStream cannot run after this point.
	select {
	case <-called:
		t.Fatal("onStream ran for a TLS 1.2 client")
	default:
	}
}

func TestFramesRoundTripOverGRPC(t *testing.T) {
	server := newTransportTestLink(t)
	received := make(chan []*linkpb.Frame, 1)
	addr := startTransportServer(t, server.Link, func(c *conn, tr *trackedConn) {
		// This handler stands in for serveInbound; mark the connection
		// authenticated so the pre-authentication read budget is off.
		if tr != nil {
			tr.authenticated()
		}
		ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
		defer cancel()
		frames := make([]*linkpb.Frame, 0, 3)
		for i := 0; i < 3; i++ {
			f, err := c.recv(ctx)
			if err != nil {
				return
			}
			frames = append(frames, f)
		}
		received <- frames
		for i := 0; i < 3; i++ {
			_ = c.sendDirect(ctx, &linkpb.Frame{Kind: 201, Payload: []byte{byte(i)}})
		}
	})
	client := newTransportTestLink(t)
	c, _ := openTestStream(t, client.Link, addr)

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := c.sendDirect(ctx, &linkpb.Frame{Kind: 200, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("client send %d: %v", i, err)
		}
	}

	var frames []*linkpb.Frame
	select {
	case frames = <-received:
	case <-time.After(eventTimeout):
		t.Fatal("server did not receive the frames")
	}
	for i, f := range frames {
		if f.GetKind() != 200 || !bytes.Equal(f.GetPayload(), []byte{byte(i)}) {
			t.Fatalf("server frame %d = kind %d payload %v", i, f.GetKind(), f.GetPayload())
		}
	}

	for i := 0; i < 3; i++ {
		f, err := c.recv(ctx)
		if err != nil {
			t.Fatalf("client recv %d: %v", i, err)
		}
		if f.GetKind() != 201 || !bytes.Equal(f.GetPayload(), []byte{byte(i)}) {
			t.Fatalf("server->client frame %d = kind %d payload %v", i, f.GetKind(), f.GetPayload())
		}
	}
}

func TestSecondStreamOnSameTransportRejected(t *testing.T) {
	server := newTransportTestLink(t)
	started := make(chan struct{}, 2)
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {
		started <- struct{}{}
	})
	client := newTransportTestLink(t)

	tc, id := dialTestConnection(t, client.Link, addr)
	cc, err := client.newGRPCClientConn(tc, id, addr)
	if err != nil {
		t.Fatalf("newGRPCClientConn: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	cli := linkpb.NewLinkClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	first, err := cli.Connect(ctx)
	if err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	_ = first
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("first stream did not reach the server")
	}

	// The server advertises one stream per connection, so the second stream
	// either waits for the first to finish or is refused by the handler; both
	// end in an error and never reach onStream.
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	second, err := cli.Connect(secondCtx)
	if err == nil {
		_, err = second.Recv()
	}
	if err == nil {
		t.Fatal("second Connect stream was served")
	}

	// The first stream still holds the single stream slot, so the server cannot
	// call onStream for the second one now that its Recv failed.
	select {
	case <-started:
		t.Fatal("second stream reached the server")
	default:
	}
}

func TestDialTimeoutUsesClock(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	})

	client := newTransportTestLink(t)
	const dialTimeout = 3 * time.Second
	client.settings.SetDialTimeout(dialTimeout)

	result := make(chan error, 1)
	go func() {
		_, _, err := client.dialTLS(context.Background(), ln.Addr().String())
		result <- err
	}()

	client.clock.WaitPending(t, 1)
	client.clock.Advance(dialTimeout)

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("dialTLS succeeded against a server that never handshakes")
		}
	case <-time.After(eventTimeout):
		t.Fatal("dialTLS did not return after the dial timeout")
	}
}

func TestOnDialHookCalled(t *testing.T) {
	server := newTransportTestLink(t)
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {})
	client := newTransportTestLink(t)

	called := make(chan string, 1)
	SetTestKnobs(client.Link, TestKnobs{OnDial: func(address string) { called <- address }})
	dialTestConnection(t, client.Link, addr)

	select {
	case got := <-called:
		if got != addr {
			t.Fatalf("onDial address = %q, want %q", got, addr)
		}
	case <-time.After(eventTimeout):
		t.Fatal("onDial hook was not called")
	}
}

func TestLargePayloadRoundTrip(t *testing.T) {
	const payloadSize = 1572864
	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("random payload: %v", err)
	}

	server := newTransportTestLink(t)
	guard := largePayloadTimeout(server.settings)
	serverReceived := make(chan []byte, 1)
	serverFailed := make(chan error, 1)
	// authenticated is closed once the handler below has marked the connection
	// authenticated, so the client can wait before it sends the large frame.
	authenticated := make(chan struct{})
	addr := startTransportServer(t, server.Link, func(c *conn, tr *trackedConn) {
		// This handler stands in for serveInbound; mark the connection
		// authenticated so a 1.5 MiB frame is not charged to the
		// pre-authentication read budget.
		if tr != nil {
			tr.authenticated()
		}
		close(authenticated)
		ctx, cancel := context.WithTimeout(context.Background(), guard)
		defer cancel()
		f, err := c.recv(ctx)
		if err != nil {
			serverFailed <- err
			return
		}
		serverReceived <- append([]byte(nil), f.GetPayload()...)
		// The echo gets its own deadline for the same reason the client gives
		// each phase one: the receive above may have used much of the guard.
		echoCtx, echoCancel := context.WithTimeout(context.Background(), guard)
		defer echoCancel()
		_ = c.sendDirect(echoCtx, &linkpb.Frame{Kind: 200, Payload: payload})
	})
	client := newTransportTestLink(t)
	c, _ := openTestStream(t, client.Link, addr)

	// The product marks a connection authenticated before it sends or accepts a
	// Hello (serveInbound in handshake.go), and only then may a frame larger
	// than the 64 KiB pre-authentication read budget arrive. The reader
	// goroutine charges bytes to that budget independently of the handler
	// goroutine above, so wait for the handler's authentication before sending
	// the large frame. Otherwise the reader can charge its first bytes to the
	// budget and close the connection. Keep this wait: removing it restores the
	// race.
	select {
	case <-authenticated:
	case <-time.After(authReadyTimeout):
		t.Fatal("server did not finish authenticating the stream")
	}

	// Each phase gets its own deadline so a slow send cannot consume the time
	// the reply is later given.
	sendCtx, sendCancel := context.WithTimeout(context.Background(), guard)
	defer sendCancel()
	if err := c.sendDirect(sendCtx, &linkpb.Frame{Kind: 200, Payload: payload}); err != nil {
		t.Fatalf("send large frame: %v", err)
	}
	select {
	case got := <-serverReceived:
		if !bytes.Equal(got, payload) {
			t.Fatal("server received a different payload")
		}
	case err := <-serverFailed:
		t.Fatalf("server did not receive the large frame: %v", err)
	case <-time.After(guard):
		t.Fatal("server did not receive the large frame")
	}

	replyCtx, replyCancel := context.WithTimeout(context.Background(), guard)
	defer replyCancel()
	reply, err := c.recv(replyCtx)
	if err != nil {
		t.Fatalf("receive large reply: %v", err)
	}
	if !bytes.Equal(reply.GetPayload(), payload) {
		t.Fatal("client received a different payload")
	}
}

func TestOversizedFrameRejectedBySender(t *testing.T) {
	server := newTransportTestLink(t)
	// serverDone reports the frame the server received, or nil when its Recv
	// ended without one. Closing the client stream drives it to completion.
	serverDone := make(chan *linkpb.Frame, 1)
	started := make(chan struct{})
	addr := startTransportServer(t, server.Link, func(c *conn, _ *trackedConn) {
		close(started)
		f, err := c.recv(context.Background())
		if err != nil {
			f = nil
		}
		serverDone <- f
	})
	client := newTransportTestLink(t)
	c, _ := openTestStream(t, client.Link, addr)
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("server handler did not start")
	}

	err := c.stream.Send(&linkpb.Frame{Kind: 200, Payload: make([]byte, 4<<20+1)})
	if err == nil {
		t.Fatal("sending a frame above the message limit succeeded")
	}
	if code := status.Code(err); code != codes.ResourceExhausted {
		t.Fatalf("oversized send error code = %v (%v), want ResourceExhausted", code, err)
	}

	// End the stream so the server's Recv returns. If the oversized frame had
	// reached the wire, the handler would report it here.
	c.closeAsync(FailConnectionLost)
	select {
	case f := <-serverDone:
		if f != nil {
			t.Fatalf("server received an oversized frame of %d payload bytes", len(f.GetPayload()))
		}
	case <-time.After(eventTimeout):
		t.Fatal("server handler did not finish")
	}
}

// TestAbortDoesNotBlock calls abort on the test goroutine while the close of
// the gRPC client connection is held open by the injected close function. A
// synchronous ClientConn.Close would block abort until the close returns, which
// the close function reports as a failure.
func TestAbortDoesNotBlock(t *testing.T) {
	server := newTransportTestLink(t)
	started := make(chan struct{}, 1)
	recvErr := make(chan error, 1)
	addr := startTransportServer(t, server.Link, func(c *conn, _ *trackedConn) {
		started <- struct{}{}
		_, err := c.recv(context.Background())
		recvErr <- err
	})
	client := newTransportTestLink(t)

	closeStarted := make(chan struct{})
	release := make(chan struct{})
	client.closeClientConn = func(*grpc.ClientConn) error {
		close(closeStarted)
		select {
		case <-release:
		case <-time.After(eventTimeout):
			// abort called the close synchronously; it should have returned
			// while the close was still in progress.
			t.Error("abort waited for the client connection close")
		}
		return nil
	}

	c, _ := openTestStream(t, client.Link, addr)

	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("server did not accept the stream")
	}

	c.abort()

	select {
	case <-closeStarted:
	case <-time.After(eventTimeout):
		t.Fatal("abort did not close the client connection")
	}
	close(release)

	select {
	case err := <-recvErr:
		if err == nil {
			t.Fatal("peer received no error after the abort")
		}
	case <-time.After(eventTimeout):
		t.Fatal("peer did not notice the abort")
	}
}

// TestConnectDoesNotStartAfterClose holds an inbound stream before the handler
// joins the link WaitGroup, closes the Link, then releases the stream. The
// handler must see the closed state and return without calling onStream.
func TestConnectDoesNotStartAfterClose(t *testing.T) {
	server := newTransportTestLink(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	server.onConnect = func() {
		close(entered)
		<-release
	}
	called := make(chan struct{}, 1)
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {
		called <- struct{}{}
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
	stream, err := linkpb.NewLinkClient(cc).Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	select {
	case <-entered:
	case <-time.After(eventTimeout):
		t.Fatal("Connect handler did not start")
	}

	closed := make(chan struct{})
	go func() {
		server.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(eventTimeout):
		t.Fatal("Close blocked")
	}
	close(release)

	// The client sees Unavailable when the released handler refuses the stream.
	// That status is sent only after the handler decided, so checking called
	// then is race free and needs no fixed wait.
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("client stream error = %v, want Unavailable", err)
	}
	select {
	case <-called:
		t.Fatal("onStream ran for a stream that arrived after Close")
	default:
	}
}

// TestServerOversizedFrameRejected pins the server side of the 4 MiB message
// limit: sending a larger frame fails locally instead of travelling to the
// peer, which would only break the link.
func TestServerOversizedFrameRejected(t *testing.T) {
	server := newTransportTestLink(t)
	sendErr := make(chan error, 1)
	addr := startTransportServer(t, server.Link, func(c *conn, _ *trackedConn) {
		ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
		defer cancel()
		sendErr <- c.sendDirect(ctx, &linkpb.Frame{Kind: 200, Payload: make([]byte, 4<<20+1)})
	})
	client := newTransportTestLink(t)
	openTestStream(t, client.Link, addr)

	select {
	case err := <-sendErr:
		if err == nil {
			t.Fatal("server sent a frame above the 4 MiB message limit")
		}
		if code := status.Code(err); code != codes.ResourceExhausted {
			t.Fatalf("oversized server send error code = %v (%v), want ResourceExhausted", code, err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("server send did not return")
	}
}

// recordingSendStream records whether a frame reached the transport.
type recordingSendStream struct {
	sent atomic.Bool
}

func (s *recordingSendStream) Send(*linkpb.Frame) error {
	s.sent.Store(true)
	return nil
}

func (s *recordingSendStream) Recv() (*linkpb.Frame, error) { return nil, io.EOF }

// frameOfEncodedSize builds a frame whose marshaled form is exactly size bytes.
func frameOfEncodedSize(t *testing.T, size int) *linkpb.Frame {
	t.Helper()
	payload := size
	for i := 0; i < 8; i++ {
		f := &linkpb.Frame{Kind: 200, Payload: make([]byte, payload)}
		n := proto.Size(f)
		if n == size {
			return f
		}
		payload -= n - size
	}
	t.Fatalf("could not build a frame of exactly %d bytes", size)
	return nil
}

// TestFrameSizeLimit pins the boundary of the transport's message limit: an
// encoded frame of exactly the limit reaches the transport, and one byte more
// is rejected as ResourceExhausted before the transport is touched.
func TestFrameSizeLimit(t *testing.T) {
	atLimit := frameOfEncodedSize(t, MaxMessageSize)
	stream := new(recordingSendStream)
	c := newConn(stream, nil, peerIdentity{}, 8)
	if err := c.sendDirect(context.Background(), atLimit); err != nil {
		t.Fatalf("sendDirect at the limit: %v", err)
	}
	if !stream.sent.Load() {
		t.Fatal("a frame at the limit did not reach the transport")
	}

	overLimit := frameOfEncodedSize(t, MaxMessageSize+1)
	stream = new(recordingSendStream)
	c = newConn(stream, nil, peerIdentity{}, 8)
	err := c.sendDirect(context.Background(), overLimit)
	if code := status.Code(err); code != codes.ResourceExhausted {
		t.Fatalf("oversized send error code = %v (%v), want ResourceExhausted", code, err)
	}
	if stream.sent.Load() {
		t.Fatal("an oversized frame reached the transport")
	}
}

// TestOversizedFrameRejectedWithoutTransport pins that the 4 MiB limit is
// enforced before the frame reaches the transport, whatever state the
// connection is in. gRPC rejects an oversized frame locally on Send, but
// sendDirect waits for that result in a separate goroutine; when the
// connection closes in the same window, the close branch wins and reports
// ErrNotEstablished, whose status code is Unknown instead of
// ResourceExhausted.
func TestOversizedFrameRejectedWithoutTransport(t *testing.T) {
	stream := new(recordingSendStream)
	c := newConn(stream, nil, peerIdentity{}, 8)
	c.closeAsync(FailConnectionLost)

	err := c.sendDirect(context.Background(), &linkpb.Frame{Kind: 200, Payload: make([]byte, MaxMessageSize+1)})
	if code := status.Code(err); code != codes.ResourceExhausted {
		t.Fatalf("oversized send error code = %v (%v), want ResourceExhausted", code, err)
	}
	if stream.sent.Load() {
		t.Fatal("the oversized frame reached the transport")
	}
}

// untrackedServerStream is a server stream whose connection was not registered
// with the unauthenticated-connection registry.
type untrackedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *untrackedServerStream) Context() context.Context     { return s.ctx }
func (s *untrackedServerStream) Send(*linkpb.Frame) error     { return nil }
func (s *untrackedServerStream) Recv() (*linkpb.Frame, error) { return nil, io.EOF }

// TestConnectRejectsUntrackedStream pins the fail-closed branch: a stream
// whose transport is not tracked cannot enforce the first-frame rules, so the
// handler refuses it instead of serving it.
func TestConnectRejectsUntrackedStream(t *testing.T) {
	l := newTransportTestLink(t)
	called := make(chan struct{}, 1)
	srv := &linkServer{link: l.Link, onStream: func(c *conn, _ *trackedConn) {
		called <- struct{}{}
		c.closeAsync(FailConnectionLost)
	}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: serverAuthInfo{id: peerIdentity{fp: Fingerprint{9}}},
	})

	err := srv.Connect(&untrackedServerStream{ctx: ctx})
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("Connect error code = %v (%v), want Internal", code, err)
	}
	select {
	case <-called:
		t.Fatal("onStream ran for a stream without a tracked connection")
	default:
	}
}
