package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link"
)

func fakenetPeer(tag byte) peerID {
	p := peerID{}
	p[0] = tag
	return p
}

// TestFakeNetRequestReply checks that a request reaches the peer's handler and
// returns its reply frame.
func TestFakeNetRequestReply(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)

	seen := make(chan peerID, 1)
	net.end(b).HandleRequest(400, func(_ context.Context, p peerID, f link.Frame) link.Frame {
		seen <- p
		return link.Frame{Kind: 401, Payload: append([]byte(nil), f.Payload...)}
	})
	net.setUp(a, b, true)

	reply, sent, err := endA.Request(context.Background(), b, link.Frame{Kind: 400, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("Request returned %v, want nil", err)
	}
	if !sent {
		t.Fatalf("Request sent = false, want true")
	}
	if reply.Kind != 401 || string(reply.Payload) != "hello" {
		t.Fatalf("reply = %+v, want kind 401 payload %q", reply, "hello")
	}
	if p := <-seen; p != a {
		t.Fatalf("handler peer = %v, want %v", p, a)
	}
	if frames := net.requests(a, b); len(frames) != 1 || frames[0].Kind != 400 || string(frames[0].Payload) != "hello" {
		t.Fatalf("requests(a, b) = %+v, want one kind-400 frame", frames)
	}
}

// TestFakeNetDownNotSent checks that a request on a down link reports sent=false
// and ErrNotEstablished.
func TestFakeNetDownNotSent(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)
	net.setUp(a, b, true)
	net.setUp(a, b, false)

	if endA.Established(b) {
		t.Fatalf("Established after setUp(false) = true, want false")
	}
	_, sent, err := endA.Request(context.Background(), b, link.Frame{Kind: 400})
	if sent {
		t.Fatalf("Request sent = true, want false on a down link")
	}
	if !errors.Is(err, link.ErrNotEstablished) {
		t.Fatalf("Request err = %v, want ErrNotEstablished", err)
	}
	if !strings.Contains(err.Error(), "fake link down") {
		t.Fatalf("Request err = %q, want it to name the fake link", err)
	}
	if frames := net.requests(a, b); len(frames) != 0 {
		t.Fatalf("requests(a, b) = %d frames, want none for an unsent request", len(frames))
	}
}

// TestFakeNetDropReplyRunsHandlerOnce checks that a dropped reply still runs the
// handler once and reports ErrLinkDown.
func TestFakeNetDropReplyRunsHandlerOnce(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)

	var handled atomic.Int32
	net.end(b).HandleRequest(400, func(_ context.Context, _ peerID, _ link.Frame) link.Frame {
		handled.Add(1)
		return link.Frame{Kind: 401}
	})
	var downs atomic.Int32
	endA.Subscribe(nil, func(peerID) { downs.Add(1) })

	net.setUp(a, b, true)
	net.dropReplies(a, b, 1)

	_, sent, err := endA.Request(context.Background(), b, link.Frame{Kind: 400})
	if !sent {
		t.Fatalf("Request sent = false, want true after the frame was sent")
	}
	if !errors.Is(err, link.ErrLinkDown) {
		t.Fatalf("Request err = %v, want ErrLinkDown", err)
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
	if got := downs.Load(); got != 1 {
		t.Fatalf("onDown ran %d times, want 1", got)
	}
	if endA.Established(b) || net.end(b).Established(a) {
		t.Fatalf("link still established after a dropped reply, want down")
	}
}

// TestFakeNetKickHook checks that Kick counts the call and runs the hooks for
// that direction.
func TestFakeNetKickHook(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)

	var calls atomic.Int32
	net.onKick(a, b, func() { calls.Add(1) })

	endA.Kick(b)
	endA.Kick(b)

	if got := net.kicks(a, b); got != 2 {
		t.Fatalf("kicks(a, b) = %d, want 2", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("kick hook ran %d times, want 2", got)
	}
	if got := net.kicks(b, a); got != 0 {
		t.Fatalf("kicks(b, a) = %d, want 0", got)
	}
}

// TestFakeNetCallbacksOnlyOnTransition checks that a repeated up or down state
// fires its callback only on the transition.
func TestFakeNetCallbacksOnlyOnTransition(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)

	var ups, downs atomic.Int32
	endA.Subscribe(func(peerID, []byte) { ups.Add(1) }, func(peerID) { downs.Add(1) })

	net.setUp(a, b, true)
	net.setUp(a, b, true)
	if got := ups.Load(); got != 1 {
		t.Fatalf("onUp ran %d times after two up events, want 1", got)
	}
	if got := downs.Load(); got != 0 {
		t.Fatalf("onDown ran %d times while the link stayed up, want 0", got)
	}

	net.setUp(a, b, false)
	net.setUp(a, b, false)
	if got := downs.Load(); got != 1 {
		t.Fatalf("onDown ran %d times after two down events, want 1", got)
	}
	if got := ups.Load(); got != 1 {
		t.Fatalf("onUp ran %d times in total, want 1", got)
	}
}

// TestFakeNetHandlerContextCancelledOnDown checks that a link going down cancels
// the request handler's context.
func TestFakeNetHandlerContextCancelledOnDown(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)

	entered := make(chan struct{})
	cancelled := make(chan error, 1)
	net.end(b).HandleRequest(400, func(ctx context.Context, _ peerID, _ link.Frame) link.Frame {
		close(entered)
		<-ctx.Done()
		cancelled <- ctx.Err()
		return link.Frame{Kind: 401}
	})
	net.setUp(a, b, true)

	done := make(chan error, 1)
	go func() {
		_, _, err := endA.Request(context.Background(), b, link.Frame{Kind: 400})
		done <- err
	}()
	<-entered
	net.setUp(a, b, false)

	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("the handler context was not canceled when the link went down")
	}
	select {
	case err := <-done:
		if !errors.Is(err, link.ErrLinkDown) {
			t.Fatalf("Request err = %v, want ErrLinkDown", err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("Request did not return after the link went down")
	}
}

// TestFakeNetDownWhileResolvingReply checks that a link going down while a reply
// is resolved reports ErrLinkDown.
func TestFakeNetDownWhileResolvingReply(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)

	net.end(b).HandleRequest(400, func(_ context.Context, _ peerID, _ link.Frame) link.Frame {
		return link.Frame{Kind: 401}
	})
	net.setUp(a, b, true)
	net.onReply(func() { net.setUp(a, b, false) })

	_, sent, err := endA.Request(context.Background(), b, link.Frame{Kind: 400})
	if !sent {
		t.Fatalf("Request sent = false, want true: the frame reached the peer")
	}
	if !errors.Is(err, link.ErrLinkDown) {
		t.Fatalf("Request err = %v, want ErrLinkDown", err)
	}
}

// TestFakeNetHandlerContextOutlivesRequest checks that the caller's context
// ending does not cancel the handler.
func TestFakeNetHandlerContextOutlivesRequest(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)

	entered := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan error, 1)
	net.end(b).HandleRequest(400, func(ctx context.Context, _ peerID, _ link.Frame) link.Frame {
		close(entered)
		<-release
		observed <- ctx.Err()
		return link.Frame{Kind: 401}
	})
	net.setUp(a, b, true)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _, _ = endA.Request(ctx, b, link.Frame{Kind: 400})
		close(done)
	}()
	<-entered
	cancel()

	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("Request did not return after the caller's context ended")
	}
	close(release)
	select {
	case err := <-observed:
		if err != nil {
			t.Fatalf("handler context error = %v, want nil: only a link drop cancels it", err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("the handler did not finish")
	}
}

// TestFakeNetCleanupCancelsAbandonedHandler checks that test cleanup cancels a
// handler a test left blocked.
func TestFakeNetCleanupCancelsAbandonedHandler(t *testing.T) {
	released := make(chan struct{})
	t.Run("abandon", func(t *testing.T) {
		net := newFakeNet(t)
		a, b := fakenetPeer(1), fakenetPeer(2)
		endA := net.end(a)
		net.end(b)

		entered := make(chan struct{})
		var once sync.Once
		net.end(b).HandleRequest(400, func(ctx context.Context, _ peerID, _ link.Frame) link.Frame {
			close(entered)
			<-ctx.Done()
			once.Do(func() { close(released) })
			return link.Frame{Kind: 401}
		})
		net.setUp(a, b, true)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_, _, _ = endA.Request(ctx, b, link.Frame{Kind: 400})
			close(done)
		}()
		<-entered
		cancel()
		select {
		case <-done:
		case <-time.After(eventTimeout):
			t.Fatal("Request did not return after the caller's context ended")
		}
	})

	select {
	case <-released:
	case <-time.After(eventTimeout):
		t.Fatal("the abandoned handler was not canceled when the test cleanup ran")
	}
}

// TestFakeNetUnsupported checks that an unhandled frame kind returns a
// *link.UnsupportedError with sent=true.
func TestFakeNetUnsupported(t *testing.T) {
	net := newFakeNet(t)
	a, b := fakenetPeer(1), fakenetPeer(2)
	endA := net.end(a)
	net.end(b)
	net.setUp(a, b, true)

	_, sent, err := endA.Request(context.Background(), b, link.Frame{Kind: 399})
	if !sent {
		t.Fatalf("Request sent = false, want true: the request reached the peer")
	}
	var ue *link.UnsupportedError
	if !errors.As(err, &ue) {
		t.Fatalf("Request err = %v, want *link.UnsupportedError", err)
	}
	if ue.Kind != 399 {
		t.Fatalf("UnsupportedError.Kind = %d, want 399", ue.Kind)
	}
	if frames := net.requests(a, b); len(frames) != 1 {
		t.Fatalf("requests(a, b) = %d frames, want 1", len(frames))
	}
}
