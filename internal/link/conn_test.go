package link

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %v", eventTimeout)
}

// TestEnqueueNeverBlocks checks that a full send queue closes the connection
// instead of blocking the caller.
func TestEnqueueNeverBlocks(t *testing.T) {
	a, _ := newPipe(t)
	block := make(chan struct{})
	a.blockSend = block
	c := newConn(a, a.closeBoth, peerIdentity{}, 4)
	c.start()
	c.startWriter()

	errs := make(chan error, 6)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 6; i++ {
			errs <- c.enqueue(&linkpb.Frame{Kind: 300, Payload: []byte{byte(i)}})
		}
	}()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("enqueue blocked on a full queue")
	}
	close(errs)

	okCount := 0
	sawFull := false
	for err := range errs {
		switch {
		case err == nil:
			okCount++
		case errors.Is(err, ErrQueueFull):
			sawFull = true
		default:
			t.Fatalf("enqueue returned %v, want nil or ErrQueueFull", err)
		}
	}
	if okCount < 4 || okCount > 5 {
		t.Fatalf("enqueue succeeded %d times, want 4 or 5", okCount)
	}
	if !sawFull {
		t.Fatal("enqueue never reported ErrQueueFull")
	}
	if got := c.reason(); got != FailQueueFull {
		t.Fatalf("reason() = %v, want %v", got, FailQueueFull)
	}
	select {
	case <-c.done():
	default:
		t.Fatal("done() is open after the queue overflowed")
	}
}

// TestCloseAsyncUnderForeignLock checks that closeAsync returns without
// waiting for the reader or the writer.
func TestCloseAsyncUnderForeignLock(t *testing.T) {
	a, _ := newPipe(t)
	var aborted atomic.Bool
	c := newConn(a, func() {
		aborted.Store(true)
		a.closeBoth()
	}, peerIdentity{}, 4)
	c.start()
	c.startWriter()

	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		mu.Lock()
		defer mu.Unlock()
		c.closeAsync(FailConnectionLost)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("closeAsync blocked while a foreign lock was held")
	}
	if !aborted.Load() {
		t.Fatal("abort was not called")
	}
	if got := c.reason(); got != FailConnectionLost {
		t.Fatalf("reason() = %v, want %v", got, FailConnectionLost)
	}
}

// TestWriterPreservesOrder checks that the single writer delivers frames in
// the order they were queued.
func TestWriterPreservesOrder(t *testing.T) {
	a, b := newPipe(t)
	c := newConn(a, a.closeBoth, peerIdentity{}, 2048)
	c.startWriter()

	const n = 1000
	for i := 0; i < n; i++ {
		if err := c.enqueue(&linkpb.Frame{Kind: 300, Payload: []byte(fmt.Sprint(i))}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		f, err := b.Recv()
		if err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if got := string(f.GetPayload()); got != fmt.Sprint(i) {
			t.Fatalf("frame %d payload = %q, want %q", i, got, fmt.Sprint(i))
		}
	}
}

// TestRequestSentFlagQueuedThenCanceled checks that a request canceled while
// it is still in the queue is never written and reports sent=false.
func TestRequestSentFlagQueuedThenCanceled(t *testing.T) {
	a, b := newPipe(t)
	block := make(chan struct{})
	a.blockSend = block
	c := newConn(a, a.closeBoth, peerIdentity{}, 8)
	c.start()
	c.startWriter()

	if err := c.enqueue(&linkpb.Frame{Kind: 300}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, func() bool { return len(c.out) == 0 })

	p, err := c.enqueueRequest(&linkpb.Frame{Kind: 400})
	if err != nil {
		t.Fatalf("enqueueRequest: %v", err)
	}
	if sent := c.cancelRequest(p); sent {
		t.Fatal("cancelRequest reported sent for a queued request")
	}

	close(block)
	f, err := b.Recv()
	if err != nil {
		t.Fatalf("recv first frame: %v", err)
	}
	if f.GetKind() != 300 {
		t.Fatalf("first frame kind = %d, want 300", f.GetKind())
	}
	waitFor(t, func() bool { return len(c.out) == 0 })
	if sent := a.sent(); len(sent) != 1 || sent[0].GetKind() != 300 {
		t.Fatalf("connection wrote %d frames, want only the first frame", len(sent))
	}
	select {
	case f := <-b.in:
		t.Fatalf("canceled request was written: kind %d", f.GetKind())
	case <-time.After(200 * time.Millisecond):
	}
}

// TestRequestSentFlagWritten checks that a request the writer already took
// reports sent=true when it is canceled.
func TestRequestSentFlagWritten(t *testing.T) {
	a, b := newPipe(t)
	c := newConn(a, a.closeBoth, peerIdentity{}, 8)
	c.startWriter()

	p, err := c.enqueueRequest(&linkpb.Frame{Kind: 400})
	if err != nil {
		t.Fatalf("enqueueRequest: %v", err)
	}
	f, err := b.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if f.GetRequestId() != p.id || f.GetIsReply() {
		t.Fatalf("written frame id=%d isReply=%v, want id=%d isReply=false", f.GetRequestId(), f.GetIsReply(), p.id)
	}
	if sent := c.cancelRequest(p); !sent {
		t.Fatal("cancelRequest reported not sent for a written request")
	}
}

// TestDeliverReplyMatchesID checks that replies reach the request with the
// same id and unknown ids are reported.
func TestDeliverReplyMatchesID(t *testing.T) {
	a, _ := newPipe(t)
	c := newConn(a, a.closeBoth, peerIdentity{}, 8)

	p1, err := c.enqueueRequest(&linkpb.Frame{Kind: 400})
	if err != nil {
		t.Fatalf("enqueueRequest first: %v", err)
	}
	p2, err := c.enqueueRequest(&linkpb.Frame{Kind: 400})
	if err != nil {
		t.Fatalf("enqueueRequest second: %v", err)
	}
	if p1.id == p2.id {
		t.Fatalf("both requests got id %d", p1.id)
	}

	reply2 := &linkpb.Frame{Kind: 401, RequestId: p2.id, IsReply: true}
	if !c.deliverReply(reply2) {
		t.Fatal("deliverReply did not find the second request")
	}
	select {
	case got := <-p2.reply:
		if got != reply2 {
			t.Fatalf("second request got a different reply")
		}
	default:
		t.Fatal("second request has no reply")
	}

	if c.deliverReply(&linkpb.Frame{RequestId: 999, IsReply: true}) {
		t.Fatal("deliverReply matched an unknown request id")
	}

	if !c.deliverReply(&linkpb.Frame{Kind: 401, RequestId: p1.id, IsReply: true}) {
		t.Fatal("deliverReply did not find the first request")
	}
}

// TestRecvHonoursContext checks that recv returns the context error instead of
// waiting for a frame.
func TestRecvHonoursContext(t *testing.T) {
	a, _ := newPipe(t)
	c := newConn(a, a.closeBoth, peerIdentity{}, 4)
	c.start()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.recv(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("recv error = %v, want context.Canceled", err)
	}
}

// TestUnreadIsReturnedFirst checks that a frame pushed back is received before
// the next frame from the stream.
func TestUnreadIsReturnedFirst(t *testing.T) {
	a, b := newPipe(t)
	c := newConn(a, a.closeBoth, peerIdentity{}, 4)
	c.start()

	x := &linkpb.Frame{Kind: 100, Payload: []byte("x")}
	c.unread(x)
	got, err := c.recv(context.Background())
	if err != nil {
		t.Fatalf("recv unread: %v", err)
	}
	if got != x {
		t.Fatalf("recv returned %v, want the unread frame", got)
	}

	if err := b.Send(&linkpb.Frame{Kind: 200, Payload: []byte("y")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	got, err = c.recv(ctx)
	if err != nil {
		t.Fatalf("recv stream frame: %v", err)
	}
	if got.GetKind() != 200 || string(got.GetPayload()) != "y" {
		t.Fatalf("recv got kind=%d payload=%q, want kind=200 payload=y", got.GetKind(), got.GetPayload())
	}
}
