package link

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

// oversizedFrame returns a state frame whose encoded form is one byte above the
// message limit.
func oversizedFrame() Frame {
	return Frame{Kind: 200, Payload: make([]byte, MaxMessageSize)}
}

// TestSendOversizedFrame checks that Send refuses an oversized frame before it
// reaches the transport and leaves the link usable for the next frame.
func TestSendOversizedFrame(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	if err := h.l.Send(dc.fp, oversizedFrame()); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Send of an oversized frame = %v, want ErrFrameTooLarge", err)
	}

	if err := h.l.Send(dc.fp, Frame{Kind: 200, Payload: []byte("small")}); err != nil {
		t.Fatalf("Send after the oversized frame: %v", err)
	}
	f := dc.recvPeer(t, 3*time.Second)
	if f.GetKind() != 200 || string(f.GetPayload()) != "small" {
		t.Fatalf("next frame = kind %d payload %q, want kind 200 payload small", f.GetKind(), f.GetPayload())
	}
}

// TestRequestOversizedFrame checks that Request refuses an oversized frame with
// sent false and does not wrap ErrNotEstablished.
func TestRequestOversizedFrame(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, sent, err := h.l.Request(ctx, dc.fp, oversizedFrame())
	if !errors.Is(err, ErrFrameTooLarge) || sent {
		t.Fatalf("Request of an oversized frame = (sent %v, err %v), want (false, ErrFrameTooLarge)", sent, err)
	}
	if errors.Is(err, ErrNotEstablished) {
		t.Fatalf("ErrFrameTooLarge wraps ErrNotEstablished: %v", err)
	}

	if err := h.l.Send(dc.fp, Frame{Kind: 200, Payload: []byte("small")}); err != nil {
		t.Fatalf("Send after the oversized request: %v", err)
	}
	if f := dc.recvPeer(t, 3*time.Second); string(f.GetPayload()) != "small" {
		t.Fatalf("next frame payload = %q, want small", f.GetPayload())
	}
}

// TestSendLatestOversizedFrame checks that SendLatest refuses an oversized
// frame before it reaches the transport and leaves the link usable.
func TestSendLatestOversizedFrame(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	if err := h.l.SendLatest(dc.fp, oversizedFrame()); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("SendLatest of an oversized frame = %v, want ErrFrameTooLarge", err)
	}

	if err := h.l.Send(dc.fp, Frame{Kind: 200, Payload: []byte("small")}); err != nil {
		t.Fatalf("Send after the oversized frame: %v", err)
	}
	if f := dc.recvPeer(t, 3*time.Second); f.GetKind() != 200 || string(f.GetPayload()) != "small" {
		t.Fatalf("next frame = kind %d payload %q, want kind 200 payload small", f.GetKind(), f.GetPayload())
	}
}

// TestSendLatestKeyOversizedFrame checks that SendLatestKey refuses an
// oversized frame the same way.
func TestSendLatestKeyOversizedFrame(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	replaced, err := h.l.SendLatestKey(dc.fp, "d:test", oversizedFrame())
	if !errors.Is(err, ErrFrameTooLarge) || replaced {
		t.Fatalf("SendLatestKey of an oversized frame = (replaced %v, err %v), want (false, ErrFrameTooLarge)", replaced, err)
	}

	if err := h.l.Send(dc.fp, Frame{Kind: 200, Payload: []byte("small")}); err != nil {
		t.Fatalf("Send after the oversized frame: %v", err)
	}
	if f := dc.recvPeer(t, 3*time.Second); string(f.GetPayload()) != "small" {
		t.Fatalf("next frame payload = %q, want small", f.GetPayload())
	}
}

// TestSendLatestAfterClose checks that a latest send on a closed connection
// returns an error even when a slot of that key is still in the map, matching
// Send's behaviour.
func TestSendLatestAfterClose(t *testing.T) {
	a, _ := newPipe(t)
	c := newConn(a, func() {}, peerIdentity{fp: Fingerprint{1}, pub: make([]byte, 32)}, 8)
	if _, err := c.enqueueLatest("d:test", &linkpb.Frame{Kind: 200, Payload: []byte("x")}); err != nil {
		t.Fatalf("enqueueLatest before close: %v", err)
	}
	c.closeAsync(FailConnectionLost)
	if _, err := c.enqueueLatest("d:test", &linkpb.Frame{Kind: 200, Payload: []byte("y")}); err == nil {
		t.Fatal("enqueueLatest after close returned nil, want an error")
	}
}

// TestOversizedReplyDropped checks that a reply frame above the limit is dropped
// instead of sent, and the connection keeps carrying replies afterwards.
func TestOversizedReplyDropped(t *testing.T) {
	h := newDispatchHarness(t)
	h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
		return Frame{Kind: 300, Payload: make([]byte, MaxMessageSize)}
	})
	h.l.HandleRequest(301, func(context.Context, Fingerprint, Frame) Frame {
		return Frame{Kind: 301, Payload: []byte("small")}
	})

	a, _ := newPipe(t)
	c := newConn(a, func() {}, peerIdentity{fp: Fingerprint{1}, pub: make([]byte, 32)}, 8)
	c.role = pairing.RoleAcceptor
	c.startWriter()
	t.Cleanup(func() { c.closeAsync(FailConnectionLost) })

	h.l.handleRequest(c, &linkpb.Frame{Kind: 300, RequestId: 7})
	if sent := a.sent(); len(sent) != 0 {
		t.Fatalf("the oversized reply reached the transport: kind %d", sent[0].GetKind())
	}

	h.l.handleRequest(c, &linkpb.Frame{Kind: 301, RequestId: 8})
	deadline := time.Now().Add(3 * time.Second)
	for len(a.sent()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the connection stopped carrying replies after the oversized one")
		}
		time.Sleep(time.Millisecond)
	}
	if got := a.sent()[0]; got.GetKind() != 301 || string(got.GetPayload()) != "small" {
		t.Fatalf("next reply = kind %d payload %q, want kind 301 payload small", got.GetKind(), got.GetPayload())
	}
}
