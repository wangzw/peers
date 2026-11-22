package link

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/pairing"
)

// blockWriter fills the peer's buffer and returns once the writer is blocked on
// the next frame, so items queued afterwards are not taken.
func blockWriter(t *testing.T, h *dispatchHarness, dc *dispatchConn) {
	t.Helper()
	for i := 0; i < pipeBuffer+1; i++ {
		if err := h.l.Send(dc.fp, Frame{Kind: 210, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("fill Send %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(eventTimeout)
	for len(dc.link.sent()) != pipeBuffer+1 {
		if time.Now().After(deadline) {
			t.Fatalf("writer did not block: %d frames recorded", len(dc.link.sent()))
		}
		time.Sleep(time.Millisecond)
	}
}

// drainFiller reads the frames used to block the writer.
func drainFiller(t *testing.T, dc *dispatchConn) {
	t.Helper()
	for i := 0; i < pipeBuffer+1; i++ {
		if f := dc.recvPeer(t); f.GetKind() != 210 {
			t.Fatalf("filler frame %d has kind %d, want 210", i, f.GetKind())
		}
	}
}

// TestSendLatestReplacesInPlace checks that a queued SendLatest frame is
// replaced where it was queued, keeping the order of the frames around it.
func TestSendLatestReplacesInPlace(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)
	blockWriter(t, h, dc)

	steps := []struct {
		latest bool
		f      Frame
	}{
		{false, Frame{Kind: 211, Payload: []byte("A")}},
		{true, Frame{Kind: 212, Payload: []byte("L1")}},
		{false, Frame{Kind: 211, Payload: []byte("B")}},
		{true, Frame{Kind: 212, Payload: []byte("L2")}},
	}
	for _, s := range steps {
		var err error
		if s.latest {
			err = h.l.SendLatest(dc.fp, s.f)
		} else {
			err = h.l.Send(dc.fp, s.f)
		}
		if err != nil {
			t.Fatalf("sending %q: %v", s.f.Payload, err)
		}
	}

	drainFiller(t, dc)
	want := []string{"211:A", "212:L2", "211:B"}
	for _, name := range want {
		f := dc.recvPeer(t)
		got := fmt.Sprintf("%d:%s", f.GetKind(), f.GetPayload())
		if got != name {
			t.Fatalf("frame = %q, want %q", got, name)
		}
	}
}

// TestSendLatestDoesNotFillQueue checks that repeated replacement of one queued
// frame never fills the send queue, and only the last content is delivered, at
// the first frame's position.
func TestSendLatestDoesNotFillQueue(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)
	blockWriter(t, h, dc)

	const n = 2000
	for i := 0; i < n; i++ {
		if err := h.l.SendLatest(dc.fp, Frame{Kind: 212, Payload: []byte(fmt.Sprintf("L%d", i))}); err != nil {
			t.Fatalf("SendLatest %d: %v", i, err)
		}
	}

	drainFiller(t, dc)
	f := dc.recvPeer(t)
	if f.GetKind() != 212 || string(f.GetPayload()) != fmt.Sprintf("L%d", n-1) {
		t.Fatalf("delivered latest = kind %d payload %q, want kind 212 payload L%d", f.GetKind(), f.GetPayload(), n-1)
	}

	if err := h.l.Send(dc.fp, Frame{Kind: 211, Payload: []byte("still up")}); err != nil {
		t.Fatalf("Send after the replacements: %v", err)
	}
	if f := dc.recvPeer(t); string(f.GetPayload()) != "still up" {
		t.Fatalf("frame after the replacements = %q, want still up", f.GetPayload())
	}
}

// TestSendLatestAfterWriterTookFrame checks that once the writer has taken a
// queued latest frame, its slot is released: a later SendLatest of the same
// kind is queued as a new frame and delivered, not folded into a slot the
// writer already drained.
func TestSendLatestAfterWriterTookFrame(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	if err := h.l.SendLatest(dc.fp, Frame{Kind: 212, Payload: []byte("L1")}); err != nil {
		t.Fatalf("SendLatest L1: %v", err)
	}
	if f := dc.recvPeer(t); string(f.GetPayload()) != "L1" {
		t.Fatalf("first frame payload = %q, want L1", f.GetPayload())
	}

	if err := h.l.SendLatest(dc.fp, Frame{Kind: 212, Payload: []byte("L2")}); err != nil {
		t.Fatalf("SendLatest L2: %v", err)
	}
	if f := dc.recvPeer(t); string(f.GetPayload()) != "L2" {
		t.Fatalf("second frame payload = %q, want L2; the slot was not released after the writer took L1", f.GetPayload())
	}
}

// TestSendLatestConcurrentWithWriter checks that replacements race with the
// writer taking frames without a data race or a lost connection.
func TestSendLatestConcurrentWithWriter(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	arrived := make(chan struct{})
	go func() {
		defer close(arrived)
		for {
			f, err := dc.peer.Recv()
			if err != nil {
				return
			}
			if f.GetKind() == 211 && string(f.GetPayload()) == "done" {
				return
			}
		}
	}()

	const workers, each = 4, 500
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := h.l.SendLatest(dc.fp, Frame{Kind: 212, Payload: []byte(fmt.Sprintf("%d-%d", w, i))}); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatalf("SendLatest: %v", err)
	default:
	}

	if err := h.l.Send(dc.fp, Frame{Kind: 211, Payload: []byte("done")}); err != nil {
		t.Fatalf("Send after concurrent replacements: %v", err)
	}
	select {
	case <-arrived:
	case <-time.After(eventTimeout):
		t.Fatal("the connection stopped carrying frames")
	}
}
