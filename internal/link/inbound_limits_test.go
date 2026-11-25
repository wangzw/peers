package link

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

func recvPeerKind(t *testing.T, dc *dispatchConn, kind FrameKind) *linkpb.Frame {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for a frame of kind %d", kind)
		}
		f, ok := tryRecvPeer(dc, remaining)
		if !ok {
			t.Fatalf("timed out waiting for a frame of kind %d", kind)
		}
		if f.GetKind() == kind {
			return f
		}
	}
}

func waitConnClosed(t *testing.T, c *conn) {
	t.Helper()
	select {
	case <-c.done():
	case <-time.After(eventTimeout):
		t.Fatalf("connection did not close; reason is %v", c.reason())
	}
}

func waitLogEvent(t *testing.T, logs *logBuffer, msg string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		for _, line := range strings.Split(logs.String(), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var entry map[string]any
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				continue
			}
			if entry["msg"] == msg {
				return entry
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("log event %q did not appear; logs:\n%s", msg, logs.String())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestStateQueueFrameLimitClosesLink checks that a state-frame queue over its
// frame count closes the connection with inbound_limit.
func TestStateQueueFrameLimitClosesLink(t *testing.T) {
	h := newDispatchHarness(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	h.l.HandleState(200, func(Fingerprint, Frame) { <-release })

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	go func() {
		for i := 0; i < 4097; i++ {
			select {
			case <-dc.c.done():
				return
			default:
			}
			if err := dc.peer.Send(&linkpb.Frame{Kind: 200, Payload: dispatchIntPayload(i)}); err != nil {
				return
			}
		}
	}()

	waitConnClosed(t, dc.c)
	if got := dc.c.reason(); got != FailInboundLimit {
		t.Fatalf("close reason = %v, want inbound_limit", got)
	}

	entry := waitLogEvent(t, h.logs, "link_inbound_limit")
	if entry["peer"] != dc.fp.String() {
		t.Errorf("log peer = %v, want %s", entry["peer"], dc.fp)
	}
	if entry["limit"] != "state_frames" {
		t.Errorf("log limit = %v, want state_frames", entry["limit"])
	}

	// Let the state handler return so the teardown runs and the down event is
	// audited.
	unblock()
	ev := h.settings.WaitEvent(t, config.EventLinkDown)
	if ev.Detail != "inbound_limit" {
		t.Errorf("link_down detail = %q, want inbound_limit", ev.Detail)
	}
	if ev.Peer == nil || ev.Peer.Fingerprint != dc.fp {
		t.Errorf("link_down peer = %+v, want %s", ev.Peer, dc.fp)
	}
	if st := h.l.State(dc.fp); st.Phase != PhaseWaiting {
		t.Errorf("acceptor phase after the close = %v, want waiting", st.Phase)
	}
}

// TestStateQueueByteLimitClosesLink checks that a few large state frames
// exceeding the byte limit close the connection with inbound_limit.
func TestStateQueueByteLimitClosesLink(t *testing.T) {
	h := newDispatchHarness(t)
	SetTestKnobs(h.l, TestKnobs{MaxStateBytes: 1024})

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	h.l.HandleState(200, func(Fingerprint, Frame) { <-release })

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	payload := make([]byte, 600)
	for i := 0; i < 3; i++ {
		if err := dc.peer.Send(&linkpb.Frame{Kind: 200, Payload: payload}); err != nil {
			break
		}
	}

	waitConnClosed(t, dc.c)
	if got := dc.c.reason(); got != FailInboundLimit {
		t.Fatalf("close reason = %v, want inbound_limit", got)
	}

	entry := waitLogEvent(t, h.logs, "link_inbound_limit")
	if entry["peer"] != dc.fp.String() {
		t.Errorf("log peer = %v, want %s", entry["peer"], dc.fp)
	}
	if entry["limit"] != "state_bytes" {
		t.Errorf("log limit = %v, want state_bytes", entry["limit"])
	}
}

// TestRequestConcurrencyLimit checks that at most 64 request handlers run at
// once for one peer.
func TestRequestConcurrencyLimit(t *testing.T) {
	h := newDispatchHarness(t)
	var started atomic.Int32
	release := make(chan struct{})
	defer close(release)
	h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
		started.Add(1)
		<-release
		return Frame{Kind: 301}
	})

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	for i := 0; i < 65; i++ {
		dc.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: uint64(i + 1)})
	}

	dispatchWait(t, "64 request handlers", func() bool { return started.Load() >= 64 })
	// Give an unbounded implementation time to start the 65th handler.
	time.Sleep(200 * time.Millisecond)
	if got := started.Load(); got != 64 {
		t.Fatalf("%d request handlers started at once, want 64", got)
	}
}

// TestRequestSlotFreedResumesInboundAndReplies checks that freeing a slot
// starts the next handler and that a reply still arrives over the same link.
func TestRequestSlotFreedResumesInboundAndReplies(t *testing.T) {
	h := newDispatchHarness(t)
	var started atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblockAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblockAll()
	h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
		started.Add(1)
		<-release
		return Frame{Kind: 301}
	})

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	for i := 0; i < 65; i++ {
		dc.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: uint64(i + 1)})
	}
	dispatchWait(t, "64 request handlers", func() bool { return started.Load() >= 64 })
	time.Sleep(100 * time.Millisecond)
	if got := started.Load(); got != 64 {
		t.Fatalf("%d request handlers started at once, want 64", got)
	}

	// Free one slot so the router starts the 65th handler.
	release <- struct{}{}
	dispatchWait(t, "the 65th request handler", func() bool { return started.Load() == 65 })

	res := startRequest(h, dc.fp, 400)
	req := recvPeerKind(t, dc, 400)
	dc.sendPeer(t, &linkpb.Frame{Kind: 400, RequestId: req.GetRequestId(), IsReply: true, Payload: []byte("ok")})
	r := waitRequestResult(t, res)
	if r.err != nil || string(r.reply.Payload) != "ok" {
		t.Fatalf("request result = %+v, want the reply ok", r)
	}
}

// TestRequestSlotsArePerPeerAcrossReconnect checks that a replacement
// connection shares the peer's 64 slots while the old handlers still run.
func TestRequestSlotsArePerPeerAcrossReconnect(t *testing.T) {
	h := newDispatchHarness(t)
	var started atomic.Int32
	release := make(chan struct{})
	defer close(release)
	h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
		started.Add(1)
		<-release
		return Frame{Kind: 301}
	})

	fp := Fingerprint{1}
	first := h.attach(fp, pairing.RoleAcceptor)
	first.awaitUp(t)
	for i := 0; i < maxConcurrentRequests; i++ {
		first.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: uint64(i + 1)})
	}
	dispatchWait(t, "64 request handlers", func() bool { return started.Load() >= 64 })
	// install waits only for the old connection's consumers, not its request
	// handlers.
	second := h.attach(fp, pairing.RoleAcceptor)
	second.awaitUp(t)

	for i := 0; i < maxConcurrentRequests; i++ {
		second.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: uint64(i + 101)})
	}
	time.Sleep(200 * time.Millisecond)
	if got := started.Load(); got != maxConcurrentRequests {
		t.Fatalf("%d request handlers from one peer started at once across a reconnect, want %d", got, maxConcurrentRequests)
	}
}

func requestSlotEntry(h *dispatchHarness, fp Fingerprint) (chan struct{}, bool) {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	slots, ok := h.l.slots[fp]
	return slots, ok
}

// TestRequestSlotsForgottenOnUnpair checks that unpairing forgets the slot
// entry once no handler holds it, and keeps it while one does.
func TestRequestSlotsForgottenOnUnpair(t *testing.T) {
	t.Run("no handler running", func(t *testing.T) {
		h := newDispatchHarness(t)
		h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
			return Frame{Kind: 301}
		})
		fp := Fingerprint{1}
		dc := h.attach(fp, pairing.RoleAcceptor)
		dc.awaitUp(t)
		// runConn creates the entry in its own goroutine, which awaitUp does
		// not wait for.
		dispatchWait(t, "the slot entry to appear", func() bool {
			_, ok := requestSlotEntry(h, fp)
			return ok
		})

		h.l.Drop(fp)
		dispatchWait(t, "the slot entry to be forgotten on unpair", func() bool {
			_, ok := requestSlotEntry(h, fp)
			return !ok
		})
	})

	t.Run("handler still running", func(t *testing.T) {
		h := newDispatchHarness(t)
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
			started <- struct{}{}
			<-release
			return Frame{Kind: 301}
		})
		fp := Fingerprint{1}
		dc := h.attach(fp, pairing.RoleAcceptor)
		dc.awaitUp(t)
		dc.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: 1})
		select {
		case <-started:
		case <-time.After(eventTimeout):
			t.Fatal("the request handler did not start")
		}

		h.l.Drop(fp)
		// Forgetting the entry while a handler holds a slot would let a re-pair
		// hand the peer a second set of 64 slots.
		if _, ok := requestSlotEntry(h, fp); !ok {
			t.Fatal("the slot entry was forgotten while a handler still held a slot")
		}

		close(release)
		dispatchWait(t, "the slot entry to be forgotten after the handler returned", func() bool {
			_, ok := requestSlotEntry(h, fp)
			return !ok
		})
	})
}

// TestStateQueueCountsInflightBytes checks that a frame being handled keeps
// counting toward the byte limit until its handler returns.
func TestStateQueueCountsInflightBytes(t *testing.T) {
	h := newDispatchHarness(t)
	SetTestKnobs(h.l, TestKnobs{MaxStateBytes: 1024})

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	started := make(chan struct{}, 1)
	h.l.HandleState(200, func(Fingerprint, Frame) {
		started <- struct{}{}
		<-release
	})

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	// The consumer pops the first frame and its handler blocks, so the frame
	// stays inflight and must keep counting.
	if err := dc.peer.Send(&linkpb.Frame{Kind: 200, Payload: make([]byte, 900)}); err != nil {
		t.Fatalf("send the first frame: %v", err)
	}
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("the state handler did not start on the first frame")
	}

	if err := dc.peer.Send(&linkpb.Frame{Kind: 200, Payload: make([]byte, 900)}); err != nil {
		t.Fatalf("send the second frame: %v", err)
	}
	waitConnClosed(t, dc.c)
	if got := dc.c.reason(); got != FailInboundLimit {
		t.Fatalf("close reason = %v, want inbound_limit", got)
	}
	entry := waitLogEvent(t, h.logs, "link_inbound_limit")
	if entry["limit"] != "state_bytes" {
		t.Errorf("log limit = %v, want state_bytes", entry["limit"])
	}
}
