package link

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// logBuffer is a concurrency-safe bytes.Buffer for a slog JSON handler.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// dispatchHarness is a Link with a started notifier and the fakes it needs.
type dispatchHarness struct {
	t        *testing.T
	l        *Link
	peers    *linktest.FakePeers
	settings *linktest.FakeSettings
	clock    *linktest.FakeClock
	logs     *logBuffer
}

func newDispatchHarness(t *testing.T) *dispatchHarness {
	t.Helper()
	peers := linktest.NewFakePeers(t)
	settings := linktest.NewFakeSettings("self")
	clock := linktest.NewFakeClock(time.Unix(1700000000, 0))
	logs := &logBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	l, err := New(Options{Peers: peers, Settings: settings, Clock: clock, Logger: logger})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l.notifier.start()
	t.Cleanup(l.notifier.stop)
	return &dispatchHarness{t: t, l: l, peers: peers, settings: settings, clock: clock, logs: logs}
}

// dispatchConn is one installed connection and the pipe end the test drives.
type dispatchConn struct {
	c         *conn
	link      *pipeEnd // the conn's stream end
	peer      *pipeEnd // the test drives this end
	fp        Fingerprint
	delivered <-chan struct{}
}

// attach installs a connection for fp and starts running it.
func (h *dispatchHarness) attach(fp Fingerprint, role pairing.Role) *dispatchConn {
	h.t.Helper()
	a, b := newPipe(h.t)
	c := newConn(a, func() {}, peerIdentity{fp: fp, pub: make([]byte, 32), remote: "127.0.0.1:1"}, h.l.sendQueueSize)
	c.role = role
	c.start()

	delivered, ok := h.l.install(fp, role, c, []byte("dispatch-salt"), h.l.dropGeneration(fp))
	if !ok {
		h.t.Fatal("install returned false")
	}
	go h.l.runConn(c, delivered)

	dc := &dispatchConn{c: c, link: a, peer: b, fp: fp, delivered: delivered}
	h.t.Cleanup(func() {
		c.closeAsync(FailConnectionLost)
		select {
		case <-c.stateDone:
		case <-time.After(eventTimeout):
			h.t.Errorf("connection did not finish")
		}
	})
	return dc
}

// awaitUp waits until every onUp callback of the connection finished.
func (dc *dispatchConn) awaitUp(t *testing.T) {
	t.Helper()
	select {
	case <-dc.delivered:
	case <-time.After(eventTimeout):
		t.Fatal("link up notification was not delivered")
	}
}

// sendPeer sends a raw frame to the link under test.
func (dc *dispatchConn) sendPeer(t *testing.T, f *linkpb.Frame) {
	t.Helper()
	if err := dc.peer.Send(f); err != nil {
		t.Fatalf("peer send: %v", err)
	}
}

// recvPeer waits up to eventTimeout for one frame from the link.
func (dc *dispatchConn) recvPeer(t *testing.T) *linkpb.Frame {
	t.Helper()
	f, ok := tryRecvPeer(dc, eventTimeout)
	if !ok {
		t.Fatal("timed out waiting for a frame from the link")
	}
	return f
}

// expectNoPeerFrame fails when the link sends a frame within d.
func (dc *dispatchConn) expectNoPeerFrame(t *testing.T, d time.Duration) {
	t.Helper()
	if f, ok := tryRecvPeer(dc, d); ok {
		t.Fatalf("unexpected frame from the link: kind %d request_id %d", f.GetKind(), f.GetRequestId())
	}
}

func tryRecvPeer(dc *dispatchConn, d time.Duration) (*linkpb.Frame, bool) {
	ch := make(chan *linkpb.Frame, 1)
	go func() {
		if f, err := dc.peer.Recv(); err == nil {
			ch <- f
		}
	}()
	select {
	case f := <-ch:
		return f, true
	case <-time.After(d):
		return nil, false
	}
}

// dispatchWait polls cond until it holds or eventTimeout passed.
func dispatchWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// dispatchIntPayload encodes a frame sequence number.
func dispatchIntPayload(n int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func TestRegisterRejectsReservedAndDuplicate(t *testing.T) {
	h := newDispatchHarness(t)
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", name)
			}
		}()
		f()
	}

	mustPanic("kind 1", func() { h.l.HandleState(1, func(Fingerprint, Frame) {}) })
	mustPanic("kind 150", func() {
		h.l.HandleRequest(150, func(context.Context, Fingerprint, Frame) Frame { return Frame{} })
	})
	mustPanic("kind 0", func() { h.l.HandleState(0, func(Fingerprint, Frame) {}) })
	mustPanic("nil state handler", func() { h.l.HandleState(300, nil) })
	mustPanic("nil request handler", func() { h.l.HandleRequest(300, nil) })

	h.l.HandleState(300, func(Fingerprint, Frame) {})
	mustPanic("duplicate state handler", func() { h.l.HandleState(300, func(Fingerprint, Frame) {}) })
	mustPanic("state then request", func() {
		h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame { return Frame{} })
	})
}

func TestSendWhenNotEstablished(t *testing.T) {
	h := newDispatchHarness(t)
	peer := Fingerprint{1}

	if err := h.l.Send(peer, Frame{Kind: 200}); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("Send without a record = %v, want ErrNotEstablished", err)
	}

	h.l.mu.Lock()
	h.l.ensureRecord(peer, pairing.RoleAcceptor, PhaseWaiting)
	h.l.mu.Unlock()
	if err := h.l.Send(peer, Frame{Kind: 200}); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("Send while waiting = %v, want ErrNotEstablished", err)
	}
}

func TestSendReservedKind(t *testing.T) {
	h := newDispatchHarness(t)
	peer := Fingerprint{1}
	if err := h.l.Send(peer, Frame{Kind: 3}); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("Send kind 3 = %v, want ErrInvalidKind", err)
	}
	_, sent, err := h.l.Request(context.Background(), peer, Frame{Kind: 1})
	if !errors.Is(err, ErrInvalidKind) || sent {
		t.Fatalf("Request kind 1 = (%v, %v), want (ErrInvalidKind, false)", sent, err)
	}
}

func TestStateFramesInArrivalOrder(t *testing.T) {
	h := newDispatchHarness(t)
	got := make(chan int, 500)
	h.l.HandleState(200, func(_ Fingerprint, f Frame) {
		got <- int(binary.BigEndian.Uint64(f.Payload))
	})

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := 0; i < 500; i++ {
			_ = dc.peer.Send(&linkpb.Frame{Kind: 200, Payload: dispatchIntPayload(i)})
		}
	}()

	for i := 0; i < 500; i++ {
		select {
		case n := <-got:
			if n != i {
				t.Fatalf("state frame %d arrived as %d", i, n)
			}
		case <-time.After(eventTimeout):
			t.Fatalf("timed out after %d state frames", i)
		}
	}
	select {
	case <-sent:
	case <-time.After(eventTimeout):
		t.Fatal("peer sender did not finish")
	}
}

func TestOnUpCompletesBeforeFirstStateFrame(t *testing.T) {
	h := newDispatchHarness(t)
	var calls atomic.Int32
	handled := make(chan struct{}, 1)
	h.l.HandleState(200, func(Fingerprint, Frame) {
		calls.Add(1)
		select {
		case handled <- struct{}{}:
		default:
		}
	})
	// The request is routed independently of the state handler, so its reply is
	// a barrier that proves the state frame has been routed while onUp is still
	// blocked.
	h.l.HandleRequest(301, func(context.Context, Fingerprint, Frame) Frame {
		return Frame{Kind: 302}
	})

	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	h.l.Subscribe(
		func(Fingerprint, []byte) { entered <- struct{}{}; <-block },
		func(Fingerprint) {},
	)

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	select {
	case <-entered:
	case <-time.After(eventTimeout):
		t.Fatal("onUp did not start")
	}

	dc.sendPeer(t, &linkpb.Frame{Kind: 200, Payload: dispatchIntPayload(1)})
	dc.sendPeer(t, &linkpb.Frame{Kind: 301, RequestId: 7})
	if reply := dc.recvPeer(t); reply.GetKind() != 302 || !reply.GetIsReply() {
		t.Fatalf("barrier reply = kind %d is_reply %v, want kind 302", reply.GetKind(), reply.GetIsReply())
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("state handler ran %d times before onUp returned", n)
	}

	close(block)
	select {
	case <-handled:
	case <-time.After(eventTimeout):
		t.Fatal("state handler did not run after onUp returned")
	}
}

func TestNoStateFrameAfterOnDown(t *testing.T) {
	h := newDispatchHarness(t)
	var calls atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	entered := make(chan struct{}, 3)
	h.l.HandleState(200, func(Fingerprint, Frame) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
	})

	down := make(chan int, 1)
	h.l.Subscribe(
		func(Fingerprint, []byte) {},
		func(Fingerprint) { down <- int(calls.Load()) },
	)

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	for i := 0; i < 3; i++ {
		dc.sendPeer(t, &linkpb.Frame{Kind: 200, Payload: dispatchIntPayload(i)})
	}
	select {
	case <-entered:
	case <-time.After(eventTimeout):
		t.Fatal("state handler did not start")
	}

	dc.c.closeAsync(FailConnectionLost)
	unblock()

	var atDown int
	select {
	case atDown = <-down:
	case <-time.After(eventTimeout):
		t.Fatal("onDown was not called")
	}
	if got := int(calls.Load()); got != atDown {
		t.Fatalf("state handler ran %d times in total, onDown saw %d", got, atDown)
	}
	if atDown != 1 {
		t.Fatalf("state handler ran %d times before onDown, want 1", atDown)
	}
}

func TestRequestNotBlockedByStateHandler(t *testing.T) {
	h := newDispatchHarness(t)
	stateEntered := make(chan struct{}, 1)
	requestRan := make(chan struct{})
	h.l.HandleState(200, func(Fingerprint, Frame) {
		stateEntered <- struct{}{}
		<-requestRan
	})
	h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
		close(requestRan)
		return Frame{Kind: 300, Payload: []byte("reply")}
	})

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	dc.sendPeer(t, &linkpb.Frame{Kind: 200, Payload: dispatchIntPayload(1)})
	select {
	case <-stateEntered:
	case <-time.After(eventTimeout):
		t.Fatal("state handler did not start")
	}

	dc.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: 5})
	f := dc.recvPeer(t)
	if f.GetKind() != 300 || f.GetRequestId() != 5 || !f.GetIsReply() {
		t.Fatalf("reply = kind %d request_id %d is_reply %v", f.GetKind(), f.GetRequestId(), f.GetIsReply())
	}
}

func TestStateHandlerCanAwaitReplyOnSameLink(t *testing.T) {
	h := newDispatchHarness(t)
	result := make(chan error, 1)
	h.l.HandleState(200, func(peer Fingerprint, _ Frame) {
		ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
		defer cancel()
		reply, sent, err := h.l.Request(ctx, peer, Frame{Kind: 300, Payload: []byte("ask")})
		if err == nil && !sent {
			err = errors.New("request was not sent")
		}
		if err == nil && string(reply.Payload) != "answer" {
			err = errors.New("unexpected reply payload")
		}
		result <- err
	})

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)
	dc.sendPeer(t, &linkpb.Frame{Kind: 200, Payload: dispatchIntPayload(1)})

	req := dc.recvPeer(t)
	if req.GetKind() != 300 || req.GetRequestId() == 0 || req.GetIsReply() {
		t.Fatalf("request frame = kind %d request_id %d is_reply %v", req.GetKind(), req.GetRequestId(), req.GetIsReply())
	}
	dc.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: req.GetRequestId(), IsReply: true, Payload: []byte("answer")})

	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("state handler blocked while awaiting a reply")
	}
}

func TestUnknownRequestGetsUnsupported(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	dc.sendPeer(t, &linkpb.Frame{Kind: 999, RequestId: 7})
	f := dc.recvPeer(t)
	kind, err := decodeUnsupported(f)
	if err != nil {
		t.Fatalf("decode unsupported reply: %v", err)
	}
	if kind != 999 {
		t.Fatalf("unsupported request kind = %d, want 999", kind)
	}
	if f.GetRequestId() != 7 || !f.GetIsReply() {
		t.Fatalf("reply = request_id %d is_reply %v", f.GetRequestId(), f.GetIsReply())
	}
}

func TestUnknownStateFrameDropped(t *testing.T) {
	h := newDispatchHarness(t)
	handled := make(chan *linkpb.Frame, 1)
	h.l.HandleState(200, func(_ Fingerprint, f Frame) {
		handled <- &linkpb.Frame{Kind: f.Kind, Payload: f.Payload}
	})
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	dc.sendPeer(t, &linkpb.Frame{Kind: 998, Payload: []byte("ignored")})
	dc.sendPeer(t, &linkpb.Frame{Kind: 200, Payload: []byte("kept")})

	select {
	case f := <-handled:
		if string(f.GetPayload()) != "kept" {
			t.Fatalf("handler saw payload %q, want kept", f.GetPayload())
		}
	case <-time.After(eventTimeout):
		t.Fatal("registered state frame was not handled")
	}
	select {
	case <-handled:
		t.Fatal("the unknown state frame reached a handler")
	default:
	}
}

type requestResult struct {
	reply Frame
	sent  bool
	err   error
}

func startRequest(h *dispatchHarness, peer Fingerprint, kind FrameKind) <-chan requestResult {
	res := make(chan requestResult, 1)
	go func() {
		f, sent, err := h.l.Request(context.Background(), peer, Frame{Kind: kind, Payload: []byte("ping")})
		res <- requestResult{reply: f, sent: sent, err: err}
	}()
	return res
}

func waitRequestResult(t *testing.T, res <-chan requestResult) requestResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(eventTimeout):
		t.Fatal("Request did not return")
		return requestResult{}
	}
}

// manualDeadline is a context the test expires explicitly. It reports
// DeadlineExceeded once expired, so a Request takes exactly the branch a real
// expired deadline takes. A test that must observe the frame written before the
// deadline fires cannot use a real timer without racing the scheduler to the
// deadline; firing this one only after the write removes that race.
type manualDeadline struct {
	done chan struct{}
}

func newManualDeadline() *manualDeadline { return &manualDeadline{done: make(chan struct{})} }

func (m *manualDeadline) Deadline() (time.Time, bool) {
	select {
	case <-m.done:
		return time.Unix(0, 0), true
	default:
		return time.Unix(1<<40, 0), true
	}
}

func (m *manualDeadline) Done() <-chan struct{} { return m.done }

func (m *manualDeadline) Err() error {
	select {
	case <-m.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (m *manualDeadline) Value(any) any { return nil }

// expire fires the deadline.
func (m *manualDeadline) expire() { close(m.done) }

func TestRequestReplyAndUnsupported(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	// A normal reply.
	res := startRequest(h, dc.fp, 300)
	req := dc.recvPeer(t)
	if req.GetKind() != 300 || req.GetRequestId() == 0 || req.GetIsReply() {
		t.Fatalf("request = kind %d request_id %d is_reply %v", req.GetKind(), req.GetRequestId(), req.GetIsReply())
	}
	dc.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: req.GetRequestId(), IsReply: true, Payload: []byte("pong")})
	r := waitRequestResult(t, res)
	if r.err != nil || !r.sent || string(r.reply.Payload) != "pong" || r.reply.Kind != 300 {
		t.Fatalf("reply = %+v, want kind 300 payload pong sent true", r)
	}

	// An Unsupported reply.
	res = startRequest(h, dc.fp, 301)
	req = dc.recvPeer(t)
	dc.sendPeer(t, &linkpb.Frame{
		Kind:      linkpb.KindUnsupported,
		RequestId: req.GetRequestId(),
		IsReply:   true,
		Payload:   marshalPayload(&linkpb.Unsupported{RequestKind: 301}),
	})
	r = waitRequestResult(t, res)
	var ue *UnsupportedError
	if !errors.As(r.err, &ue) || ue.Kind != 301 || !r.sent {
		t.Fatalf("unsupported result = %+v, want *UnsupportedError{301} sent true", r)
	}
}

func TestRequestSentSemantics(t *testing.T) {
	t.Run("not established", func(t *testing.T) {
		h := newDispatchHarness(t)
		_, sent, err := h.l.Request(context.Background(), Fingerprint{1}, Frame{Kind: 300})
		if sent || !errors.Is(err, ErrNotEstablished) {
			t.Fatalf("Request = (%v, %v), want (false, ErrNotEstablished)", sent, err)
		}
	})

	t.Run("queued then canceled", func(t *testing.T) {
		h := newDispatchHarness(t)
		dc := h.attach(Fingerprint{1}, pairing.RoleDialer)
		dc.awaitUp(t)

		block := make(chan struct{})
		var blockOnce sync.Once
		release := func() { blockOnce.Do(func() { close(block) }) }
		dc.link.blockSend = block
		defer release()
		if err := h.l.Send(dc.fp, Frame{Kind: 200}); err != nil {
			t.Fatalf("first Send: %v", err)
		}
		if err := h.l.Send(dc.fp, Frame{Kind: 200}); err != nil {
			t.Fatalf("second Send: %v", err)
		}
		dispatchWait(t, "the writer to take one frame", func() bool { return len(dc.c.out) == 1 })

		// The writer stays blocked for the whole deadline, so the result is
		// fixed: Request queues the frame, gives up when the deadline expires,
		// and reports that it never wrote it.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, sent, err := h.l.Request(ctx, dc.fp, Frame{Kind: 300})
		if sent || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Request = (%v, %v), want (false, deadline exceeded)", sent, err)
		}

		release()
		// The writer drains the queue: it writes the two fillers and skips the
		// canceled request. Read the two known frames, then hold a negative
		// window for a third: the writer may already have taken the canceled
		// request from the queue but not yet sent it when the queue empties, so
		// a synchronous count alone can miss the late write.
		for i := 0; i < 2; i++ {
			if f := dc.recvPeer(t); f.GetKind() != 200 {
				t.Fatalf("frame %d has kind %d, want the filler kind 200", i, f.GetKind())
			}
		}
		dc.expectNoPeerFrame(t, 100*time.Millisecond)
		// Supplement: the synchronous count once the queue drained.
		dispatchWait(t, "the writer to drain the queue", func() bool { return len(dc.c.out) == 0 })
		if frames := dc.link.sent(); len(frames) != 2 {
			t.Fatalf("peer got %d frames, want the two fillers only", len(frames))
		}
	})

	t.Run("written then context expires", func(t *testing.T) {
		h := newDispatchHarness(t)
		dc := h.attach(Fingerprint{1}, pairing.RoleDialer)
		dc.awaitUp(t)

		// Expire the deadline only after the frame reached the transport, so the
		// Request is in the written state when it observes the deadline.
		ctx := newManualDeadline()
		res := make(chan requestResult, 1)
		go func() {
			f, sent, err := h.l.Request(ctx, dc.fp, Frame{Kind: 300})
			res <- requestResult{reply: f, sent: sent, err: err}
		}()
		req := dc.recvPeer(t)
		if req.GetKind() != 300 {
			t.Fatalf("request kind = %d, want 300", req.GetKind())
		}
		ctx.expire()
		r := waitRequestResult(t, res)
		if !r.sent || !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatalf("Request = (%v, %v), want (true, deadline exceeded)", r.sent, r.err)
		}
	})

	t.Run("written then link down", func(t *testing.T) {
		h := newDispatchHarness(t)
		dc := h.attach(Fingerprint{1}, pairing.RoleDialer)
		dc.awaitUp(t)

		res := startRequest(h, dc.fp, 300)
		req := dc.recvPeer(t)
		if req.GetKind() != 300 {
			t.Fatalf("request kind = %d, want 300", req.GetKind())
		}
		dc.c.closeAsync(FailConnectionLost)
		r := waitRequestResult(t, res)
		if !r.sent || !errors.Is(r.err, ErrLinkDown) {
			t.Fatalf("Request = (%v, %v), want (true, ErrLinkDown)", r.sent, r.err)
		}
	})
}

func TestSendDoesNotBlockUnderTopicLock(t *testing.T) {
	h := newDispatchHarness(t)
	SetTestKnobs(h.l, TestKnobs{SendQueueSize: 8})

	dc := h.attach(Fingerprint{1}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	var m sync.Mutex
	downCalled := make(chan struct{}, 1)
	h.l.Subscribe(
		func(Fingerprint, []byte) {},
		func(Fingerprint) {
			m.Lock()
			downCalled <- struct{}{}
			m.Unlock()
		},
	)

	block := make(chan struct{})
	dc.link.blockSend = block
	defer close(block)

	m.Lock()
	deadline := time.Now().Add(eventTimeout)
	var sendErr error
	for time.Now().Before(deadline) {
		if err := h.l.Send(dc.fp, Frame{Kind: 200}); err != nil {
			sendErr = err
			break
		}
	}
	m.Unlock()
	if !errors.Is(sendErr, ErrQueueFull) {
		t.Fatalf("Send loop ended with %v, want ErrQueueFull", sendErr)
	}

	select {
	case <-downCalled:
	case <-time.After(eventTimeout):
		t.Fatal("onDown was not called after the queue filled")
	}
}

func TestPeerInfoObservedByRole(t *testing.T) {
	addrs := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "x"},
		{Kind: "relay", Addr: "y"},
	}

	t.Run("dialer", func(t *testing.T) {
		h := newDispatchHarness(t)
		peer := Fingerprint{3}
		dc := h.attach(peer, pairing.RoleDialer)
		dc.awaitUp(t)

		dc.sendPeer(t, encodePeerInfo("b2", addrs))
		// handlePeerInfo records the alias and the address list in two
		// separate calls. Waiting on the alias alone can return in the window
		// before the address list is recorded, so wait for both.
		h.peers.Wait(t, func() bool {
			obs := h.peers.Observed(peer)
			return len(obs.Aliases) == 1 && len(obs.AcceptorAddresses) == 1
		})

		obs := h.peers.Observed(peer)
		if len(obs.Aliases) != 1 || obs.Aliases[0] != "b2" {
			t.Fatalf("observed aliases = %v, want [b2]", obs.Aliases)
		}
		if len(obs.AcceptorAddresses) != 1 {
			t.Fatalf("observed address lists = %d, want 1", len(obs.AcceptorAddresses))
		}
		got := obs.AcceptorAddresses[0]
		if len(got) != len(addrs) {
			t.Fatalf("observed addresses = %+v, want %+v", got, addrs)
		}
		for i := range addrs {
			if got[i] != addrs[i] {
				t.Fatalf("observed addresses = %+v, want %+v", got, addrs)
			}
		}
	})

	t.Run("acceptor", func(t *testing.T) {
		h := newDispatchHarness(t)
		peer := Fingerprint{4}
		// A marker state frame behind the PeerInfo frame is the barrier: state
		// frames of one connection are handled in arrival order, so the marker
		// runs only after PeerInfo was fully processed.
		processed := make(chan struct{})
		h.l.HandleState(201, func(Fingerprint, Frame) { close(processed) })
		dc := h.attach(peer, pairing.RoleAcceptor)
		dc.awaitUp(t)

		dc.sendPeer(t, encodePeerInfo("b2", addrs))
		dc.sendPeer(t, &linkpb.Frame{Kind: 201})
		select {
		case <-processed:
		case <-time.After(eventTimeout):
			t.Fatal("state handler did not process the marker frame")
		}
		obs := h.peers.Observed(peer)
		if len(obs.Aliases) != 1 || obs.Aliases[0] != "b2" {
			t.Fatalf("observed aliases = %v, want [b2]", obs.Aliases)
		}
		if lists := obs.AcceptorAddresses; len(lists) != 0 {
			t.Fatalf("acceptor recorded address lists: %+v", lists)
		}
	})
}

func TestAnnounceAliasAndAddresses(t *testing.T) {
	h := newDispatchHarness(t)
	addrs := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.20:17443"},
		{Kind: config.AdvertisedKindDirect, Addr: "b.vpn.example:17443"},
	}
	h.settings.SetAlias("me")
	h.settings.SetAdvertised(addrs)

	acc := h.attach(Fingerprint{5}, pairing.RoleAcceptor)
	dial := h.attach(Fingerprint{6}, pairing.RoleDialer)
	acc.awaitUp(t)
	dial.awaitUp(t)

	h.l.AnnounceAlias()
	info, err := decodePeerInfo(acc.recvPeer(t))
	if err != nil {
		t.Fatalf("decode acceptor PeerInfo: %v", err)
	}
	if info.GetAlias() != "me" {
		t.Fatalf("acceptor PeerInfo alias = %q, want me", info.GetAlias())
	}
	got := addrsFromPB(info.GetAdvertisedAddresses())
	if len(got) != len(addrs) || got[0] != addrs[0] || got[1] != addrs[1] {
		t.Fatalf("acceptor PeerInfo addresses = %+v, want %+v", got, addrs)
	}

	info, err = decodePeerInfo(dial.recvPeer(t))
	if err != nil {
		t.Fatalf("decode dialer PeerInfo: %v", err)
	}
	if info.GetAlias() != "me" || len(info.GetAdvertisedAddresses()) != 0 {
		t.Fatalf("dialer PeerInfo = alias %q addresses %+v, want me and none", info.GetAlias(), info.GetAdvertisedAddresses())
	}

	h.l.AnnounceAddresses()
	info, err = decodePeerInfo(acc.recvPeer(t))
	if err != nil {
		t.Fatalf("decode second acceptor PeerInfo: %v", err)
	}
	got = addrsFromPB(info.GetAdvertisedAddresses())
	if len(got) != len(addrs) || got[0] != addrs[0] || got[1] != addrs[1] {
		t.Fatalf("announced addresses = %+v, want %+v", got, addrs)
	}
	// Supplement: AnnounceAddresses targets acceptors only, so the dialer must
	// not have received a second announcement. The synchronous count is only a
	// quick extra check; the negative window below is what catches a late frame.
	dispatchWait(t, "the dialer writer to drain", func() bool { return len(dial.c.out) == 0 })
	if got := len(dial.link.sent()); got != 1 {
		t.Fatalf("dialer wrote %d frames, want only the alias announcement", got)
	}
	// Negative window: a frame queued after the check still shows up here.
	dial.expectNoPeerFrame(t, 100*time.Millisecond)
}

// TestCloseWaitsForRequestHandler pins the promise that Close returns only
// after every request handler returned. The handler holds a gate open, so a
// Close that does not wait for it returns early and fails the test.
func TestCloseWaitsForRequestHandler(t *testing.T) {
	h := newDispatchHarness(t)
	dc := h.attach(Fingerprint{7}, pairing.RoleAcceptor)
	dc.awaitUp(t)

	entered := make(chan struct{})
	gate := make(chan struct{})
	h.l.HandleRequest(300, func(context.Context, Fingerprint, Frame) Frame {
		close(entered)
		<-gate
		return Frame{Kind: 301}
	})

	dc.sendPeer(t, &linkpb.Frame{Kind: 300, RequestId: 11})
	select {
	case <-entered:
	case <-time.After(eventTimeout):
		t.Fatal("request handler did not start")
	}

	closed := make(chan struct{})
	go func() {
		h.l.Close()
		close(closed)
	}()

	// Close must block until the handler returns. A correct Close waits for the
	// handler and cannot return while the gate stays open, so this window only
	// detects the bug and cannot fail a correct Close. There is no state that
	// says "Close is blocked", so the window is the assertion, not a fallback.
	select {
	case <-closed:
		close(gate)
		t.Fatal("Close returned while a request handler was still running")
	case <-time.After(200 * time.Millisecond):
	}

	close(gate)
	select {
	case <-closed:
	case <-time.After(eventTimeout):
		t.Fatal("Close did not return after the request handler finished")
	}
}
