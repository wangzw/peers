package helper

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

func rereadRequest(id uint64, s *helperpb.Session) *helperpb.HelperRequest {
	return &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Reread{Reread: &helperpb.RereadRequest{Session: s}},
	}
}

// TestServeRequestWithoutOperationIsRejected checks that a request with no operation is answered with BAD_REQUEST and its id.
func TestServeRequestWithoutOperationIsRejected(t *testing.T) {
	side := newServeSide(t, &fakeOps{}, Identity{UID: 1})
	side.readReady()

	side.send(&helperpb.HelperRequest{Id: 9})
	resp := responseOf(t, side.recv())
	if resp.GetId() != 9 {
		t.Errorf("response id = %d, want 9", resp.GetId())
	}
	e := resp.GetError()
	if e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST {
		t.Fatalf("response = %v, want BAD_REQUEST", resp)
	}
	if got := e.GetMessage(); got != "request has no operation" {
		t.Fatalf("error message = %q, want %q", got, "request has no operation")
	}
}

// TestServeZeroRequestIdIsRejected checks that request id 0 is answered with BAD_REQUEST naming the zero id.
func TestServeZeroRequestIdIsRejected(t *testing.T) {
	side := newServeSide(t, &fakeOps{}, Identity{UID: 1})
	side.readReady()

	side.send(lookupRequest(0, "/c", "/s"))
	resp := responseOf(t, side.recv())
	if resp.GetId() != 0 {
		t.Errorf("response id = %d, want 0", resp.GetId())
	}
	e := resp.GetError()
	if e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST {
		t.Fatalf("response = %v, want BAD_REQUEST", resp)
	}
	if got := e.GetMessage(); got != "request id is zero" {
		t.Fatalf("error message = %q, want %q", got, "request id is zero")
	}
}

// TestServeRereadReturnsSession checks that Reread answers with the record the operations layer returns.
func TestServeRereadReturnsSession(t *testing.T) {
	ops := &fakeOps{}
	ops.sessions = map[string]inbox.Session{
		"/s": {ConfigDir: "/c", PID: 4242, Name: "a", Status: "busy", SocketPath: "/s"},
	}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(rereadRequest(3, &helperpb.Session{SocketPath: "/s"}))
	resp := responseOf(t, side.recv())
	sess := resp.GetSession()
	if sess == nil {
		t.Fatalf("response = %v, want a session", resp)
	}
	if sess.GetName() != "a" || sess.GetPid() != 4242 || sess.GetStatus() != "busy" {
		t.Fatalf("session = %v, want the fakeOps record", sess)
	}
}

// TestServeRereadMissingIsNoRecord checks that Reread of a record that does not exist answers NO_RECORD.
func TestServeRereadMissingIsNoRecord(t *testing.T) {
	side := newServeSide(t, &fakeOps{}, Identity{UID: 1})
	side.readReady()

	// A nil session arrives with an empty socket path, which matches no record.
	side.send(rereadRequest(4, nil))
	resp := responseOf(t, side.recv())
	e := resp.GetError()
	if e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_NO_RECORD {
		t.Fatalf("response = %v, want NO_RECORD", resp)
	}
}

// TestServeDeliverTimeoutIsRequired checks that a Deliver without a positive timeout is answered BAD_REQUEST and never run.
func TestServeDeliverTimeoutIsRequired(t *testing.T) {
	side := newServeSide(t, &fakeOps{}, Identity{UID: 1})
	side.readReady()

	requests := []*helperpb.HelperRequest{
		{Id: 1, Op: &helperpb.HelperRequest_Deliver{Deliver: &helperpb.DeliverRequest{}}},
		deliverRequest(2, "m", 0),
	}
	for _, req := range requests {
		side.send(req)
		resp := responseOf(t, side.recv())
		if resp.GetId() != req.GetId() {
			t.Errorf("response id = %d, want %d", resp.GetId(), req.GetId())
		}
		e := resp.GetError()
		if e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_REQUEST {
			t.Fatalf("request %d: response = %v, want BAD_REQUEST", req.GetId(), resp)
		}
		if got := e.GetMessage(); got != "deliver timeout is missing or not positive" {
			t.Fatalf("request %d: error message = %q, want the timeout reason", req.GetId(), got)
		}
	}
	if got := deliveredCount(side.ops); got != 0 {
		t.Fatalf("deliveries = %d, want 0 for requests without a valid timeout", got)
	}
}

// TestServeDeliverUnknownFailureCode checks that a known delivery error keeps its code while an unknown one becomes FAILED.
func TestServeDeliverUnknownFailureCode(t *testing.T) {
	ops := &fakeOps{deliverFn: func(_ context.Context, _ inbox.Session, _, _, msgID string, _ time.Duration) error {
		if msgID == "partial" {
			return inbox.ErrPartial
		}
		return errors.New("boom")
	}}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	for i, tc := range []struct {
		msgID string
		code  helperpb.HelperErrorCode
	}{
		{"partial", helperpb.HelperErrorCode_HELPER_ERROR_CODE_PARTIAL},
		{"unknown", helperpb.HelperErrorCode_HELPER_ERROR_CODE_FAILED},
	} {
		side.send(deliverRequest(uint64(i+1), tc.msgID, time.Second))
		resp := responseOf(t, side.recv())
		e := resp.GetError()
		if e == nil || e.GetCode() != tc.code {
			t.Fatalf("message %q: response = %v, want code %v", tc.msgID, resp, tc.code)
		}
	}
}

// TestServeWatchFailureReportsFailed checks that a failed watch is answered with FAILED and the error text.
func TestServeWatchFailureReportsFailed(t *testing.T) {
	ops := &fakeOps{watchErr: errors.New("watch failed")}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(watchRequest(1, "/c"))
	resp := responseOf(t, side.recv())
	if resp.GetWatchStarted() != nil {
		t.Fatalf("response = %v, want an error, not WatchStarted", resp)
	}
	e := resp.GetError()
	if e == nil || e.GetCode() != helperpb.HelperErrorCode_HELPER_ERROR_CODE_FAILED {
		t.Fatalf("response = %v, want FAILED", resp)
	}
	if got := e.GetMessage(); got != "watch failed" {
		t.Fatalf("error message = %q, want %q", got, "watch failed")
	}
}

// TestServeWatchEventAfterStartedIsForwarded checks that a watch event written after WatchStarted reaches the client with its watch id and pid.
func TestServeWatchEventAfterStartedIsForwarded(t *testing.T) {
	ops := &fakeOps{}
	side := newServeSide(t, ops, Identity{UID: 1})
	side.readReady()

	side.send(watchRequest(1, "/c"))
	if resp := responseOf(t, side.recv()); resp.GetWatchStarted() == nil {
		t.Fatalf("response = %v, want WatchStarted", resp)
	}

	// The event is written on the watcher's goroutine; the response pipe
	// blocks until this test reads it.
	go ops.fire("/c", 21)
	event := watchEventOf(t, side.recv())
	if event.GetWatchId() != 1 || event.GetPid() != 21 {
		t.Fatalf("event = %v, want {watch 1, pid 21}", event)
	}
}

// TestServerClosedDoesNotServe checks that requests and a watch arriving after cleanup are not served and write nothing.
func TestServerClosedDoesNotServe(t *testing.T) {
	var out bytes.Buffer
	ops := &fakeOps{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newServer(ctx, &out, ops, Identity{UID: 1})

	s.cleanup()
	// A second cleanup must be a no-op: it only waits for the handlers.
	s.cleanup()
	if s.ctx.Err() == nil {
		t.Fatal("cleanup did not cancel the server context")
	}

	requests := []*helperpb.HelperRequest{
		lookupRequest(1, "/c", "/s"),
		rereadRequest(2, &helperpb.Session{SocketPath: "/s"}),
		deliverRequest(3, "m", time.Second),
		watchRequest(4, "/c"),
		unwatchRequest(5, 1),
		cancelRequest(6, 1),
	}
	for _, req := range requests {
		if err := s.handle(req); err != nil {
			t.Fatalf("handle(%T) = %v, want nil", req.GetOp(), err)
		}
	}

	s.serveWatch(7, &helperpb.WatchRequest{ConfigDir: "/c"})

	if out.Len() != 0 {
		t.Fatalf("wrote %q after cleanup, want no response", out.String())
	}
	if got := deliveredCount(ops); got != 0 {
		t.Fatalf("deliveries = %d, want 0 after cleanup", got)
	}
	if got := stoppedCount(ops, "/c"); got != 0 {
		t.Fatalf("stops = %d, want 0: serveWatch on a closed server never started a watch", got)
	}
}

// TestServeWatchStoppedWhenClosedDuringStart checks that a watch started while cleanup runs is stopped and not announced.
func TestServeWatchStoppedWhenClosedDuringStart(t *testing.T) {
	var out bytes.Buffer
	ops := &fakeOps{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newServer(ctx, &out, ops, Identity{UID: 1})
	ops.onWatch = s.cleanup

	s.serveWatch(1, &helperpb.WatchRequest{ConfigDir: "/c"})

	if got := stoppedCount(ops, "/c"); got != 1 {
		t.Fatalf("stops = %d, want 1: the watch started during cleanup must be stopped", got)
	}
	if out.Len() != 0 {
		t.Fatalf("wrote %q, want no WatchStarted after cleanup", out.String())
	}
}

// failingWriter returns err on every write after the first allowed one.
type failingWriter struct {
	mu    sync.Mutex
	calls int
	allow int
	err   error
	first chan struct{}
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.calls++
	n := w.calls
	first := w.first
	w.mu.Unlock()
	if n == 1 && first != nil {
		close(first)
	}
	if n > w.allow {
		return 0, w.err
	}
	return len(p), nil
}

// TestServeReadyWriteFailureIsReturned checks that Serve returns the error from writing Ready.
func TestServeReadyWriteFailureIsReturned(t *testing.T) {
	sentinel := errors.New("ready write failed")
	w := &failingWriter{allow: 0, err: sentinel}

	err := Serve(context.Background(), bytes.NewReader(nil), w, &fakeOps{}, Identity{UID: 1})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Serve = %v, want the Ready write error", err)
	}
}

// TestServeWriteFailureEndsServe checks that a failed response write ends Serve with that error.
func TestServeWriteFailureEndsServe(t *testing.T) {
	sentinel := errors.New("response write failed")
	first := make(chan struct{})
	w := &failingWriter{allow: 1, err: sentinel, first: first}

	reqR, reqW := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- Serve(context.Background(), reqR, w, &fakeOps{}, Identity{UID: 1}) }()
	t.Cleanup(func() { _ = reqW.Close() })

	select {
	case <-first:
	case <-time.After(processTimeout):
		t.Fatal("Serve did not write Ready")
	}
	if err := newMsgWriter(reqW).write(lookupRequest(1, "/c", "/s")); err != nil {
		t.Fatalf("writing request: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatalf("Serve = %v, want the response write error", err)
		}
	case <-time.After(processTimeout):
		t.Fatal("Serve did not return after a failed response write")
	}
}
