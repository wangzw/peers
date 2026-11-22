package helper

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

// scriptedChild plays the helper process on the other end of a Client's
// pipes. Tests drive it message by message.
type scriptedChild struct {
	reqs *msgReader
	msgs *msgWriter
	msgW *io.PipeWriter
}

// newScriptedChild connects a Client to a scripted child, completes the
// handshake and returns both.
func newScriptedChild(t *testing.T) (*Client, *scriptedChild) {
	t.Helper()
	toChildR, toChildW := io.Pipe()
	fromChildR, fromChildW := io.Pipe()
	c := NewClient(fromChildR, toChildW)
	sc := &scriptedChild{
		reqs: newMsgReader(toChildR),
		msgs: newMsgWriter(fromChildW),
		msgW: fromChildW,
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = fromChildW.Close()
	})

	sc.send(t, readyMessage())
	if _, err := c.WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	return c, sc
}

// next reads the next request, failing the test after three seconds.
func (s *scriptedChild) next(t *testing.T) *helperpb.HelperRequest {
	t.Helper()
	type result struct {
		req *helperpb.HelperRequest
		err error
	}
	ch := make(chan result, 1)
	go func() {
		var req helperpb.HelperRequest
		err := s.reqs.read(&req)
		ch <- result{req: &req, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("scripted child read: %v", r.err)
		}
		return r.req
	case <-time.After(processTimeout):
		t.Fatal("timed out waiting for a request")
		return nil
	}
}

func (s *scriptedChild) send(t *testing.T, msg *helperpb.HelperMessage) {
	t.Helper()
	if err := s.msgs.write(msg); err != nil {
		t.Fatalf("scripted child write: %v", err)
	}
}

// die closes the child's write end, which the Client sees as a lost helper.
func (s *scriptedChild) die() {
	_ = s.msgW.Close()
}

func readyMessage() *helperpb.HelperMessage {
	return &helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_Ready{Ready: &helperpb.Ready{
			ProtocolVersion: helperpb.ProtocolVersion,
		}},
	}
}

func sessionMessage(id uint64, s *helperpb.Session) *helperpb.HelperMessage {
	return &helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_Response{Response: &helperpb.HelperResponse{
			Id:     id,
			Result: &helperpb.HelperResponse_Session{Session: s},
		}},
	}
}

func doneMessage(id uint64) *helperpb.HelperMessage {
	return &helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_Response{Response: &helperpb.HelperResponse{
			Id:     id,
			Result: &helperpb.HelperResponse_Done{Done: &helperpb.Done{}},
		}},
	}
}

func watchStartedMessage(id, watchID uint64) *helperpb.HelperMessage {
	return &helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_Response{Response: &helperpb.HelperResponse{
			Id:     id,
			Result: &helperpb.HelperResponse_WatchStarted{WatchStarted: &helperpb.WatchStarted{WatchId: watchID}},
		}},
	}
}

func errorMessage(id uint64, code helperpb.HelperErrorCode, msg string) *helperpb.HelperMessage {
	return &helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_Response{Response: &helperpb.HelperResponse{
			Id:     id,
			Result: &helperpb.HelperResponse_Error{Error: &helperpb.HelperError{Code: code, Message: msg}},
		}},
	}
}

func watchEventMessage(watchID uint64, pid int64) *helperpb.HelperMessage {
	return &helperpb.HelperMessage{
		Body: &helperpb.HelperMessage_WatchEvent{WatchEvent: &helperpb.WatchEvent{WatchId: watchID, Pid: pid}},
	}
}

func TestClientLookupRoundTrip(t *testing.T) {
	c, child := newScriptedChild(t)

	type result struct {
		s   inbox.Session
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		s, err := c.Lookup(context.Background(), "/c", "/s")
		resCh <- result{s: s, err: err}
	}()

	req := child.next(t)
	if req.GetLookup() == nil {
		t.Fatalf("request op = %T, want lookup", req.GetOp())
	}
	if got := req.GetLookup().GetConfigDir(); got != "/c" {
		t.Fatalf("config_dir = %q, want /c", got)
	}
	if got := req.GetLookup().GetSocketPath(); got != "/s" {
		t.Fatalf("socket_path = %q, want /s", got)
	}

	child.send(t, sessionMessage(req.GetId(), &helperpb.Session{
		ConfigDir:  "/c",
		Pid:        1 << 40,
		Name:       "a",
		Status:     "busy",
		SocketPath: "/s",
	}))

	r := <-resCh
	if r.err != nil {
		t.Fatalf("Lookup: %v", r.err)
	}
	want := inbox.Session{ConfigDir: "/c", PID: 1 << 40, Name: "a", Status: "busy", SocketPath: "/s"}
	if r.s != want {
		t.Fatalf("Lookup = %+v, want %+v", r.s, want)
	}
}

func TestClientErrorsAreSentinels(t *testing.T) {
	t.Run("lookup", func(t *testing.T) {
		c, child := newScriptedChild(t)
		errCh := make(chan error, 1)
		go func() {
			_, err := c.Lookup(context.Background(), "/c", "/s")
			errCh <- err
		}()
		req := child.next(t)
		child.send(t, errorMessage(req.GetId(), helperpb.HelperErrorCode_HELPER_ERROR_CODE_NO_RECORD, "no record"))
		if err := <-errCh; !errors.Is(err, inbox.ErrNoRecord) {
			t.Fatalf("Lookup error = %v, want inbox.ErrNoRecord", err)
		}
	})

	t.Run("deliver", func(t *testing.T) {
		c, child := newScriptedChild(t)
		errCh := make(chan error, 1)
		go func() {
			errCh <- c.Deliver(context.Background(), inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Second)
		}()
		req := child.next(t)
		child.send(t, errorMessage(req.GetId(), helperpb.HelperErrorCode_HELPER_ERROR_CODE_UNCERTAIN, "maybe"))
		if err := <-errCh; !errors.Is(err, inbox.ErrUncertain) {
			t.Fatalf("Deliver error = %v, want inbox.ErrUncertain", err)
		}
	})
}

func TestClientDeliverInFlightWhenHelperDiesIsUncertain(t *testing.T) {
	c, child := newScriptedChild(t)
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Deliver(context.Background(), inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", 5*time.Second)
	}()

	req := child.next(t)
	if req.GetDeliver() == nil {
		t.Fatalf("request op = %T, want deliver", req.GetOp())
	}
	child.die()

	select {
	case err := <-errCh:
		if !errors.Is(err, inbox.ErrUncertain) {
			t.Fatalf("Deliver error = %v, want inbox.ErrUncertain", err)
		}
		if !errors.Is(err, ErrHelperGone) {
			t.Fatalf("Deliver error = %v, want ErrHelperGone", err)
		}
	case <-time.After(processTimeout):
		t.Fatal("Deliver did not return after the helper died")
	}
}

func TestClientDeliverAfterGoneIsNotWritten(t *testing.T) {
	c, child := newScriptedChild(t)
	child.die()
	select {
	case <-c.Done():
	case <-time.After(processTimeout):
		t.Fatal("Done not closed after the helper died")
	}

	err := c.Deliver(context.Background(), inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Second)
	if !errors.Is(err, inbox.ErrNotWritten) {
		t.Fatalf("Deliver error = %v, want inbox.ErrNotWritten", err)
	}
	if !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Deliver error = %v, want ErrHelperGone", err)
	}
}

func TestClientDeliverCancelWaitsForOutcome(t *testing.T) {
	c, child := newScriptedChild(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", 5*time.Second)
	}()

	req := child.next(t)
	if req.GetDeliver() == nil {
		t.Fatalf("request op = %T, want deliver", req.GetOp())
	}
	deliverID := req.GetId()
	cancel()

	cancelReq := child.next(t)
	if cancelReq.GetCancel() == nil {
		t.Fatalf("request op = %T, want cancel", cancelReq.GetOp())
	}
	if got := cancelReq.GetCancel().GetRequestId(); got != deliverID {
		t.Fatalf("cancel request_id = %d, want %d", got, deliverID)
	}
	child.send(t, errorMessage(deliverID, helperpb.HelperErrorCode_HELPER_ERROR_CODE_NOT_WRITTEN, "not written"))

	select {
	case err := <-errCh:
		if !errors.Is(err, inbox.ErrNotWritten) {
			t.Fatalf("Deliver error = %v, want inbox.ErrNotWritten", err)
		}
		if errors.Is(err, context.Canceled) {
			t.Fatalf("Deliver error = %v, want the write outcome, not context.Canceled", err)
		}
	case <-time.After(processTimeout):
		t.Fatal("Deliver did not return after the cancel outcome")
	}
}

func TestClientDeliverTimeoutClampedToDeadline(t *testing.T) {
	c, child := newScriptedChild(t)
	const deadline = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Minute)
	}()

	req := child.next(t)
	if req.GetDeliver() == nil {
		t.Fatalf("request op = %T, want deliver", req.GetOp())
	}
	got := req.GetDeliver().GetTimeout().AsDuration()
	if got > deadline {
		t.Fatalf("deliver timeout = %v, want at most %v", got, deadline)
	}
	if got < deadline/2 {
		t.Fatalf("deliver timeout = %v, want close to the %v deadline", got, deadline)
	}

	child.send(t, doneMessage(req.GetId()))
	if err := <-errCh; err != nil {
		t.Fatalf("Deliver: %v", err)
	}
}

func TestClientWatchStopNoLateCallbacks(t *testing.T) {
	c, child := newScriptedChild(t)

	var mu sync.Mutex
	var pids []int
	onChange := func(pid int) {
		mu.Lock()
		pids = append(pids, pid)
		mu.Unlock()
	}

	stopCh := make(chan func(), 1)
	go func() {
		stop, err := c.Watch(context.Background(), "/cfg", onChange)
		if err != nil {
			t.Errorf("Watch: %v", err)
			stopCh <- func() {}
			return
		}
		stopCh <- stop
	}()

	req := child.next(t)
	if req.GetWatch() == nil {
		t.Fatalf("request op = %T, want watch", req.GetOp())
	}
	if got := req.GetWatch().GetConfigDir(); got != "/cfg" {
		t.Fatalf("watch config_dir = %q, want /cfg", got)
	}
	child.send(t, watchStartedMessage(req.GetId(), 1))
	stop := <-stopCh

	child.send(t, watchEventMessage(1, 7))
	child.send(t, watchEventMessage(1, 7))
	clientWaitForPids(t, &mu, &pids, 2)

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()

	unwatchReq := child.next(t)
	if unwatchReq.GetUnwatch() == nil {
		t.Fatalf("request op = %T, want unwatch", unwatchReq.GetOp())
	}
	if got := unwatchReq.GetUnwatch().GetWatchId(); got != 1 {
		t.Fatalf("unwatch watch_id = %d, want 1", got)
	}
	child.send(t, doneMessage(unwatchReq.GetId()))
	select {
	case <-stopped:
	case <-time.After(processTimeout):
		t.Fatal("stop did not return after the unwatch reply")
	}

	child.send(t, watchEventMessage(1, 8))
	// A subsequent request/response pair proves the read loop has consumed the
	// event: both messages travel on the same child-to-broker stream.
	synced := make(chan struct{})
	go func() {
		defer close(synced)
		if _, err := c.Lookup(context.Background(), "/c", "/s"); err != nil {
			t.Errorf("sync Lookup: %v", err)
		}
	}()
	syncReq := child.next(t)
	child.send(t, sessionMessage(syncReq.GetId(), &helperpb.Session{Name: "sync"}))
	<-synced

	mu.Lock()
	n := len(pids)
	mu.Unlock()
	if n != 2 {
		t.Fatalf("callbacks after stop = %d total, want 2", n)
	}
}

func clientWaitForPids(t *testing.T, mu *sync.Mutex, pids *[]int, want int) {
	t.Helper()
	deadline := time.Now().Add(processTimeout)
	for {
		mu.Lock()
		n := len(*pids)
		mu.Unlock()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("callbacks = %d, want %d", n, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClientUnknownResponseIdIgnored(t *testing.T) {
	c, child := newScriptedChild(t)

	child.send(t, doneMessage(999))

	type result struct {
		s   inbox.Session
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		s, err := c.Lookup(context.Background(), "/c", "/s")
		resCh <- result{s: s, err: err}
	}()

	req := child.next(t)
	child.send(t, sessionMessage(req.GetId(), &helperpb.Session{Name: "a"}))
	r := <-resCh
	if r.err != nil {
		t.Fatalf("Lookup: %v", r.err)
	}
	if r.s.Name != "a" {
		t.Fatalf("Lookup name = %q, want a", r.s.Name)
	}
}

func TestClientSecondReadyIsProtocolError(t *testing.T) {
	c, child := newScriptedChild(t)
	child.send(t, readyMessage())

	select {
	case <-c.Done():
	case <-time.After(processTimeout):
		t.Fatal("Done not closed after a second Ready")
	}
	if _, err := c.Lookup(context.Background(), "/c", "/s"); !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Lookup error = %v, want ErrHelperGone", err)
	}
}

// TestClientGoneIsUnavailable checks that losing the pipe to the helper wraps
// ErrUnavailable.
func TestClientGoneIsUnavailable(t *testing.T) {
	c, child := newScriptedChild(t)
	child.die()
	select {
	case <-c.Done():
	case <-time.After(processTimeout):
		t.Fatal("Done was not closed after the child's pipe closed")
	}
	if _, err := c.Lookup(context.Background(), "/c", "/s"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
}

// TestClientPermissionErrorIsNotUnavailable checks that an error the helper
// reports for a record it could not read stays an ordinary error: the helper
// ran and answered, so it is not the thing that failed.
func TestClientPermissionErrorIsNotUnavailable(t *testing.T) {
	c, child := newScriptedChild(t)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Lookup(context.Background(), "/c", "/s")
		errCh <- err
	}()
	req := child.next(t)
	child.send(t, errorMessage(req.GetId(), helperpb.HelperErrorCode_HELPER_ERROR_CODE_FAILED,
		"inbox: reading session registry: permission denied"))
	err := <-errCh
	if err == nil {
		t.Fatal("Lookup succeeded")
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it not to wrap ErrUnavailable", err)
	}
}

// TestClientLookupDeadlineIsUnavailable checks that a helper that accepted the
// request but never answered is reported with ErrUnavailable when the call's
// deadline expires. The helper is up and the request reached it, so this is the
// helper failing to answer, not a record that cannot be read.
func TestClientLookupDeadlineIsUnavailable(t *testing.T) {
	c, child := newScriptedChild(t)

	// The caller's own deadline is the assertion: the helper accepted the
	// request and never answered, so the call must end when it expires.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Lookup(ctx, "/c", "/s")
		errCh <- err
	}()

	// Read the request so the write completes; no response follows.
	child.next(t)
	err := <-errCh
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lookup error = %v, want it to keep the context deadline", err)
	}
}

// TestClientWatchDeadlineIsUnavailable checks the same for the
// session-directory watch request.
func TestClientWatchDeadlineIsUnavailable(t *testing.T) {
	c, child := newScriptedChild(t)

	// The caller's own deadline is the assertion for the watch path too.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Watch(ctx, "/c", func(int) {})
		errCh <- err
	}()

	child.next(t)
	err := <-errCh
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Watch error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v, want it to keep the context deadline", err)
	}
}

// TestClientRereadDeadlineIsUnavailable checks that a helper that accepted the
// reread request but never answered is reported with ErrUnavailable when the
// call's deadline expires. The helper is up and the request reached it, so this
// is the helper failing to answer, not a record that cannot be read.
func TestClientRereadDeadlineIsUnavailable(t *testing.T) {
	c, child := newScriptedChild(t)

	// The caller's own deadline is the assertion for the reread path too.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Reread(ctx, inbox.Session{SocketPath: "/s"})
		errCh <- err
	}()

	// Read the request so the write completes; no response follows.
	child.next(t)
	err := <-errCh
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Reread error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reread error = %v, want it to keep the context deadline", err)
	}
}

// TestClientCancelIsNotUnavailable checks that a caller's own cancellation is
// not reported as a helper failure: the helper is not the thing that ended the
// call.
func TestClientCancelIsNotUnavailable(t *testing.T) {
	c, child := newScriptedChild(t)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Lookup(ctx, "/c", "/s")
		errCh <- err
	}()
	child.next(t)
	cancel()
	err := <-errCh
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want a caller cancellation not to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Lookup error = %v, want context.Canceled", err)
	}
}

func TestClientRereadCancelUnblocksPipeWrite(t *testing.T) {
	// The scripted child never reads, so the request write blocks on the pipe.
	// Cancellation must return rather than wait for a reader that is gone.
	const timeout = 100 * time.Millisecond
	c, _ := newScriptedChild(t)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := c.Reread(ctx, inbox.Session{SocketPath: "/s"})
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(timeout + processTimeout):
		t.Fatal("Reread did not return at its deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reread error = %v, want context.DeadlineExceeded", err)
	}
	if !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Reread error = %v, want ErrHelperGone: the connection cannot be reused", err)
	}
}

// TestClientWriteCancelIsNotUnavailable checks that a caller's cancellation
// while the request is still being written is not reported as a helper
// failure. The child never reads, so the write is blocked on the pipe when the
// cancellation arrives; the connection is unusable afterwards, but the helper
// is not what ended the call.
func TestClientWriteCancelIsNotUnavailable(t *testing.T) {
	c, _ := newScriptedChild(t)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Reread(ctx, inbox.Session{SocketPath: "/s"})
		errCh <- err
	}()

	cancel()
	err := <-errCh
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Reread error = %v, want a caller cancellation not to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Reread error = %v, want context.Canceled", err)
	}
}

func TestClientDeliverOversizeIsNotWritten(t *testing.T) {
	c, _ := newScriptedChild(t)
	session := inbox.Session{SocketPath: "/s"}
	text := string(make([]byte, maxMessageSize+1))

	err := c.Deliver(context.Background(), session, "sig", text, "m1", time.Second)
	if !errors.Is(err, inbox.ErrNotWritten) {
		t.Fatalf("Deliver error = %v, want inbox.ErrNotWritten", err)
	}
	if errors.Is(err, inbox.ErrUncertain) {
		t.Fatalf("Deliver error = %v, want a definite not-written result", err)
	}
}

// unwatchWaitBound is how long the test waits for stop to return after the
// helper leaves an unwatch unanswered. The product bounds the request by
// unwatchTimeout; the package fallback covers handing the result back to the
// test goroutine on a loaded runner after that timer fires.
const unwatchWaitBound = unwatchTimeout + processTimeout

func TestClientUnwatchBoundedWhenReplyMissing(t *testing.T) {
	c, child := newScriptedChild(t)

	stopCh := make(chan func(), 1)
	go func() {
		stop, err := c.Watch(context.Background(), "/cfg", func(int) {})
		if err != nil {
			t.Errorf("Watch: %v", err)
			stopCh <- func() {}
			return
		}
		stopCh <- stop
	}()
	req := child.next(t)
	child.send(t, watchStartedMessage(req.GetId(), 1))
	stop := <-stopCh

	stopped := make(chan struct{})
	began := time.Now()
	go func() {
		stop()
		close(stopped)
	}()
	// Read the unwatch request but never answer it.
	if unwatchReq := child.next(t); unwatchReq.GetUnwatch() == nil {
		t.Fatalf("request op = %T, want unwatch", unwatchReq.GetOp())
	}

	select {
	case <-stopped:
	case <-time.After(unwatchWaitBound):
		t.Fatal("stop did not return without an unwatch reply")
	}
	if elapsed := time.Since(began); elapsed < unwatchTimeout {
		t.Fatalf("stop returned after %v, want it to wait for the %v unwatch timeout", elapsed, unwatchTimeout)
	}
}

func TestClientFirstMessageNotReady(t *testing.T) {
	toChildR, toChildW := io.Pipe()
	fromChildR, fromChildW := io.Pipe()
	t.Cleanup(func() {
		_ = toChildW.Close()
		_ = toChildR.Close()
		_ = fromChildW.Close()
		_ = fromChildR.Close()
	})

	c := NewClient(fromChildR, toChildW)
	if err := newMsgWriter(fromChildW).write(doneMessage(1)); err != nil {
		t.Fatalf("write first message: %v", err)
	}
	if _, err := c.WaitReady(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatalf("WaitReady error = %v, want ErrProtocol", err)
	}
}
