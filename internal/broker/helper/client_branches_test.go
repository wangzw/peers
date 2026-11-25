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

// blockingWriteCloser signals when a write starts and then blocks until it is
// closed, which is how a pipe write behaves when the peer never reads.
type blockingWriteCloser struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func newBlockingWriteCloser() *blockingWriteCloser {
	return &blockingWriteCloser{started: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingWriteCloser) Write(p []byte) (int, error) {
	w.startOnce.Do(func() { close(w.started) })
	<-w.release
	return 0, io.ErrClosedPipe
}

func (w *blockingWriteCloser) Close() error {
	w.closeOnce.Do(func() { close(w.release) })
	return nil
}

// recordWriteCloser counts write calls and can fail them.
type recordWriteCloser struct {
	mu     sync.Mutex
	writes int
	err    error
}

func (w *recordWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes++
	if w.err != nil {
		return 0, w.err
	}
	return len(p), nil
}

func (w *recordWriteCloser) Close() error { return nil }

func (w *recordWriteCloser) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

// newIdleClient returns a Client whose read loop is blocked on an empty pipe,
// so tests can drive its unexported parts without a peer.
func newIdleClient(t *testing.T) *Client {
	t.Helper()
	return newIdleClientWith(t, &recordWriteCloser{})
}

func newIdleClientWith(t *testing.T, wc io.WriteCloser) *Client {
	t.Helper()
	r, w := io.Pipe()
	c := NewClient(r, wc)
	t.Cleanup(func() {
		_ = c.Close()
		_ = w.Close()
		_ = r.Close()
	})
	return c
}

// TestClientUnknownBodyIsProtocolError checks that an empty message body ends the connection and reports ErrHelperGone.
func TestClientUnknownBodyIsProtocolError(t *testing.T) {
	c, child := newScriptedChild(t)
	child.send(t, &helperpb.HelperMessage{})

	select {
	case <-c.Done():
	case <-time.After(processTimeout):
		t.Fatal("Done was not closed after an empty message body")
	}
	if _, err := c.Lookup(context.Background(), "/c", "/s"); !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Lookup error = %v, want ErrHelperGone", err)
	}
}

// TestClientShutdownIsIdempotent checks that a second shutdown is a no-op that leaves Done closed without panicking.
func TestClientShutdownIsIdempotent(t *testing.T) {
	c := newIdleClient(t)
	c.shutdown(io.EOF)
	c.shutdown(errors.New("late report"))

	select {
	case <-c.Done():
	default:
		t.Fatal("Done is not closed after shutdown")
	}
}

// TestClientDuplicateResponseIsDropped checks that a second response for the same request id is discarded.
func TestClientDuplicateResponseIsDropped(t *testing.T) {
	c := newIdleClient(t)
	id, p, ok := c.register(nil)
	if !ok {
		t.Fatal("register failed on a live client")
	}
	resp := &helperpb.HelperResponse{
		Id:     id,
		Result: &helperpb.HelperResponse_Done{Done: &helperpb.Done{}},
	}
	c.dispatchResponse(resp)
	c.dispatchResponse(resp)

	select {
	case got := <-p.ch:
		if got.GetId() != id {
			t.Fatalf("delivered response id = %d, want %d", got.GetId(), id)
		}
	case <-time.After(processTimeout):
		t.Fatal("the first response was not delivered")
	}
	select {
	case extra := <-p.ch:
		t.Fatalf("a second response for id %d was delivered: %v", id, extra)
	default:
	}
}

// TestClientWaitReadyGoneBeforeReady checks that a helper which ends before Ready reports ErrHelperGone wrapping ErrUnavailable.
func TestClientWaitReadyGoneBeforeReady(t *testing.T) {
	fromR, fromW := io.Pipe()
	toR, toW := io.Pipe()
	c := NewClient(fromR, toW)
	t.Cleanup(func() {
		_ = c.Close()
		_ = fromW.Close()
		_ = fromR.Close()
		_ = toR.Close()
		_ = toW.Close()
	})

	_ = fromW.Close()

	_, err := c.WaitReady(context.Background())
	if !errors.Is(err, ErrHelperGone) {
		t.Fatalf("WaitReady error = %v, want ErrHelperGone", err)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("WaitReady error = %v, want it to wrap ErrUnavailable", err)
	}
}

// TestClientWaitReadyDeadlineIsUnavailable checks that a Ready wait ended by its deadline wraps ErrUnavailable and keeps DeadlineExceeded.
func TestClientWaitReadyDeadlineIsUnavailable(t *testing.T) {
	c := newIdleClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := c.WaitReady(ctx)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("WaitReady error = %v, want it to wrap ErrUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady error = %v, want it to keep the context deadline", err)
	}
}

// TestClientLookupSendFailureIsHelperGone checks that a Lookup whose request cannot be sent reports ErrHelperGone wrapping ErrUnavailable.
func TestClientLookupSendFailureIsHelperGone(t *testing.T) {
	c, _ := newScriptedChild(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := c.Lookup(context.Background(), "/c", "/s")
	if !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Lookup error = %v, want ErrHelperGone", err)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup error = %v, want it to wrap ErrUnavailable", err)
	}
}

// TestClientDeliverSendFailureIsNotWritten checks that a Deliver whose request cannot be sent reports ErrNotWritten, not ErrUncertain.
func TestClientDeliverSendFailureIsNotWritten(t *testing.T) {
	c, _ := newScriptedChild(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := c.Deliver(context.Background(), inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Second)
	if !errors.Is(err, inbox.ErrNotWritten) {
		t.Fatalf("Deliver error = %v, want inbox.ErrNotWritten", err)
	}
	if !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Deliver error = %v, want ErrHelperGone", err)
	}
	if errors.Is(err, inbox.ErrUncertain) {
		t.Fatalf("Deliver error = %v, want a definite not-written result", err)
	}
}

// TestClientDeliverWriteInterruptedByContext checks that a Deliver still writing when its context ends reports an uncertain outcome and keeps the context error.
func TestClientDeliverWriteInterruptedByContext(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		r, w := io.Pipe()
		wc := newBlockingWriteCloser()
		c := NewClient(r, wc)
		t.Cleanup(func() {
			_ = wc.Close()
			_ = w.Close()
			_ = r.Close()
			_ = c.Close()
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- c.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Minute)
		}()
		select {
		case <-wc.started:
		case <-time.After(processTimeout):
			t.Fatal("the deliver request was never written")
		}
		cancel()

		err := waitCallError(t, "Deliver", errCh)
		if !errors.Is(err, inbox.ErrUncertain) {
			t.Fatalf("Deliver error = %v, want inbox.ErrUncertain", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Deliver error = %v, want it to keep context.Canceled", err)
		}
		if errors.Is(err, inbox.ErrNotWritten) {
			t.Fatalf("Deliver error = %v, want an uncertain outcome", err)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		r, w := io.Pipe()
		wc := newBlockingWriteCloser()
		c := NewClient(r, wc)
		t.Cleanup(func() {
			_ = wc.Close()
			_ = w.Close()
			_ = r.Close()
			_ = c.Close()
		})

		ctx := newTestDeadlineContext()
		errCh := make(chan error, 1)
		go func() {
			errCh <- c.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Minute)
		}()
		select {
		case <-wc.started:
		case <-time.After(processTimeout):
			t.Fatal("the deliver request was never written")
		}
		ctx.end()

		err := waitCallError(t, "Deliver", errCh)
		if !errors.Is(err, inbox.ErrUncertain) {
			t.Fatalf("Deliver error = %v, want inbox.ErrUncertain", err)
		}
		if !errors.Is(err, ErrHelperGone) {
			t.Fatalf("Deliver error = %v, want ErrHelperGone", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Deliver error = %v, want it to keep context.DeadlineExceeded", err)
		}
	})
}

func waitCallError(t *testing.T, call string, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(processTimeout):
		t.Fatalf("%s did not return", call)
		return nil
	}
}

// testDeadlineContext is a context the test ends itself: Done is closed by end and Err then reports DeadlineExceeded, so no timer races the goroutine under test.
type testDeadlineContext struct {
	done chan struct{}
}

func newTestDeadlineContext() *testDeadlineContext {
	return &testDeadlineContext{done: make(chan struct{})}
}

func (c *testDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *testDeadlineContext) Done() <-chan struct{} { return c.done }

func (c *testDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *testDeadlineContext) Value(any) any { return nil }

func (c *testDeadlineContext) end() { close(c.done) }

// TestClientDeliverWithCanceledContext checks that a Deliver called with an already cancelled context returns context.Canceled without wrapping ErrUnavailable.
func TestClientDeliverWithCanceledContext(t *testing.T) {
	c, _ := newScriptedChild(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Deliver error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Deliver error = %v, want a caller cancellation not to wrap ErrUnavailable", err)
	}
}

// TestClientHelperDiesBeforeAnswer checks that a helper which reads a request and then loses the connection reports ErrHelperGone for each kind.
func TestClientHelperDiesBeforeAnswer(t *testing.T) {
	t.Run("lookup", func(t *testing.T) {
		c, child := newScriptedChild(t)
		errCh := make(chan error, 1)
		go func() {
			_, err := c.Lookup(context.Background(), "/c", "/s")
			errCh <- err
		}()
		child.next(t)
		child.die()
		if err := waitCallError(t, "Lookup", errCh); !errors.Is(err, ErrHelperGone) {
			t.Fatalf("Lookup error = %v, want ErrHelperGone", err)
		}
	})

	t.Run("reread", func(t *testing.T) {
		c, child := newScriptedChild(t)
		errCh := make(chan error, 1)
		go func() {
			_, err := c.Reread(context.Background(), inbox.Session{SocketPath: "/s"})
			errCh <- err
		}()
		child.next(t)
		child.die()
		if err := waitCallError(t, "Reread", errCh); !errors.Is(err, ErrHelperGone) {
			t.Fatalf("Reread error = %v, want ErrHelperGone", err)
		}
	})

	t.Run("watch", func(t *testing.T) {
		c, child := newScriptedChild(t)
		errCh := make(chan error, 1)
		go func() {
			_, err := c.Watch(context.Background(), "/cfg", func(int) {})
			errCh <- err
		}()
		child.next(t)
		child.die()
		if err := waitCallError(t, "Watch", errCh); !errors.Is(err, ErrHelperGone) {
			t.Fatalf("Watch error = %v, want ErrHelperGone", err)
		}
	})
}

// TestClientAfterGoneRequestsFailFast checks that Reread and Watch return ErrHelperGone once the connection has ended.
func TestClientAfterGoneRequestsFailFast(t *testing.T) {
	c, child := newScriptedChild(t)
	child.die()
	select {
	case <-c.Done():
	case <-time.After(processTimeout):
		t.Fatal("Done was not closed after the helper died")
	}

	if _, err := c.Reread(context.Background(), inbox.Session{SocketPath: "/s"}); !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Reread error = %v, want ErrHelperGone", err)
	}
	if _, err := c.Watch(context.Background(), "/cfg", func(int) {}); !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Watch error = %v, want ErrHelperGone", err)
	}
}

// TestClientWatchSendFailureIsHelperGone checks that a Watch whose request cannot be sent returns ErrHelperGone and no stop.
func TestClientWatchSendFailureIsHelperGone(t *testing.T) {
	c, _ := newScriptedChild(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	stop, err := c.Watch(context.Background(), "/cfg", func(int) {})
	if err == nil {
		t.Fatalf("Watch error = nil, want a send failure")
	}
	if stop != nil {
		t.Fatal("Watch returned a stop for a request that was never written")
	}
	if !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Watch error = %v, want ErrHelperGone", err)
	}
}

// TestClientWatchErrorResponseIsSentinel checks that an error response to Watch maps its code to the inbox sentinel.
func TestClientWatchErrorResponseIsSentinel(t *testing.T) {
	c, child := newScriptedChild(t)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Watch(context.Background(), "/cfg", func(int) {})
		errCh <- err
	}()
	req := child.next(t)
	child.send(t, errorMessage(req.GetId(), helperpb.HelperErrorCode_HELPER_ERROR_CODE_PARTIAL, "partial"))

	err := waitCallError(t, "Watch", errCh)
	if !errors.Is(err, inbox.ErrPartial) {
		t.Fatalf("Watch error = %v, want inbox.ErrPartial", err)
	}
}

// TestClientWatchUnexpectedResponseIsProtocolError checks that a non-watch response to Watch reports ErrProtocol.
func TestClientWatchUnexpectedResponseIsProtocolError(t *testing.T) {
	c, child := newScriptedChild(t)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Watch(context.Background(), "/cfg", func(int) {})
		errCh <- err
	}()
	req := child.next(t)
	child.send(t, doneMessage(req.GetId()))

	if err := waitCallError(t, "Watch", errCh); !errors.Is(err, ErrProtocol) {
		t.Fatalf("Watch error = %v, want ErrProtocol", err)
	}
}

// TestClientUnwatchAfterGoneSendsNothing checks that an unwatch writes once before the connection ends and nothing after.
func TestClientUnwatchAfterGoneSendsNothing(t *testing.T) {
	wc := &recordWriteCloser{}
	c := newIdleClientWith(t, wc)

	done := make(chan struct{})
	go func() {
		c.unwatch(1)
		close(done)
	}()
	deadline := time.Now().Add(processTimeout)
	for wc.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("unwatch did not write its request on a live connection")
		}
		time.Sleep(time.Millisecond)
	}
	c.dispatchResponse(&helperpb.HelperResponse{Id: 1, Result: &helperpb.HelperResponse_Done{Done: &helperpb.Done{}}})
	select {
	case <-done:
	case <-time.After(processTimeout):
		t.Fatal("unwatch did not return after its response")
	}

	c.shutdown(io.EOF)

	before := wc.count()
	c.unwatch(2)
	if got := wc.count(); got != before {
		t.Fatalf("unwatch wrote %d request(s) after the connection ended, want 0", got-before)
	}
}

// TestClientUnwatchWriteFailureIsBounded checks that a failed unwatch write returns at once instead of waiting for a reply.
func TestClientUnwatchWriteFailureIsBounded(t *testing.T) {
	wc := &recordWriteCloser{err: errors.New("write failed")}
	c := newIdleClientWith(t, wc)

	done := make(chan struct{})
	go func() {
		c.unwatch(1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(unwatchTimeout / 2):
		t.Fatalf("unwatch did not return within %v after its write failed", unwatchTimeout/2)
	}
	if got := wc.count(); got != 1 {
		t.Fatalf("unwatch write attempts = %d, want 1", got)
	}
}

// TestClientSessionResultProtocolError checks that a Lookup answered with the wrong result type reports ErrProtocol.
func TestClientSessionResultProtocolError(t *testing.T) {
	c, child := newScriptedChild(t)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Lookup(context.Background(), "/c", "/s")
		errCh <- err
	}()
	req := child.next(t)
	child.send(t, doneMessage(req.GetId()))

	if err := waitCallError(t, "Lookup", errCh); !errors.Is(err, ErrProtocol) {
		t.Fatalf("Lookup error = %v, want ErrProtocol", err)
	}
}

// TestClientDeliverResultProtocolError checks that a Deliver answered with the wrong result type reports ErrProtocol.
func TestClientDeliverResultProtocolError(t *testing.T) {
	c, child := newScriptedChild(t)
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Deliver(context.Background(), inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Second)
	}()
	req := child.next(t)
	child.send(t, sessionMessage(req.GetId(), &helperpb.Session{Name: "a"}))

	if err := waitCallError(t, "Deliver", errCh); !errors.Is(err, ErrProtocol) {
		t.Fatalf("Deliver error = %v, want ErrProtocol", err)
	}
}

// TestClientErrorCodeMapping checks that every helper error code maps to its inbox sentinel and an empty message falls back to the code name.
func TestClientErrorCodeMapping(t *testing.T) {
	cases := []struct {
		code helperpb.HelperErrorCode
		want error
	}{
		{helperpb.HelperErrorCode_HELPER_ERROR_CODE_NO_RECORD, inbox.ErrNoRecord},
		{helperpb.HelperErrorCode_HELPER_ERROR_CODE_PARTIAL, inbox.ErrPartial},
		{helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_SIGNATURE, inbox.ErrBadSignature},
		{helperpb.HelperErrorCode_HELPER_ERROR_CODE_NOT_WRITTEN, inbox.ErrNotWritten},
		{helperpb.HelperErrorCode_HELPER_ERROR_CODE_UNCERTAIN, inbox.ErrUncertain},
		{helperpb.HelperErrorCode_HELPER_ERROR_CODE_CANCELED, context.Canceled},
	}
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			c, child := newScriptedChild(t)
			errCh := make(chan error, 1)
			go func() {
				_, err := c.Lookup(context.Background(), "/c", "/s")
				errCh <- err
			}()
			req := child.next(t)
			child.send(t, errorMessage(req.GetId(), tc.code, "reported"))
			if err := waitCallError(t, "Lookup", errCh); !errors.Is(err, tc.want) {
				t.Fatalf("Lookup error = %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("empty message", func(t *testing.T) {
		c, child := newScriptedChild(t)
		errCh := make(chan error, 1)
		go func() {
			_, err := c.Lookup(context.Background(), "/c", "/s")
			errCh <- err
		}()
		req := child.next(t)
		child.send(t, errorMessage(req.GetId(), helperpb.HelperErrorCode_HELPER_ERROR_CODE_FAILED, ""))
		err := waitCallError(t, "Lookup", errCh)
		if err == nil {
			t.Fatal("Lookup succeeded")
		}
		if got, want := err.Error(), helperpb.HelperErrorCode_HELPER_ERROR_CODE_FAILED.String(); got != want {
			t.Fatalf("Lookup error = %q, want the code name %q", got, want)
		}
	})
}

// TestClientCancelDeliverWhenHelperDies checks that a cancelled Deliver whose helper dies before answering reports ErrUncertain wrapping ErrHelperGone.
func TestClientCancelDeliverWhenHelperDies(t *testing.T) {
	c, child := newScriptedChild(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Minute)
	}()
	child.next(t)
	cancel()
	if req := child.next(t); req.GetCancel() == nil {
		t.Fatalf("request op = %T, want cancel", req.GetOp())
	}
	child.die()

	err := waitCallError(t, "Deliver", errCh)
	if !errors.Is(err, inbox.ErrUncertain) {
		t.Fatalf("Deliver error = %v, want inbox.ErrUncertain", err)
	}
	if !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Deliver error = %v, want ErrHelperGone", err)
	}
}

// TestClientCancelDeliverUnansweredIsUncertain checks that a cancelled Deliver with an unanswered cancel request reports ErrUncertain without ErrHelperGone.
func TestClientCancelDeliverUnansweredIsUncertain(t *testing.T) {
	c, child := newScriptedChild(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Minute)
	}()
	child.next(t)
	cancel()
	// Read the cancel request and leave it unanswered: the outcome is unknown.
	if req := child.next(t); req.GetCancel() == nil {
		t.Fatalf("request op = %T, want cancel", req.GetOp())
	}

	err := waitCallError(t, "Deliver", errCh)
	if !errors.Is(err, inbox.ErrUncertain) {
		t.Fatalf("Deliver error = %v, want inbox.ErrUncertain", err)
	}
	if errors.Is(err, ErrHelperGone) {
		t.Fatalf("Deliver error = %v, want the helper to be considered alive", err)
	}
}
