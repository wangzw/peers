package helper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
)

var (
	// ErrUnavailable reports that the account helper could not be started or
	// that communication with it failed. It marks the failures that belong to
	// the helper itself, as opposed to a session record that exists but cannot
	// be read, which stays an ordinary error.
	ErrUnavailable = errors.New("helper: account helper unavailable")
	// ErrHelperGone reports that the connection to the account helper is lost.
	// A lost pipe is a communication failure, so it wraps ErrUnavailable.
	ErrHelperGone = fmt.Errorf("%w: the account helper connection was lost", ErrUnavailable)
	// ErrProtocol reports a violation of the helper pipe protocol. The helper
	// is unusable when it speaks the protocol wrong, so it wraps ErrUnavailable.
	ErrProtocol = fmt.Errorf("%w: helper protocol violation", ErrUnavailable)
)

// deadlineUnavailable converts a context that ended at its deadline into a
// helper failure. A deadline on a helper call bounds how long the helper may
// take to start or answer, so its expiry means the helper did not respond. The
// context error stays reachable through errors.Is, because callers discard a
// stuck helper on a deadline. A cancellation is the caller's own doing and is
// returned unchanged, so it is not mistaken for a helper failure.
func deadlineUnavailable(ctx context.Context) error {
	err := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

// cancelWait is how long Deliver waits for the outcome of a request after it
// has asked the helper to cancel it.
const cancelWait = time.Second

// unwatchTimeout bounds asking the helper to stop a watch. A stopped helper
// must not keep the caller, which may be a Broker event loop, waiting.
const unwatchTimeout = time.Second

// writeCancelGrace is how long a write cancelled by its context has to report
// that it completed anyway. A pipe reader that consumed the frame unblocks the
// writer, but the cancellation can still win the race against the writer being
// scheduled, and that completed frame must not be mistaken for a lost one.
const writeCancelGrace = 100 * time.Millisecond

// pending is one request waiting for its response.
type pending struct {
	ch chan *helperpb.HelperResponse
	// watch is the entry to register when the response starts a watch. The
	// read loop registers it before it reads any later message, so no watch
	// event can arrive before its callback is known.
	watch *watch
}

// watch is one registered watch callback. mu is held while the callback runs,
// so stop does not return while a callback is still in progress and no later
// callback can start after it returns.
type watch struct {
	onChange func(pid int)

	mu     sync.Mutex
	active bool

	// id and abandoned are guarded by Client.mu.
	id        uint64
	abandoned bool
}

// Client is the Broker side of one account helper connection.
type Client struct {
	w         *msgWriter
	wc        io.WriteCloser
	closeOnce sync.Once
	closeErr  error

	mu       sync.Mutex
	nextID   uint64
	pendings map[uint64]*pending
	watches  map[uint64]*watch
	readErr  error
	ended    bool

	ready    chan struct{}
	readyVal *helperpb.Ready
	done     chan struct{}
}

// NewClient starts the read loop over r (the child's stdout) and writes requests to w (its stdin).
func NewClient(r io.Reader, w io.WriteCloser) *Client {
	c := &Client{
		w:        newMsgWriter(w),
		wc:       w,
		pendings: make(map[uint64]*pending),
		watches:  make(map[uint64]*watch),
		ready:    make(chan struct{}),
		done:     make(chan struct{}),
	}
	go c.readLoop(newMsgReader(r))
	return c
}

// readLoop reads messages until the connection ends or the peer violates the
// protocol. A first message that is not Ready, a second Ready, an unknown body
// or a framing error all end it.
func (c *Client) readLoop(r *msgReader) {
	first := true
	for {
		var msg helperpb.HelperMessage
		if err := r.read(&msg); err != nil {
			c.shutdown(err)
			return
		}
		if first {
			first = false
			ready := msg.GetReady()
			if ready == nil {
				c.shutdown(ErrProtocol)
				return
			}
			c.readyVal = ready
			close(c.ready)
			continue
		}
		switch {
		case msg.GetReady() != nil:
			c.shutdown(ErrProtocol)
			return
		case msg.GetResponse() != nil:
			c.dispatchResponse(msg.GetResponse())
		case msg.GetWatchEvent() != nil:
			c.dispatchEvent(msg.GetWatchEvent())
		default:
			c.shutdown(ErrProtocol)
			return
		}
	}
}

// shutdown records why the read loop ended and wakes every waiter. Channels
// are closed rather than sent on so a waiter that already removed its entry
// cannot be confused with one that is still registered.
func (c *Client) shutdown(err error) {
	c.mu.Lock()
	if c.ended {
		c.mu.Unlock()
		return
	}
	c.ended = true
	c.readErr = err
	for id, p := range c.pendings {
		close(p.ch)
		delete(c.pendings, id)
	}
	c.mu.Unlock()
	close(c.done)
}

// dispatchResponse delivers a response to the waiter that owns its id. A
// response for an unknown or already abandoned id is dropped.
func (c *Client) dispatchResponse(resp *helperpb.HelperResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.pendings[resp.GetId()]
	if !ok {
		return
	}
	if ws := resp.GetWatchStarted(); ws != nil && p.watch != nil && !p.watch.abandoned {
		p.watch.mu.Lock()
		p.watch.active = true
		p.watch.mu.Unlock()
		p.watch.id = ws.GetWatchId()
		c.watches[p.watch.id] = p.watch
	}
	select {
	case p.ch <- resp:
	default:
	}
}

// dispatchEvent runs one watch callback in the read loop, which keeps events
// for the same watch in order. The callback runs without Client.mu, so it may
// stop another watch; stopping its own would wait on the lock this call holds.
func (c *Client) dispatchEvent(ev *helperpb.WatchEvent) {
	c.mu.Lock()
	w := c.watches[ev.GetWatchId()]
	c.mu.Unlock()
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.active {
		w.onChange(int(ev.GetPid()))
	}
	w.mu.Unlock()
}

// register reserves the next request id and the channel its response arrives
// on. It fails once the read loop has ended.
func (c *Client) register(w *watch) (uint64, *pending, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ended {
		return 0, nil, false
	}
	c.nextID++
	id := c.nextID
	p := &pending{ch: make(chan *helperpb.HelperResponse, 1), watch: w}
	c.pendings[id] = p
	return id, p, true
}

func (c *Client) remove(id uint64) {
	c.mu.Lock()
	delete(c.pendings, id)
	c.mu.Unlock()
}

// send writes one request, giving up when ctx ends. A write that finished
// just before cancellation is reported as done. A blocked pipe write cannot be
// interrupted directly, so after writeCancelGrace the write end is closed; the
// connection is then unusable, which keeps the wait bounded, and a frame that
// was not written completely must not be followed by another on the stream.
func (c *Client) send(ctx context.Context, req *helperpb.HelperRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- c.w.write(req) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}

	timer := time.NewTimer(writeCancelGrace)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
	}
	_ = c.wc.Close()
	// ctx ended before this point, so the write error only confirms the
	// connection is unusable; the caller learns the cancellation.
	if err := <-done; err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

// goneError wraps a send failure as ErrHelperGone while keeping the context
// error, if that is what ended the write, visible to errors.Is. A caller's
// cancellation is returned unchanged: the helper is not what ended the call,
// so it must not be reported as a helper failure. A deadline does belong to the
// helper, because it bounds how long the helper may take to accept a request.
func goneError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return err
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrHelperGone, err)
	default:
		return fmt.Errorf("%w: %v", ErrHelperGone, err)
	}
}

// deliverSendError classifies a failure before the request was on the wire.
// An oversize frame was never written. A write interrupted by its context may
// have been read in part or in full, so its outcome is unknown. A caller's
// cancellation keeps that uncertain outcome but is not a helper failure; a
// deadline bounds the helper, so it does wrap ErrHelperGone.
func deliverSendError(err error) error {
	switch {
	case errors.Is(err, errOversize):
		return fmt.Errorf("%w: %v", inbox.ErrNotWritten, err)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %w", inbox.ErrUncertain, err)
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w: %w", inbox.ErrUncertain, ErrHelperGone, err)
	default:
		return fmt.Errorf("%w: %w: %v", inbox.ErrNotWritten, ErrHelperGone, err)
	}
}

// WaitReady returns the Ready message; ErrProtocol if the first message is not Ready.
func (c *Client) WaitReady(ctx context.Context) (*helperpb.Ready, error) {
	select {
	case <-c.ready:
		return c.readyVal, nil
	case <-c.done:
		select {
		case <-c.ready:
			return c.readyVal, nil
		default:
		}
		c.mu.Lock()
		err := c.readErr
		c.mu.Unlock()
		if errors.Is(err, ErrProtocol) {
			return nil, ErrProtocol
		}
		return nil, fmt.Errorf("%w: %v", ErrHelperGone, err)
	case <-ctx.Done():
		return nil, deadlineUnavailable(ctx)
	}
}

func (c *Client) Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error) {
	id, p, registered := c.register(nil)
	if !registered {
		return inbox.Session{}, ErrHelperGone
	}
	defer c.remove(id)

	req := &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Lookup{Lookup: &helperpb.LookupRequest{
			ConfigDir:  configDir,
			SocketPath: socketPath,
		}},
	}
	if err := c.send(ctx, req); err != nil {
		return inbox.Session{}, goneError(err)
	}
	select {
	case resp, ok := <-p.ch:
		if !ok {
			return inbox.Session{}, ErrHelperGone
		}
		return clientSessionResult(resp)
	case <-ctx.Done():
		return inbox.Session{}, deadlineUnavailable(ctx)
	}
}

func (c *Client) Reread(ctx context.Context, s inbox.Session) (inbox.Session, error) {
	id, p, registered := c.register(nil)
	if !registered {
		return inbox.Session{}, ErrHelperGone
	}
	defer c.remove(id)

	req := &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Reread{Reread: &helperpb.RereadRequest{
			Session: toProtoSession(s),
		}},
	}
	if err := c.send(ctx, req); err != nil {
		return inbox.Session{}, goneError(err)
	}
	select {
	case resp, ok := <-p.ch:
		if !ok {
			return inbox.Session{}, ErrHelperGone
		}
		return clientSessionResult(resp)
	case <-ctx.Done():
		return inbox.Session{}, deadlineUnavailable(ctx)
	}
}

func (c *Client) Watch(ctx context.Context, configDir string, onChange func(pid int)) (stop func(), err error) {
	w := &watch{onChange: onChange}
	id, p, registered := c.register(w)
	if !registered {
		return nil, ErrHelperGone
	}
	defer c.remove(id)

	req := &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Watch{Watch: &helperpb.WatchRequest{ConfigDir: configDir}},
	}
	if err := c.send(ctx, req); err != nil {
		return nil, goneError(err)
	}
	select {
	case resp, ok := <-p.ch:
		if !ok {
			return nil, ErrHelperGone
		}
		if e := resp.GetError(); e != nil {
			return nil, clientError(e)
		}
		ws := resp.GetWatchStarted()
		if ws == nil {
			return nil, ErrProtocol
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				w.mu.Lock()
				w.active = false
				w.mu.Unlock()
				c.mu.Lock()
				delete(c.watches, ws.GetWatchId())
				c.mu.Unlock()
				c.unwatch(ws.GetWatchId())
			})
		}, nil
	case <-ctx.Done():
		// The response may still be in flight. Mark the entry abandoned so
		// the read loop does not publish a callback that can never be
		// stopped.
		c.mu.Lock()
		w.abandoned = true
		w.mu.Lock()
		w.active = false
		w.mu.Unlock()
		if w.id != 0 && c.watches[w.id] == w {
			delete(c.watches, w.id)
		}
		c.mu.Unlock()
		return nil, deadlineUnavailable(ctx)
	}
}

// unwatch asks the helper to stop one watch and waits for its reply. It runs
// on a bounded background context so that stopping a watch still works while
// the caller's context is already done, and a stopped helper cannot hold the
// caller past unwatchTimeout; a lost connection ends the wait sooner.
func (c *Client) unwatch(watchID uint64) {
	c.mu.Lock()
	if c.ended {
		c.mu.Unlock()
		return
	}
	c.nextID++
	id := c.nextID
	p := &pending{ch: make(chan *helperpb.HelperResponse, 1)}
	c.pendings[id] = p
	c.mu.Unlock()
	defer c.remove(id)

	req := &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Unwatch{Unwatch: &helperpb.UnwatchRequest{WatchId: watchID}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), unwatchTimeout)
	defer cancel()
	if err := c.send(ctx, req); err != nil {
		return
	}
	select {
	case <-p.ch:
	case <-ctx.Done():
	}
}

func (c *Client) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	c.mu.Lock()
	ended := c.ended
	c.mu.Unlock()
	if ended {
		return fmt.Errorf("%w: %w", inbox.ErrNotWritten, ErrHelperGone)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	id, p, registered := c.register(nil)
	if !registered {
		return fmt.Errorf("%w: %w", inbox.ErrNotWritten, ErrHelperGone)
	}
	defer c.remove(id)

	req := &helperpb.HelperRequest{
		Id: id,
		Op: &helperpb.HelperRequest_Deliver{Deliver: &helperpb.DeliverRequest{
			Session:   toProtoSession(s),
			Signature: sig,
			Text:      text,
			MessageId: msgID,
			Timeout:   durationpb.New(timeout),
		}},
	}
	if err := c.send(ctx, req); err != nil {
		return deliverSendError(err)
	}
	select {
	case resp, ok := <-p.ch:
		if !ok {
			return fmt.Errorf("%w: %w", inbox.ErrUncertain, ErrHelperGone)
		}
		return clientDeliverResult(resp)
	case <-ctx.Done():
		return c.cancelDeliver(id, p)
	}
}

// cancelDeliver asks the helper to cancel a request whose caller gave up and
// waits briefly for the helper to report whether the write happened. The
// answer decides between a retryable and an unknown outcome.
func (c *Client) cancelDeliver(id uint64, p *pending) error {
	c.mu.Lock()
	var cancelID uint64
	if !c.ended {
		c.nextID++
		cancelID = c.nextID
	}
	c.mu.Unlock()

	if cancelID != 0 {
		req := &helperpb.HelperRequest{
			Id: cancelID,
			Op: &helperpb.HelperRequest_Cancel{Cancel: &helperpb.CancelRequest{RequestId: id}},
		}
		sendCtx, cancel := context.WithTimeout(context.Background(), cancelWait)
		_ = c.send(sendCtx, req)
		cancel()
	}

	timer := time.NewTimer(cancelWait)
	defer timer.Stop()
	select {
	case resp, ok := <-p.ch:
		if !ok {
			return fmt.Errorf("%w: %w", inbox.ErrUncertain, ErrHelperGone)
		}
		return clientDeliverResult(resp)
	case <-timer.C:
		return inbox.ErrUncertain
	}
}

// Done is closed once the read loop ends.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Close closes w; it is idempotent.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.wc.Close()
	})
	return c.closeErr
}

func clientSessionResult(resp *helperpb.HelperResponse) (inbox.Session, error) {
	if e := resp.GetError(); e != nil {
		return inbox.Session{}, clientError(e)
	}
	if s := resp.GetSession(); s != nil {
		return fromProtoSession(s), nil
	}
	return inbox.Session{}, ErrProtocol
}

func clientDeliverResult(resp *helperpb.HelperResponse) error {
	if e := resp.GetError(); e != nil {
		return clientError(e)
	}
	if resp.GetDone() != nil {
		return nil
	}
	return ErrProtocol
}

// clientError maps a helper error back to the sentinel the broker compares
// with. The five definite outcomes keep their identity; everything else is a
// plain error whose text comes from the helper.
func clientError(e *helperpb.HelperError) error {
	switch e.GetCode() {
	case helperpb.HelperErrorCode_HELPER_ERROR_CODE_NO_RECORD:
		return inbox.ErrNoRecord
	case helperpb.HelperErrorCode_HELPER_ERROR_CODE_PARTIAL:
		return inbox.ErrPartial
	case helperpb.HelperErrorCode_HELPER_ERROR_CODE_BAD_SIGNATURE:
		return inbox.ErrBadSignature
	case helperpb.HelperErrorCode_HELPER_ERROR_CODE_NOT_WRITTEN:
		return inbox.ErrNotWritten
	case helperpb.HelperErrorCode_HELPER_ERROR_CODE_UNCERTAIN:
		return inbox.ErrUncertain
	case helperpb.HelperErrorCode_HELPER_ERROR_CODE_CANCELED:
		return context.Canceled
	default:
		msg := e.GetMessage()
		if msg == "" {
			msg = e.GetCode().String()
		}
		return errors.New(msg)
	}
}
