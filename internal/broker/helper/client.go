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
	// ErrUnavailable reports that the account helper could not be started or that communication with it failed, as opposed to a session record that cannot be read.
	ErrUnavailable = errors.New("helper: account helper unavailable")
	// ErrHelperGone reports that the connection to the account helper is lost; it wraps ErrUnavailable.
	ErrHelperGone = fmt.Errorf("%w: the account helper connection was lost", ErrUnavailable)
	// ErrProtocol reports a violation of the helper pipe protocol; it wraps ErrUnavailable.
	ErrProtocol = fmt.Errorf("%w: helper protocol violation", ErrUnavailable)
)

// deadlineUnavailable turns a deadline into a helper failure, keeping the context error reachable, and returns a cancellation unchanged.
func deadlineUnavailable(ctx context.Context) error {
	err := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

const cancelWait = time.Second

const unwatchTimeout = time.Second

// writeCancelGrace is how long a cancelled write gets to report that it completed anyway before the write end is closed.
const writeCancelGrace = 100 * time.Millisecond

type pending struct {
	ch chan *helperpb.HelperResponse
	// watch is the entry to register when the response starts a watch; the read loop registers it before reading any later message.
	watch *watch
}

// watch is one registered watch callback; mu is held while it runs, so stop does not return while a callback is in progress.
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

// dispatchEvent runs one watch callback in the read loop so events for a watch
// stay in order. The callback runs without Client.mu, so it may stop another
// watch, but it must not stop its own: stop waits on the w.mu this call holds,
// which would deadlock.
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

// send writes one request and gives up when ctx ends; a blocked pipe write cannot be interrupted, so after writeCancelGrace the write end is closed and the connection becomes unusable.
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
	// ctx ended before this point, so a write error only confirms the connection is unusable.
	if err := <-done; err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

// goneError wraps a send failure as ErrHelperGone, keeping a context error reachable, and returns a caller's cancellation unchanged.
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

// deliverSendError classifies a failure before the request reached the pipe: an oversize frame was not written, and a write interrupted by its context is uncertain.
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

// WaitReady returns the Ready message. It returns ErrProtocol if the first message is not Ready and ErrHelperGone if the connection ends first; a deadline is reported as ErrUnavailable.
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

// Lookup returns the session whose record carries socketPath.
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

// Reread returns the current record of s. It reports inbox.ErrNoRecord or
// inbox.ErrPartial for the record and ErrHelperGone when the helper is gone;
// the caller's own cancellation or deadline is returned as it is or wrapped in
// ErrUnavailable.
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

// Watch reports changes under configDir through onChange and returns a function
// that stops it. It returns the helper's error for a failed watch, ErrProtocol
// for a malformed reply, ErrHelperGone when the helper is gone, ErrUnavailable
// when ctx's deadline expires, and context.Canceled when ctx is canceled.
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
		// The response may still be in flight, so mark the entry abandoned to keep an unstoppable callback from being published.
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

// unwatch asks the helper to stop one watch on a bounded background context, so it still works after the caller's context ends and cannot hold the caller past unwatchTimeout.
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

// Deliver writes text to s's inbox socket under sig and msgID, bounded by
// timeout or an earlier caller deadline. It reports inbox.ErrNotWritten,
// inbox.ErrUncertain or inbox.ErrBadSignature, wrapping ErrHelperGone when the
// helper is gone.
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

// clientError maps a helper error back: CANCELED becomes context.Canceled, the
// other known codes become the inbox sentinel they name, and an unknown code
// becomes a plain error carrying the helper's text.
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
