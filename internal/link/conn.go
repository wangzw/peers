package link

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

type frameStream interface {
	Send(*linkpb.Frame) error
	Recv() (*linkpb.Frame, error)
}

type peerIdentity struct {
	fp     Fingerprint
	pub    []byte
	remote string
}

// connInQueueSize bounds the buffer between the reader goroutine and the
// consumers; a full queue stops the reader, letting the transport apply flow
// control.
const connInQueueSize = 16

const (
	reqQueued int32 = iota
	reqWritten
	reqCanceled
)

// conn is one transport connection: a single gRPC stream carrying link frames.
type conn struct {
	id     peerIdentity
	role   pairing.Role // set by the handshake before install; read-only afterwards
	stream frameStream
	abort  func() // closes the transport; must not block; safe to call repeatedly

	in       chan *linkpb.Frame
	unreadMu sync.Mutex
	// unreadStack holds frames pushed back by pairing; it is a stack so the
	// most recent Unread is received first.
	unreadStack []*linkpb.Frame

	// readAhead holds one read permit until the handshake finishes, so at most
	// one frame waits in in before authentication; startWriter opens the gate
	// for good.
	readAhead chan struct{}
	authed    chan struct{}
	authOnce  sync.Once

	out    chan outItem
	sendMu sync.Mutex // serialises sendDirect before the writer starts
	// outMu guards latest, the queued latest slot of each key. The writer
	// removes a slot while holding it, so a later replacement is queued anew.
	outMu    sync.Mutex
	latest   map[string]*latestSlot
	writerOn atomic.Bool

	closed    chan struct{}
	closeOnce sync.Once
	failure   atomic.Uint32
	// lingering is set by rejectAndLinger, which then owns the close after the
	// linger; a failed handshake must not close the connection out from under
	// a Reject still in the transport's write buffer.
	lingering atomic.Bool

	stateDone chan struct{}
	stateOnce sync.Once

	nextID  atomic.Uint64
	pendMu  sync.Mutex
	pending map[uint64]*pendingReq

	ctx    context.Context // canceled by closeAsync; request handlers derive from it
	cancel context.CancelFunc
}

type outItem struct {
	f      *linkpb.Frame
	req    *pendingReq
	latest *latestSlot
}

// latestSlot holds the frame of one queued latest item; mu keeps the writer's
// read of f from racing with a replacement.
type latestSlot struct {
	mu  sync.Mutex
	key string
	f   *linkpb.Frame
}

// pendingReq is one request waiting for its reply: state is queued until the
// writer takes it, written once the writer calls the transport, and canceled
// when the caller gave up while it was still queued.
type pendingReq struct {
	id    uint64
	state atomic.Int32
	reply chan *linkpb.Frame // cap 1
}

// newConn builds a connection around an already open stream; it starts no
// goroutine, so the caller runs start and startWriter.
func newConn(stream frameStream, abort func(), id peerIdentity, queueSize int) *conn {
	if abort == nil {
		abort = func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{
		id:        id,
		stream:    stream,
		abort:     abort,
		in:        make(chan *linkpb.Frame, connInQueueSize),
		out:       make(chan outItem, queueSize),
		latest:    map[string]*latestSlot{},
		closed:    make(chan struct{}),
		stateDone: make(chan struct{}),
		pending:   make(map[uint64]*pendingReq),
		ctx:       ctx,
		cancel:    cancel,
		readAhead: make(chan struct{}, 1),
		authed:    make(chan struct{}),
	}
	c.readAhead <- struct{}{}
	return c
}

func (c *conn) start() {
	go c.readLoop()
}

func (c *conn) readLoop() {
	for {
		if !c.acquireReadPermit() {
			return
		}
		f, err := c.stream.Recv()
		if err != nil {
			c.closeAsync(FailConnectionLost)
			return
		}
		select {
		case c.in <- f:
		case <-c.closed:
			return
		}
	}
}

// acquireReadPermit waits until the connection may read another frame; the
// handshake phase has exactly one permit, so the reader cannot run ahead of
// the frame being processed, and authentication opens the gate permanently.
func (c *conn) acquireReadPermit() bool {
	select {
	case <-c.authed:
		return true
	default:
	}
	select {
	case <-c.readAhead:
		return true
	case <-c.authed:
		return true
	case <-c.closed:
		return false
	}
}

func (c *conn) releaseReadPermit() {
	select {
	case <-c.authed:
		return
	default:
	}
	select {
	case c.readAhead <- struct{}{}:
	default:
	}
}

func (c *conn) allowReadAhead() {
	c.authOnce.Do(func() { close(c.authed) })
}

// recv returns the next frame, preferring frames pushed back with unread. It
// returns ctx.Err() when ctx is done first, and an error wrapping
// ErrNotEstablished when the connection is closed and no frame is buffered.
func (c *conn) recv(ctx context.Context) (*linkpb.Frame, error) {
	c.unreadMu.Lock()
	if n := len(c.unreadStack); n > 0 {
		f := c.unreadStack[n-1]
		c.unreadStack = c.unreadStack[:n-1]
		c.unreadMu.Unlock()
		return f, nil
	}
	c.unreadMu.Unlock()

	if f, ok := c.tryRecv(); ok {
		return f, nil
	}
	select {
	case f := <-c.in:
		c.releaseReadPermit()
		return f, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		// A frame may have been buffered before the reader noticed the
		// transport error; deliver it before reporting the close.
		if f, ok := c.tryRecv(); ok {
			return f, nil
		}
		return nil, fmt.Errorf("%w: connection closed", ErrNotEstablished)
	}
}

func (c *conn) tryRecv() (*linkpb.Frame, bool) {
	select {
	case f := <-c.in:
		c.releaseReadPermit()
		return f, true
	default:
		return nil, false
	}
}

// unread pushes a frame back to the head of the receive queue; safe for
// concurrent use.
func (c *conn) unread(f *linkpb.Frame) {
	c.unreadMu.Lock()
	c.unreadStack = append(c.unreadStack, f)
	c.unreadMu.Unlock()
}

// sendDirect writes a frame before the writer goroutine starts; the handshake
// uses it, and once the connection runs all writes go through the queue.
func (c *conn) sendDirect(ctx context.Context, f *linkpb.Frame) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.writerOn.Load() {
		return errors.New("sendDirect called after the writer started")
	}
	if err := checkFrameSize(f); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- c.stream.Send(f) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// The abandoned Send goroutine ends once abort closes the transport.
		return ctx.Err()
	case <-c.closed:
		select {
		case err := <-done:
			return err
		default:
		}
		return fmt.Errorf("%w: connection closed", ErrNotEstablished)
	}
}

// startWriter starts the writer goroutine; after this sendDirect refuses to
// send and all writes go through enqueue. It also opens the read gate.
func (c *conn) startWriter() {
	c.sendMu.Lock()
	c.writerOn.Store(true)
	c.sendMu.Unlock()
	c.allowReadAhead()
	go c.writeLoop()
}

// writeLoop drains the send queue in order; a request canceled while queued is
// skipped and a transport error closes the connection.
func (c *conn) writeLoop() {
	for {
		select {
		case item := <-c.out:
			if item.req != nil && !item.req.state.CompareAndSwap(reqQueued, reqWritten) {
				continue
			}
			f := item.f
			if item.latest != nil {
				// Remove the slot before reading it: a latest send that arrives
				// after this enqueues a new frame instead of replacing one that
				// is already going out.
				c.outMu.Lock()
				if c.latest[item.latest.key] == item.latest {
					delete(c.latest, item.latest.key)
				}
				c.outMu.Unlock()
				item.latest.mu.Lock()
				f = item.latest.f
				item.latest.mu.Unlock()
			}
			if err := c.stream.Send(f); err != nil {
				c.closeAsync(FailConnectionLost)
				return
			}
		case <-c.closed:
			return
		}
	}
}

// enqueue queues a frame without blocking; a full queue closes the connection
// so a peer cannot accumulate an unbounded backlog.
func (c *conn) enqueue(f *linkpb.Frame) error {
	return c.push(outItem{f: f})
}

// enqueueLatest queues f under key; when a frame with the same key is queued
// and the writer has not taken it, that frame is replaced in place without a
// new queue position and replaced reports true. A replacement after the writer
// took the frame is queued as a new item.
func (c *conn) enqueueLatest(key string, f *linkpb.Frame) (replaced bool, err error) {
	c.outMu.Lock()
	defer c.outMu.Unlock()
	select {
	case <-c.closed:
		return false, c.closedErr()
	default:
	}
	if slot, ok := c.latest[key]; ok {
		slot.mu.Lock()
		slot.f = f
		slot.mu.Unlock()
		return true, nil
	}
	slot := &latestSlot{key: key, f: f}
	c.latest[key] = slot
	if err := c.push(outItem{latest: slot}); err != nil {
		delete(c.latest, key)
		return false, err
	}
	return false, nil
}

// enqueueRequest assigns the next request id, registers the waiter and queues
// the frame. The size check runs after the id is filled, so it covers the frame
// as it will be written; a failed check or enqueue removes the waiter again.
func (c *conn) enqueueRequest(f *linkpb.Frame) (*pendingReq, error) {
	id := c.nextID.Add(1)
	p := &pendingReq{id: id, reply: make(chan *linkpb.Frame, 1)}
	f.RequestId = id
	f.IsReply = false
	if frameExceedsLimit(f) {
		return nil, ErrFrameTooLarge
	}

	c.pendMu.Lock()
	c.pending[id] = p
	c.pendMu.Unlock()

	if err := c.push(outItem{f: f, req: p}); err != nil {
		c.pendMu.Lock()
		delete(c.pending, id)
		c.pendMu.Unlock()
		return nil, err
	}
	return p, nil
}

// push applies the enqueue rules to any queued item; it never blocks and never
// waits for the network.
func (c *conn) push(it outItem) error {
	select {
	case <-c.closed:
		return c.closedErr()
	default:
	}
	select {
	case c.out <- it:
		return nil
	case <-c.closed:
		return c.closedErr()
	default:
		select {
		case <-c.closed:
			return c.closedErr()
		default:
		}
		c.closeAsync(FailQueueFull)
		return ErrQueueFull
	}
}

// closedErr describes a queue operation on a closed connection; one closed
// because its queue filled keeps reporting ErrQueueFull so callers see the
// cause rather than a generic close.
func (c *conn) closedErr() error {
	if c.reason() == FailQueueFull {
		return ErrQueueFull
	}
	return fmt.Errorf("%w: connection closed", ErrNotEstablished)
}

func (c *conn) deliverReply(f *linkpb.Frame) bool {
	id := f.GetRequestId()
	c.pendMu.Lock()
	p, ok := c.pending[id]
	if ok {
		delete(c.pending, id)
	}
	c.pendMu.Unlock()
	if !ok {
		return false
	}
	select {
	case p.reply <- f:
	default:
	}
	return true
}

// cancelRequest gives up on a request and reports whether the writer had
// already taken the frame; false means the frame will not be written.
func (c *conn) cancelRequest(p *pendingReq) (sent bool) {
	c.pendMu.Lock()
	delete(c.pending, p.id)
	c.pendMu.Unlock()
	return !p.state.CompareAndSwap(reqQueued, reqCanceled)
}

// closeAsync closes the connection once, recording the reason, cancelling the
// connection context, signalling done and aborting the transport. It blocks on
// nothing and may be called while holding any lock.
func (c *conn) closeAsync(reason FailReason) {
	c.closeOnce.Do(func() {
		c.failure.Store(uint32(reason))
		c.cancel()
		close(c.closed)
		c.abort()
	})
}

func (c *conn) done() <-chan struct{} {
	return c.closed
}

func (c *conn) reason() FailReason {
	return FailReason(c.failure.Load())
}

func (c *conn) markStateDone() {
	c.stateOnce.Do(func() { close(c.stateDone) })
}
