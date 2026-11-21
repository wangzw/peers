package link

import (
	"context"
	"sync"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

// Inbound limits of one connection. A state-frame queue larger than the frame
// or byte limit closes the connection instead of growing without bound, and at
// most maxConcurrentRequests request handlers of one peer run at once, shared
// by every connection of that peer.
const (
	defaultMaxStateFrames = 4096
	defaultMaxStateBytes  = 64 << 20
	maxConcurrentRequests = 64

	// evInboundLimit is the runtime log event recorded when a connection
	// exceeds an inbound limit.
	evInboundLimit = "link_inbound_limit"
	// stateLimitFrames and stateLimitBytes name the limit that closed the
	// connection in the evInboundLimit log entry.
	stateLimitFrames = "state_frames"
	stateLimitBytes  = "state_bytes"
)

// runConn runs one installed connection until it ends: it starts the writer and
// the two consumers, then records the teardown of the link. It is called by the
// goroutine that installed the connection and returns only after the connection
// and its state handler finished.
func (l *Link) runConn(c *conn, delivered <-chan struct{}) {
	c.startWriter()

	q := newStateQueue(l.maxStateFrames, l.maxStateBytes)
	slots := l.requestSlots(c.id.fp)
	var consumers sync.WaitGroup
	consumers.Add(2)
	go func() {
		defer consumers.Done()
		l.routeFrames(c, q, slots)
	}()
	go func() {
		defer consumers.Done()
		l.processStateFrames(c, delivered, q)
	}()

	<-c.done()
	consumers.Wait()
	c.markStateDone()
	l.teardown(c)
}

// requestSlots returns the request slot semaphore of a peer, creating it on
// first use. All connections of one peer share it, so a reconnect cannot stack
// another maxConcurrentRequests handlers on top of the ones still running: a
// replaced connection's handlers outlive its consumers, and a per-connection
// semaphore would hand the peer a fresh set on every install.
//
// The map holds one small channel per paired peer, not per peer ever installed:
// unpairing a peer forgets its entry once no handler of that peer holds a slot,
// and the last such handler forgets it when it returns.
func (l *Link) requestSlots(peer Fingerprint) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.slots == nil {
		l.slots = make(map[Fingerprint]chan struct{})
	}
	s := l.slots[peer]
	if s == nil {
		s = make(chan struct{}, maxConcurrentRequests)
		l.slots[peer] = s
	}
	return s
}

// forgetSlotsLocked removes the slot semaphore of an unpaired peer when no
// handler holds a slot. An entry whose handlers still run must stay: a
// replacement connection would otherwise get a fresh set of slots on top of
// them. The last of those handlers removes it when it returns. The caller must
// hold l.mu.
func (l *Link) forgetSlotsLocked(peer Fingerprint) {
	if slots := l.slots[peer]; slots != nil && len(slots) == 0 {
		delete(l.slots, peer)
	}
}

// releaseSlot returns one request slot of peer and, when peer was unpaired and
// no handler holds a slot any more, forgets the semaphore. It must not be
// called with l.mu held.
func (l *Link) releaseSlot(peer Fingerprint, slots chan struct{}) {
	<-slots
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.records[peer] == nil && len(slots) == 0 && l.slots[peer] == slots {
		delete(l.slots, peer)
	}
}

// routeFrames reads frames from the connection and sends each one to its
// destination: replies go to the waiting request, requests wait for a handler
// slot, and state frames enter the connection's bounded queue. It stops reading
// when the connection closes or either inbound limit is exceeded.
func (l *Link) routeFrames(c *conn, q *stateQueue, slots chan struct{}) {
	defer q.close()
	for {
		f, err := c.recv(context.Background())
		if err != nil {
			return
		}
		switch classify(f) {
		case classReply:
			if !c.deliverReply(f) {
				l.logger.Debug("link: dropping reply with an unknown request id",
					"peer", c.id.fp.String(), "request_id", f.GetRequestId())
			}
		case classRequest:
			if !l.dispatchRequest(c, f, slots) {
				return
			}
		case classState:
			// A peer that overruns a limit loses only its own connection; the
			// queue never blocks the router, so replies keep flowing until the
			// close.
			if limit := q.push(f); limit != "" {
				l.logger.Warn(evInboundLimit, "peer", c.id.fp.String(), "limit", limit)
				c.closeAsync(FailInboundLimit)
				return
			}
		default:
			l.logger.Warn("link: dropping an invalid frame",
				"peer", c.id.fp.String(), "kind", f.GetKind(),
				"request_id", f.GetRequestId(), "is_reply", f.GetIsReply())
		}
	}
}

// dispatchRequest waits for a free request slot, then starts the handler of one
// request frame and joins it to the link WaitGroup, so Close waits until every
// request handler returned. It reports whether the router may keep reading:
// false means the connection closed while a slot was awaited.
//
// At most maxConcurrentRequests handlers of one peer run, across all its
// connections: a replacement connection shares the slots its predecessor's
// still-running handlers hold. When every slot is taken the router waits here
// instead of reading, and the transport's flow control pushes back on the peer.
// Waiting ends as soon as the connection closes, so Close never hangs on it.
//
// The closed check and the Add share Link.mu with Close, which keeps Add from
// racing with the Wait in Close. A request that arrives after Close is dropped.
func (l *Link) dispatchRequest(c *conn, f *linkpb.Frame, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
	case <-c.done():
		return false
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		l.releaseSlot(c.id.fp, slots)
		return false
	}
	l.wg.Add(1)
	l.mu.Unlock()

	go func() {
		defer l.wg.Done()
		defer l.releaseSlot(c.id.fp, slots)
		l.handleRequest(c, f)
	}()
	return true
}

// handleRequest answers one request frame. An unregistered kind gets an
// Unsupported reply; the payload is never interpreted by link itself.
func (l *Link) handleRequest(c *conn, f *linkpb.Frame) {
	l.regMu.RLock()
	h := l.requestHandlers[f.GetKind()]
	l.regMu.RUnlock()

	var reply *linkpb.Frame
	if h == nil {
		reply = unsupportedReply(f)
	} else {
		r := h(c.ctx, c.id.fp, Frame{Kind: f.GetKind(), Payload: f.GetPayload()})
		reply = &linkpb.Frame{
			Kind:      r.Kind,
			RequestId: f.GetRequestId(),
			IsReply:   true,
			Payload:   r.Payload,
		}
	}
	if frameExceedsLimit(reply) {
		l.logger.Warn("link: dropping a reply",
			"peer", c.id.fp.String(), "kind", reply.GetKind(), "reason", "frame_too_large")
		return
	}
	if err := c.enqueue(reply); err != nil {
		l.logger.Debug("link: dropping a reply",
			"peer", c.id.fp.String(), "kind", reply.GetKind(), "err", err)
	}
}

// processStateFrames handles state frames one at a time, in arrival order. It
// waits until every onUp callback of the connection returned, so upper layers
// never see state from a link they do not know is up. State frames still queued
// when the connection ends are discarded.
func (l *Link) processStateFrames(c *conn, delivered <-chan struct{}, q *stateQueue) {
	select {
	case <-delivered:
	case <-c.done():
		return
	}
	for {
		select {
		case <-c.done():
			return
		default:
		}
		f, ok := q.pop()
		if !ok {
			return
		}
		l.handleStateFrame(c, f)
		q.done(f)
	}
}

// handleStateFrame dispatches one state frame to the handler registered for its
// kind. Handshake frames that arrive late and kinds with no handler are dropped
// without changing the link.
func (l *Link) handleStateFrame(c *conn, f *linkpb.Frame) {
	switch f.GetKind() {
	case linkpb.KindPeerInfo:
		l.handlePeerInfo(c, f)
	case linkpb.KindHello, linkpb.KindReject:
		l.logger.Warn("link: dropping an unexpected handshake frame",
			"peer", c.id.fp.String(), "kind", f.GetKind())
	default:
		l.regMu.RLock()
		h := l.stateHandlers[f.GetKind()]
		l.regMu.RUnlock()
		if h == nil {
			// Unknown state frames are dropped; the link stays up.
			return
		}
		h(c.id.fp, Frame{Kind: f.GetKind(), Payload: f.GetPayload()})
	}
}

// handlePeerInfo passes the peer's self-reported alias to the peer table and,
// when this side placed the call, the advertised addresses as dial candidates.
// An accept-side link ignores the address list.
func (l *Link) handlePeerInfo(c *conn, f *linkpb.Frame) {
	info, err := decodePeerInfo(f)
	if err != nil {
		l.logger.Warn("link: dropping a malformed PeerInfo",
			"peer", c.id.fp.String(), "err", err)
		return
	}
	l.peers.ObserveAlias(c.id.fp, info.GetAlias())
	if c.role == pairing.RoleDialer {
		l.peers.ObserveAcceptorAddresses(c.id.fp, addrsFromPB(info.GetAdvertisedAddresses()))
	}
}

// stateQueue is the bounded state-frame queue of one connection. It is bounded
// by both the number of frames and their total payload, counting the frame the
// consumer is currently handling: a peer that exceeds either limit has its
// connection closed, which is the only way to stop one authenticated peer from
// exhausting this host's memory. Closing only this peer's connection keeps a
// state handler that waits for a reply on the same link from deadlocking,
// because the router never blocks on a state frame.
type stateQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	frames   []*linkpb.Frame
	inflight int // frames popped and not yet released
	bytes    int // total payload length of frames and inflight frames
	// maxFrames bounds frames plus inflight; maxBytes bounds bytes.
	maxFrames int
	maxBytes  int
	closed    bool
}

func newStateQueue(maxFrames, maxBytes int) *stateQueue {
	q := &stateQueue{maxFrames: maxFrames, maxBytes: maxBytes}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push appends a frame unless it would exceed the frame or byte limit. It
// returns the name of the limit that was exceeded; an empty string means the
// frame was queued. It never blocks and drops the frame after close.
func (q *stateQueue) push(f *linkpb.Frame) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ""
	}
	if len(q.frames)+q.inflight+1 > q.maxFrames {
		return stateLimitFrames
	}
	size := len(f.GetPayload())
	if q.bytes+size > q.maxBytes {
		return stateLimitBytes
	}
	q.frames = append(q.frames, f)
	q.bytes += size
	q.cond.Signal()
	return ""
}

// close discards queued frames and wakes the consumer.
func (q *stateQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.frames = nil
	q.inflight = 0
	q.bytes = 0
	q.cond.Broadcast()
	q.mu.Unlock()
}

// pop returns the oldest frame. It returns false once the queue is closed. The
// frame stays under the limit until done is called.
func (q *stateQueue) pop() (*linkpb.Frame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.frames) == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return nil, false
	}
	f := q.frames[0]
	q.frames[0] = nil
	q.frames = q.frames[1:]
	q.inflight++
	return f, true
}

// done releases a frame returned by pop after the consumer finished with it.
// It is safe to call after close.
func (q *stateQueue) done(f *linkpb.Frame) {
	q.mu.Lock()
	if q.inflight > 0 {
		q.inflight--
	}
	if size := len(f.GetPayload()); q.bytes >= size {
		q.bytes -= size
	}
	q.mu.Unlock()
}
