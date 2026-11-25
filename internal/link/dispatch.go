package link

import (
	"context"
	"sync"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

const (
	defaultMaxStateFrames = 4096
	defaultMaxStateBytes  = 64 << 20
	maxConcurrentRequests = 64

	evInboundLimit   = "link_inbound_limit"
	stateLimitFrames = "state_frames"
	stateLimitBytes  = "state_bytes"
)

// runConn runs one installed connection until it ends and records the
// teardown; it returns only after the connection and its state handler
// finished.
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
// another maxConcurrentRequests handlers on top of the ones still running.
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
// handler holds a slot. An entry whose handlers still run must stay, because a
// replacement connection would otherwise get a fresh set on top of them; the
// last handler removes it. The caller must hold l.mu.
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

// routeFrames sends each frame to its destination: replies to the waiting
// request, requests to a handler slot, and state frames into the connection's
// bounded queue.
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

// dispatchRequest waits for a free request slot, then starts the handler and
// joins it to the link WaitGroup, so Close waits until every handler returned.
// At most maxConcurrentRequests handlers of one peer run across all its
// connections; waiting ends as soon as the connection closes. The closed check
// and the Add share Link.mu with Close, keeping Add from racing with its Wait.
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

// handleRequest answers one request frame; an unregistered kind gets an
// Unsupported reply, and link never interprets the payload itself.
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

// processStateFrames handles state frames one at a time in arrival order,
// after every onUp callback of the connection returned; frames still queued
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

// handleStateFrame dispatches a state frame to its handler; late handshake
// frames and kinds with no handler are dropped without changing the link.
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
			return
		}
		h(c.id.fp, Frame{Kind: f.GetKind(), Payload: f.GetPayload()})
	}
}

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

// stateQueue bounds the state frames of one connection by count and payload,
// counting the frame being handled. Exceeding either limit closes only this
// connection; the router never blocks on a state frame, so a state handler that
// waits for a reply on the same link cannot deadlock.
type stateQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	frames    []*linkpb.Frame
	inflight  int
	bytes     int
	maxFrames int
	maxBytes  int
	closed    bool
}

func newStateQueue(maxFrames, maxBytes int) *stateQueue {
	q := &stateQueue{maxFrames: maxFrames, maxBytes: maxBytes}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push appends a frame unless it would exceed the frame or byte limit, and
// returns the name of the exceeded limit; an empty string means queued. It
// never blocks and drops the frame after close.
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

func (q *stateQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.frames = nil
	q.inflight = 0
	q.bytes = 0
	q.cond.Broadcast()
	q.mu.Unlock()
}

// pop returns the oldest frame, or false once the queue is closed; the frame
// stays under the limit until done is called.
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
