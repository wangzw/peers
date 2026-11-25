package broker

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/link"
)

type linkPair [2]peerID

func pairKey(a, b peerID) linkPair {
	if lessPeer(a, b) {
		return linkPair{a, b}
	}
	return linkPair{b, a}
}

func lessPeer(a, b peerID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

type fakeNet struct {
	mu sync.Mutex

	ends           map[peerID]*fakeEnd
	up             map[linkPair]bool
	down           map[linkPair]chan struct{}
	reqs           map[linkPair][]link.Frame
	kickCounts     map[linkPair]int
	kickHooks      map[linkPair][]func()
	drops          map[linkPair]int
	replyHook      func()
	handlerCancels map[int]context.CancelFunc
	nextHandler    int
	closed         bool
}

func newFakeNet(t *testing.T) *fakeNet {
	t.Helper()
	n := &fakeNet{
		ends:           make(map[peerID]*fakeEnd),
		up:             make(map[linkPair]bool),
		down:           make(map[linkPair]chan struct{}),
		reqs:           make(map[linkPair][]link.Frame),
		kickCounts:     make(map[linkPair]int),
		kickHooks:      make(map[linkPair][]func()),
		drops:          make(map[linkPair]int),
		handlerCancels: make(map[int]context.CancelFunc),
	}
	t.Cleanup(n.stop)
	return n
}

func (n *fakeNet) stop() {
	n.mu.Lock()
	n.closed = true
	cancels := make([]context.CancelFunc, 0, len(n.handlerCancels))
	for _, cancel := range n.handlerCancels {
		cancels = append(cancels, cancel)
	}
	n.handlerCancels = make(map[int]context.CancelFunc)
	n.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (n *fakeNet) watchHandler(cancel context.CancelFunc) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		cancel()
		return 0
	}
	n.nextHandler++
	n.handlerCancels[n.nextHandler] = cancel
	return n.nextHandler
}

func (n *fakeNet) unwatchHandler(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlerCancels, id)
}

func (n *fakeNet) end(self peerID) *fakeEnd {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.endLocked(self)
}

func (n *fakeNet) endLocked(self peerID) *fakeEnd {
	if e := n.ends[self]; e != nil {
		return e
	}
	e := &fakeEnd{
		net:      n,
		self:     self,
		handlers: make(map[link.FrameKind]func(context.Context, peerID, link.Frame) link.Frame),
	}
	n.ends[self] = e
	return e
}

// setUp marks the link between a and b up or down and fires the matching
// callbacks on both ends; callbacks fire only on a transition, in order and
// outside the network lock.
func (n *fakeNet) setUp(a, b peerID, up bool) {
	n.mu.Lock()

	key := pairKey(a, b)
	wasUp := n.up[key]
	changed := false
	var closing chan struct{}
	switch {
	case up && !wasUp:
		changed = true
		n.up[key] = true
		n.down[key] = make(chan struct{})
	case !up && wasUp:
		changed = true
		delete(n.up, key)
		closing = n.down[key]
		delete(n.down, key)
	}

	endA := n.endLocked(a)
	endB := n.endLocked(b)
	var upsA, upsB []func(peerID, []byte)
	var downsA, downsB []func(peerID)
	if changed {
		upsA, downsA = endA.callbacksLocked()
		upsB, downsB = endB.callbacksLocked()
	}
	n.mu.Unlock()

	if !changed {
		return
	}
	if closing != nil {
		close(closing)
	}
	if up {
		for _, f := range upsA {
			f(b, nil)
		}
		for _, f := range upsB {
			f(a, nil)
		}
		return
	}
	for _, f := range downsA {
		f(b)
	}
	for _, f := range downsB {
		f(a)
	}
}

// dropReplies loses the next count replies to requests sent from from to to;
// the handler still runs and the requester sees the link go down.
func (n *fakeNet) dropReplies(from, to peerID, count int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drops[linkPair{from, to}] = count
}

// onReply runs f when a reply reaches the caller, before the request resolves
// it, making the reply-versus-down race deterministic.
func (n *fakeNet) onReply(f func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replyHook = f
}

func (n *fakeNet) reachedReply() {
	n.mu.Lock()
	f := n.replyHook
	n.mu.Unlock()
	if f != nil {
		f()
	}
}

func (n *fakeNet) onKick(self, peer peerID, f func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := linkPair{self, peer}
	n.kickHooks[key] = append(n.kickHooks[key], f)
}

func (n *fakeNet) requests(from, to peerID) []link.Frame {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]link.Frame(nil), n.reqs[linkPair{from, to}]...)
}

func (n *fakeNet) kicks(self, peer peerID) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.kickCounts[linkPair{self, peer}]
}

func (n *fakeNet) linkState(self, peer peerID) (up bool, down <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := pairKey(self, peer)
	ch := n.down[key]
	if ch == nil {
		ch = make(chan struct{})
		n.down[key] = ch
	}
	return n.up[key], ch
}

func (n *fakeNet) recordRequest(from, to peerID, f link.Frame) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := linkPair{from, to}
	n.reqs[key] = append(n.reqs[key], f)
}

func (n *fakeNet) takeDrop(from, to peerID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := linkPair{from, to}
	if n.drops[key] <= 0 {
		return false
	}
	n.drops[key]--
	return true
}

// fakeEnd is one Broker's view of the fake network; it implements deliveryLink.
type fakeEnd struct {
	net  *fakeNet
	self peerID

	mu       sync.Mutex
	handlers map[link.FrameKind]func(context.Context, peerID, link.Frame) link.Frame
	ups      []func(peerID, []byte)
	downs    []func(peerID)
}

var _ deliveryLink = (*fakeEnd)(nil)

// Request follows the link contract: sent is false when the link is not
// established and true once the frame is out, even if the reply is lost.
func (e *fakeEnd) Request(ctx context.Context, p peerID, f link.Frame) (link.Frame, bool, error) {
	up, down := e.net.linkState(e.self, p)
	if !up {
		return link.Frame{}, false, fmt.Errorf("%w: fake link down", link.ErrNotEstablished)
	}
	e.net.recordRequest(e.self, p, f)

	handler := e.net.end(p).handler(f.Kind)
	if handler == nil {
		return link.Frame{}, true, &link.UnsupportedError{Kind: f.Kind}
	}

	// The handler runs on its own context, cancelled only when the connection
	// ends; its cancel is registered so test cleanup ends an abandoned handler.
	handlerCtx, cancelHandler := context.WithCancel(context.Background())
	handlerID := e.net.watchHandler(cancelHandler)
	go func() {
		select {
		case <-down:
			cancelHandler()
		case <-handlerCtx.Done():
		}
	}()

	replies := make(chan link.Frame, 1)
	go func() {
		defer e.net.unwatchHandler(handlerID)
		defer cancelHandler()
		replies <- handler(handlerCtx, e.self, f)
	}()

	select {
	case reply := <-replies:
		e.net.reachedReply()
		// A link that went down before the reply is resolved wins over the reply,
		// so an unordered select cannot report success for a dropped link.
		select {
		case <-down:
			return link.Frame{}, true, link.ErrLinkDown
		default:
		}
		if e.net.takeDrop(e.self, p) {
			e.net.setUp(e.self, p, false)
			return link.Frame{}, true, link.ErrLinkDown
		}
		return reply, true, nil
	case <-down:
		return link.Frame{}, true, link.ErrLinkDown
	case <-ctx.Done():
		return link.Frame{}, true, ctx.Err()
	}
}

func (e *fakeEnd) HandleRequest(kind link.FrameKind, h func(context.Context, peerID, link.Frame) link.Frame) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[kind] = h
}

func (e *fakeEnd) handler(kind link.FrameKind) func(context.Context, peerID, link.Frame) link.Frame {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handlers[kind]
}

func (e *fakeEnd) Subscribe(onUp func(peerID, []byte), onDown func(peerID)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if onUp != nil {
		e.ups = append(e.ups, onUp)
	}
	if onDown != nil {
		e.downs = append(e.downs, onDown)
	}
}

func (e *fakeEnd) Established(p peerID) bool {
	up, _ := e.net.linkState(e.self, p)
	return up
}

func (e *fakeEnd) Kick(p peerID) {
	n := e.net
	key := linkPair{e.self, p}
	n.mu.Lock()
	n.kickCounts[key]++
	hooks := append([]func(){}, n.kickHooks[key]...)
	n.mu.Unlock()
	for _, f := range hooks {
		f()
	}
}

func (e *fakeEnd) callbacksLocked() (ups []func(peerID, []byte), downs []func(peerID)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]func(peerID, []byte){}, e.ups...), append([]func(peerID){}, e.downs...)
}
