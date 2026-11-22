package directory

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// Test doubles for the directory package. They let tests form a network of
// Directory instances in one process, advance time without waiting, and inspect
// the frames each endpoint sent.

// fakeClock is a clock whose current instant and timers are driven by Advance.
// The zero value is usable: it starts at the zero time and lazily creates its
// condition variable.
type fakeClock struct {
	mu     sync.Mutex
	cond   *sync.Cond
	now    time.Time
	timers []*fakeTimer
}

var _ clock = (*fakeClock)(nil)

// newFakeClock returns a fake clock reset to start. With no start argument the
// zero time is used.
func newFakeClock(start ...time.Time) *fakeClock {
	c := &fakeClock{}
	if len(start) > 0 {
		c.now = start[0]
	}
	return c
}

// condLocked returns the condition variable, creating it on first use. The
// caller must hold c.mu.
func (c *fakeClock) condLocked() *sync.Cond {
	if c.cond == nil {
		c.cond = sync.NewCond(&c.mu)
	}
	return c.cond
}

// Now returns the fake current instant.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer registers a timer that fires when Advance moves the clock to or past
// its deadline.
func (c *fakeClock) NewTimer(d time.Duration) timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &fakeTimer{clock: c, deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, tm)
	c.condLocked().Broadcast()
	return tm
}

// Advance moves the clock forward and fires every pending timer whose deadline
// is not after the new instant.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, tm := range c.timers {
		if tm.fired || tm.stopped || tm.deadline.After(c.now) {
			continue
		}
		tm.fired = true
		select {
		case tm.ch <- c.now:
		default:
		}
	}
	c.condLocked().Broadcast()
}

// WaitTimers blocks until at least n untriggered timers exist. It fails the
// test after eventTimeout.
func (c *fakeClock) WaitTimers(t *testing.T, n int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	cond := c.condLocked()
	timedOut := false
	watchdog := time.AfterFunc(eventTimeout, func() {
		c.mu.Lock()
		timedOut = true
		cond.Broadcast()
		c.mu.Unlock()
	})
	defer watchdog.Stop()

	for c.pendingLocked() < n {
		if timedOut {
			t.Fatalf("fakeClock: timed out waiting for %d pending timers (have %d)", n, c.pendingLocked())
		}
		cond.Wait()
	}
}

// pendingLocked counts timers that have neither fired nor been stopped. The
// caller must hold c.mu.
func (c *fakeClock) pendingLocked() int {
	n := 0
	for _, tm := range c.timers {
		if !tm.fired && !tm.stopped {
			n++
		}
	}
	return n
}

// fakeTimer is one timer created by fakeClock.
type fakeTimer struct {
	clock    *fakeClock
	deadline time.Time
	ch       chan time.Time
	fired    bool
	stopped  bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

// Stop prevents the timer from firing. It reports whether the timer was still
// pending.
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	t.clock.condLocked().Broadcast()
	return true
}

var (
	errFakeNotEstablished = errors.New("fake net: link not established")
	errFakeUnsupported    = errors.New("fake net: no request handler")
)

// sentFrame is one frame an endpoint sent through fakeLinks.
type sentFrame struct {
	to      Fingerprint
	kind    uint32
	payload []byte
}

// fakeNet is an in-process network of fakeLinks endpoints.
type fakeNet struct {
	mu        sync.Mutex
	cond      *sync.Cond
	endpoints map[Fingerprint]*fakeLinks
	directed  map[Fingerprint]map[Fingerprint]*directedLink
	upDone    map[Fingerprint]map[Fingerprint]bool // upDone[a][b]: a's onUp for b returned
	active    int                                  // deliveries and request handlers currently running
	done      uint64                               // completed deliveries and request handlers
	timedOut  bool
}

// newFakeNet creates an empty fake network.
func newFakeNet() *fakeNet {
	n := &fakeNet{
		endpoints: make(map[Fingerprint]*fakeLinks),
		directed:  make(map[Fingerprint]map[Fingerprint]*directedLink),
		upDone:    make(map[Fingerprint]map[Fingerprint]bool),
	}
	n.cond = sync.NewCond(&n.mu)
	return n
}

// Endpoint returns the fakeLinks for fp, creating it on first use.
func (n *fakeNet) Endpoint(fp Fingerprint) *fakeLinks {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.endpointLocked(fp)
}

// endpointLocked returns the endpoint for fp, creating it. The caller must hold
// n.mu.
func (n *fakeNet) endpointLocked(fp Fingerprint) *fakeLinks {
	if ep, ok := n.endpoints[fp]; ok {
		return ep
	}
	ep := &fakeLinks{
		net:             n,
		fp:              fp,
		stateHandlers:   make(map[uint32]func(Fingerprint, []byte)),
		requestHandlers: make(map[uint32]func(context.Context, Fingerprint, []byte) (uint32, []byte)),
		status:          make(map[Fingerprint]LinkView),
	}
	n.endpoints[fp] = ep
	return ep
}

// Connect establishes a bidirectional link between a and b. Both endpoints
// receive their onUp notification, a before b, with a 32-byte zero salt. As on
// a real link, no frame is dispatched to an endpoint until its own onUp
// notification has returned: a peer sending its full list from onUp must not
// reach a directory that has not recorded the link as established yet.
func (n *fakeNet) Connect(a, b Fingerprint) {
	n.mu.Lock()
	ea := n.endpointLocked(a)
	eb := n.endpointLocked(b)
	if n.directed[a] == nil {
		n.directed[a] = make(map[Fingerprint]*directedLink)
	}
	if n.directed[b] == nil {
		n.directed[b] = make(map[Fingerprint]*directedLink)
	}
	if _, ok := n.directed[a][b]; ok {
		n.mu.Unlock()
		return
	}
	ab := &directedLink{net: n, from: a, dst: eb}
	ba := &directedLink{net: n, from: b, dst: ea}
	n.directed[a][b] = ab
	n.directed[b][a] = ba
	n.mu.Unlock()

	go ab.run()
	go ba.run()

	salt := make([]byte, 32)
	if up := ea.upCallback(); up != nil {
		up(b, salt)
	}
	n.markUp(a, b)
	if up := eb.upCallback(); up != nil {
		up(a, salt)
	}
	n.markUp(b, a)
}

// markUp records that fp's onUp notification for peer has returned and wakes
// the delivery goroutines waiting on it.
func (n *fakeNet) markUp(fp, peer Fingerprint) {
	n.mu.Lock()
	if n.upDone[fp] == nil {
		n.upDone[fp] = make(map[Fingerprint]bool)
	}
	n.upDone[fp][peer] = true
	n.cond.Broadcast()
	n.mu.Unlock()
}

// upReturnedLocked reports whether fp's onUp notification for peer has
// returned. The caller must hold n.mu.
func (n *fakeNet) upReturnedLocked(fp, peer Fingerprint) bool {
	return n.upDone[fp] != nil && n.upDone[fp][peer]
}

// deliveryGateHeld waits until the delivery goroutine for the link from->to
// has either parked a queued frame at the onUp gate or taken the frame for
// delivery. It reports whether the gate held the frame. The caller runs inside
// the destination's onUp notification. It waits without a fixed delay: one of
// the two states is reached as soon as the delivery goroutine runs.
func (n *fakeNet) deliveryGateHeld(from, to Fingerprint) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	l := n.directed[from][to]
	if l == nil {
		return false
	}
	for !l.gated && l.dequeued == 0 {
		n.cond.Wait()
	}
	return l.gated && l.dequeued == 0
}

// Disconnect tears down the link between a and b and delivers their onDown
// notifications, a before b.
func (n *fakeNet) Disconnect(a, b Fingerprint) {
	n.mu.Lock()
	ab := n.directed[a][b]
	ba := n.directed[b][a]
	if ab == nil && ba == nil {
		n.mu.Unlock()
		return
	}
	delete(n.directed[a], b)
	delete(n.directed[b], a)
	delete(n.upDone[a], b)
	delete(n.upDone[b], a)
	if ab != nil {
		ab.closed = true
		ab.queue = nil
	}
	if ba != nil {
		ba.closed = true
		ba.queue = nil
	}
	ea := n.endpoints[a]
	eb := n.endpoints[b]
	n.cond.Broadcast()
	n.mu.Unlock()

	if ea != nil {
		if down := ea.downCallback(); down != nil {
			down(b)
		}
	}
	if eb != nil {
		if down := eb.downCallback(); down != nil {
			down(a)
		}
	}
}

// TestFakeNetHoldsFramesUntilUpReturns pins the link contract that frames are
// dispatched to an endpoint only after its own onUp notification has returned.
// Directories send their first full list from onUp, so a frame delivered any
// earlier would reach a peer that has not recorded the link as established yet
// and be dropped. B's onUp waits for the delivery goroutine to reach the onUp
// gate: the gated order parks the queued frame there, while an ungated order
// dispatches it and the test fails.
func TestFakeNetHoldsFramesUntilUpReturns(t *testing.T) {
	net := newFakeNet()
	a := newTestIdentity(1).Fingerprint()
	b := newTestIdentity(2).Fingerprint()
	ea := net.Endpoint(a)
	eb := net.Endpoint(b)

	var upReturned atomic.Bool
	var handled atomic.Int32
	eb.HandleState(300, func(peer Fingerprint, payload []byte) {
		if !upReturned.Load() {
			t.Errorf("B received a frame before its onUp returned")
		}
		handled.Add(1)
	})

	// B's onUp waits for the delivery goroutine to reach the gate with the
	// queued frame. The gated implementation parks the frame there; an ungated
	// one dispatches it, which deliveryGateHeld reports and the test fails.
	// The flag is set at the end of onUp as the contract requires.
	enteredUp := make(chan struct{})
	eb.Subscribe(func(peer Fingerprint, salt []byte) {
		close(enteredUp)
		if !net.deliveryGateHeld(a, b) {
			t.Errorf("B received a frame before its onUp returned")
		}
		upReturned.Store(true)
	}, nil)

	// A's onUp sends the first frame, as a directory does on link-up.
	ea.Subscribe(func(peer Fingerprint, salt []byte) {
		if err := ea.Send(b, 300, []byte("first")); err != nil {
			t.Errorf("send on link-up: %v", err)
		}
	}, nil)

	connected := make(chan struct{})
	go func() {
		net.Connect(a, b)
		close(connected)
	}()

	// A's onUp ran before B's was entered, so the frame is already queued.
	<-enteredUp
	<-connected
	net.Settle(t)
	if got := handled.Load(); got != 1 {
		t.Fatalf("B handled %d frames after its onUp returned, want 1", got)
	}
}

// connected reports whether the directed link from a to b exists.
func (n *fakeNet) connected(a, b Fingerprint) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.directed[a] != nil && n.directed[a][b] != nil
}

// busyLocked reports whether any delivery is running or any queue is non-empty.
func (n *fakeNet) busyLocked() bool {
	if n.active > 0 {
		return true
	}
	for _, m := range n.directed {
		for _, l := range m {
			if len(l.queue) > 0 {
				return true
			}
		}
	}
	return false
}

// Settle waits until the network is idle and the given wait groups have
// finished. It repeats the round when new activity appeared during it and fails
// the test after eventTimeout.
func (n *fakeNet) Settle(t *testing.T, inflight ...*sync.WaitGroup) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)

	n.mu.Lock()
	n.timedOut = false
	n.mu.Unlock()

	watchdog := time.AfterFunc(time.Until(deadline), func() {
		n.mu.Lock()
		n.timedOut = true
		n.cond.Broadcast()
		n.mu.Unlock()
	})
	defer watchdog.Stop()

	for {
		if time.Now().After(deadline) {
			t.Fatal("fakeNet: Settle timed out")
		}

		n.mu.Lock()
		for n.busyLocked() {
			if n.timedOut {
				n.mu.Unlock()
				t.Fatal("fakeNet: Settle timed out waiting for the network to go idle")
			}
			n.cond.Wait()
		}
		before := n.done
		n.mu.Unlock()

		for _, wg := range inflight {
			if !waitGroupWithin(wg, time.Until(deadline)) {
				t.Fatal("fakeNet: Settle timed out waiting for inflight work")
			}
		}

		n.mu.Lock()
		idle := !n.busyLocked()
		after := n.done
		n.mu.Unlock()
		if idle && after == before {
			return
		}
	}
}

// waitGroupWithin waits for wg, reporting false when timeout elapses first.
func waitGroupWithin(wg *sync.WaitGroup, timeout time.Duration) bool {
	if wg == nil {
		return true
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	tm := time.NewTimer(timeout)
	defer tm.Stop()
	select {
	case <-done:
		return true
	case <-tm.C:
		return false
	}
}

// directedLink is one direction of a fake link: a FIFO queue plus the single
// goroutine that drains it.
type directedLink struct {
	net      *fakeNet
	from     Fingerprint
	dst      *fakeLinks
	queue    []fakeFrame
	closed   bool
	gated    bool // a queued frame was parked at the onUp gate
	dequeued int  // frames taken for delivery
}

// fakeFrame is one frame queued on a directed link.
type fakeFrame struct {
	kind    uint32
	payload []byte
}

// run delivers queued state frames in order until the link is closed. Frames
// wait while the destination has not returned from its onUp notification for
// this link.
func (l *directedLink) run() {
	for {
		l.net.mu.Lock()
		for !l.closed && (len(l.queue) == 0 || !l.net.upReturnedLocked(l.dst.fp, l.from)) {
			if !l.closed && len(l.queue) > 0 {
				l.gated = true
				l.net.cond.Broadcast()
			}
			l.net.cond.Wait()
		}
		if len(l.queue) == 0 && l.closed {
			l.net.mu.Unlock()
			return
		}
		f := l.queue[0]
		l.queue = l.queue[1:]
		l.dequeued++
		l.net.active++
		l.net.mu.Unlock()

		l.deliver(f)

		l.net.mu.Lock()
		l.net.active--
		l.net.done++
		l.net.cond.Broadcast()
		l.net.mu.Unlock()
	}
}

// deliver calls the destination's state handler, if any.
func (l *directedLink) deliver(f fakeFrame) {
	l.dst.mu.Lock()
	h := l.dst.stateHandlers[f.kind]
	l.dst.mu.Unlock()
	if h == nil {
		return
	}
	h(l.from, f.payload)
}

// fakeLinks is one endpoint of fakeNet. It implements Links.
type fakeLinks struct {
	net *fakeNet
	fp  Fingerprint

	mu              sync.Mutex
	stateHandlers   map[uint32]func(Fingerprint, []byte)
	requestHandlers map[uint32]func(context.Context, Fingerprint, []byte) (uint32, []byte)
	status          map[Fingerprint]LinkView
	sent            []sentFrame
	sendHook        func(to Fingerprint, kind uint32)
	onUp            func(Fingerprint, []byte)
	onDown          func(Fingerprint)
}

var _ Links = (*fakeLinks)(nil)

// Send enqueues one state frame. It fails when the link is not established.
func (l *fakeLinks) Send(peer Fingerprint, kind uint32, payload []byte) error {
	l.mu.Lock()
	hook := l.sendHook
	l.mu.Unlock()

	if !l.net.connected(l.fp, peer) {
		return errFakeNotEstablished
	}
	if hook != nil {
		hook(peer, kind)
	}

	l.net.mu.Lock()
	link := l.net.directed[l.fp][peer]
	if link == nil {
		l.net.mu.Unlock()
		return errFakeNotEstablished
	}
	p := append([]byte(nil), payload...)
	link.queue = append(link.queue, fakeFrame{kind: kind, payload: p})
	l.net.cond.Broadcast()
	l.net.mu.Unlock()

	l.mu.Lock()
	l.sent = append(l.sent, sentFrame{to: peer, kind: kind, payload: p})
	l.mu.Unlock()
	return nil
}

// Request calls the peer's request handler in a new goroutine. When the link is
// not established it reports sent == false.
func (l *fakeLinks) Request(ctx context.Context, peer Fingerprint, kind uint32, payload []byte) (uint32, []byte, bool, error) {
	if !l.net.connected(l.fp, peer) {
		return 0, nil, false, nil
	}

	l.mu.Lock()
	l.sent = append(l.sent, sentFrame{to: peer, kind: kind, payload: append([]byte(nil), payload...)})
	l.mu.Unlock()

	l.net.mu.Lock()
	dst := l.net.endpointLocked(peer)
	l.net.active++
	l.net.mu.Unlock()

	dst.mu.Lock()
	h := dst.requestHandlers[kind]
	dst.mu.Unlock()

	type result struct {
		kind    uint32
		payload []byte
		err     error
	}
	resCh := make(chan result, 1)
	go func() {
		defer func() {
			l.net.mu.Lock()
			l.net.active--
			l.net.done++
			l.net.cond.Broadcast()
			l.net.mu.Unlock()
		}()
		if h == nil {
			resCh <- result{err: errFakeUnsupported}
			return
		}
		rk, rp := h(ctx, l.fp, payload)
		resCh <- result{kind: rk, payload: rp}
	}()

	select {
	case r := <-resCh:
		return r.kind, r.payload, true, r.err
	case <-ctx.Done():
		return 0, nil, true, ctx.Err()
	}
}

// HandleState registers the handler for state frames of one kind.
func (l *fakeLinks) HandleState(kind uint32, h func(peer Fingerprint, payload []byte)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stateHandlers[kind] = h
}

// HandleRequest registers the handler for request frames of one kind.
func (l *fakeLinks) HandleRequest(kind uint32, h func(ctx context.Context, peer Fingerprint, payload []byte) (replyKind uint32, reply []byte)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.requestHandlers[kind] = h
}

// Subscribe registers link up and down notifications.
func (l *fakeLinks) Subscribe(onUp func(peer Fingerprint, salt []byte), onDown func(peer Fingerprint)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onUp = onUp
	l.onDown = onDown
}

// Status returns the LinkView set by SetStatus, or a view derived from the fake
// network's connections.
func (l *fakeLinks) Status(peer Fingerprint) LinkView {
	l.mu.Lock()
	v, ok := l.status[peer]
	l.mu.Unlock()
	if ok {
		return v
	}
	if l.net.connected(l.fp, peer) {
		return LinkView{Phase: LinkEstablished}
	}
	return LinkView{}
}

// SetStatus overrides the LinkView Status returns for peer.
func (l *fakeLinks) SetStatus(peer Fingerprint, view LinkView) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.status[peer] = view
}

// Sent returns a copy of every frame this endpoint sent.
func (l *fakeLinks) Sent() []sentFrame {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]sentFrame, len(l.sent))
	for i, f := range l.sent {
		out[i] = sentFrame{to: f.to, kind: f.kind, payload: append([]byte(nil), f.payload...)}
	}
	return out
}

// SetSendHook sets the hook called before Send enqueues a frame. The hook runs
// without any endpoint lock held.
func (l *fakeLinks) SetSendHook(hook func(to Fingerprint, kind uint32)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sendHook = hook
}

func (l *fakeLinks) upCallback() func(Fingerprint, []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.onUp
}

func (l *fakeLinks) downCallback() func(Fingerprint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.onDown
}

// fakePeers is an in-memory PeerTable. The zero value is usable.
type fakePeers struct {
	mu    sync.Mutex
	peers map[Fingerprint]pairing.Peer
}

var _ PeerTable = (*fakePeers)(nil)

// newFakePeers returns an empty peer table.
func newFakePeers() *fakePeers { return &fakePeers{} }

// Add inserts or replaces a peer.
func (p *fakePeers) Add(peer pairing.Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers == nil {
		p.peers = make(map[Fingerprint]pairing.Peer)
	}
	p.peers[peer.Fingerprint] = peer
}

// Remove deletes a peer.
func (p *fakePeers) Remove(fp Fingerprint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.peers, fp)
}

// IsPeer reports whether fp is in the table.
func (p *fakePeers) IsPeer(fp Fingerprint) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.peers[fp]
	return ok
}

// Peers returns a copy of the table.
func (p *fakePeers) Peers() []pairing.Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]pairing.Peer, 0, len(p.peers))
	for _, peer := range p.peers {
		out = append(out, peer)
	}
	return out
}

// EffectiveAlias returns LocalAlias when set, and Alias otherwise.
func (p *fakePeers) EffectiveAlias(fp Fingerprint) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	peer, ok := p.peers[fp]
	if !ok {
		return ""
	}
	if peer.LocalAlias != "" {
		return peer.LocalAlias
	}
	return peer.Alias
}

// fakeSettings is a mutable Settings. The zero value uses the config defaults
// for every limit.
type fakeSettings struct {
	mu         sync.Mutex
	public     bool
	alias      string
	advertised []config.AdvertisedAddr
	dir        string
	reconcile  time.Duration
	maxRecords int
	maxPerLink int
	maxTopics  int
}

var _ Settings = (*fakeSettings)(nil)

// newFakeSettings returns settings whose data directory is dir.
func newFakeSettings(dir string) *fakeSettings { return &fakeSettings{dir: dir} }

func (s *fakeSettings) Public() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.public
}

func (s *fakeSettings) Alias() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alias
}

func (s *fakeSettings) Advertised() []config.AdvertisedAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.AdvertisedAddr(nil), s.advertised...)
}

func (s *fakeSettings) DataDir() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir, nil
}

func (s *fakeSettings) ReconcileInterval() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reconcile == 0 {
		return 10 * time.Minute
	}
	return s.reconcile
}

func (s *fakeSettings) MaxRecords() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxRecords == 0 {
		return 1000
	}
	return s.maxRecords
}

func (s *fakeSettings) MaxRecordsPerLink() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxPerLink == 0 {
		return 250
	}
	return s.maxPerLink
}

func (s *fakeSettings) MaxTopicsPerPublisher() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxTopics == 0 {
		return 16
	}
	return s.maxTopics
}

// SetPublic changes the public flag.
func (s *fakeSettings) SetPublic(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.public = v
}

// SetAlias changes the configured alias.
func (s *fakeSettings) SetAlias(v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alias = v
}

// SetAdvertised replaces the advertised addresses.
func (s *fakeSettings) SetAdvertised(v []config.AdvertisedAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advertised = append([]config.AdvertisedAddr(nil), v...)
}

// SetDataDir changes the data directory.
func (s *fakeSettings) SetDataDir(v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dir = v
}

// SetReconcileInterval changes the reconcile interval.
func (s *fakeSettings) SetReconcileInterval(v time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconcile = v
}

// SetMaxRecords changes the total record limit.
func (s *fakeSettings) SetMaxRecords(v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxRecords = v
}

// SetMaxRecordsPerLink changes the per-link record limit.
func (s *fakeSettings) SetMaxRecordsPerLink(v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxPerLink = v
}

// SetMaxTopicsPerPublisher changes the per-publisher topic limit.
func (s *fakeSettings) SetMaxTopicsPerPublisher(v int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxTopics = v
}

// testIdentity is an Identity backed by an Ed25519 key generated from a seed.
type testIdentity struct {
	priv ed25519.PrivateKey
	pub  ed25519.PublicKey
	fp   Fingerprint
}

var _ Identity = (*testIdentity)(nil)

// newTestIdentity builds an identity from 32 bytes all equal to seed.
func newTestIdentity(seed byte) Identity {
	var s [ed25519.SeedSize]byte
	for i := range s {
		s[i] = seed
	}
	priv := ed25519.NewKeyFromSeed(s[:])
	pub := priv.Public().(ed25519.PublicKey)
	return &testIdentity{
		priv: priv,
		pub:  pub,
		fp:   pairing.FingerprintOf(pub),
	}
}

func (id *testIdentity) Fingerprint() Fingerprint { return id.fp }

func (id *testIdentity) PublicKey() []byte { return append([]byte(nil), id.pub...) }

func (id *testIdentity) Sign(data []byte) []byte { return ed25519.Sign(id.priv, data) }

// lockedBuffer is an io.Writer safe for concurrent use.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns everything written so far.
func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newLogBuffer returns a text logger writing at debug level to a locked buffer.
func newLogBuffer() (*slog.Logger, *lockedBuffer) {
	b := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return logger, b
}
