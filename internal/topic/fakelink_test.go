package topic

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
	"google.golang.org/protobuf/proto"
)

var errFakeNotEstablished = errors.New("fakelink: link to peer is not established")

// The link is marked down, matching link's bounded queue.
var errFakeQueueFull = fmt.Errorf("%w: send queue full", errFakeNotEstablished)

// It matches link's own sendQueueSize.
const fakeSendQueueLimit = 1024

type fakeEventKind uint8

const (
	fakeEventFrame fakeEventKind = iota
	fakeEventUp
	fakeEventDown
)

// A frame whose generation is older than the peer's current generation is
// dropped, modelling a frame lost with a connection that has since closed.
type fakeEvent struct {
	kind  fakeEventKind
	peer  link.Fingerprint
	gen   uint64
	frame link.Frame
	salt  []byte
}

type fakePeer struct {
	up   bool
	gen  uint64
	conn *fakeLink

	stalled     bool
	outstanding int
	pending     []*fakePending
	// pendingByKey indexes the latest pending frame of each key so a newer
	// latest frame replaces it.
	pendingByKey map[string]*fakePending
}

type fakePending struct {
	key   string
	frame link.Frame
}

type fakeFrame struct {
	Kind    uint32
	Payload []byte
	Latest  bool
	Key     string
}

// One scheduler goroutine per endpoint runs events in FIFO order, so callbacks
// for one peer are serial and no frame is delivered after its onDown. Callbacks
// run without the lock held, and Send only records and enqueues the frame.
type fakeLink struct {
	self link.Fingerprint

	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	running bool
	queue   []fakeEvent

	peers    map[link.Fingerprint]*fakePeer
	handlers map[link.FrameKind]func(peer link.Fingerprint, f link.Frame)
	onUp     func(peer link.Fingerprint, salt []byte)
	onDown   func(peer link.Fingerprint)
	hook     func(peer link.Fingerprint, f link.Frame)
	failSend map[link.Fingerprint]bool
	sendErr  map[link.FrameKind]error
	sent     map[link.Fingerprint][]fakeFrame
}

func newFakeLink(t *testing.T, self link.Fingerprint) *fakeLink {
	t.Helper()
	l := &fakeLink{
		self:     self,
		peers:    map[link.Fingerprint]*fakePeer{},
		handlers: map[link.FrameKind]func(peer link.Fingerprint, f link.Frame){},
		failSend: map[link.Fingerprint]bool{},
		sendErr:  map[link.FrameKind]error{},
		sent:     map[link.Fingerprint][]fakeFrame{},
	}
	l.cond = sync.NewCond(&l.mu)
	go l.run()
	t.Cleanup(l.stop)
	return l
}

// It calls the test hook first without holding the lock, then returns
// errFakeNotEstablished unless the link is established and not failing.
func (l *fakeLink) Send(peer link.Fingerprint, f link.Frame) error {
	_, err := l.send(peer, f, "")
	return err
}

func (l *fakeLink) SendLatest(peer link.Fingerprint, f link.Frame) error {
	_, err := l.send(peer, f, fmt.Sprintf("k:%d", f.Kind))
	return err
}

// A stalled peer's queued frame with the same key is replaced in place.
func (l *fakeLink) SendLatestKey(peer link.Fingerprint, key string, f link.Frame) (replaced bool, err error) {
	return l.send(peer, f, key)
}

// A per-kind error installed by setSendError is returned instead of delivering;
// while the peer is stalled the frame is queued, a same-key frame is replaced,
// and a queue past its limit marks the link down.
func (l *fakeLink) send(peer link.Fingerprint, f link.Frame, key string) (replaced bool, err error) {
	l.mu.Lock()
	hook := l.hook
	l.mu.Unlock()
	if hook != nil {
		hook(peer, f)
	}

	payload := append([]byte(nil), f.Payload...)

	l.mu.Lock()
	st := l.peers[peer]
	if st == nil || !st.up || l.failSend[peer] {
		l.mu.Unlock()
		return false, errFakeNotEstablished
	}
	if err := l.sendErr[link.FrameKind(f.Kind)]; err != nil {
		l.mu.Unlock()
		return false, err
	}
	l.sent[peer] = append(l.sent[peer], fakeFrame{Kind: uint32(f.Kind), Payload: payload, Latest: key != "", Key: key})

	if st.stalled {
		if key != "" {
			if pf := st.pendingByKey[key]; pf != nil {
				pf.frame = link.Frame{Kind: f.Kind, Payload: payload}
				l.mu.Unlock()
				return true, nil
			}
		}
		limit := fakeSendQueueLimit
		if st.outstanding >= limit {
			l.downLocked(peer)
			l.mu.Unlock()
			return false, errFakeQueueFull
		}
		pf := &fakePending{key: key, frame: link.Frame{Kind: f.Kind, Payload: payload}}
		st.outstanding++
		st.pending = append(st.pending, pf)
		if key != "" {
			st.pendingByKey[key] = pf
		}
		l.mu.Unlock()
		return false, nil
	}
	conn := st.conn
	l.mu.Unlock()

	if conn != nil {
		conn.deliver(l.self, link.Frame{Kind: f.Kind, Payload: payload})
	}
	return false, nil
}

func (l *fakeLink) stall(peer link.Fingerprint) {
	l.mu.Lock()
	l.peerLocked(peer).stalled = true
	l.mu.Unlock()
}

func (l *fakeLink) resume(peer link.Fingerprint) bool {
	l.mu.Lock()
	st := l.peers[peer]
	if st == nil {
		l.mu.Unlock()
		return false
	}
	st.stalled = false
	pending := st.pending
	st.pending = nil
	st.pendingByKey = map[string]*fakePending{}
	st.outstanding = 0
	conn := st.conn
	up := st.up
	l.mu.Unlock()

	if conn != nil {
		for _, pf := range pending {
			conn.deliver(l.self, pf.frame)
		}
	}
	return up
}

func (l *fakeLink) isUp(peer link.Fingerprint) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.peers[peer]
	return st != nil && st.up
}

func (l *fakeLink) HandleState(kind link.FrameKind, h func(peer link.Fingerprint, f link.Frame)) {
	l.mu.Lock()
	l.handlers[kind] = h
	l.mu.Unlock()
}

func (l *fakeLink) Subscribe(onUp func(peer link.Fingerprint, salt []byte), onDown func(peer link.Fingerprint)) {
	l.mu.Lock()
	l.onUp, l.onDown = onUp, onDown
	l.mu.Unlock()
}

// The frame carries the endpoint's current generation, so a send on a connection
// that has since gone down is dropped.
func (l *fakeLink) deliver(from link.Fingerprint, f link.Frame) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.peers[from]
	if st == nil || !st.up {
		return
	}
	l.queue = append(l.queue, fakeEvent{kind: fakeEventFrame, peer: from, gen: st.gen, frame: f})
	l.cond.Broadcast()
}

func (l *fakeLink) up(peer link.Fingerprint, salt []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.upLocked(peer, salt)
}

func (l *fakeLink) upLocked(peer link.Fingerprint, salt []byte) {
	st := l.peerLocked(peer)
	st.up = true
	st.gen++
	st.stalled = false
	st.outstanding = 0
	st.pending = nil
	st.pendingByKey = map[string]*fakePending{}
	l.queue = append(l.queue, fakeEvent{kind: fakeEventUp, peer: peer, gen: st.gen, salt: append([]byte(nil), salt...)})
	l.cond.Broadcast()
}

func (l *fakeLink) down(peer link.Fingerprint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.downLocked(peer)
}

func (l *fakeLink) downLocked(peer link.Fingerprint) {
	st := l.peerLocked(peer)
	st.up = false
	st.gen++
	st.stalled = false
	st.outstanding = 0
	st.pending = nil
	st.pendingByKey = map[string]*fakePending{}
	l.queue = append(l.queue, fakeEvent{kind: fakeEventDown, peer: peer, gen: st.gen})
	l.cond.Broadcast()
}

func (l *fakeLink) inject(peer link.Fingerprint, kind uint32, msg proto.Message) {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic("fakelink: marshal injected frame: " + err.Error())
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var gen uint64
	if st := l.peers[peer]; st != nil {
		gen = st.gen
	}
	l.queue = append(l.queue, fakeEvent{
		kind:  fakeEventFrame,
		peer:  peer,
		gen:   gen,
		frame: link.Frame{Kind: link.FrameKind(kind), Payload: b},
	})
	l.cond.Broadcast()
}

func (l *fakeLink) sentFrames(peer link.Fingerprint) []fakeFrame {
	l.mu.Lock()
	defer l.mu.Unlock()
	src := l.sent[peer]
	out := make([]fakeFrame, len(src))
	for i, f := range src {
		out[i] = fakeFrame{Kind: f.Kind, Payload: append([]byte(nil), f.Payload...), Latest: f.Latest, Key: f.Key}
	}
	return out
}

func (l *fakeLink) clearSent(peer link.Fingerprint) {
	l.mu.Lock()
	delete(l.sent, peer)
	l.mu.Unlock()
}

// The hook is called at the start of every send without the fakeLink's lock
// held.
func (l *fakeLink) setSendHook(hook func(peer link.Fingerprint, f link.Frame)) {
	l.mu.Lock()
	l.hook = hook
	l.mu.Unlock()
}

func (l *fakeLink) setSendError(kind link.FrameKind, err error) {
	l.mu.Lock()
	if err == nil {
		delete(l.sendErr, kind)
	} else {
		l.sendErr[kind] = err
	}
	l.mu.Unlock()
}

func (l *fakeLink) setFailSend(peer link.Fingerprint, fail bool) {
	l.mu.Lock()
	if fail {
		l.failSend[peer] = true
	} else {
		delete(l.failSend, peer)
	}
	l.mu.Unlock()
}

func (l *fakeLink) run() {
	for {
		l.mu.Lock()
		for !l.closed && len(l.queue) == 0 {
			l.cond.Wait()
		}
		if l.closed {
			l.mu.Unlock()
			return
		}
		ev := l.queue[0]
		l.queue[0] = fakeEvent{}
		l.queue = l.queue[1:]
		l.running = true

		var stateHandler func(peer link.Fingerprint, f link.Frame)
		var onUp func(peer link.Fingerprint, salt []byte)
		var onDown func(peer link.Fingerprint)
		var curGen uint64
		if st := l.peers[ev.peer]; st != nil {
			curGen = st.gen
		}
		switch ev.kind {
		case fakeEventFrame:
			stateHandler = l.handlers[ev.frame.Kind]
		case fakeEventUp:
			onUp = l.onUp
		case fakeEventDown:
			onDown = l.onDown
		}
		l.mu.Unlock()

		switch ev.kind {
		case fakeEventFrame:
			if ev.gen >= curGen && stateHandler != nil {
				stateHandler(ev.peer, ev.frame)
			}
		case fakeEventUp:
			if onUp != nil {
				onUp(ev.peer, ev.salt)
			}
		case fakeEventDown:
			if onDown != nil {
				onDown(ev.peer)
			}
		}

		l.mu.Lock()
		l.running = false
		l.cond.Broadcast()
		l.mu.Unlock()
	}
}

func (l *fakeLink) stop() {
	l.mu.Lock()
	l.closed = true
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *fakeLink) peerLocked(peer link.Fingerprint) *fakePeer {
	st := l.peers[peer]
	if st == nil {
		st = &fakePeer{pendingByKey: map[string]*fakePending{}}
		l.peers[peer] = st
	}
	return st
}

// Both onUp events are queued before either endpoint can send, so no frame is
// forwarded before the receiver knows the link is up.
func connect(a, b *fakeLink, salt []byte) {
	a.mu.Lock()
	b.mu.Lock()
	a.peerLocked(b.self).conn = b
	b.peerLocked(a.self).conn = a
	a.upLocked(b.self, salt)
	b.upLocked(a.self, salt)
	b.mu.Unlock()
	a.mu.Unlock()
}

func disconnect(a, b *fakeLink) {
	a.mu.Lock()
	b.mu.Lock()
	if st := a.peers[b.self]; st != nil && st.conn == b {
		st.conn = nil
	}
	if st := b.peers[a.self]; st != nil && st.conn == a {
		st.conn = nil
	}
	a.downLocked(b.self)
	b.downLocked(a.self)
	b.mu.Unlock()
	a.mu.Unlock()
}

// flushAll waits until every fakeLink has an empty queue and an idle scheduler,
// confirming quiescence with all their locks held so no forwarded frame is
// missed. A timer after eventTimeout only wakes the waiters so a stuck drain
// fails rather than hangs.
func flushAll(t *testing.T, ls ...*fakeLink) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	timer := time.AfterFunc(eventTimeout, func() {
		for _, l := range ls {
			l.mu.Lock()
			l.cond.Broadcast()
			l.mu.Unlock()
		}
	})
	defer timer.Stop()

	for {
		for _, l := range ls {
			if !l.waitIdle(deadline) {
				t.Fatalf("fakelink: queues did not drain within %s", eventTimeout)
			}
		}
		if allIdle(ls) {
			return
		}
	}
}

// Holding all the links' locks before reading any of them keeps a frame
// forwarded by one link from being missed on one already checked.
func allIdle(ls []*fakeLink) bool {
	for _, l := range ls {
		l.mu.Lock()
	}
	defer func() {
		for i := len(ls) - 1; i >= 0; i-- {
			ls[i].mu.Unlock()
		}
	}()
	for _, l := range ls {
		if len(l.queue) > 0 || l.running {
			return false
		}
	}
	return true
}

func (l *fakeLink) waitIdle(deadline time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.queue) > 0 || l.running {
		if !time.Now().Before(deadline) {
			return false
		}
		l.cond.Wait()
	}
	return true
}

func fakeLinkFingerprint(b byte) link.Fingerprint {
	var fp link.Fingerprint
	fp[len(fp)-1] = b
	return fp
}

func decodeDigests(t *testing.T, f fakeFrame) *topicpb.TopicDigests {
	t.Helper()
	m := &topicpb.TopicDigests{}
	if err := proto.Unmarshal(f.Payload, m); err != nil {
		t.Fatalf("fakelink: decoding TopicDigests frame: %v", err)
	}
	return m
}

func decodeRoster(t *testing.T, f fakeFrame) *topicpb.Roster {
	t.Helper()
	m := &topicpb.Roster{}
	if err := proto.Unmarshal(f.Payload, m); err != nil {
		t.Fatalf("fakelink: decoding Roster frame: %v", err)
	}
	return m
}

func decodeDelta(t *testing.T, f fakeFrame) *topicpb.RosterDelta {
	t.Helper()
	m := &topicpb.RosterDelta{}
	if err := proto.Unmarshal(f.Payload, m); err != nil {
		t.Fatalf("fakelink: decoding RosterDelta frame: %v", err)
	}
	return m
}

// TestFakeLinkDeliversInOrder checks that frames sent to a connected endpoint are
// delivered in order.
func TestFakeLinkDeliversInOrder(t *testing.T) {
	a := newFakeLink(t, fakeLinkFingerprint(1))
	b := newFakeLink(t, fakeLinkFingerprint(2))

	var mu sync.Mutex
	var got []byte
	kind := link.FrameKind(topicpb.KindTopicDigests)
	b.HandleState(kind, func(_ link.Fingerprint, f link.Frame) {
		mu.Lock()
		got = append(got, f.Payload[0])
		mu.Unlock()
	})

	connect(a, b, []byte("salt"))
	flushAll(t, a, b)

	const n = 100
	for i := 0; i < n; i++ {
		if err := a.Send(b.self, link.Frame{Kind: kind, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("Send frame %d: %v", i, err)
		}
	}
	flushAll(t, a, b)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("received %d frames, want %d", len(got), n)
	}
	for i, payload := range got {
		if payload != byte(i) {
			t.Fatalf("frame %d has payload %d, want %d", i, payload, i)
		}
	}
}

// TestFakeLinkSendFailsWhenDown checks that a send before connecting and after
// disconnecting fails and records no frame.
func TestFakeLinkSendFailsWhenDown(t *testing.T) {
	a := newFakeLink(t, fakeLinkFingerprint(1))
	b := newFakeLink(t, fakeLinkFingerprint(2))
	frame := link.Frame{Kind: link.FrameKind(topicpb.KindRoster), Payload: []byte{1}}

	if err := a.Send(b.self, frame); !errors.Is(err, errFakeNotEstablished) {
		t.Fatalf("Send before connect: err = %v, want %v", err, errFakeNotEstablished)
	}
	if got := a.sentFrames(b.self); len(got) != 0 {
		t.Fatalf("Send before connect recorded %d frames, want 0", len(got))
	}

	connect(a, b, []byte("salt"))
	flushAll(t, a, b)
	disconnect(a, b)
	flushAll(t, a, b)

	if err := a.Send(b.self, frame); !errors.Is(err, errFakeNotEstablished) {
		t.Fatalf("Send after disconnect: err = %v, want %v", err, errFakeNotEstablished)
	}
	if got := a.sentFrames(b.self); len(got) != 0 {
		t.Fatalf("Send after disconnect recorded %d frames, want 0", len(got))
	}
}

// TestFakeLinkDropsFramesOfOldGeneration checks that frames queued under an old
// connection are dropped after the link is rebuilt.
func TestFakeLinkDropsFramesOfOldGeneration(t *testing.T) {
	a := newFakeLink(t, fakeLinkFingerprint(1))
	b := newFakeLink(t, fakeLinkFingerprint(2))
	kind := link.FrameKind(topicpb.KindRoster)

	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var got []byte
	var events []string

	b.HandleState(kind, func(_ link.Fingerprint, f link.Frame) {
		mu.Lock()
		got = append(got, f.Payload[0])
		events = append(events, "frame")
		first := len(got) == 1
		mu.Unlock()
		if first {
			close(started)
			<-release
		}
	})
	b.Subscribe(
		func(link.Fingerprint, []byte) {
			mu.Lock()
			events = append(events, "up")
			mu.Unlock()
		},
		func(link.Fingerprint) {
			mu.Lock()
			events = append(events, "down")
			mu.Unlock()
		},
	)

	connect(a, b, []byte("salt-1"))
	flushAll(t, a, b)

	if err := a.Send(b.self, link.Frame{Kind: kind, Payload: []byte{1}}); err != nil {
		t.Fatalf("Send first frame: %v", err)
	}
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatalf("first frame was not delivered within %s", eventTimeout)
	}

	if err := a.Send(b.self, link.Frame{Kind: kind, Payload: []byte{2}}); err != nil {
		t.Fatalf("Send second frame: %v", err)
	}
	if err := a.Send(b.self, link.Frame{Kind: kind, Payload: []byte{3}}); err != nil {
		t.Fatalf("Send third frame: %v", err)
	}

	disconnect(a, b)
	connect(a, b, []byte("salt-2"))
	close(release)
	flushAll(t, a, b)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("received frames %v, want only the first frame", got)
	}
	want := []string{"up", "frame", "down", "up"}
	if len(events) != len(want) {
		t.Fatalf("callback events %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("callback events %v, want %v", events, want)
		}
	}
}

// TestFakeLinkFlushWaits checks that flushAll waits for every handler to run.
func TestFakeLinkFlushWaits(t *testing.T) {
	a := newFakeLink(t, fakeLinkFingerprint(1))
	b := newFakeLink(t, fakeLinkFingerprint(2))
	kind := link.FrameKind(topicpb.KindRosterDelta)

	var count atomic.Int32
	b.HandleState(kind, func(link.Fingerprint, link.Frame) {
		count.Add(1)
	})

	connect(a, b, []byte("salt"))
	flushAll(t, a, b)

	const n = 25
	for i := 0; i < n; i++ {
		if err := a.Send(b.self, link.Frame{Kind: kind, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("Send frame %d: %v", i, err)
		}
	}
	flushAll(t, a, b)

	if got := count.Load(); got != n {
		t.Fatalf("handler ran %d times, want %d", got, n)
	}
}

// TestFakeLinkFlushWaitsForForwardedFrame checks that flushAll does not return
// while a frame forwarded during the pass is still queued.
func TestFakeLinkFlushWaitsForForwardedFrame(t *testing.T) {
	kind := link.FrameKind(topicpb.KindTopicDigests)

	for iter := 0; iter < 20; iter++ {
		a := newFakeLink(t, fakeLinkFingerprint(1))
		b := newFakeLink(t, fakeLinkFingerprint(2))

		var count atomic.Int32
		a.HandleState(kind, func(link.Fingerprint, link.Frame) {
			count.Add(1)
		})
		b.HandleState(kind, func(link.Fingerprint, link.Frame) {
			if err := b.Send(a.self, link.Frame{Kind: kind, Payload: []byte{9}}); err != nil {
				t.Errorf("iteration %d: b.Send: %v", iter, err)
			}
		})

		connect(a, b, []byte("salt"))
		flushAll(t, a, b)

		if err := a.Send(b.self, link.Frame{Kind: kind, Payload: []byte{1}}); err != nil {
			t.Fatalf("iteration %d: a.Send: %v", iter, err)
		}
		flushAll(t, a, b)

		if got := count.Load(); got != 1 {
			t.Fatalf("iteration %d: flushAll returned before the forwarded frame was delivered, got %d", iter, got)
		}
	}
}
