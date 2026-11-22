package link

import (
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// The fakes must implement the narrow interfaces link depends on.
var (
	_ Peers    = (*linktest.FakePeers)(nil)
	_ Settings = (*linktest.FakeSettings)(nil)
	_ Clock    = (*linktest.FakeClock)(nil)
)

// recordedEvent is one callback the test harness observed.
type recordedEvent struct {
	up   bool
	peer Fingerprint
	salt []byte
}

// recordHarness is a Link with a recording subscription and a started
// notifier.
type recordHarness struct {
	t        *testing.T
	l        *Link
	peers    *linktest.FakePeers
	settings *linktest.FakeSettings
	clock    *linktest.FakeClock

	mu     sync.Mutex
	events []recordedEvent
}

func newRecordHarness(t *testing.T) *recordHarness {
	t.Helper()
	return newRecordHarnessWith(t, linktest.NewFakePeers(t), linktest.NewFakeSettings("self"), linktest.NewFakeClock(time.Unix(1700000000, 0)))
}

func newRecordHarnessWith(t *testing.T, peers *linktest.FakePeers, settings *linktest.FakeSettings, clock *linktest.FakeClock) *recordHarness {
	t.Helper()
	l, err := New(Options{Peers: peers, Settings: settings, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &recordHarness{t: t, l: l, peers: peers, settings: settings, clock: clock}
	l.Subscribe(h.onUp, h.onDown)
	l.notifier.start()
	t.Cleanup(l.notifier.stop)
	return h
}

func (h *recordHarness) onUp(peer Fingerprint, salt []byte) {
	h.mu.Lock()
	h.events = append(h.events, recordedEvent{up: true, peer: peer, salt: append([]byte(nil), salt...)})
	h.mu.Unlock()
}

func (h *recordHarness) onDown(peer Fingerprint) {
	h.mu.Lock()
	h.events = append(h.events, recordedEvent{peer: peer})
	h.mu.Unlock()
}

// snapshot returns a copy of the recorded events.
func (h *recordHarness) snapshot() []recordedEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]recordedEvent(nil), h.events...)
}

// waitCount blocks until n events were recorded and returns them.
func (h *recordHarness) waitCount(t *testing.T, n int) []recordedEvent {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		evs := h.snapshot()
		if len(evs) >= n {
			return evs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d events, got %+v", n, evs)
		}
		time.Sleep(time.Millisecond)
	}
}

// kickChan returns the kick channel of peer's record, or nil.
func (h *recordHarness) kickChan(peer Fingerprint) chan struct{} {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	rec := h.l.records[peer]
	if rec == nil {
		return nil
	}
	return rec.kick
}

// kickSignals drains and counts the pending kick signals.
func kickSignals(ch chan struct{}) int {
	if ch == nil {
		return 0
	}
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			return n
		}
	}
}

// newRecordConn builds a connection around one end of an in-memory pipe.
func newRecordConn(t *testing.T, peer Fingerprint, role pairing.Role) *conn {
	t.Helper()
	a, _ := newPipe(t)
	c := newConn(a, func() {}, peerIdentity{fp: peer, pub: make([]byte, 32), remote: "127.0.0.1:1"}, 8)
	c.role = role
	return c
}

// waitAudit returns at least n audit events of kind.
func waitAudit(t *testing.T, s *linktest.FakeSettings, kind config.EventKind, n int) []config.Event {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		var out []config.Event
		for _, e := range s.Events() {
			if e.Event == kind {
				out = append(out, e)
			}
		}
		if len(out) >= n {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d %s events, got %d", n, kind, len(out))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestInstallNotifiesUpWithSalt(t *testing.T) {
	h := newRecordHarness(t)
	peer := Fingerprint{1}
	c := newRecordConn(t, peer, pairing.RoleDialer)
	salt := []byte("salt-one")

	delivered, ok := h.l.install(peer, pairing.RoleDialer, c, salt, h.l.dropGeneration(peer))
	if !ok {
		t.Fatal("install returned false")
	}
	<-delivered

	evs := h.waitCount(t, 1)
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(evs), evs)
	}
	if !evs[0].up || evs[0].peer != peer || string(evs[0].salt) != "salt-one" {
		t.Fatalf("unexpected up event: %+v", evs[0])
	}

	state := h.l.State(peer)
	if state.Phase != PhaseEstablished || state.Role != pairing.RoleDialer {
		t.Fatalf("state after install = %+v", state)
	}

	audits := waitAudit(t, h.settings, config.EventLinkUp, 1)
	if audits[0].MergeKey != "" {
		t.Fatalf("link_up MergeKey = %q, want empty", audits[0].MergeKey)
	}
	if audits[0].Peer == nil || audits[0].Peer.Fingerprint != [32]byte(peer) {
		t.Fatalf("link_up Peer = %+v", audits[0].Peer)
	}
}

func TestReplaceNotifiesDownThenUpOnce(t *testing.T) {
	peers := linktest.NewFakePeers(t)
	settings := linktest.NewFakeSettings("self")
	clock := linktest.NewFakeClock(time.Unix(1700000000, 0))
	peer := Fingerprint{2}

	for i := 0; i < 200; i++ {
		h := newRecordHarnessWith(t, peers, settings, clock)
		salt1 := []byte{byte(i), 1}
		salt2 := []byte{byte(i), 2}

		c1 := newRecordConn(t, peer, pairing.RoleAcceptor)
		gen := h.l.dropGeneration(peer)
		delivered1, ok := h.l.install(peer, pairing.RoleAcceptor, c1, salt1, gen)
		if !ok {
			t.Fatalf("iteration %d: install c1 returned false", i)
		}
		<-delivered1

		c2 := newRecordConn(t, peer, pairing.RoleAcceptor)
		var (
			wg         sync.WaitGroup
			delivered2 <-chan struct{}
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			c1.closeAsync(FailConnectionLost)
			c1.markStateDone()
			h.l.teardown(c1)
		}()
		go func() {
			defer wg.Done()
			delivered2, _ = h.l.install(peer, pairing.RoleAcceptor, c2, salt2, gen)
		}()
		wg.Wait()
		if delivered2 == nil {
			t.Fatalf("iteration %d: install c2 returned false", i)
		}
		<-delivered2

		evs := h.waitCount(t, 3)
		if len(evs) != 3 {
			t.Fatalf("iteration %d: got %d events, want 3: %+v", i, len(evs), evs)
		}
		if !evs[0].up || string(evs[0].salt) != string(salt1) {
			t.Fatalf("iteration %d: first event = %+v", i, evs[0])
		}
		if evs[1].up {
			t.Fatalf("iteration %d: second event is not down: %+v", i, evs[1])
		}
		if !evs[2].up || string(evs[2].salt) != string(salt2) {
			t.Fatalf("iteration %d: third event = %+v", i, evs[2])
		}
		select {
		case <-c1.done():
		default:
			t.Fatalf("iteration %d: replaced connection is still open", i)
		}
	}
}

func TestTeardownSetsWaitingOrBackoff(t *testing.T) {
	h := newRecordHarness(t)

	acceptor := Fingerprint{3}
	cA := newRecordConn(t, acceptor, pairing.RoleAcceptor)
	dA, ok := h.l.install(acceptor, pairing.RoleAcceptor, cA, []byte("a"), h.l.dropGeneration(acceptor))
	if !ok {
		t.Fatal("install acceptor returned false")
	}
	<-dA
	cA.closeAsync(FailQueueFull)
	cA.markStateDone()
	h.l.teardown(cA)

	state := h.l.State(acceptor)
	if state.Phase != PhaseWaiting {
		t.Fatalf("acceptor phase after teardown = %v, want waiting", state.Phase)
	}
	if state.LastFailure != FailQueueFull {
		t.Fatalf("acceptor failure after teardown = %v", state.LastFailure)
	}

	dialer := Fingerprint{4}
	cB := newRecordConn(t, dialer, pairing.RoleDialer)
	dB, ok := h.l.install(dialer, pairing.RoleDialer, cB, []byte("b"), h.l.dropGeneration(dialer))
	if !ok {
		t.Fatal("install dialer returned false")
	}
	<-dB
	cB.closeAsync(FailConnectionLost)
	cB.markStateDone()
	h.l.teardown(cB)

	state = h.l.State(dialer)
	if state.Phase != PhaseBackoff {
		t.Fatalf("dialer phase after teardown = %v, want backoff", state.Phase)
	}
	if state.LastFailure != FailConnectionLost {
		t.Fatalf("dialer failure after teardown = %v", state.LastFailure)
	}

	if evs := h.waitCount(t, 4); len(evs) != 4 {
		t.Fatalf("got %d events, want 4: %+v", len(evs), evs)
	}

	downs := waitAudit(t, h.settings, config.EventLinkDown, 2)
	if downs[0].Detail != "queue_full" || downs[0].MergeKey != "" {
		t.Fatalf("first link_down = %+v", downs[0])
	}
	if downs[1].Detail != "connection_lost" || downs[1].MergeKey != "" {
		t.Fatalf("second link_down = %+v", downs[1])
	}
	if ups := waitAudit(t, h.settings, config.EventLinkUp, 2); len(ups) != 2 || ups[0].MergeKey != "" || ups[1].MergeKey != "" {
		t.Fatalf("link_up events = %+v", ups)
	}
}

func TestDropIsAsyncAndOrdered(t *testing.T) {
	h := newRecordHarness(t)
	peer := Fingerprint{5}
	role := pairing.RoleAcceptor

	c1 := newRecordConn(t, peer, role)
	d1, ok := h.l.install(peer, role, c1, []byte("one"), h.l.dropGeneration(peer))
	if !ok {
		t.Fatal("install c1 returned false")
	}
	<-d1

	var m sync.Mutex
	m.Lock()
	dropReturned := make(chan struct{})
	go func() {
		h.l.Drop(peer)
		close(dropReturned)
	}()
	select {
	case <-dropReturned:
	case <-time.After(eventTimeout):
		t.Fatal("Drop blocked while the caller held a lock")
	}

	gen := h.l.dropGeneration(peer)
	c2 := newRecordConn(t, peer, role)
	installed := make(chan bool, 1)
	go func() {
		_, ok := h.l.install(peer, role, c2, []byte("two"), gen)
		installed <- ok
	}()

	// install must wait for the in-flight Drop. The checks below read the state
	// that forces the wait, but that state alone does not show install stayed
	// blocked; what proves install only completed after Drop is the event order
	// checked at the end of the test.
	h.l.mu.Lock()
	dropping := h.l.dropping[peer] != nil
	h.l.mu.Unlock()
	if !dropping {
		t.Fatal("install ran without an in-flight Drop to wait for")
	}
	for _, ev := range h.snapshot() {
		if ev.peer == peer && !ev.up {
			t.Fatal("the drop's down event was posted before install was released")
		}
	}
	// Negative window: a late install that ignored the drop still returns here.
	select {
	case <-installed:
		t.Fatal("install returned before Drop finished")
	case <-time.After(50 * time.Millisecond):
	}

	c1.markStateDone()
	m.Unlock()

	select {
	case ok := <-installed:
		if !ok {
			t.Fatal("install after Drop returned false")
		}
	case <-time.After(eventTimeout):
		t.Fatal("install did not finish after Drop")
	}

	evs := h.waitCount(t, 3)
	if len(evs) != 3 {
		t.Fatalf("got %d events, want 3: %+v", len(evs), evs)
	}
	if !evs[0].up || string(evs[0].salt) != "one" {
		t.Fatalf("first event = %+v", evs[0])
	}
	if evs[1].up {
		t.Fatalf("second event is not down: %+v", evs[1])
	}
	if !evs[2].up || string(evs[2].salt) != "two" {
		t.Fatalf("third event = %+v", evs[2])
	}
}

func TestInstallFailsAfterDropGeneration(t *testing.T) {
	h := newRecordHarness(t)
	peer := Fingerprint{6}
	gen := h.l.dropGeneration(peer)
	h.l.Drop(peer)

	c := newRecordConn(t, peer, pairing.RoleAcceptor)
	if _, ok := h.l.install(peer, pairing.RoleAcceptor, c, []byte("x"), gen); ok {
		t.Fatal("install with a stale generation succeeded")
	}
}

func TestOnDownCallbackMayTakeLockHeldBySender(t *testing.T) {
	h := newRecordHarness(t)
	peer := Fingerprint{7}
	var m sync.Mutex
	entered := make(chan struct{})
	h.l.Subscribe(
		func(Fingerprint, []byte) {},
		func(Fingerprint) {
			m.Lock()
			close(entered)
			m.Unlock()
		},
	)

	c := newRecordConn(t, peer, pairing.RoleDialer)
	d, ok := h.l.install(peer, pairing.RoleDialer, c, []byte("s"), h.l.dropGeneration(peer))
	if !ok {
		t.Fatal("install returned false")
	}
	<-d

	m.Lock()
	dropReturned := make(chan struct{})
	go func() {
		h.l.Drop(peer)
		close(dropReturned)
	}()
	select {
	case <-dropReturned:
	case <-time.After(eventTimeout):
		t.Fatal("Drop blocked while the caller held the callback's lock")
	}
	m.Unlock()
	c.markStateDone()

	select {
	case <-entered:
	case <-time.After(eventTimeout):
		t.Fatal("onDown was not called after Drop")
	}
}

func TestCallbacksMayCallLinkMethods(t *testing.T) {
	h := newRecordHarness(t)
	peer := Fingerprint{8}
	other := Fingerprint{9}
	done := make(chan struct{})
	h.l.Subscribe(
		func(Fingerprint, []byte) {
			_ = h.l.State(other)
			h.l.Kick(other)
			h.l.Drop(other)
			close(done)
		},
		func(Fingerprint) {},
	)

	c := newRecordConn(t, peer, pairing.RoleAcceptor)
	if _, ok := h.l.install(peer, pairing.RoleAcceptor, c, []byte("s"), h.l.dropGeneration(peer)); !ok {
		t.Fatal("install returned false")
	}
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("onUp calling Link methods deadlocked")
	}
}

func TestNotificationsAreSerialAndOrdered(t *testing.T) {
	h := newRecordHarness(t)
	peers := []Fingerprint{{10}, {11}, {12}}

	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	seen := make(chan Fingerprint, len(peers))
	h.l.Subscribe(
		func(peer Fingerprint, _ []byte) {
			if peer == peers[0] {
				entered <- struct{}{}
				<-block
			}
			seen <- peer
		},
		func(Fingerprint) {},
	)

	for i, peer := range peers {
		c := newRecordConn(t, peer, pairing.RoleAcceptor)
		if _, ok := h.l.install(peer, pairing.RoleAcceptor, c, []byte{byte(i)}, h.l.dropGeneration(peer)); !ok {
			t.Fatalf("install peer %d returned false", i)
		}
		if i == 0 {
			select {
			case <-entered:
			case <-time.After(eventTimeout):
				t.Fatal("first callback did not start")
			}
		}
	}
	select {
	case peer := <-seen:
		t.Fatalf("second callback delivered while the first still blocks: %v", peer)
	default:
	}

	close(block)
	for i, want := range peers {
		select {
		case got := <-seen:
			if got != want {
				t.Fatalf("event %d delivered for %v, want %v", i, got, want)
			}
		case <-time.After(eventTimeout):
			t.Fatalf("event %d was not delivered", i)
		}
	}
}

func TestKickOnlyInDialerBackoff(t *testing.T) {
	h := newRecordHarness(t)

	// Accept-side record, waiting.
	acceptor := Fingerprint{12}
	cA := newRecordConn(t, acceptor, pairing.RoleAcceptor)
	dA, ok := h.l.install(acceptor, pairing.RoleAcceptor, cA, []byte("a"), h.l.dropGeneration(acceptor))
	if !ok {
		t.Fatal("install acceptor returned false")
	}
	<-dA
	cA.closeAsync(FailConnectionLost)
	cA.markStateDone()
	h.l.teardown(cA)
	h.l.Kick(acceptor)
	if n := kickSignals(h.kickChan(acceptor)); n != 0 {
		t.Fatalf("Kick on an accept-side record sent %d signals", n)
	}

	// Dialer record, established.
	established := Fingerprint{13}
	cE := newRecordConn(t, established, pairing.RoleDialer)
	dE, ok := h.l.install(established, pairing.RoleDialer, cE, []byte("e"), h.l.dropGeneration(established))
	if !ok {
		t.Fatal("install established returned false")
	}
	<-dE
	h.l.Kick(established)
	if n := kickSignals(h.kickChan(established)); n != 0 {
		t.Fatalf("Kick on an established link sent %d signals", n)
	}

	// Dialer record, rejected.
	rejected := Fingerprint{14}
	h.l.mu.Lock()
	h.l.ensureRecord(rejected, pairing.RoleDialer, PhaseRejected)
	h.l.mu.Unlock()
	h.l.Kick(rejected)
	if n := kickSignals(h.kickChan(rejected)); n != 0 {
		t.Fatalf("Kick on a rejected record sent %d signals", n)
	}

	// Dialer record, backoff.
	backoff := Fingerprint{15}
	cB := newRecordConn(t, backoff, pairing.RoleDialer)
	dB, ok := h.l.install(backoff, pairing.RoleDialer, cB, []byte("b"), h.l.dropGeneration(backoff))
	if !ok {
		t.Fatal("install backoff returned false")
	}
	<-dB
	cB.closeAsync(FailConnectionLost)
	cB.markStateDone()
	h.l.teardown(cB)

	kicked := make(chan struct{})
	go func() {
		h.l.Kick(backoff)
		h.l.Kick(backoff)
		close(kicked)
	}()
	select {
	case <-kicked:
	case <-time.After(eventTimeout):
		t.Fatal("repeated Kick blocked")
	}
	if n := kickSignals(h.kickChan(backoff)); n != 1 {
		t.Fatalf("two Kicks in backoff left %d signals, want 1", n)
	}
}

// waitNotifierStopped waits until stop set the stopped flag. The caller may
// release a blocked callback only after this returns, so the events queued
// behind it are still queued when stop runs. Releasing it earlier lets the
// delivery goroutine take the next event as in-flight before stop was called,
// and stop is allowed to let an in-flight delivery finish.
func waitNotifierStopped(t *testing.T, n *notifier) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		n.mu.Lock()
		stopped := n.stopped
		n.mu.Unlock()
		if stopped {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("notifier did not stop")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStopDiscardsAndReleasesWaiters(t *testing.T) {
	h := newRecordHarness(t)
	first := Fingerprint{16}
	second := Fingerprint{17}

	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	var called atomicCount
	h.l.Subscribe(
		func(peer Fingerprint, _ []byte) {
			if peer == first {
				entered <- struct{}{}
				<-block
			}
			called.inc()
		},
		func(Fingerprint) {},
	)

	c1 := newRecordConn(t, first, pairing.RoleAcceptor)
	if _, ok := h.l.install(first, pairing.RoleAcceptor, c1, []byte("1"), h.l.dropGeneration(first)); !ok {
		t.Fatal("install first returned false")
	}
	select {
	case <-entered:
	case <-time.After(eventTimeout):
		t.Fatal("first callback did not start")
	}

	c2 := newRecordConn(t, second, pairing.RoleAcceptor)
	delivered, ok := h.l.install(second, pairing.RoleAcceptor, c2, []byte("2"), h.l.dropGeneration(second))
	if !ok {
		t.Fatal("install second returned false")
	}

	stopped := make(chan struct{})
	go func() {
		h.l.notifier.stop()
		close(stopped)
	}()
	// Wait for stop to commit before releasing the blocked callback. Otherwise
	// the delivery goroutine may take the second event as in-flight before
	// stop is called, and the assertion below would count its callback even
	// though stop is allowed to let an in-flight delivery finish.
	waitNotifierStopped(t, h.l.notifier)
	close(block)

	select {
	case <-stopped:
	case <-time.After(eventTimeout):
		t.Fatal("stop did not return")
	}
	select {
	case <-delivered:
	default:
		t.Fatal("delivered of a discarded up event was not closed")
	}
	if n := called.get(); n != 1 {
		t.Fatalf("callbacks ran %d times after stop, want 1", n)
	}
}

// atomicCount is a small counter so the test needs no atomic import elsewhere.
type atomicCount struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCount) inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *atomicCount) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}
