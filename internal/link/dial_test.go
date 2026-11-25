package link

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// unreachableAddr is intercepted by dialContext and fails the dial, so a test
// that needs a failed dial does not depend on a port staying free.
const unreachableAddr = "127.0.0.1:1"

type dialEvent struct {
	up   bool
	peer Fingerprint
	salt []byte
}

type dialHarness struct {
	l        *Link
	peers    *linktest.FakePeers
	settings *linktest.FakeSettings
	clock    *linktest.FakeClock

	mu     sync.Mutex
	events []dialEvent
	dialed []string
}

func newDialHarness(t *testing.T) *dialHarness {
	t.Helper()
	return newDialHarnessWithClock(t, nil)
}

func newDialHarnessWithClock(t *testing.T, wrap func(*linktest.FakeClock) Clock) *dialHarness {
	t.Helper()
	peers := linktest.NewFakePeers(t)
	settings := linktest.NewFakeSettings("self")
	clock := linktest.NewFakeClock(time.Unix(1_700_000_000, 0))
	var linkClock Clock = clock
	if wrap != nil {
		linkClock = wrap(clock)
	}
	h := &dialHarness{peers: peers, settings: settings, clock: clock}
	l, err := New(Options{
		Peers:    peers,
		Settings: settings,
		Clock:    linkClock,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.l = l
	l.dialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == unreachableAddr {
			return nil, &net.OpError{Op: "dial", Net: network, Err: errors.New("connection refused")}
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	l.Subscribe(h.onUp, h.onDown)

	t.Cleanup(l.notifier.stop)
	t.Cleanup(func() {
		for _, peer := range peers.Peers() {
			l.Drop(peer.Fingerprint)
		}
	})
	return h
}

func (h *dialHarness) onUp(peer Fingerprint, salt []byte) {
	h.mu.Lock()
	h.events = append(h.events, dialEvent{up: true, peer: peer, salt: append([]byte(nil), salt...)})
	h.mu.Unlock()
}

func (h *dialHarness) onDown(peer Fingerprint) {
	h.mu.Lock()
	h.events = append(h.events, dialEvent{peer: peer})
	h.mu.Unlock()
}

func (h *dialHarness) recordDial(address string) {
	h.mu.Lock()
	h.dialed = append(h.dialed, address)
	h.mu.Unlock()
}

func (h *dialHarness) start() {
	SetTestKnobs(h.l, TestKnobs{OnDial: h.recordDial})
	h.l.Start()
}

func (h *dialHarness) dialedAddrs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.dialed...)
}

func (h *dialHarness) dialCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.dialed)
}

func (h *dialHarness) waitDialCount(t *testing.T, n int) {
	t.Helper()
	waitFor(t, func() bool { return h.dialCount() >= n })
}

func (h *dialHarness) waitEventCount(t *testing.T, n int) []dialEvent {
	t.Helper()
	waitFor(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.events) >= n
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]dialEvent(nil), h.events...)
}

func (h *dialHarness) waitUp(t *testing.T, peer Fingerprint) []byte {
	t.Helper()
	waitFor(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, ev := range h.events {
			if ev.up && ev.peer == peer {
				return true
			}
		}
		return false
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ev := range h.events {
		if ev.up && ev.peer == peer {
			return ev.salt
		}
	}
	return nil
}

func (h *dialHarness) upCount(peer Fingerprint) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, ev := range h.events {
		if ev.up && ev.peer == peer {
			n++
		}
	}
	return n
}

func (h *dialHarness) downCount(peer Fingerprint) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, ev := range h.events {
		if !ev.up && ev.peer == peer {
			n++
		}
	}
	return n
}

func (h *dialHarness) state(peer Fingerprint) LinkState {
	return h.l.State(peer)
}

func (h *dialHarness) waitState(t *testing.T, peer Fingerprint, what string, cond func(LinkState) bool) LinkState {
	t.Helper()
	waitFor(t, func() bool { return cond(h.l.State(peer)) })
	st := h.l.State(peer)
	if !cond(st) {
		t.Fatalf("link state of %s: %+v (%s)", peer, st, what)
	}
	return st
}

func (h *dialHarness) waitWorkers(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		h.l.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatalf("dial loop did not exit within %v", eventTimeout)
	}
}

func (h *dialHarness) hasEvent(kind config.EventKind) bool {
	for _, ev := range h.settings.Events() {
		if ev.Event == kind {
			return true
		}
	}
	return false
}

func serveAcceptor(t *testing.T, h *dialHarness) string {
	return serveAcceptorWith(t, h, nil)
}

func serveAcceptorWith(t *testing.T, h *dialHarness, onStream func(c *conn, tr *trackedConn)) string {
	t.Helper()
	if onStream == nil {
		onStream = func(c *conn, tr *trackedConn) {
			if delivered, ok := h.l.serveInbound(c, tr); ok {
				h.l.runConn(c, delivered)
			}
		}
	}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ln := &trackingListener{Listener: raw, reg: h.l.unauth}
	srv := h.l.newGRPCServer(onStream)
	h.l.notifier.start()
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
	})
	return raw.Addr().String()
}

// TestREQBrokerPairing019_StartDialsDialerPeersImmediately checks that Start
// dials every peer this side is the dialer of and never dials an
// acceptor-side peer.
func TestREQBrokerPairing019_StartDialsDialerPeersImmediately(t *testing.T) {
	dialer := newDialHarness(t)
	acceptor := newDialHarness(t)
	addr := serveAcceptor(t, acceptor)

	dialerFP := dialer.peers.Fingerprint()
	acceptorFP := acceptor.peers.Fingerprint()
	dialer.peers.Add(acceptorFP, pairing.RoleDialer, "acceptor", addr)
	acceptor.peers.Add(dialerFP, pairing.RoleAcceptor, "dialer")

	otherFP := linktest.NewIdentity(t).Fingerprint
	dialer.peers.Add(otherFP, pairing.RoleAcceptor, "other")

	dialer.start()

	dialer.waitUp(t, acceptorFP)
	acceptor.waitUp(t, dialerFP)

	if st := dialer.state(otherFP); st.Phase != PhaseWaiting {
		t.Fatalf("acceptor-side peer phase = %v, want waiting", st.Phase)
	}
	addrs := dialer.dialedAddrs()
	if len(addrs) != 1 || addrs[0] != addr {
		t.Fatalf("dialed %v, want only %s", addrs, addr)
	}
}

// TestDialOrderUsesLastConnectedFirst checks that a dial round tries the last
// connected address first.
func TestDialOrderUsesLastConnectedFirst(t *testing.T) {
	dialer := newDialHarness(t)
	acceptor := newDialHarness(t)
	good := serveAcceptor(t, acceptor)
	bad := unreachableAddr

	dialerFP := dialer.peers.Fingerprint()
	acceptorFP := acceptor.peers.Fingerprint()
	dialer.peers.Add(acceptorFP, pairing.RoleDialer, "acceptor", bad, good)
	dialer.peers.SetCandidates(acceptorFP, []string{bad, good}, good)
	acceptor.peers.Add(dialerFP, pairing.RoleAcceptor, "dialer")

	dialer.start()
	dialer.waitUp(t, acceptorFP)

	addrs := dialer.dialedAddrs()
	if len(addrs) == 0 || addrs[0] != good {
		t.Fatalf("first dialed address %v, want %s", addrs, good)
	}
}

// TestFingerprintMismatchSkipsAddress checks that an address whose certificate
// fingerprint is not the peer's is abandoned and the next one is tried.
func TestFingerprintMismatchSkipsAddress(t *testing.T) {
	dialer := newDialHarness(t)
	target := newDialHarness(t)
	impostor := newDialHarness(t)

	targetAddr := serveAcceptor(t, target)
	var impostorStreams atomic.Int32
	impostorAddr := serveAcceptorWith(t, impostor, func(c *conn, tr *trackedConn) {
		impostorStreams.Add(1)
		c.closeAsync(FailProtocol)
	})

	dialerFP := dialer.peers.Fingerprint()
	targetFP := target.peers.Fingerprint()
	dialer.peers.Add(targetFP, pairing.RoleDialer, "target", impostorAddr, targetAddr)
	target.peers.Add(dialerFP, pairing.RoleAcceptor, "dialer")

	dialer.start()
	dialer.waitUp(t, targetFP)

	if n := impostorStreams.Load(); n != 0 {
		t.Fatalf("impostor saw %d streams, want 0", n)
	}
	if st := dialer.state(targetFP); st.Phase != PhaseEstablished {
		t.Fatalf("phase = %v, want established", st.Phase)
	}
}

// TestAllCandidatesFailBackoff checks that a round in which every candidate
// fails enters backoff, with the interval doubling and the timer armed for the
// published NextDial.
func TestAllCandidatesFailBackoff(t *testing.T) {
	dialer := newDialHarness(t)
	fp := linktest.NewIdentity(t).Fingerprint
	addr := unreachableAddr
	dialer.peers.Add(fp, pairing.RoleDialer, "peer", addr)

	dialer.start()

	first := dialer.waitState(t, fp, "first backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && !st.NextDial.IsZero()
	})
	if first.LastFailure != FailUnreachable {
		t.Fatalf("last failure = %v, want unreachable", first.LastFailure)
	}
	if want := dialer.clock.Now().Add(time.Second); !first.NextDial.Equal(want) {
		t.Fatalf("next dial = %v, want %v", first.NextDial, want)
	}

	dialer.clock.WaitPending(t, 1)
	dialer.clock.Advance(time.Second)
	dialer.waitDialCount(t, 2)

	second := dialer.waitState(t, fp, "second backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && st.NextDial.After(dialer.clock.Now())
	})
	if want := dialer.clock.Now().Add(2 * time.Second); !second.NextDial.Equal(want) {
		t.Fatalf("next dial = %v, want %v", second.NextDial, want)
	}

	dialer.clock.WaitPending(t, 1)
	earliest, ok := dialer.clock.Earliest()
	if !ok {
		t.Fatal("no pending backoff timer")
	}
	if !earliest.Equal(second.NextDial) {
		t.Fatalf("earliest timer deadline = %v, want next dial %v", earliest, second.NextDial)
	}
}

// delayedAfterFuncClock delays every timer registration, widening the window
// between a backoff becoming visible and its timer being armed.
type delayedAfterFuncClock struct {
	Clock
	delay time.Duration
}

func (c delayedAfterFuncClock) AfterFunc(d time.Duration, f func()) func() bool {
	time.Sleep(c.delay)
	return c.Clock.AfterFunc(d, f)
}

// TestBackoffTimerIsRegisteredWhenPhaseIsVisible checks that a dialer which
// publishes PhaseBackoff has already armed the timer that ends the backoff.
func TestBackoffTimerIsRegisteredWhenPhaseIsVisible(t *testing.T) {
	dialer := newDialHarnessWithClock(t, func(c *linktest.FakeClock) Clock {
		return delayedAfterFuncClock{Clock: c, delay: 250 * time.Millisecond}
	})
	fp := linktest.NewIdentity(t).Fingerprint
	dialer.peers.Add(fp, pairing.RoleDialer, "peer", unreachableAddr)
	dialer.start()

	dialer.waitState(t, fp, "backoff published with its timer", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && !st.NextDial.IsZero()
	})
	before := dialer.dialCount()
	dialer.clock.Advance(time.Second)
	dialer.waitDialCount(t, before+1)
}

// TestKickEndsBackoff checks that Kick makes a dial loop in backoff dial at
// once.
func TestKickEndsBackoff(t *testing.T) {
	dialer := newDialHarness(t)
	fp := linktest.NewIdentity(t).Fingerprint
	dialer.peers.Add(fp, pairing.RoleDialer, "peer", unreachableAddr)

	dialer.start()
	dialer.waitState(t, fp, "backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff
	})
	before := dialer.dialCount()

	dialer.l.Kick(fp)
	dialer.waitDialCount(t, before+1)
}

// TestREQBrokerPairing035_RejectedStopsRedialing checks that a peer rejecting
// with not-a-peer moves the link to rejected, audits it and stops dialing.
func TestREQBrokerPairing035_RejectedStopsRedialing(t *testing.T) {
	dialer := newDialHarness(t)
	acceptor := newDialHarness(t)
	addr := serveAcceptor(t, acceptor)

	acceptorFP := acceptor.peers.Fingerprint()
	dialer.peers.Add(acceptorFP, pairing.RoleDialer, "acceptor", addr)

	dialer.start()

	st := dialer.waitState(t, acceptorFP, "rejected", func(st LinkState) bool {
		return st.Phase == PhaseRejected
	})
	if st.LastFailure != FailNotAPeer {
		t.Fatalf("last failure = %v, want not_a_peer", st.LastFailure)
	}
	waitFor(t, func() bool { return dialer.hasEvent(config.EventRejectedByPeer) })
	ev := dialer.settings.Events()
	var found bool
	for _, e := range ev {
		if e.Event == config.EventRejectedByPeer {
			found = true
			if e.Peer == nil || e.Peer.Fingerprint != acceptorFP {
				t.Fatalf("rejected_by_peer peer = %+v, want %s", e.Peer, acceptorFP)
			}
		}
	}
	if !found {
		t.Fatal("no rejected_by_peer audit event")
	}

	before := dialer.dialCount()
	dialer.waitWorkers(t)
	if n := dialer.clock.Pending(); n != 0 {
		t.Fatalf("%d fake timers pending after the rejection, want none", n)
	}
	for i := 0; i < 3; i++ {
		dialer.clock.Advance(10 * time.Minute)
	}
	dialer.l.Kick(acceptorFP)
	if got := dialer.dialCount(); got != before {
		t.Fatalf("dialed %d more times after rejection, want none", got-before)
	}
}

// TestRoleMismatchBacksOff checks that a role mismatch puts the dialer into
// backoff and it dials again after the interval.
func TestRoleMismatchBacksOff(t *testing.T) {
	dialer := newDialHarness(t)
	acceptor := newDialHarness(t)
	addr := serveAcceptor(t, acceptor)

	dialerFP := dialer.peers.Fingerprint()
	acceptorFP := acceptor.peers.Fingerprint()
	dialer.peers.Add(acceptorFP, pairing.RoleDialer, "acceptor", addr)
	acceptor.peers.Add(dialerFP, pairing.RoleDialer, "dialer")

	dialer.start()

	st := dialer.waitState(t, acceptorFP, "backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff
	})
	if st.LastFailure != FailRoleMismatch {
		t.Fatalf("last failure = %v, want role_mismatch", st.LastFailure)
	}

	dialer.clock.WaitPending(t, 1)
	before := dialer.dialCount()
	dialer.clock.Advance(time.Second)
	dialer.waitDialCount(t, before+1)
}

// TestREQBrokerPairing018_ReconnectAfterOutage checks that a dialer backs off
// with connection_lost after an outage and reconnects after the interval.
func TestREQBrokerPairing018_ReconnectAfterOutage(t *testing.T) {
	dialer := newDialHarness(t)
	acceptor := newDialHarness(t)
	acceptorAddr := serveAcceptor(t, acceptor)
	proxy := linktest.NewProxy(t, acceptorAddr)

	dialerFP := dialer.peers.Fingerprint()
	acceptorFP := acceptor.peers.Fingerprint()
	dialer.peers.Add(acceptorFP, pairing.RoleDialer, "acceptor", proxy.Addr())
	acceptor.peers.Add(dialerFP, pairing.RoleAcceptor, "dialer")

	dialer.start()
	dialer.waitUp(t, acceptorFP)
	acceptor.waitUp(t, dialerFP)

	obs := dialer.peers.Observed(acceptorFP)
	if len(obs.Connected) == 0 || obs.Connected[len(obs.Connected)-1] != proxy.Addr() {
		t.Fatalf("connected addresses = %v, want last %s", obs.Connected, proxy.Addr())
	}

	proxy.CloseConns()
	waitFor(t, func() bool { return dialer.downCount(acceptorFP) >= 1 })

	st := dialer.waitState(t, acceptorFP, "backoff after outage", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && !st.NextDial.IsZero()
	})
	if st.LastFailure != FailConnectionLost {
		t.Fatalf("last failure = %v, want connection_lost", st.LastFailure)
	}
	if want := dialer.clock.Now().Add(time.Second); !st.NextDial.Equal(want) {
		t.Fatalf("next dial = %v, want %v", st.NextDial, want)
	}

	dialer.clock.WaitPending(t, 1)
	dialer.clock.Advance(time.Second)
	waitFor(t, func() bool { return dialer.upCount(acceptorFP) >= 2 })
	if st := dialer.state(acceptorFP); st.Phase != PhaseEstablished {
		t.Fatalf("phase after reconnect = %v, want established", st.Phase)
	}
}

// TestBackoffCappedOverManyFailures checks that the backoff interval stays
// within the configured maximum over many failures.
func TestBackoffCappedOverManyFailures(t *testing.T) {
	dialer := newDialHarness(t)
	fp := linktest.NewIdentity(t).Fingerprint
	dialer.peers.Add(fp, pairing.RoleDialer, "peer", unreachableAddr)
	dialer.start()

	const max = 60 * time.Second
	for i := 0; i < 10; i++ {
		st := dialer.waitState(t, fp, "backoff", func(st LinkState) bool {
			return st.Phase == PhaseBackoff && !st.NextDial.IsZero()
		})
		d := st.NextDial.Sub(dialer.clock.Now())
		if d <= 0 || d > max {
			t.Fatalf("round %d backoff = %v, want within (0, %v]", i+1, d, max)
		}
		dialer.clock.Advance(d)
		dialer.waitDialCount(t, i+2)
	}
}

// TestBackoffReadsSettingsEachTime checks that a reconnect bound changed during
// a backoff applies to the next round.
func TestBackoffReadsSettingsEachTime(t *testing.T) {
	dialer := newDialHarness(t)
	fp := linktest.NewIdentity(t).Fingerprint
	dialer.peers.Add(fp, pairing.RoleDialer, "peer", unreachableAddr)
	dialer.start()

	dialer.waitState(t, fp, "first backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && !st.NextDial.IsZero()
	})
	dialer.settings.SetReconnect(3*time.Second, 60*time.Second)

	dialer.clock.WaitPending(t, 1)
	dialer.clock.Advance(time.Second)
	dialer.waitDialCount(t, 2)

	st := dialer.waitState(t, fp, "second backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && st.NextDial.After(dialer.clock.Now())
	})
	if want := dialer.clock.Now().Add(6 * time.Second); !st.NextDial.Equal(want) {
		t.Fatalf("next dial = %v, want %v", st.NextDial, want)
	}
}

// TestSuccessResetsBackoff checks that an established link resets the doubling
// backoff to the configured minimum.
func TestSuccessResetsBackoff(t *testing.T) {
	dialer := newDialHarness(t)
	acceptor := newDialHarness(t)
	proxy := linktest.NewProxy(t, serveAcceptor(t, acceptor))

	acceptorFP := acceptor.peers.Fingerprint()
	dialerFP := dialer.peers.Fingerprint()
	dialer.peers.Add(acceptorFP, pairing.RoleDialer, "acceptor", unreachableAddr)
	acceptor.peers.Add(dialerFP, pairing.RoleAcceptor, "dialer")
	dialer.start()

	dialer.waitState(t, acceptorFP, "first backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && st.NextDial.After(dialer.clock.Now())
	})
	dialer.clock.WaitPending(t, 1)
	dialer.clock.Advance(time.Second)

	second := dialer.waitState(t, acceptorFP, "second backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && st.NextDial.After(dialer.clock.Now())
	})
	if want := dialer.clock.Now().Add(2 * time.Second); !second.NextDial.Equal(want) {
		t.Fatalf("second next dial = %v, want %v", second.NextDial, want)
	}

	dialer.peers.SetCandidates(acceptorFP, []string{proxy.Addr()}, "")
	dialer.clock.WaitPending(t, 1)
	dialer.clock.Advance(2 * time.Second)
	dialer.waitUp(t, acceptorFP)
	acceptor.waitUp(t, dialerFP)

	proxy.CloseConns()
	waitFor(t, func() bool { return dialer.downCount(acceptorFP) >= 1 })
	st := dialer.waitState(t, acceptorFP, "backoff after outage", func(st LinkState) bool {
		return st.Phase == PhaseBackoff && st.NextDial.After(dialer.clock.Now())
	})
	if want := dialer.clock.Now().Add(time.Second); !st.NextDial.Equal(want) {
		t.Fatalf("next dial after success = %v, want %v (backoff not reset)", st.NextDial, want)
	}
}

// TestPairDialRunsConnAfterDrop checks that a connection PairDial installed
// still runs after Drop removes the record, so Drop's down notification
// completes.
func TestPairDialRunsConnAfterDrop(t *testing.T) {
	dialer, acceptor, addr := pairHarness(t)
	acceptorFP := acceptor.peers.Fingerprint()

	var dropOnce sync.Once
	dialer.peers.SetConnectedHook(func(fp Fingerprint, _ string) {
		if fp != acceptorFP {
			return
		}
		dropOnce.Do(func() { dialer.l.Drop(fp) })
	})

	if err := dialer.l.PairDial(context.Background(), addr, linktest.FakePairRun(dialer.peers, "acceptor")); err != nil {
		t.Fatalf("PairDial: %v", err)
	}

	waitFor(t, func() bool { return dialer.downCount(acceptorFP) >= 1 })

	dialed := make(chan error, 1)
	go func() {
		dialed <- dialer.l.PairDial(context.Background(), addr, linktest.FakePairRun(dialer.peers, "acceptor"))
	}()
	select {
	case err := <-dialed:
		if err != nil {
			t.Fatalf("second PairDial: %v", err)
		}
	case <-time.After(eventTimeout):
		t.Fatalf("second PairDial did not return within %v", eventTimeout)
	}
	waitFor(t, func() bool { return dialer.upCount(acceptorFP) >= 2 })

	closed := make(chan struct{})
	go func() {
		dialer.l.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(eventTimeout):
		t.Fatalf("Close did not return within %v", eventTimeout)
	}
}

// TestClosedDialLoopClosesInitialConnection checks that startDialLoop closes an
// initial connection Close never collected before running it.
func TestClosedDialLoopClosesInitialConnection(t *testing.T) {
	h := newDialHarness(t)
	var (
		c       *conn
		handled atomic.Bool
	)
	h.l.HandleState(200, func(Fingerprint, Frame) {
		handled.Store(true)
		c.closeAsync(FailConnectionLost)
	})
	h.l.Close()

	fp := Fingerprint{0xC1}
	c = newConn(nil, nil, peerIdentity{fp: fp}, 8)
	c.in <- &linkpb.Frame{Kind: 200}
	delivered := make(chan struct{})
	close(delivered)
	h.l.startDialLoop(fp, c, delivered, false)

	if handled.Load() {
		t.Fatal("state handler ran after Close returned")
	}
}

// TestDropStopsDialLoop checks that Drop removes the record, stops the dial loop
// and cancels its timer.
func TestDropStopsDialLoop(t *testing.T) {
	dialer := newDialHarness(t)
	fp := linktest.NewIdentity(t).Fingerprint
	dialer.peers.Add(fp, pairing.RoleDialer, "peer", unreachableAddr)
	dialer.start()

	dialer.waitState(t, fp, "backoff", func(st LinkState) bool {
		return st.Phase == PhaseBackoff
	})
	before := dialer.dialCount()

	dialer.l.Drop(fp)
	if st := dialer.state(fp); st.Phase != PhaseNone {
		t.Fatalf("phase after Drop = %v, want none", st.Phase)
	}
	dialer.waitWorkers(t)
	if n := dialer.clock.Pending(); n != 0 {
		t.Fatalf("%d fake timers pending after Drop, want none", n)
	}
	dialer.clock.Advance(time.Hour)
	if got := dialer.dialCount(); got != before {
		t.Fatalf("dialed %d more times after Drop, want none", got-before)
	}
}
