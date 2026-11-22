package link_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// eventTimeout is the package-wide fallback for every wait in these tests. It
// only decides how long a wait that will never complete takes to fail: the
// success path selects on the event itself and never spends this time, so a
// larger value costs nothing. It equals the product's default
// link.keepalive_interval (15s) plus link.keepalive_timeout (10s), the same
// values linktest.FakeSettings returns: a link the product still considers
// alive must not be declared dead by the test before the product's own
// keepalive policy would.
const eventTimeout = 15*time.Second + 10*time.Second

// node is one Link together with its fake collaborators and a recording
// subscription. Tests build it through link.New so they exercise the same
// public constructor as production.
type node struct {
	*link.Link
	Peers    *linktest.FakePeers
	Settings *linktest.FakeSettings
	Clock    *linktest.FakeClock

	mu     sync.Mutex
	events []linkEvent
	notify chan struct{}
}

// linkEvent is one link up or down notification recorded for a node.
type linkEvent struct {
	up   bool
	peer link.Fingerprint
	salt []byte
}

// newNode builds a node with freshly generated collaborators, a fixed fake
// clock, a discard logger and a recording subscription. opts may change any
// link.Options field before construction.
func newNode(t *testing.T, alias string, opts ...func(*link.Options)) *node {
	t.Helper()
	return newNodeFrom(t,
		linktest.NewFakePeers(t),
		linktest.NewFakeSettings(alias),
		linktest.NewFakeClock(time.Unix(1_700_000_000, 0)),
		opts...)
}

// newNodeFrom builds a node around collaborators a test already owns. A restart
// test uses it to keep the same identity, peer table and settings while the
// Link instance changes.
func newNodeFrom(t *testing.T, peers *linktest.FakePeers, settings *linktest.FakeSettings, clock *linktest.FakeClock, opts ...func(*link.Options)) *node {
	t.Helper()
	options := link.Options{
		Peers:    peers,
		Settings: settings,
		Clock:    clock,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, opt := range opts {
		opt(&options)
	}
	l, err := link.New(options)
	if err != nil {
		t.Fatalf("link.New: %v", err)
	}
	n := &node{
		Link:     l,
		Peers:    peers,
		Settings: settings,
		Clock:    clock,
		notify:   make(chan struct{}),
	}
	l.Subscribe(n.onUp, n.onDown)
	t.Cleanup(l.Close)
	return n
}

func (n *node) onUp(peer link.Fingerprint, salt []byte) {
	n.mu.Lock()
	n.events = append(n.events, linkEvent{up: true, peer: peer, salt: append([]byte(nil), salt...)})
	n.wakeLocked()
	n.mu.Unlock()
}

func (n *node) onDown(peer link.Fingerprint) {
	n.mu.Lock()
	n.events = append(n.events, linkEvent{peer: peer})
	n.wakeLocked()
	n.mu.Unlock()
}

// wakeLocked wakes every waiter. The caller must hold n.mu.
func (n *node) wakeLocked() {
	close(n.notify)
	n.notify = make(chan struct{})
}

func (n *node) changed() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.notify
}

func (n *node) snapshot() []linkEvent {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]linkEvent, len(n.events))
	for i, ev := range n.events {
		out[i] = linkEvent{up: ev.up, peer: ev.peer, salt: append([]byte(nil), ev.salt...)}
	}
	return out
}

func (n *node) eventsOf(peer link.Fingerprint) []linkEvent {
	var out []linkEvent
	for _, ev := range n.snapshot() {
		if ev.peer == peer {
			out = append(out, ev)
		}
	}
	return out
}

func (n *node) upCount(peer link.Fingerprint) int {
	count := 0
	for _, ev := range n.eventsOf(peer) {
		if ev.up {
			count++
		}
	}
	return count
}

func (n *node) downCount(peer link.Fingerprint) int {
	count := 0
	for _, ev := range n.eventsOf(peer) {
		if !ev.up {
			count++
		}
	}
	return count
}

func (n *node) lastSalt(peer link.Fingerprint) []byte {
	events := n.eventsOf(peer)
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].up {
			return events[i].salt
		}
	}
	return nil
}

func (n *node) waitUp(t *testing.T, peer link.Fingerprint, atLeast int) {
	t.Helper()
	waitUntil(t, "link up", n.changed, func() bool { return n.upCount(peer) >= atLeast })
}

func (n *node) waitDown(t *testing.T, peer link.Fingerprint, atLeast int) {
	t.Helper()
	waitUntil(t, "link down", n.changed, func() bool { return n.downCount(peer) >= atLeast })
}

// waitUntil re-evaluates cond after every wake-up on changed and fails the test
// when cond is still false after eventTimeout.
func waitUntil(t testing.TB, what string, changed func() <-chan struct{}, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		ch := changed()
		if cond() {
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("link_test: timed out waiting for %s", what)
		}
		select {
		case <-ch:
		case <-time.After(remaining):
		}
	}
}

// waitWithin polls cond until it holds or d passes and reports whether it
// held. Unlike waitUntil it lets a test collect several independent failures
// before it ends.
func waitWithin(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// waitState watches a LinkState that has no notification surface of its own.
// It polls the public State method until cond holds, with the same eventTimeout
// bound as the event-driven waits.
func waitState(t *testing.T, l *link.Link, peer link.Fingerprint, what string, cond func(link.LinkState) bool) link.LinkState {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		st := l.State(peer)
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("link_test: timed out waiting for %s; state = %+v", what, st)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// listenAt binds n to a loopback port and returns the address it listens on.
func listenAt(t *testing.T, n *node) string {
	t.Helper()
	if err := n.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := n.Addr()
	if addr == nil {
		t.Fatal("Addr returned nil after Listen")
	}
	return addr.String()
}

// connect introduces dialer and acceptor, binds the acceptor to a loopback port
// and waits for both sides to report the link up.
func connect(t *testing.T, dialer, acceptor *node) {
	t.Helper()
	addr := listenAt(t, acceptor)
	connectVia(t, dialer, acceptor, addr)
}

// connectVia is connect with an explicit dial address, used when the dialer
// reaches the acceptor through a proxy.
func connectVia(t *testing.T, dialer, acceptor *node, dialAddr string) {
	t.Helper()
	linktest.Introduce(dialer.Peers, acceptor.Peers, dialer.Settings.Alias(), acceptor.Settings.Alias(), dialAddr)
	dialer.Start()
	acceptor.Start()
	dialer.waitUp(t, acceptor.Peers.Fingerprint(), 1)
	acceptor.waitUp(t, dialer.Peers.Fingerprint(), 1)
}

// startAcceptorThenDialer starts the accepting node before the dialing node.
//
// Link.Start creates one record per peer in the table, and the accepting side
// records a rejected inbound version only on an existing record
// (noteVersionFailure). A dial that reaches the listening acceptor before the
// acceptor has started is still rejected and the dialer still backs off, but
// the acceptor's record is created afterwards at PhaseWaiting with no failure.
// The dialer then stays in backoff, so no later event corrects the acceptor
// state and the test waits forever for a failure that was already handled.
// Starting the acceptor first removes that ordering race instead of widening a
// wait.
func startAcceptorThenDialer(acceptor, dialer *node) {
	acceptor.Start()
	dialer.Start()
}

// dialRecorder counts the addresses handed to the OnDial hook and wakes
// waiters on every attempt.
type dialRecorder struct {
	mu      sync.Mutex
	count   int
	changed chan struct{}
}

func newDialRecorder() *dialRecorder {
	return &dialRecorder{changed: make(chan struct{})}
}

func (d *dialRecorder) dial(string) {
	d.mu.Lock()
	d.count++
	close(d.changed)
	d.changed = make(chan struct{})
	d.mu.Unlock()
}

func (d *dialRecorder) n() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

func (d *dialRecorder) changedCh() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.changed
}

func (d *dialRecorder) waitAtLeast(t *testing.T, n int) {
	t.Helper()
	waitUntil(t, "dial attempts", d.changedCh, func() bool { return d.n() >= n })
}

// assertNoDial fails if a dial is recorded within d. A negative assertion has
// no event to wait for, so it waits for the absence of the event instead.
func (d *dialRecorder) assertNoDial(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case <-d.changedCh():
		t.Fatal("link dialed after Close")
	case <-time.After(within):
	}
}

func sameAddrs(a, b []config.AdvertisedAddr) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsAlias(aliases []string, want string) bool {
	for _, alias := range aliases {
		if alias == want {
			return true
		}
	}
	return false
}

// syncBuffer is a concurrency-safe bytes.Buffer for a slog JSON handler.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// logHasVersionMismatch reports whether text holds the version-mismatch log
// line with the given local and peer versions.
func logHasVersionMismatch(text string, local, peer uint32) bool {
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry["msg"] != "link: protocol version mismatch" {
			continue
		}
		localValue, okLocal := entry["local"].(float64)
		peerValue, okPeer := entry["peer_version"].(float64)
		return okLocal && okPeer && uint32(localValue) == local && uint32(peerValue) == peer
	}
	return false
}

// TestREQBrokerPairing027_AcceptorNeverDials checks that only the dialing side
// redials after a transport break and that Kick cannot make an acceptor dial.
func TestREQBrokerPairing027_AcceptorNeverDials(t *testing.T) {
	acceptor := newNode(t, "acceptor")
	dialer := newNode(t, "dialer")
	dials := newDialRecorder()
	link.SetTestKnobs(acceptor.Link, link.TestKnobs{OnDial: dials.dial})

	addr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, addr)
	connectVia(t, dialer, acceptor, proxy.Addr())

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()
	if got := dials.n(); got != 0 {
		t.Fatalf("acceptor dialed %d times before the link broke", got)
	}

	proxy.CloseConns()
	acceptor.waitDown(t, fpDialer, 1)
	dialer.waitDown(t, fpAcceptor, 1)

	// Kick on the accepting side must not start a dial or disturb recovery.
	acceptor.Kick(fpDialer)

	dialer.Clock.WaitPending(t, 1)
	dialer.Clock.Advance(time.Second)
	dialer.waitUp(t, fpAcceptor, 2)
	acceptor.waitUp(t, fpDialer, 2)

	acceptor.Kick(fpDialer)
	if got := dials.n(); got != 0 {
		t.Fatalf("acceptor dialed %d times across the reconnect", got)
	}
}

// TestREQBrokerPairing019_RestartRestoresBothRoles checks that a restarted Link
// immediately redials the peers it dials and accepts the peers that dial it.
func TestREQBrokerPairing019_RestartRestoresBothRoles(t *testing.T) {
	d := newNode(t, "d")
	x := newNode(t, "x")
	dAddr := listenAt(t, d)
	linktest.Introduce(x.Peers, d.Peers, "x", "d", dAddr)

	xAddr := listenAt(t, x)
	e := newNode(t, "e")
	linktest.Introduce(e.Peers, x.Peers, "e", "x", xAddr)

	d.Start()
	x.Start()
	e.Start()
	x.waitUp(t, d.Peers.Fingerprint(), 1)
	d.waitUp(t, x.Peers.Fingerprint(), 1)
	x.waitUp(t, e.Peers.Fingerprint(), 1)
	e.waitUp(t, x.Peers.Fingerprint(), 1)

	fpX := x.Peers.Fingerprint()
	oldSaltWithD := x.lastSalt(d.Peers.Fingerprint())
	oldSaltWithE := x.lastSalt(e.Peers.Fingerprint())
	if len(oldSaltWithD) == 0 || len(oldSaltWithE) == 0 {
		t.Fatal("restart test needs the salts of both roles recorded before Close")
	}

	x.Close()
	d.waitDown(t, fpX, 1)
	e.waitDown(t, fpX, 1)

	// Same identity, peer table and settings; the Link instance and its
	// listener are new.
	restarted := newNodeFrom(t, x.Peers, x.Settings, linktest.NewFakeClock(time.Unix(1_700_000_000, 0)))
	if err := restarted.Listen(xAddr); err != nil {
		t.Fatalf("restarted Listen(%s): %v", xAddr, err)
	}
	restarted.Start()

	// The dialing side reconnects immediately, without any clock advance.
	restarted.waitUp(t, d.Peers.Fingerprint(), 1)
	d.waitUp(t, fpX, 2)

	// The accepting side comes back on its own backoff schedule.
	e.Clock.WaitPending(t, 1)
	e.Clock.Advance(time.Second)
	e.waitUp(t, fpX, 2)
	restarted.waitUp(t, e.Peers.Fingerprint(), 1)

	newSaltWithD := restarted.lastSalt(d.Peers.Fingerprint())
	newSaltWithE := restarted.lastSalt(e.Peers.Fingerprint())
	if bytes.Equal(oldSaltWithD, newSaltWithD) {
		t.Fatal("salt with the dialed peer did not change across the restart")
	}
	if bytes.Equal(oldSaltWithE, newSaltWithE) {
		t.Fatal("salt with the accepting peer did not change across the restart")
	}
	if len(newSaltWithD) != 64 || len(newSaltWithE) != 64 {
		t.Fatalf("restarted salts have lengths %d and %d, want 64", len(newSaltWithD), len(newSaltWithE))
	}
}

// TestREQBrokerPairing022_UnpairedPeerRejectedUntilRepair checks that a peer
// removed from the peer table is rejected until it pairs again, and that both
// sides audit the event.
func TestREQBrokerPairing022_UnpairedPeerRejectedUntilRepair(t *testing.T) {
	acceptor := newNode(t, "acceptor")
	dialer := newNode(t, "dialer")
	connect(t, dialer, acceptor)

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()

	acceptor.Peers.Remove(fpDialer)
	acceptor.Drop(fpDialer)
	dialer.waitDown(t, fpAcceptor, 1)
	acceptor.waitDown(t, fpDialer, 1)

	dialer.Clock.WaitPending(t, 1)
	dialer.Clock.Advance(time.Second)

	acceptor.Settings.WaitEvent(t, config.EventConnectionRejected)
	dialer.Settings.WaitEvent(t, config.EventRejectedByPeer)

	st := dialer.State(fpAcceptor)
	if st.Phase != link.PhaseRejected || st.LastFailure != link.FailNotAPeer {
		t.Fatalf("dialer state after rejection = %+v, want rejected with not_a_peer", st)
	}

	acceptor.Peers.SetAccept(linktest.FakePairAccept(acceptor.Peers, "dialer"))
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	if err := dialer.PairDial(ctx, acceptor.Addr().String(), linktest.FakePairRun(dialer.Peers, "acceptor")); err != nil {
		t.Fatalf("PairDial repair: %v", err)
	}
	dialer.waitUp(t, fpAcceptor, 2)
	acceptor.waitUp(t, fpDialer, 2)

	if st := dialer.State(fpAcceptor); st.Phase != link.PhaseEstablished {
		t.Fatalf("dialer state after repair = %+v, want established", st)
	}
}

// TestREQBrokerPairing015_StrangerRejected checks that a peer the acceptor does
// not know is rejected and leaves no trace in the acceptor's peer table.
func TestREQBrokerPairing015_StrangerRejected(t *testing.T) {
	acceptor := newNode(t, "a")
	stranger := newNode(t, "c")
	addr := listenAt(t, acceptor)
	acceptor.Start()

	stranger.Peers.Add(acceptor.Peers.Fingerprint(), pairing.RoleDialer, "a", addr)
	stranger.Start()

	acceptor.Settings.WaitEvent(t, config.EventConnectionRejected)
	stranger.Settings.WaitEvent(t, config.EventRejectedByPeer)

	fpA := acceptor.Peers.Fingerprint()
	fpC := stranger.Peers.Fingerprint()

	st := stranger.State(fpA)
	if st.Phase != link.PhaseRejected || st.LastFailure != link.FailNotAPeer {
		t.Fatalf("stranger state = %+v, want rejected with not_a_peer", st)
	}
	if st := acceptor.State(fpC); st.Phase != link.PhaseNone {
		t.Fatalf("acceptor state for the stranger = %+v, want none", st)
	}
	if role := acceptor.Peers.Role(fpC); role != pairing.RoleUnknown {
		t.Fatalf("acceptor peer table role for the stranger = %v, want unknown", role)
	}
	obs := acceptor.Peers.Observed(fpC)
	if len(obs.Aliases) != 0 || len(obs.AcceptorAddresses) != 0 || len(obs.Connected) != 0 {
		t.Fatalf("acceptor recorded an unknown peer: %+v", obs)
	}
}

// TestREQBrokerPairing034_DialerUsesUpdatedAcceptorList checks that the dialer
// picks up an address list announced during the link and later reconnects
// through the new candidate.
func TestREQBrokerPairing034_DialerUsesUpdatedAcceptorList(t *testing.T) {
	acceptor := newNode(t, "a")
	dialer := newNode(t, "b")
	directAddr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, directAddr)

	acceptor.Settings.SetAdvertised([]config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: proxy.Addr()},
	})
	connectVia(t, dialer, acceptor, proxy.Addr())

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()
	dialer.Peers.Wait(t, func() bool {
		return len(dialer.Peers.Observed(fpAcceptor).AcceptorAddresses) >= 1
	})

	acceptor.Settings.SetAdvertised([]config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: proxy.Addr()},
		{Kind: config.AdvertisedKindDirect, Addr: directAddr},
	})
	acceptor.AnnounceAddresses()
	dialer.Peers.Wait(t, func() bool {
		for _, list := range dialer.Peers.Observed(fpAcceptor).AcceptorAddresses {
			for _, addr := range list {
				if addr.Kind == config.AdvertisedKindDirect && addr.Addr == directAddr {
					return true
				}
			}
		}
		return false
	})

	proxy.Close()
	dialer.waitDown(t, fpAcceptor, 1)
	acceptor.waitDown(t, fpDialer, 1)

	dialer.Clock.WaitPending(t, 1)
	dialer.Clock.Advance(time.Second)
	dialer.waitUp(t, fpAcceptor, 2)
	acceptor.waitUp(t, fpDialer, 2)

	connected := dialer.Peers.Observed(fpAcceptor).Connected
	if len(connected) == 0 || connected[len(connected)-1] != directAddr {
		t.Fatalf("dialer reconnected through %v, want the announced direct address %s", connected, directAddr)
	}
}

// TestREQBrokerConfig030_031_ListCarriedOnlyByAcceptor checks that the acceptor
// carries its address list in order and that a dialer never reports one.
func TestREQBrokerConfig030_031_ListCarriedOnlyByAcceptor(t *testing.T) {
	acceptor := newNode(t, "a")
	dialer := newNode(t, "b")

	want := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.10:17443"},
		{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.11:17443"},
	}
	acceptor.Settings.SetAdvertised(want)
	dialer.Settings.SetAdvertised([]config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "203.0.113.20:17443"},
	})

	connect(t, dialer, acceptor)

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()
	dialer.Peers.Wait(t, func() bool {
		return len(dialer.Peers.Observed(fpAcceptor).AcceptorAddresses) >= 1
	})
	lists := dialer.Peers.Observed(fpAcceptor).AcceptorAddresses
	if got := lists[len(lists)-1]; !sameAddrs(got, want) {
		t.Fatalf("dialer observed addresses %v, want %v in order", got, want)
	}
	if obs := acceptor.Peers.Observed(fpDialer); len(obs.AcceptorAddresses) != 0 {
		t.Fatalf("acceptor recorded an address list from the dialer: %v", obs.AcceptorAddresses)
	}
}

// TestREQBrokerPairing002_003_AliasAnnounced checks that AnnounceAlias carries a
// changed alias in both directions.
func TestREQBrokerPairing002_003_AliasAnnounced(t *testing.T) {
	acceptor := newNode(t, "initial-a")
	dialer := newNode(t, "initial-b")
	connect(t, dialer, acceptor)

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()

	acceptor.Settings.SetAlias("build-box")
	acceptor.AnnounceAlias()
	dialer.Peers.Wait(t, func() bool {
		return containsAlias(dialer.Peers.Observed(fpAcceptor).Aliases, "build-box")
	})

	dialer.Settings.SetAlias("test-box")
	dialer.AnnounceAlias()
	acceptor.Peers.Wait(t, func() bool {
		return containsAlias(acceptor.Peers.Observed(fpDialer).Aliases, "test-box")
	})
}

// TestREQBrokerPairing013_TrafficEncrypted checks that no plaintext alias or
// payload appears in the bytes a proxy forwards between two links.
func TestREQBrokerPairing013_TrafficEncrypted(t *testing.T) {
	const (
		aliasMarker   = "ALIAS-MARKER-7f3a"
		payloadMarker = "PAYLOAD-MARKER-91c2"
		requestMarker = "REQUEST-MARKER-55d0"
	)

	acceptor := newNode(t, "acceptor-"+aliasMarker)
	dialer := newNode(t, "dialer-"+aliasMarker)

	received := make(chan []byte, 1)
	acceptor.HandleState(200, func(_ link.Fingerprint, f link.Frame) {
		received <- append([]byte(nil), f.Payload...)
	})
	acceptor.HandleRequest(201, func(_ context.Context, _ link.Fingerprint, _ link.Frame) link.Frame {
		return link.Frame{Kind: 201, Payload: []byte("ack")}
	})

	addr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, addr)
	connectVia(t, dialer, acceptor, proxy.Addr())
	fpAcceptor := acceptor.Peers.Fingerprint()

	if err := dialer.Send(fpAcceptor, link.Frame{Kind: 200, Payload: []byte(payloadMarker)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-received:
		if string(got) != payloadMarker {
			t.Fatalf("state payload = %q, want %q", got, payloadMarker)
		}
	case <-time.After(eventTimeout):
		t.Fatal("acceptor did not receive the state frame")
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	reply, sent, err := dialer.Request(ctx, fpAcceptor, link.Frame{Kind: 201, Payload: []byte(requestMarker)})
	if err != nil || !sent {
		t.Fatalf("Request: sent=%v err=%v", sent, err)
	}
	if string(reply.Payload) != "ack" {
		t.Fatalf("reply payload = %q, want ack", reply.Payload)
	}

	captured := proxy.Captured()
	for _, marker := range []string{aliasMarker, payloadMarker, requestMarker} {
		if bytes.Contains(captured, []byte(marker)) {
			t.Fatalf("proxy captured plaintext %q", marker)
		}
	}
}

// TestAcceptorReplacesStaleConnection checks that a new connection from the
// same peer replaces a stale one: one down then one up, a fresh salt, and Send
// goes over the new connection.
func TestAcceptorReplacesStaleConnection(t *testing.T) {
	acceptor := newNode(t, "a")
	dialer := newNode(t, "b")
	directAddr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, directAddr)
	connectVia(t, dialer, acceptor, proxy.Addr())

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()

	// Freeze the proxy so the acceptor cannot notice the first dialer leaving.
	proxy.Pause()
	dialer.Close()

	restarted := newNodeFrom(t, dialer.Peers, dialer.Settings, linktest.NewFakeClock(time.Unix(1_700_000_000, 0)))
	restarted.Peers.SetCandidates(fpAcceptor, []string{directAddr}, directAddr)
	received := make(chan []byte, 1)
	restarted.HandleState(200, func(_ link.Fingerprint, f link.Frame) {
		received <- append([]byte(nil), f.Payload...)
	})
	restarted.Start()

	// The peer table still remembers the proxy as the last connected address,
	// which it tries first. The proxy is paused, so that attempt would wait for
	// the fake clock forever; advancing past the dial timeout makes the dialer
	// move on to the direct candidate the test just installed.
	restarted.Clock.WaitPending(t, 1)
	restarted.Clock.Advance(5 * time.Second)

	acceptor.waitUp(t, fpDialer, 2)
	restarted.waitUp(t, fpAcceptor, 1)

	events := acceptor.eventsOf(fpDialer)
	if len(events) != 3 {
		t.Fatalf("acceptor events = %+v, want up, down, up", events)
	}
	if !events[0].up || events[1].up || !events[2].up {
		t.Fatalf("acceptor event order = %+v, want up, down, up", events)
	}
	if bytes.Equal(events[0].salt, events[2].salt) {
		t.Fatal("the replacing connection reused the old salt")
	}

	if err := acceptor.Send(fpDialer, link.Frame{Kind: 200, Payload: []byte("after-replace")}); err != nil {
		t.Fatalf("Send after replacement: %v", err)
	}
	select {
	case got := <-received:
		if string(got) != "after-replace" {
			t.Fatalf("new connection payload = %q", got)
		}
	case <-time.After(eventTimeout):
		t.Fatal("the new connection did not receive the frame")
	}
}

// TestBothDialersReportRoleMismatch checks that two Brokers that both dial each
// other back off with role_mismatch and keep dialing with the same reason.
func TestBothDialersReportRoleMismatch(t *testing.T) {
	a := newNode(t, "a")
	b := newNode(t, "b")
	aDials := newDialRecorder()
	bDials := newDialRecorder()
	link.SetTestKnobs(a.Link, link.TestKnobs{OnDial: aDials.dial})
	link.SetTestKnobs(b.Link, link.TestKnobs{OnDial: bDials.dial})

	aAddr := listenAt(t, a)
	bAddr := listenAt(t, b)
	fpA := a.Peers.Fingerprint()
	fpB := b.Peers.Fingerprint()
	a.Peers.Add(fpB, pairing.RoleDialer, "b", bAddr)
	b.Peers.Add(fpA, pairing.RoleDialer, "a", aAddr)

	a.Start()
	b.Start()
	aDials.waitAtLeast(t, 1)
	bDials.waitAtLeast(t, 1)

	wantBackoff := func(st link.LinkState) bool {
		return st.Phase == link.PhaseBackoff && st.LastFailure == link.FailRoleMismatch
	}
	waitState(t, a.Link, fpB, "a to back off with role_mismatch", wantBackoff)
	waitState(t, b.Link, fpA, "b to back off with role_mismatch", wantBackoff)

	// After a clock advance both sides dial again and still report the same
	// reason.
	aBefore, bBefore := aDials.n(), bDials.n()
	a.Clock.Advance(time.Second)
	b.Clock.Advance(time.Second)
	aDials.waitAtLeast(t, aBefore+1)
	bDials.waitAtLeast(t, bBefore+1)
	waitState(t, a.Link, fpB, "a to back off again with role_mismatch", wantBackoff)
	waitState(t, b.Link, fpA, "b to back off again with role_mismatch", wantBackoff)
}

// TestProtocolVersionIncompatible checks that a version mismatch is rejected,
// recorded on both sides and logged with both versions.
func TestProtocolVersionIncompatible(t *testing.T) {
	var logs syncBuffer
	acceptor := newNode(t, "a", func(o *link.Options) {
		o.ProtocolVersion = linkpb.ProtocolVersion + 1
		o.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	})
	dialer := newNode(t, "b")
	addr := listenAt(t, acceptor)
	linktest.Introduce(dialer.Peers, acceptor.Peers, "b", "a", addr)
	startAcceptorThenDialer(acceptor, dialer)

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()

	waitState(t, dialer.Link, fpAcceptor, "dialer to back off with version_incompatible", func(st link.LinkState) bool {
		return st.Phase == link.PhaseBackoff && st.LastFailure == link.FailVersionIncompatible
	})

	if got := acceptor.State(fpDialer).PeerVersion; got != linkpb.ProtocolVersion {
		t.Fatalf("acceptor recorded peer version %d, want %d", got, linkpb.ProtocolVersion)
	}
	if !logHasVersionMismatch(logs.String(), linkpb.ProtocolVersion+1, linkpb.ProtocolVersion) {
		t.Fatalf("acceptor log does not name both versions:\n%s", logs.String())
	}
}

// TestAcceptorDetectsSilentPeerByKeepalive checks that the accepting side
// closes a link whose peer stopped answering. Unlike the other tests this one
// depends on gRPC's real keepalive timers, which fire about 1.5 seconds after
// the proxy stops forwarding: one second until the probe and 500ms until its
// timeout. It is the only integration test that relies on real time.
func TestAcceptorDetectsSilentPeerByKeepalive(t *testing.T) {
	acceptor := newNode(t, "a")
	acceptor.Settings.SetKeepalive(time.Second, 500*time.Millisecond)
	dialer := newNode(t, "b")

	addr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, addr)
	connectVia(t, dialer, acceptor, proxy.Addr())

	fpDialer := dialer.Peers.Fingerprint()
	proxy.Pause()

	acceptor.waitDown(t, fpDialer, 1)
	st := acceptor.State(fpDialer)
	if st.Phase != link.PhaseWaiting || st.LastFailure != link.FailConnectionLost {
		t.Fatalf("acceptor state after the silent peer = %+v, want waiting with connection_lost", st)
	}
}

// TestClosedLinkRejectsStartAndPairDial checks that a closed Link refuses to
// start any new work.
func TestClosedLinkRejectsStartAndPairDial(t *testing.T) {
	n := newNode(t, "closed")
	dials := newDialRecorder()
	link.SetTestKnobs(n.Link, link.TestKnobs{OnDial: dials.dial})
	n.Close()

	runCalled := false
	err := n.PairDial(context.Background(), "127.0.0.1:1", func(pairing.PairConn) error {
		runCalled = true
		return nil
	})
	if !errors.Is(err, link.ErrClosed) {
		t.Fatalf("PairDial on a closed link = %v, want ErrClosed", err)
	}
	if runCalled {
		t.Fatal("PairDial called run on a closed link")
	}

	n.Start()
	// Supplement: Start runs asynchronously, so reading the count right after it
	// returns has almost no discriminating power; the negative window below is
	// what catches a dial started after Start.
	if got := dials.n(); got != 0 {
		t.Fatalf("a closed link dialed %d times after Start", got)
	}
	// Negative window: a late dial still shows up here.
	dials.assertNoDial(t, 100*time.Millisecond)

	if err := n.Listen("127.0.0.1:0"); !errors.Is(err, link.ErrClosed) {
		t.Fatalf("Listen on a closed link = %v, want ErrClosed", err)
	}
}

// TestTopicStyleLockingDoesNotDeadlock exercises the lock order an upper layer
// uses: it holds one mutex while calling Send, and its up, down and state
// callbacks take the same mutex. Concurrent reconnects must not form a cycle.
func TestTopicStyleLockingDoesNotDeadlock(t *testing.T) {
	acceptor := newNode(t, "a")
	dialer := newNode(t, "b")

	addr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, addr)
	connectVia(t, dialer, acceptor, proxy.Addr())

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()

	var lock sync.Mutex
	var upHits, downHits, stateHits atomic.Int64
	acceptor.Subscribe(
		func(link.Fingerprint, []byte) { lock.Lock(); upHits.Add(1); lock.Unlock() },
		func(link.Fingerprint) { lock.Lock(); downHits.Add(1); lock.Unlock() },
	)
	acceptor.HandleState(200, func(link.Fingerprint, link.Frame) {
		lock.Lock()
		stateHits.Add(1)
		lock.Unlock()
	})
	dialer.HandleState(200, func(link.Fingerprint, link.Frame) {})

	stop := make(chan struct{})
	var senders sync.WaitGroup
	for i := 0; i < 8; i++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			// Pace the senders with a ticker; this bounds the send queues, it is
			// not synchronization.
			ticker := time.NewTicker(100 * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lock.Lock()
				_ = acceptor.Send(fpDialer, link.Frame{Kind: 200, Payload: []byte("to-dialer")})
				_ = dialer.Send(fpAcceptor, link.Frame{Kind: 200, Payload: []byte("to-acceptor")})
				lock.Unlock()
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()
	}

	for i := 0; i < 3; i++ {
		upAcceptor := acceptor.upCount(fpDialer)
		upDialer := dialer.upCount(fpAcceptor)
		proxy.CloseConns()
		waitState(t, dialer.Link, fpAcceptor, "dialer to back off after a proxy break", func(st link.LinkState) bool {
			return st.Phase == link.PhaseBackoff && !st.NextDial.IsZero()
		})
		dialer.Clock.Advance(time.Second)
		dialer.waitUp(t, fpAcceptor, upDialer+1)
		acceptor.waitUp(t, fpDialer, upAcceptor+1)
	}

	// Drop and rebuild the dialing side twice while the senders keep running.
	acceptor.Peers.SetAccept(linktest.FakePairAccept(acceptor.Peers, "dialer"))
	for i := 0; i < 2; i++ {
		upAcceptor := acceptor.upCount(fpDialer)
		upDialer := dialer.upCount(fpAcceptor)
		dialer.Drop(fpAcceptor)
		ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
		err := dialer.PairDial(ctx, proxy.Addr(), linktest.FakePairRun(dialer.Peers, "acceptor"))
		cancel()
		if err != nil {
			t.Fatalf("PairDial rebuild: %v", err)
		}
		dialer.waitUp(t, fpAcceptor, upDialer+1)
		acceptor.waitUp(t, fpDialer, upAcceptor+1)
	}

	close(stop)
	done := make(chan struct{})
	go func() {
		senders.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatalf("send goroutines did not stop within %v", eventTimeout)
	}
}

// TestPairDialRunsInstalledConnAfterDropCancelsLoop covers the window between
// startDialLoop launching its goroutine and that goroutine reaching the dial
// loop. Drop during the window cancels the loop context, but the connection was
// already installed, so it must still run. Drop posts onDown only after the
// connection's state handler exits, and every later install of the peer waits
// for that Drop. A loop that checks its context first leaves the connection
// unrun and the peer stuck behind an unfinished Drop: onDown never arrives and
// a later PairDial never returns.
func TestPairDialRunsInstalledConnAfterDropCancelsLoop(t *testing.T) {
	dialer := newNode(t, "dialer")
	acceptor := newNode(t, "acceptor")
	addr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, addr)
	connectVia(t, dialer, acceptor, proxy.Addr())

	fpAcceptor := acceptor.Peers.Fingerprint()
	acceptor.Peers.SetAccept(linktest.FakePairAccept(acceptor.Peers, "dialer"))

	// Hold the goroutine the next startDialLoop launches just before it runs
	// the installed connection and enters the dial loop.
	loopHeld := make(chan struct{})
	releaseLoop := make(chan struct{})
	var heldOnce sync.Once
	link.SetTestKnobs(dialer.Link, link.TestKnobs{
		BeforeDialLoop: func() {
			heldOnce.Do(func() {
				close(loopHeld)
				<-releaseLoop
			})
		},
	})

	if err := dialer.PairDial(context.Background(), proxy.Addr(), linktest.FakePairRun(dialer.Peers, "acceptor")); err != nil {
		t.Fatalf("PairDial: %v", err)
	}
	select {
	case <-loopHeld:
	case <-time.After(eventTimeout):
		t.Fatal("the dial loop goroutine did not reach the test hook")
	}

	// The repair replaced the old connection, whose down event is queued
	// before the new up event. Wait for the up event so the down count below
	// measures the Drop, not that replacement.
	dialer.waitUp(t, fpAcceptor, 2)
	downBeforeDrop := dialer.downCount(fpAcceptor)

	dialer.Drop(fpAcceptor)
	close(releaseLoop)

	var failed bool
	if !waitWithin(eventTimeout, func() bool { return dialer.downCount(fpAcceptor) > downBeforeDrop }) {
		t.Errorf("Drop did not post onDown within %v: down count stayed at %d", eventTimeout, downBeforeDrop)
		failed = true
	}

	// The Drop finished with the onDown; the same peer must now be installable
	// again instead of waiting on that Drop forever.
	pairdialDone := make(chan error, 1)
	go func() {
		pairdialDone <- dialer.PairDial(context.Background(), proxy.Addr(), linktest.FakePairRun(dialer.Peers, "acceptor"))
	}()
	select {
	case err := <-pairdialDone:
		if err != nil {
			t.Errorf("PairDial after Drop: %v", err)
			failed = true
		}
	case <-time.After(eventTimeout):
		t.Errorf("PairDial after Drop did not return within %v", eventTimeout)
		failed = true
	}

	closed := make(chan struct{})
	go func() {
		dialer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(eventTimeout):
		t.Errorf("Close did not return within %v", eventTimeout)
		failed = true
	}
	if failed {
		t.FailNow()
	}
}
