package link

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

type hsEvent struct {
	up   bool
	peer Fingerprint
	salt []byte
}

type hsResult struct {
	delivered <-chan struct{}
	ok        bool
}

type hsPairFirst struct {
	kind    uint32
	payload []byte
	err     error
}

type hsDialResult struct {
	salt        []byte
	failure     FailReason
	peerVersion uint32
	err         error
}

type hsBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *hsBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *hsBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type handshakeHarness struct {
	l        *Link
	peers    *linktest.FakePeers
	settings *linktest.FakeSettings
	clock    *linktest.FakeClock
	log      *hsBuffer

	mu     sync.Mutex
	events []hsEvent
}

func newHandshakeHarness(t *testing.T) *handshakeHarness {
	t.Helper()
	return newHandshakeHarnessWith(t,
		linktest.NewFakePeers(t),
		linktest.NewFakeSettings("self"),
		linktest.NewFakeClock(time.Unix(1700000000, 0)))
}

func newHandshakeHarnessWith(t *testing.T, peers *linktest.FakePeers, settings *linktest.FakeSettings, clock *linktest.FakeClock) *handshakeHarness {
	t.Helper()
	logBuf := new(hsBuffer)
	l, err := New(Options{
		Peers:    peers,
		Settings: settings,
		Clock:    clock,
		Logger:   slog.New(slog.NewTextHandler(logBuf, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &handshakeHarness{l: l, peers: peers, settings: settings, clock: clock, log: logBuf}
	l.Subscribe(h.onUp, h.onDown)
	l.notifier.start()
	t.Cleanup(l.notifier.stop)
	return h
}

func (h *handshakeHarness) onUp(peer Fingerprint, salt []byte) {
	h.mu.Lock()
	h.events = append(h.events, hsEvent{up: true, peer: peer, salt: append([]byte(nil), salt...)})
	h.mu.Unlock()
}

func (h *handshakeHarness) onDown(peer Fingerprint) {
	h.mu.Lock()
	h.events = append(h.events, hsEvent{peer: peer})
	h.mu.Unlock()
}

func (h *handshakeHarness) waitEvents(t *testing.T, n int) []hsEvent {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		h.mu.Lock()
		evs := append([]hsEvent(nil), h.events...)
		h.mu.Unlock()
		if len(evs) >= n {
			return evs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d events, got %+v", n, evs)
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *handshakeHarness) eventCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.events)
}

func (h *handshakeHarness) newConn(t *testing.T, fp Fingerprint) (c *conn, local, peer *pipeEnd) {
	t.Helper()
	return h.newConnFrom(t, fp, "127.0.0.1:1")
}

func (h *handshakeHarness) newConnFrom(t *testing.T, fp Fingerprint, remote string) (c *conn, local, peer *pipeEnd) {
	t.Helper()
	local, peer = newPipe(t)
	c = newConn(local, func() {}, peerIdentity{fp: fp, pub: make([]byte, 32), remote: remote}, 8)
	c.start()
	return c, local, peer
}

func (h *handshakeHarness) serve(c *conn, tr *trackedConn) <-chan hsResult {
	ch := make(chan hsResult, 1)
	go func() {
		delivered, ok := h.l.serveInbound(c, tr)
		ch <- hsResult{delivered: delivered, ok: ok}
	}()
	return ch
}

func waitHSResult(t *testing.T, ch <-chan hsResult) hsResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(eventTimeout):
		t.Fatal("serveInbound did not return")
		return hsResult{}
	}
}

func hsSend(t *testing.T, p *pipeEnd, f *linkpb.Frame) {
	t.Helper()
	if err := p.Send(f); err != nil {
		t.Fatalf("send frame: %v", err)
	}
}

func hsRecv(t *testing.T, p *pipeEnd) *linkpb.Frame {
	t.Helper()
	type result struct {
		f   *linkpb.Frame
		err error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := p.Recv()
		ch <- result{f: f, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv frame: %v", r.err)
		}
		return r.f
	case <-time.After(eventTimeout):
		t.Fatal("timed out receiving a frame")
		return nil
	}
}

func hsWaitClosed(t *testing.T, c *conn) {
	t.Helper()
	select {
	case <-c.done():
	case <-time.After(eventTimeout):
		t.Fatal("connection was not closed")
	}
}

func hsNonce(b byte) []byte { return bytes.Repeat([]byte{b}, nonceSize) }

func hsAssertNotClosed(t *testing.T, c *conn) {
	t.Helper()
	select {
	case <-c.done():
		t.Fatal("connection closed before the linger deadline")
	default:
	}
}

func hsDial(t *testing.T, l *Link, c *conn) <-chan hsDialResult {
	t.Helper()
	ch := make(chan hsDialResult, 1)
	go func() {
		salt, failure, peerVersion, err := l.dialerHandshake(c)
		ch <- hsDialResult{salt: salt, failure: failure, peerVersion: peerVersion, err: err}
	}()
	return ch
}

func waitHSDial(t *testing.T, ch <-chan hsDialResult) hsDialResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(eventTimeout):
		t.Fatal("dialerHandshake did not return")
		return hsDialResult{}
	}
}

func hsAddAcceptor(peers *linktest.FakePeers, fp Fingerprint, alias string) {
	peers.Add(fp, pairing.RoleAcceptor, alias)
}

// TestREQBrokerPairing015_UnknownFingerprintRejected checks that an unknown
// peer is refused without learning the local alias or advertised addresses.
func TestREQBrokerPairing015_UnknownFingerprintRejected(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xA1}
	c, local, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "stranger", hsNonce(1), nil))
	rej := hsRecv(t, p)
	if rej.GetKind() != linkpb.KindReject {
		t.Fatalf("first reply kind = %d, want reject", rej.GetKind())
	}
	reason, err := decodeReject(rej)
	if err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_NOT_A_PEER {
		t.Fatalf("reject reason = %v, want not a peer", reason)
	}
	hsAssertNotClosed(t, c)

	h.clock.Advance(h.l.rejectLinger)
	hsWaitClosed(t, c)
	if got := len(local.sent()); got != 1 {
		t.Fatalf("link sent %d frames to the unknown peer, want exactly the reject", got)
	}

	if r := waitHSResult(t, res); r.ok {
		t.Fatal("unknown fingerprint was installed")
	}

	ev := h.settings.WaitEvent(t, config.EventConnectionRejected)
	if ev.Peer == nil || ev.Peer.Fingerprint != [32]byte(peer) {
		t.Fatalf("audit Peer = %+v, want fingerprint %x", ev.Peer, peer)
	}
	if obs := h.peers.Observed(peer); len(obs.Aliases) != 0 || len(obs.AcceptorAddresses) != 0 || len(obs.Connected) != 0 {
		t.Fatalf("unexpected observations of an unknown peer: %+v", obs)
	}
	if state := h.l.State(peer); state.Phase != PhaseNone {
		t.Fatalf("unknown peer state = %+v, want none", state)
	}
}

// TestUnknownFingerprintNonHelloFirstFrameRejected checks that an arbitrary
// first kind is answered the same way and leaves an audit record.
func TestUnknownFingerprintNonHelloFirstFrameRejected(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xA2}
	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, &linkpb.Frame{Kind: 200})
	rej := hsRecv(t, p)
	reason, err := decodeReject(rej)
	if err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_NOT_A_PEER {
		t.Fatalf("reject reason = %v, want not a peer", reason)
	}

	h.clock.Advance(h.l.rejectLinger)
	hsWaitClosed(t, c)
	if r := waitHSResult(t, res); r.ok {
		t.Fatal("connection with an unknown peer was installed")
	}
	ev := h.settings.WaitEvent(t, config.EventConnectionRejected)
	if ev.Peer == nil || ev.Peer.Fingerprint != [32]byte(peer) {
		t.Fatalf("audit Peer = %+v, want fingerprint %x", ev.Peer, peer)
	}
}

// TestNothingSentBeforeFirstFrame checks that the accepting side stays silent
// until the peer identifies itself.
func TestNothingSentBeforeFirstFrame(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xA3}
	hsAddAcceptor(h.peers, peer, "peer")
	c, local, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	select {
	case <-local.recvWaiting():
	case <-time.After(eventTimeout):
		t.Fatal("the link did not start reading the first frame")
	}
	if sent := local.sent(); len(sent) != 0 {
		t.Fatalf("the link sent %d frames before the first frame arrived", len(sent))
	}

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", hsNonce(2), nil))
	hello := hsRecv(t, p)
	if hello.GetKind() != linkpb.KindHello {
		t.Fatalf("reply kind = %d, want hello", hello.GetKind())
	}
	r := waitHSResult(t, res)
	if !r.ok {
		t.Fatal("handshake failed")
	}
	<-r.delivered
	h.waitEvents(t, 1)
}

// TestRoleMismatchRejected checks that a peer this Broker dials is not accepted
// as an inbound link.
func TestRoleMismatchRejected(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xA4}
	h.peers.Add(peer, pairing.RoleDialer, "peer")
	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", hsNonce(3), nil))
	rej := hsRecv(t, p)
	reason, err := decodeReject(rej)
	if err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_ROLE_MISMATCH {
		t.Fatalf("reject reason = %v, want role mismatch", reason)
	}
	if !strings.Contains(h.log.String(), "dial") {
		t.Fatalf("role mismatch was not logged: %s", h.log.String())
	}

	h.clock.Advance(h.l.rejectLinger)
	hsWaitClosed(t, c)
	if r := waitHSResult(t, res); r.ok {
		t.Fatal("role mismatch was installed")
	}
	if evs := h.settings.Events(); len(evs) != 0 {
		t.Fatalf("role mismatch wrote audit events: %+v", evs)
	}
	if n := h.eventCount(); n != 0 {
		t.Fatalf("role mismatch produced %d notifications", n)
	}
}

// TestRejectLingerExitsWhenLinkCloses checks that closing the Link ends the
// reject linger goroutine and leaves no timer pending.
func TestRejectLingerExitsWhenLinkCloses(t *testing.T) {
	h := newHandshakeHarness(t)
	exited := make(chan struct{})
	var exitOnce sync.Once
	SetTestKnobs(h.l, TestKnobs{
		RejectLinger: time.Hour,
		OnRejectLingerExit: func() {
			exitOnce.Do(func() { close(exited) })
		},
	})

	peer := Fingerprint{0xB1}
	h.peers.Add(peer, pairing.RoleDialer, "peer")
	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", hsNonce(21), nil))
	rej := hsRecv(t, p)
	reason, err := decodeReject(rej)
	if err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_ROLE_MISMATCH {
		t.Fatalf("reject reason = %v, want role mismatch", reason)
	}
	h.clock.WaitPending(t, 1)

	h.l.Close()

	select {
	case <-exited:
	case <-time.After(eventTimeout):
		t.Fatalf("reject linger goroutine did not exit within %v of Close", eventTimeout)
	}
	if n := h.clock.Pending(); n != 0 {
		t.Fatalf("%d timers still pending after the linger goroutine exited, want none", n)
	}
	hsWaitClosed(t, c)
	if r := waitHSResult(t, res); r.ok {
		t.Fatal("role mismatch was installed")
	}
}

// TestVersionMismatchRejected checks that the accepting side refuses a
// different protocol version, stays waiting, and records the peer's version.
func TestVersionMismatchRejected(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xA5}
	hsAddAcceptor(h.peers, peer, "peer")
	h.l.mu.Lock()
	h.l.ensureRecord(peer, pairing.RoleAcceptor, PhaseWaiting)
	h.l.mu.Unlock()

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion+1, "peer", hsNonce(4), nil))
	rej := hsRecv(t, p)
	reason, err := decodeReject(rej)
	if err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE {
		t.Fatalf("reject reason = %v, want version incompatible", reason)
	}
	if r := waitHSResult(t, res); r.ok {
		t.Fatal("version mismatch was installed")
	}

	state := h.l.State(peer)
	if state.Phase != PhaseWaiting {
		t.Fatalf("phase = %v, want waiting", state.Phase)
	}
	if state.LastFailure != FailVersionIncompatible {
		t.Fatalf("last failure = %v, want version incompatible", state.LastFailure)
	}
	if state.PeerVersion != linkpb.ProtocolVersion+1 {
		t.Fatalf("peer version = %d, want %d", state.PeerVersion, linkpb.ProtocolVersion+1)
	}
	logs := h.log.String()
	local := fmt.Sprintf("local=%d", linkpb.ProtocolVersion)
	peerVersion := fmt.Sprintf("peer_version=%d", linkpb.ProtocolVersion+1)
	if !strings.Contains(logs, "protocol version mismatch") ||
		!strings.Contains(logs, local) || !strings.Contains(logs, peerVersion) {
		t.Fatalf("version mismatch log does not name both versions: %s", logs)
	}
}

// TestREQBrokerConfig030_AcceptorHelloCarriesAddresses checks the acceptor's
// Hello content and that it ignores the dialer's address list.
func TestREQBrokerConfig030_AcceptorHelloCarriesAddresses(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xA6}
	hsAddAcceptor(h.peers, peer, "peer")
	advertised := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.20:17443"},
		{Kind: config.AdvertisedKindDirect, Addr: "b.vpn.example:17443"},
	}
	h.settings.SetAlias("self")
	h.settings.SetAdvertised(advertised)

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer-alias", hsNonce(5), []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.9:1"},
	}))
	f := hsRecv(t, p)
	hello, err := decodeHello(f)
	if err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	if hello.GetAlias() != "self" {
		t.Fatalf("hello alias = %q, want self", hello.GetAlias())
	}
	if len(hello.GetNonce()) != nonceSize {
		t.Fatalf("hello nonce is %d bytes, want %d", len(hello.GetNonce()), nonceSize)
	}
	if got := addrsFromPB(hello.GetAdvertisedAddresses()); !reflect.DeepEqual(got, advertised) {
		t.Fatalf("advertised addresses = %+v, want %+v", got, advertised)
	}

	obs := h.peers.Observed(peer)
	if len(obs.Aliases) != 1 || obs.Aliases[0] != "peer-alias" {
		t.Fatalf("observed aliases = %+v, want [peer-alias]", obs.Aliases)
	}
	if len(obs.AcceptorAddresses) != 0 {
		t.Fatalf("accepting side recorded the dialer's addresses: %+v", obs.AcceptorAddresses)
	}

	r := waitHSResult(t, res)
	if !r.ok {
		t.Fatal("handshake failed")
	}
	<-r.delivered
	h.waitEvents(t, 1)
}

// TestSaltIsDialerNonceThenAcceptorNonce checks the salt order.
func TestSaltIsDialerNonceThenAcceptorNonce(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xA7}
	hsAddAcceptor(h.peers, peer, "peer")
	peerNonce := hsNonce(0x11)

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)
	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", peerNonce, nil))
	hello, err := decodeHello(hsRecv(t, p))
	if err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	r := waitHSResult(t, res)
	if !r.ok {
		t.Fatal("handshake failed")
	}
	<-r.delivered

	evs := h.waitEvents(t, 1)
	want := append(append([]byte(nil), peerNonce...), hello.GetNonce()...)
	if len(evs[0].salt) != 2*nonceSize {
		t.Fatalf("salt is %d bytes, want %d", len(evs[0].salt), 2*nonceSize)
	}
	if !bytes.Equal(evs[0].salt, want) {
		t.Fatalf("salt = %x, want %x", evs[0].salt, want)
	}
}

func pairHandshake(t *testing.T, h *handshakeHarness, peer Fingerprint) (hsPairFirst, <-chan hsResult) {
	t.Helper()
	firsts := make(chan hsPairFirst, 1)
	h.peers.SetAccept(func(conn pairing.PairConn) error {
		kind, payload, err := conn.Receive(context.Background())
		firsts <- hsPairFirst{kind: kind, payload: payload, err: err}
		if err != nil {
			_ = conn.Close()
			return err
		}
		conn.Unread(kind, payload)
		return linktest.FakePairAccept(h.peers, "acceptor")(conn)
	})

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)
	hsSend(t, p, &linkpb.Frame{Kind: pairingpb.KindPairRequest, Payload: []byte("request-payload")})

	var first hsPairFirst
	select {
	case first = <-firsts:
	case <-time.After(eventTimeout):
		t.Fatal("pairing Accept was not called")
	}

	challenge := hsRecv(t, p)
	if challenge.GetKind() != pairingpb.KindPairChallenge {
		t.Fatalf("frame after request has kind %d, want challenge", challenge.GetKind())
	}
	if challenge.GetRequestId() != 0 || challenge.GetIsReply() {
		t.Fatalf("challenge carries request fields: %+v", challenge)
	}

	hsSend(t, p, &linkpb.Frame{Kind: pairingpb.KindPairConfirm})
	done := hsRecv(t, p)
	if done.GetKind() != pairingpb.KindPairDone {
		t.Fatalf("frame after confirm has kind %d, want done", done.GetKind())
	}
	if done.GetRequestId() != 0 || done.GetIsReply() {
		t.Fatalf("done carries request fields: %+v", done)
	}

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "dialer", hsNonce(6), nil))
	serverHello := hsRecv(t, p)
	if serverHello.GetKind() != linkpb.KindHello {
		t.Fatalf("server reply kind = %d, want hello", serverHello.GetKind())
	}
	return first, res
}

// TestPairRequestHandedToAccept checks that the first pairing frame reaches
// pairing unchanged and the connection becomes a link afterwards.
func TestPairRequestHandedToAccept(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xB1}

	first, res := pairHandshake(t, h, peer)
	if first.err != nil {
		t.Fatalf("accept receive: %v", first.err)
	}
	if first.kind != pairingpb.KindPairRequest {
		t.Fatalf("accept received kind %d, want pair request", first.kind)
	}
	if !bytes.Equal(first.payload, []byte("request-payload")) {
		t.Fatalf("accept received payload %q, want request-payload", first.payload)
	}

	r := waitHSResult(t, res)
	if !r.ok {
		t.Fatal("pairing connection was not installed")
	}
	<-r.delivered
	evs := h.waitEvents(t, 1)
	if !evs[0].up || evs[0].peer != peer || len(evs[0].salt) != 2*nonceSize {
		t.Fatalf("unexpected up event: %+v", evs[0])
	}
}

// TestPairRequestAcceptFailureCloses checks that a failed pairing leaves no
// connection and no audit event behind.
func TestPairRequestAcceptFailureCloses(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xB2}
	h.peers.SetAccept(func(pairing.PairConn) error { return errors.New("accept failed") })

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)
	hsSend(t, p, &linkpb.Frame{Kind: pairingpb.KindPairRequest})

	if r := waitHSResult(t, res); r.ok {
		t.Fatal("failed pairing was installed")
	}
	hsWaitClosed(t, c)
	if n := h.eventCount(); n != 0 {
		t.Fatalf("failed pairing produced %d notifications", n)
	}
	if evs := h.settings.Events(); len(evs) != 0 {
		t.Fatalf("failed pairing wrote audit events: %+v", evs)
	}
}

// TestPairRequestForKnownPeerStillGoesToPairing checks that re-pairing bypasses
// the normal Hello path.
func TestPairRequestForKnownPeerStillGoesToPairing(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xB3}
	hsAddAcceptor(h.peers, peer, "known")

	first, res := pairHandshake(t, h, peer)
	if first.kind != pairingpb.KindPairRequest || first.err != nil {
		t.Fatalf("accept first frame = %+v, want a pair request", first)
	}
	r := waitHSResult(t, res)
	if !r.ok {
		t.Fatal("known peer's pairing connection was not installed")
	}
	<-r.delivered
	if evs := h.waitEvents(t, 1); !evs[0].up {
		t.Fatalf("first event is not up: %+v", evs[0])
	}
}

// TestDropBetweenRoleLookupAndInstall checks that a Drop racing the handshake
// prevents the stale connection from being installed.
func TestDropBetweenRoleLookupAndInstall(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xB4}
	hsAddAcceptor(h.peers, peer, "peer")
	h.peers.SetRoleHook(func(fp Fingerprint) {
		h.l.Drop(fp)
	})

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)
	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", hsNonce(7), nil))

	if r := waitHSResult(t, res); r.ok {
		t.Fatal("dropped peer's connection was installed")
	}
	hsWaitClosed(t, c)
	if n := h.eventCount(); n != 0 {
		t.Fatalf("dropped handshake produced %d notifications", n)
	}
	if state := h.l.State(peer); state.Phase != PhaseNone {
		t.Fatalf("state after Drop = %+v, want none", state)
	}
}

// TestDialerHandshakeOutcomes checks the result of every first reply the
// dialing side can receive.
func TestDialerHandshakeOutcomes(t *testing.T) {
	cases := []struct {
		name          string
		respond       func(t *testing.T, h *handshakeHarness, p *pipeEnd)
		wantFailure   FailReason
		wantVersion   uint32
		wantSaltBytes int
		wantLog       bool
	}{
		{
			name: "hello",
			respond: func(t *testing.T, h *handshakeHarness, p *pipeEnd) {
				hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "acceptor", hsNonce(8), nil))
			},
			wantFailure:   FailNone,
			wantSaltBytes: 2 * nonceSize,
		},
		{
			name: "reject not a peer",
			respond: func(t *testing.T, h *handshakeHarness, p *pipeEnd) {
				hsSend(t, p, encodeReject(linkpb.RejectReason_REJECT_REASON_NOT_A_PEER))
			},
			wantFailure: FailNotAPeer,
		},
		{
			name: "reject role mismatch",
			respond: func(t *testing.T, h *handshakeHarness, p *pipeEnd) {
				hsSend(t, p, encodeReject(linkpb.RejectReason_REJECT_REASON_ROLE_MISMATCH))
			},
			wantFailure: FailRoleMismatch,
		},
		{
			name: "reject version incompatible",
			respond: func(t *testing.T, h *handshakeHarness, p *pipeEnd) {
				hsSend(t, p, encodeReject(linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE))
			},
			wantFailure: FailVersionIncompatible,
		},
		{
			name: "reject with no reason",
			respond: func(t *testing.T, h *handshakeHarness, p *pipeEnd) {
				hsSend(t, p, &linkpb.Frame{Kind: linkpb.KindReject})
			},
			wantFailure: FailProtocol,
		},
		{
			name: "hello with another version",
			respond: func(t *testing.T, h *handshakeHarness, p *pipeEnd) {
				hsSend(t, p, encodeHello(linkpb.ProtocolVersion+1, "acceptor", hsNonce(9), nil))
				rej := hsRecv(t, p)
				reason, err := decodeReject(rej)
				if err != nil {
					t.Fatalf("decode reject: %v", err)
				}
				if reason != linkpb.RejectReason_REJECT_REASON_PROTOCOL_VERSION_INCOMPATIBLE {
					t.Fatalf("dialer reply reason = %v, want version incompatible", reason)
				}
			},
			wantFailure: FailVersionIncompatible,
			wantVersion: linkpb.ProtocolVersion + 1,
			wantLog:     true,
		},
		{
			name: "silence",
			respond: func(t *testing.T, h *handshakeHarness, p *pipeEnd) {
				h.clock.WaitPending(t, 1)
				h.clock.Advance(h.l.helloTimeout)
			},
			wantFailure: FailConnectionLost,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHandshakeHarness(t)
			peer := Fingerprint{0xD1}
			c, _, p := h.newConn(t, peer)
			results := hsDial(t, h.l, c)

			clientHello := hsRecv(t, p)
			if clientHello.GetKind() != linkpb.KindHello {
				t.Fatalf("dialer's first frame kind = %d, want hello", clientHello.GetKind())
			}
			tc.respond(t, h, p)

			r := waitHSDial(t, results)
			if r.failure != tc.wantFailure {
				t.Fatalf("failure = %v, want %v (err %v)", r.failure, tc.wantFailure, r.err)
			}
			if r.peerVersion != tc.wantVersion {
				t.Fatalf("peer version = %d, want %d", r.peerVersion, tc.wantVersion)
			}
			if tc.wantLog {
				logs := h.log.String()
				local := fmt.Sprintf("local=%d", linkpb.ProtocolVersion)
				peerVersion := fmt.Sprintf("peer_version=%d", tc.wantVersion)
				if !strings.Contains(logs, "protocol version mismatch") ||
					!strings.Contains(logs, local) || !strings.Contains(logs, peerVersion) {
					t.Fatalf("dialer version mismatch log does not name both versions: %s", logs)
				}
			}
			if tc.wantFailure == FailNone {
				if r.err != nil {
					t.Fatalf("successful handshake returned %v", r.err)
				}
				if len(r.salt) != tc.wantSaltBytes {
					t.Fatalf("salt is %d bytes, want %d", len(r.salt), tc.wantSaltBytes)
				}
				obs := h.peers.Observed(peer)
				if len(obs.Aliases) != 1 || obs.Aliases[0] != "acceptor" {
					t.Fatalf("observed aliases = %+v, want [acceptor]", obs.Aliases)
				}
			} else if r.err == nil {
				t.Fatal("failed handshake returned no error")
			}
		})
	}
}

// TestDialerHelloHasNoAddressesAndObservesBeforeReturn checks the dialer's
// Hello content and that the acceptor's list reaches the peer table before the
// handshake returns.
func TestDialerHelloHasNoAddressesAndObservesBeforeReturn(t *testing.T) {
	h := newHandshakeHarness(t)
	h.settings.SetAdvertised([]config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.99:17443"},
	})
	peer := Fingerprint{0xD2}
	c, _, p := h.newConn(t, peer)
	results := hsDial(t, h.l, c)

	hello, err := decodeHello(hsRecv(t, p))
	if err != nil {
		t.Fatalf("decode client hello: %v", err)
	}
	if len(hello.GetAdvertisedAddresses()) != 0 {
		t.Fatalf("dialer advertised addresses: %+v", hello.GetAdvertisedAddresses())
	}
	if len(hello.GetNonce()) != nonceSize {
		t.Fatalf("dialer nonce is %d bytes, want %d", len(hello.GetNonce()), nonceSize)
	}

	want := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "203.0.113.7:17443"},
		{Kind: "relay", Addr: "relay.example:17443"},
	}
	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "acceptor", hsNonce(10), want))

	r := waitHSDial(t, results)
	if r.failure != FailNone || r.err != nil {
		t.Fatalf("handshake failed: %+v", r)
	}

	obs := h.peers.Observed(peer)
	if len(obs.AcceptorAddresses) != 1 {
		t.Fatalf("acceptor address observations = %+v, want one", obs.AcceptorAddresses)
	}
	if !reflect.DeepEqual(obs.AcceptorAddresses[0], want) {
		t.Fatalf("observed acceptor addresses = %+v, want %+v", obs.AcceptorAddresses[0], want)
	}
	if len(obs.Aliases) != 1 || obs.Aliases[0] != "acceptor" {
		t.Fatalf("observed aliases = %+v, want [acceptor]", obs.Aliases)
	}
}

// TestServeInboundAuthenticatesTrackedConn checks the first-frame deadline is
// cancelled and the connection leaves the unauthenticated registry on success.
func TestServeInboundAuthenticatesTrackedConn(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xE1}
	hsAddAcceptor(h.peers, peer, "peer")

	raw, other := net.Pipe()
	t.Cleanup(func() {
		_ = other.Close()
		_ = raw.Close()
	})
	tr := h.l.unauth.track(raw)
	if n := h.l.unauth.count(); n != 1 {
		t.Fatalf("unauthenticated count = %d, want 1", n)
	}

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, tr)
	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", hsNonce(11), nil))
	if f := hsRecv(t, p); f.GetKind() != linkpb.KindHello {
		t.Fatalf("reply kind = %d, want hello", f.GetKind())
	}

	r := waitHSResult(t, res)
	if !r.ok {
		t.Fatal("handshake failed")
	}
	<-r.delivered
	if n := h.l.unauth.count(); n != 0 {
		t.Fatalf("unauthenticated count after handshake = %d, want 0", n)
	}
	if n := h.clock.Pending(); n != 0 {
		t.Fatalf("%d timers are still pending after authentication", n)
	}
}
