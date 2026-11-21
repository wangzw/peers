package link

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// pairHarness builds an acceptor that serves links and a dialer that is ready
// to pair. The acceptor accepts FakePairAccept unless the test replaces it.
func pairHarness(t *testing.T) (dialer, acceptor *dialHarness, addr string) {
	t.Helper()
	acceptor = newDialHarness(t)
	addr = serveAcceptor(t, acceptor)
	acceptor.peers.SetAccept(linktest.FakePairAccept(acceptor.peers, "dialer"))
	dialer = newDialHarness(t)
	dialer.l.Start()
	return dialer, acceptor, addr
}

func TestPairDialHandsOverAndSendsHello(t *testing.T) {
	dialer, acceptor, addr := pairHarness(t)
	dialerFP := dialer.peers.Fingerprint()
	acceptorFP := acceptor.peers.Fingerprint()

	err := dialer.l.PairDial(context.Background(), addr, linktest.FakePairRun(dialer.peers, "acceptor"))
	if err != nil {
		t.Fatalf("PairDial: %v", err)
	}

	dialerSalt := dialer.waitUp(t, acceptorFP)
	acceptorSalt := acceptor.waitUp(t, dialerFP)
	if !bytes.Equal(dialerSalt, acceptorSalt) {
		t.Fatalf("salts differ: dialer %x, acceptor %x", dialerSalt, acceptorSalt)
	}
	if len(dialerSalt) != 64 {
		t.Fatalf("salt length = %d, want 64", len(dialerSalt))
	}

	st := dialer.state(acceptorFP)
	if st.Phase != PhaseEstablished || st.Role != pairing.RoleDialer {
		t.Fatalf("state = %+v, want established as dialer", st)
	}
	obs := dialer.peers.Observed(acceptorFP)
	if len(obs.Connected) == 0 || obs.Connected[len(obs.Connected)-1] != addr {
		t.Fatalf("connected addresses = %v, want last %s", obs.Connected, addr)
	}
}

func TestREQBrokerPairing020_DialerWithoutListenerBothDirections(t *testing.T) {
	dialer, acceptor, addr := pairHarness(t)
	dialerFP := dialer.peers.Fingerprint()
	acceptorFP := acceptor.peers.Fingerprint()

	dialerStates := make(chan string, 1)
	acceptorStates := make(chan string, 1)
	dialer.l.HandleState(200, func(_ Fingerprint, f Frame) { dialerStates <- string(f.Payload) })
	acceptor.l.HandleState(200, func(_ Fingerprint, f Frame) { acceptorStates <- string(f.Payload) })
	dialer.l.HandleRequest(300, func(_ context.Context, _ Fingerprint, f Frame) Frame {
		return Frame{Kind: 300, Payload: append([]byte("dialer:"), f.Payload...)}
	})
	acceptor.l.HandleRequest(300, func(_ context.Context, _ Fingerprint, f Frame) Frame {
		return Frame{Kind: 300, Payload: append([]byte("acceptor:"), f.Payload...)}
	})

	if err := dialer.l.PairDial(context.Background(), addr, linktest.FakePairRun(dialer.peers, "acceptor")); err != nil {
		t.Fatalf("PairDial: %v", err)
	}
	dialer.waitUp(t, acceptorFP)
	acceptor.waitUp(t, dialerFP)

	if err := acceptor.l.Send(dialerFP, Frame{Kind: 200, Payload: []byte("to-dialer")}); err != nil {
		t.Fatalf("acceptor Send: %v", err)
	}
	select {
	case got := <-dialerStates:
		if got != "to-dialer" {
			t.Fatalf("dialer state frame = %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dialer did not receive the acceptor's state frame")
	}

	if err := dialer.l.Send(acceptorFP, Frame{Kind: 200, Payload: []byte("to-acceptor")}); err != nil {
		t.Fatalf("dialer Send: %v", err)
	}
	select {
	case got := <-acceptorStates:
		if got != "to-acceptor" {
			t.Fatalf("acceptor state frame = %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("acceptor did not receive the dialer's state frame")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reply, sent, err := dialer.l.Request(ctx, acceptorFP, Frame{Kind: 300, Payload: []byte("d")})
	if err != nil || !sent {
		t.Fatalf("dialer Request: sent=%v err=%v", sent, err)
	}
	if string(reply.Payload) != "acceptor:d" {
		t.Fatalf("dialer reply = %q", reply.Payload)
	}

	reply, sent, err = acceptor.l.Request(ctx, dialerFP, Frame{Kind: 300, Payload: []byte("a")})
	if err != nil || !sent {
		t.Fatalf("acceptor Request: sent=%v err=%v", sent, err)
	}
	if string(reply.Payload) != "dialer:a" {
		t.Fatalf("acceptor reply = %q", reply.Payload)
	}
}

func TestPairDialWrapsDialFailure(t *testing.T) {
	dialer := newDialHarness(t)
	runCalled := false
	err := dialer.l.PairDial(context.Background(), unreachableAddr, func(pairing.PairConn) error {
		runCalled = true
		return nil
	})
	if !errors.Is(err, pairing.ErrDialFailed) {
		t.Fatalf("PairDial error = %v, want ErrDialFailed", err)
	}
	if runCalled {
		t.Fatal("run was called after a failed dial")
	}
}

func TestPairDialReturnsRunErrorUnchanged(t *testing.T) {
	acceptor := newDialHarness(t)
	handlerStarted := make(chan struct{}, 1)
	streamDone := make(chan struct{}, 1)
	addr := serveAcceptorWith(t, acceptor, func(c *conn, tr *trackedConn) {
		handlerStarted <- struct{}{}
		if _, ok := acceptor.l.serveInbound(c, tr); !ok {
			streamDone <- struct{}{}
		}
	})
	dialer := newDialHarness(t)

	errSentinel := errors.New("pairing run failed")
	err := dialer.l.PairDial(context.Background(), addr, func(pairing.PairConn) error {
		select {
		case <-handlerStarted:
		case <-time.After(3 * time.Second):
			t.Error("acceptor handler did not start")
		}
		return errSentinel
	})
	if !errors.Is(err, errSentinel) {
		t.Fatalf("PairDial error = %v, want %v", err, errSentinel)
	}
	select {
	case <-streamDone:
	case <-time.After(3 * time.Second):
		t.Fatal("acceptor side stream did not end")
	}
}

func TestPairConnReceiveHonoursContext(t *testing.T) {
	acceptor := newDialHarness(t)
	addr := serveAcceptor(t, acceptor)
	dialer := newDialHarness(t)

	err := dialer.l.PairDial(context.Background(), addr, func(pc pairing.PairConn) error {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _, err := pc.Receive(ctx)
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PairDial error = %v, want context.DeadlineExceeded", err)
	}
}

func TestPairDialAfterRunHelloFailure(t *testing.T) {
	acceptor := newDialHarness(t)
	addr := serveAcceptor(t, acceptor)
	// The accept function completes the pairing frames but does not commit the
	// dialer, so the acceptor rejects the following Hello.
	acceptor.peers.SetAccept(pairAcceptUncommitted())
	dialer := newDialHarness(t)
	dialer.l.Start()

	acceptorFP := acceptor.peers.Fingerprint()
	if err := dialer.l.PairDial(context.Background(), addr, linktest.FakePairRun(dialer.peers, "acceptor")); err != nil {
		t.Fatalf("PairDial: %v", err)
	}

	st := dialer.waitState(t, acceptorFP, "rejected", func(st LinkState) bool {
		return st.Phase == PhaseRejected
	})
	if st.LastFailure != FailNotAPeer {
		t.Fatalf("last failure = %v, want not_a_peer", st.LastFailure)
	}
	waitFor(t, func() bool { return dialer.hasEvent(config.EventRejectedByPeer) })
}

func TestRepairReplacesExistingLink(t *testing.T) {
	dialer, acceptor, addr := pairHarness(t)
	acceptorFP := acceptor.peers.Fingerprint()

	if err := dialer.l.PairDial(context.Background(), addr, linktest.FakePairRun(dialer.peers, "acceptor")); err != nil {
		t.Fatalf("first PairDial: %v", err)
	}
	firstSalt := dialer.waitUp(t, acceptorFP)

	repair := func(pc pairing.PairConn) error {
		if err := linktest.FakePairRun(dialer.peers, "acceptor")(pc); err != nil {
			return err
		}
		// pairing calls Drop while re-pairing an existing peer.
		dialer.l.Drop(pc.PeerFingerprint())
		return nil
	}
	if err := dialer.l.PairDial(context.Background(), addr, repair); err != nil {
		t.Fatalf("second PairDial: %v", err)
	}

	events := dialer.waitEventCount(t, 3)
	if len(events) != 3 {
		t.Fatalf("events = %+v, want exactly up, down, up", events)
	}
	if !events[0].up || events[1].up || !events[2].up {
		t.Fatalf("events = %+v, want up, down, up", events)
	}
	if bytes.Equal(events[0].salt, events[2].salt) {
		t.Fatal("salt of the new link equals the old one")
	}
	if !bytes.Equal(events[0].salt, firstSalt) {
		t.Fatal("first up event did not carry the first salt")
	}
	if st := dialer.state(acceptorFP); st.Phase != PhaseEstablished {
		t.Fatalf("phase after repair = %v, want established", st.Phase)
	}
}

func TestPairDialCallerCancelAfterReturnKeepsLink(t *testing.T) {
	dialer, acceptor, addr := pairHarness(t)
	acceptorFP := acceptor.peers.Fingerprint()
	dialerFP := dialer.peers.Fingerprint()

	ctx, cancel := context.WithCancel(context.Background())
	if err := dialer.l.PairDial(ctx, addr, linktest.FakePairRun(dialer.peers, "acceptor")); err != nil {
		t.Fatalf("PairDial: %v", err)
	}
	dialer.waitUp(t, acceptorFP)
	acceptor.waitUp(t, dialerFP)
	cancel()

	received := make(chan string, 1)
	acceptor.l.HandleState(200, func(_ Fingerprint, f Frame) { received <- string(f.Payload) })
	if err := dialer.l.Send(acceptorFP, Frame{Kind: 200, Payload: []byte("after-cancel")}); err != nil {
		t.Fatalf("Send after cancel: %v", err)
	}
	select {
	case got := <-received:
		if got != "after-cancel" {
			t.Fatalf("state frame = %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("link died with the caller's context")
	}
}

// pairAcceptUncommitted performs the acceptor half of the pairing frame
// exchange and pushes the link Hello back, but never adds the dialer to the
// peer table.
func pairAcceptUncommitted() func(pairing.PairConn) error {
	return func(conn pairing.PairConn) error {
		fail := func(err error) error {
			_ = conn.Close()
			return err
		}
		ctx := context.Background()
		if err := pairExpect(ctx, conn, pairingpb.KindPairRequest); err != nil {
			return fail(err)
		}
		if err := conn.Send(ctx, pairingpb.KindPairChallenge, nil); err != nil {
			return fail(err)
		}
		if err := pairExpect(ctx, conn, pairingpb.KindPairConfirm); err != nil {
			return fail(err)
		}
		if err := conn.Send(ctx, pairingpb.KindPairDone, nil); err != nil {
			return fail(err)
		}
		kind, payload, err := conn.Receive(ctx)
		if err != nil {
			return fail(err)
		}
		if kind != linkpb.KindHello {
			return fail(fmt.Errorf("pairing hand-off frame kind %d, want %d", kind, linkpb.KindHello))
		}
		conn.Unread(kind, payload)
		return nil
	}
}

// pairExpect reads one frame and requires the expected kind.
func pairExpect(ctx context.Context, conn pairing.PairConn, want uint32) error {
	kind, _, err := conn.Receive(ctx)
	if err != nil {
		return err
	}
	if kind != want {
		return fmt.Errorf("pairing frame kind %d, want %d", kind, want)
	}
	return nil
}
