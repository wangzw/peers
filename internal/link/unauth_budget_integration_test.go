package link_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// TestAuthenticatedLinkAcceptsLargeFrameAfterHello is the regression guard for
// the pre-authentication read budget: the acceptor leaves the registry as soon
// as it has verified the dialer's Hello, before it answers with its own. The
// dialer sends a 1 MiB state frame immediately after it reads that Hello,
// while the acceptor is held in the window between sending its Hello and
// installing the link. If the acceptor authenticated after answering instead,
// the frame would be charged to the 64 KiB budget and the connection closed.
func TestAuthenticatedLinkAcceptsLargeFrameAfterHello(t *testing.T) {
	helloSent := make(chan struct{})
	release := make(chan struct{})
	readPastBudget := make(chan struct{})
	var readOnce sync.Once

	acceptor := newNode(t, "acceptor")
	link.SetTestKnobs(acceptor.Link, link.TestKnobs{
		// Hold the accepting side after it answers the dialer, before install.
		AfterHelloSent: func() {
			close(helloSent)
			<-release
		},
		// Observe the bytes the accepting side reads, including the ones read
		// after it left the registry.
		OnUnauthenticatedRead: func(total int64) {
			if total > 64<<10 {
				readOnce.Do(func() { close(readPastBudget) })
			}
		},
	})
	dialer := newNode(t, "dialer")

	payload := bytes.Repeat([]byte("p"), 1<<20)
	received := make(chan []byte, 1)
	acceptor.HandleState(200, func(_ link.Fingerprint, f link.Frame) {
		received <- append([]byte(nil), f.Payload...)
	})

	addr := listenAt(t, acceptor)
	linktest.Introduce(dialer.Peers, acceptor.Peers, dialer.Settings.Alias(), acceptor.Settings.Alias(), addr)
	dialer.Start()
	acceptor.Start()
	fpAcceptor := acceptor.Peers.Fingerprint()

	// The dialer installs its side as soon as it reads the acceptor's Hello,
	// while the acceptor is still held before install.
	dialer.waitUp(t, fpAcceptor, 1)
	select {
	case <-helloSent:
	case <-time.After(eventTimeout):
		t.Fatal("the acceptor never sent its Hello")
	}

	if err := dialer.Send(fpAcceptor, link.Frame{Kind: 200, Payload: payload}); err != nil {
		t.Fatalf("Send a 1 MiB frame after the Hello: %v", err)
	}

	// Let the frame travel before releasing the acceptor. The observer runs
	// after the budget check, so when it fires the outcome is already decided:
	// an acceptor that was still unauthenticated closed the connection here.
	select {
	case <-readPastBudget:
	case <-time.After(eventTimeout):
		t.Fatal("the accepting side never read the frame past the pre-authentication budget")
	}
	close(release)

	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatalf("acceptor received %d bytes, want the 1 MiB payload", len(got))
		}
	case <-time.After(eventTimeout):
		t.Fatal("acceptor did not receive the 1 MiB state frame")
	}
	if st := acceptor.State(dialer.Peers.Fingerprint()); st.Phase != link.PhaseEstablished {
		t.Fatalf("acceptor link state = %+v, want established", st)
	}
}

// TestPairingHandshakeStaysWithinReadBudget checks that a complete pairing
// handshake with ordinary alias and address fields stays below the 64 KiB
// pre-authentication read budget and never logs cause byte_budget.
func TestPairingHandshakeStaysWithinReadBudget(t *testing.T) {
	var logs syncBuffer
	acceptor := newNode(t, "acceptor", func(o *link.Options) {
		o.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})
	dialer := newNode(t, "dialer")
	addr := listenAt(t, acceptor)
	acceptor.Start()
	dialer.Start()

	fpAcceptor := acceptor.Peers.Fingerprint()
	fpDialer := dialer.Peers.Fingerprint()
	acceptor.Peers.SetAccept(linktest.FakePairAccept(acceptor.Peers, "dialer"))

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	if err := dialer.PairDial(ctx, addr, linktest.FakePairRun(dialer.Peers, "acceptor")); err != nil {
		t.Fatalf("PairDial: %v", err)
	}

	dialer.waitUp(t, fpAcceptor, 1)
	acceptor.waitUp(t, fpDialer, 1)

	if strings.Contains(logs.String(), `"cause":"byte_budget"`) {
		t.Fatalf("a normal pairing handshake hit the pre-authentication read budget:\n%s", logs.String())
	}
}
