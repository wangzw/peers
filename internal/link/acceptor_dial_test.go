package link_test

import (
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// TestREQBrokerPairing027_AcceptorNeverDialsWithCandidates checks that the
// accepting side never dials after a teardown even when its peer table holds a
// candidate address.
func TestREQBrokerPairing027_AcceptorNeverDialsWithCandidates(t *testing.T) {
	acceptor := newNode(t, "acceptor")
	dialer := newNode(t, "dialer")
	dials := newDialRecorder()
	link.SetTestKnobs(acceptor.Link, link.TestKnobs{OnDial: dials.dial})

	addr := listenAt(t, acceptor)
	proxy := linktest.NewProxy(t, addr)
	connectVia(t, dialer, acceptor, proxy.Addr())

	fpDialer := dialer.Peers.Fingerprint()
	if got := dials.n(); got != 0 {
		t.Fatalf("acceptor dialed %d times before the link broke", got)
	}

	// Register the dialer as RoleDialer because FakePeers offers candidates only
	// to such an entry, and only after the link is up: the established record
	// keeps its acceptor role.
	acceptor.Peers.Add(fpDialer, pairing.RoleDialer, "dialer", "127.0.0.1:1")

	proxy.CloseConns()
	acceptor.waitDown(t, fpDialer, 1)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		acceptor.Clock.Advance(2 * time.Minute)
		if dials.n() > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if got := dials.n(); got != 0 {
		t.Fatalf("acceptor dialed %d times after the link broke", got)
	}
	if st := acceptor.State(fpDialer); st.Phase != link.PhaseWaiting || st.Role != pairing.RoleAcceptor || !st.NextDial.IsZero() {
		t.Fatalf("acceptor state after the teardown = %+v, want a waiting acceptor with no next dial", st)
	}
}
