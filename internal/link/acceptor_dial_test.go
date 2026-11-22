package link_test

import (
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// TestREQBrokerPairing027_AcceptorNeverDialsWithCandidates checks that the
// accepting side does not dial a peer after a teardown even when its peer table
// holds a candidate address for that peer. TestREQBrokerPairing027 leaves the
// acceptor's candidate list empty, so a dial loop started on the accepting side
// would find nothing to dial and go unnoticed there.
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

	// FakePeers returns candidates only for an entry whose recorded role is
	// RoleDialer. Replace the acceptor's entry for the dialer after the link is
	// established: the established record keeps its acceptor role, while the
	// peer table now offers an address a stray dial loop could use.
	acceptor.Peers.Add(fpDialer, pairing.RoleDialer, "dialer", "127.0.0.1:1")

	proxy.CloseConns()
	acceptor.waitDown(t, fpDialer, 1)

	// Advance the acceptor's clock past the longest reconnect interval. Every
	// step fires a backoff timer registered at any point in the window, so a
	// dial loop started by the teardown gets its turn.
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
	// A dial loop that armed a backoff timer without dialing yet moves the record
	// into PhaseBackoff and publishes a NextDial; check that state, not only the
	// dial count.
	if st := acceptor.State(fpDialer); st.Phase != link.PhaseWaiting || st.Role != pairing.RoleAcceptor || !st.NextDial.IsZero() {
		t.Fatalf("acceptor state after the teardown = %+v, want a waiting acceptor with no next dial", st)
	}
}
