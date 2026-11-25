package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

// TestOtherCallsNeverDial checks that only Connect reaches the network, while
// list, disconnect and key creation never dial.
func TestOtherCallsNeverDial(t *testing.T) {
	var dials, connects atomic.Int32
	p := &fakePairing{
		peers: []pairing.Peer{
			{Fingerprint: fp(2), Alias: "hostB", Role: pairing.RoleDialer},
		},
		connectFn: func(context.Context, string, string, string) (pairing.Peer, string, error) {
			connects.Add(1)
			return pairing.Peer{}, "", nil
		},
		keyFn: func() (string, time.Time, []string, error) {
			return "K7QF-2M9X-HT4C-PWZ3", time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC), nil, nil
		},
	}
	l := &fakePeerLinks{
		status: map[pairing.Fingerprint]PeerLinkStatus{},
		pairDialFn: func(context.Context, string, func(pairing.PairConn) error) error {
			dials.Add(1)
			return nil
		},
	}
	tb := newTestBroker(t, withPeers(p, l))

	if _, err := tb.client.ListBrokers(context.Background(), &localpb.ListBrokersRequest{}); err != nil {
		t.Fatalf("ListBrokers: %v", err)
	}
	if _, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: "hostB"}); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if _, err := tb.client.CreatePairingKey(context.Background(), &localpb.CreatePairingKeyRequest{}); err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}

	if got := dials.Load(); got != 0 {
		t.Errorf("PairDial calls = %d, want 0", got)
	}
	if got := connects.Load(); got != 0 {
		t.Errorf("Connect calls = %d, want 0", got)
	}
}
