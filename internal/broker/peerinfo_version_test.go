package broker

import (
	"context"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// TestStatusDetailVersionWithoutPeerVersion checks that a version mismatch
// explains the upgrade even without the peer's version.
func TestStatusDetailVersionWithoutPeerVersion(t *testing.T) {
	want := "link protocol version incompatible with this peer; upgrade the older Broker"
	got := peerStatusDetail(PeerLinkStatus{Phase: LinkBackoff, Failure: FailureVersionIncompatible})
	if got != want {
		t.Fatalf("status detail = %q, want %q", got, want)
	}
}

// TestStatusDetailVersionWithPeerVersion checks that a version mismatch names
// the peer's version when it is known.
func TestStatusDetailVersionWithPeerVersion(t *testing.T) {
	want := "link protocol version incompatible with this peer; upgrade the older Broker: peer speaks link protocol version 2"
	got := peerStatusDetail(peerLinkStatus(link.LinkState{
		Phase:       link.PhaseWaiting,
		LastFailure: link.FailVersionIncompatible,
		PeerVersion: 2,
	}))
	if got != want {
		t.Fatalf("status detail = %q, want %q", got, want)
	}
}

// TestListBrokersAndStatusShareDetail checks that ListBrokers and Status show the
// same version detail for a backing-off peer.
func TestListBrokersAndStatusShareDetail(t *testing.T) {
	want := "link protocol version incompatible with this peer; upgrade the older Broker: peer speaks link protocol version 2"
	p := &fakePairing{peers: []pairing.Peer{
		{Fingerprint: fp(5), Alias: "hostB", Role: pairing.RoleDialer},
	}}
	l := &fakePeerLinks{status: map[pairing.Fingerprint]PeerLinkStatus{
		fp(5): {
			Phase:   LinkBackoff,
			Failure: FailureVersionIncompatible,
			Detail:  "peer speaks link protocol version 2",
		},
	}}
	tb := newTestBroker(t, withPeers(p, l))

	listed, err := tb.client.ListBrokers(context.Background(), &localpb.ListBrokersRequest{})
	if err != nil {
		t.Fatalf("ListBrokers: %v", err)
	}
	peers := listBrokersPeers(listed.GetBrokers())
	if len(peers) != 1 {
		t.Fatalf("ListBrokers returned %d peers, want 1: %v", len(peers), listed.GetBrokers())
	}
	if got := peers[0].GetStatusDetail(); got != want {
		t.Errorf("ListBrokers status_detail = %q, want %q", got, want)
	}

	status, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(status.GetPeers()) != 1 {
		t.Fatalf("Status returned %d peers, want 1: %v", len(status.GetPeers()), status.GetPeers())
	}
	if got := status.GetPeers()[0].GetStatusDetail(); got != want {
		t.Errorf("Status status_detail = %q, want %q", got, want)
	}
}
