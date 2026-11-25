package broker

import (
	"context"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/pairing"
)

// TestPairingSatisfiesPeerStore carries no body: the compile-time assertions in
// peers_fakes_test.go fail the build if pairing no longer satisfies PeerStore.
func TestPairingSatisfiesPeerStore(t *testing.T) {}

// TestFakePairingUnpairRemoves checks that the fake's Unpair removes the peer
// from every lookup.
func TestFakePairingUnpairRemoves(t *testing.T) {
	p := &fakePairing{peers: []pairing.Peer{
		{Fingerprint: fp(1), Alias: "one"},
		{Fingerprint: fp(2), Alias: "two"},
	}}

	if err := p.Unpair(fp(1)); err != nil {
		t.Fatalf("Unpair: %v", err)
	}
	if p.IsPeer(fp(1)) {
		t.Error("IsPeer(fp(1)) = true after Unpair, want false")
	}
	if got := p.Role(fp(1)); got != pairing.RoleUnknown {
		t.Errorf("Role(fp(1)) = %v, want RoleUnknown", got)
	}
	if _, ok := p.ResolveAlias("one"); ok {
		t.Error(`ResolveAlias("one") still resolves after Unpair`)
	}
	peers := p.Peers()
	if len(peers) != 1 || peers[0].Fingerprint != fp(2) {
		t.Errorf("Peers() = %v, want only fp(2)", peers)
	}
}

// TestFakePairingCallsOmitKey checks that the fake's call log never contains the
// pairing key.
func TestFakePairingCallsOmitKey(t *testing.T) {
	p := &fakePairing{}
	// The double has no connectFn, so this fails after recording the call; the
	// record is what the test inspects.
	if _, _, err := p.Connect(context.Background(), "h:1", "SECRET-KEY", ""); err == nil {
		t.Fatal("Connect with no connectFn returned no error")
	}
	if len(p.calls) != 1 {
		t.Fatalf("calls = %v, want exactly one entry", p.calls)
	}
	for _, call := range p.calls {
		if strings.Contains(call, "SECRET-KEY") {
			t.Fatalf("call log leaked the pairing key: %q", call)
		}
	}
}
