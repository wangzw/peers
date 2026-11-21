package broker

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// disconnectTestPeer builds a dialer peer with the given fingerprint and
// self-reported alias.
func disconnectTestPeer(f pairing.Fingerprint, alias string) pairing.Peer {
	return pairing.Peer{Fingerprint: f, Alias: alias, Role: pairing.RoleDialer}
}

func disconnectPairingCalls(p *fakePairing) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func disconnectLinkCalls(l *fakePeerLinks) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

func disconnectHasCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
}

func TestDisconnectByAlias(t *testing.T) {
	// REQ-broker-pairing-021: disconnecting a peer removes the pairing.
	p := &fakePairing{peers: []pairing.Peer{disconnectTestPeer(fp(2), "hostB")}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))

	if _, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: "hostB"}); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if !disconnectHasCall(disconnectPairingCalls(p), "Unpair") {
		t.Fatalf("pairing calls = %v, want Unpair", disconnectPairingCalls(p))
	}
	if p.IsPeer(fp(2)) {
		t.Fatal("peer is still paired after Disconnect")
	}
}

func TestDisconnectByFingerprint(t *testing.T) {
	// REQ-broker-pairing-021: the fingerprint identifies the peer when its
	// effective alias is not at hand.
	p := &fakePairing{peers: []pairing.Peer{disconnectTestPeer(fp(2), "hostB")}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))

	ref := strings.ToUpper(fp(2).String())
	if _, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: ref}); err != nil {
		t.Fatalf("Disconnect(%q): %v", ref, err)
	}
	if p.IsPeer(fp(2)) {
		t.Fatal("peer is still paired after Disconnect by fingerprint")
	}
}

func TestDisconnectUnknown(t *testing.T) {
	p := &fakePairing{peers: []pairing.Peer{disconnectTestPeer(fp(2), "hostB")}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))

	refs := []string{
		"hostZ",                 // no such alias
		fp(9).String(),          // full fingerprint, not a peer
		"0202",                  // a prefix of an existing peer's fingerprint
		strings.Repeat("z", 64), // 64 characters that are not hex
	}
	for _, ref := range refs {
		_, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: ref})
		detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
		if !strings.Contains(detail.GetMessage(), "no peer Broker has the alias or fingerprint") {
			t.Fatalf("Disconnect(%q) message = %q, want the unknown-reference message", ref, detail.GetMessage())
		}
		if strings.Contains(detail.GetMessage(), "list_brokers") {
			t.Fatalf("Disconnect(%q) message = %q, want no MCP tool name", ref, detail.GetMessage())
		}
	}
	if disconnectHasCall(disconnectPairingCalls(p), "Unpair") {
		t.Fatalf("pairing calls = %v, want no Unpair for unknown references", disconnectPairingCalls(p))
	}
}

func TestDisconnectEmpty(t *testing.T) {
	p := &fakePairing{peers: []pairing.Peer{disconnectTestPeer(fp(2), "hostB")}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))

	_, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{})
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT)
	if !strings.Contains(detail.GetMessage(), "broker is required") {
		t.Fatalf("message = %q, want a required-argument explanation", detail.GetMessage())
	}
}

func TestDisconnectDoesNotDropDirectly(t *testing.T) {
	// The link is closed by pairing through the injected Transport.Drop, not by
	// the handler.
	p := &fakePairing{peers: []pairing.Peer{disconnectTestPeer(fp(2), "hostB")}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))

	if _, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: "hostB"}); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	for _, call := range disconnectLinkCalls(l) {
		if strings.HasPrefix(call, "Drop ") {
			t.Fatalf("link calls = %v, want no direct Drop", disconnectLinkCalls(l))
		}
	}
}

func TestDisconnectAuditsAliasBeforeRemoval(t *testing.T) {
	// REQ-broker-config-007: the unpaired record names the peer by the alias it
	// had before removal.
	p := &fakePairing{peers: []pairing.Peer{
		{Fingerprint: fp(2), Alias: "hostB", LocalAlias: "lab-b", Role: pairing.RoleDialer},
	}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))
	id, address := auditRegister(t, tb, "api-worker")

	if _, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: "lab-b", AgentId: id}); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	events := auditsOf(tb.cfg, config.EventUnpaired)
	if len(events) != 1 {
		t.Fatalf("unpaired audit events = %d, want 1", len(events))
	}
	e := events[0]
	if e.Peer == nil {
		t.Fatal("Peer = nil, want the removed peer's reference")
	}
	if e.Peer.Fingerprint != fp(2) {
		t.Fatalf("Peer.Fingerprint = %v, want %v", e.Peer.Fingerprint, fp(2))
	}
	if e.Peer.Alias != "lab-b" {
		t.Fatalf("Peer.Alias = %q, want the alias before removal %q", e.Peer.Alias, "lab-b")
	}
	if e.Actor != address {
		t.Fatalf("Actor = %q, want the registered agent address %q", e.Actor, address)
	}
	if want := usernameFor(uint32(os.Geteuid())); e.Account != want {
		t.Fatalf("Account = %q, want %q", e.Account, want)
	}
}

func TestDisconnectUnpairErrorNotAudited(t *testing.T) {
	p := &fakePairing{
		peers:     []pairing.Peer{disconnectTestPeer(fp(2), "hostB")},
		unpairErr: pairing.ErrStore,
	}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))

	_, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: "hostB"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INTERNAL)
	if events := auditsOf(tb.cfg, config.EventUnpaired); len(events) != 0 {
		t.Fatalf("unpaired audit events = %d, want 0 after a failed Unpair", len(events))
	}
}

func TestDisconnectBadAgentNoSideEffect(t *testing.T) {
	p := &fakePairing{peers: []pairing.Peer{disconnectTestPeer(fp(2), "hostB")}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))

	_, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: "hostB", AgentId: "nope"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)
	if calls := disconnectPairingCalls(p); len(calls) != 0 {
		t.Fatalf("pairing calls = %v, want none before the actor is known", calls)
	}
	if events := auditsOf(tb.cfg, config.EventUnpaired); len(events) != 0 {
		t.Fatalf("unpaired audit events = %d, want 0", len(events))
	}
}

func TestResolvePeerRef(t *testing.T) {
	p := &fakePairing{peers: []pairing.Peer{
		disconnectTestPeer(fp(2), "hostB"),
		{Fingerprint: fp(3), Alias: "hostC", LocalAlias: "lab-c", Role: pairing.RoleAcceptor},
	}}

	tests := []struct {
		name     string
		ref      string
		want     pairing.Fingerprint
		wantCode localpb.ErrorCode
	}{
		{name: "alias", ref: "hostB", want: fp(2)},
		{name: "effective alias", ref: "lab-c", want: fp(3)},
		{name: "lower hex", ref: fp(2).String(), want: fp(2)},
		{name: "upper hex", ref: strings.ToUpper(fp(2).String()), want: fp(2)},
		{name: "empty", ref: "", wantCode: localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{name: "unknown alias", ref: "hostZ", wantCode: localpb.ErrorCode_ERROR_CODE_NOT_FOUND},
		{name: "unknown fingerprint", ref: fp(9).String(), wantCode: localpb.ErrorCode_ERROR_CODE_NOT_FOUND},
		{name: "prefix", ref: "0202", wantCode: localpb.ErrorCode_ERROR_CODE_NOT_FOUND},
		{name: "64 non-hex", ref: strings.Repeat("z", 64), wantCode: localpb.ErrorCode_ERROR_CODE_NOT_FOUND},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolvePeerRef(p, tt.ref)
			if tt.wantCode != localpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
				if err == nil {
					t.Fatalf("resolvePeerRef(%q) = %v, want code %v", tt.ref, got, tt.wantCode)
				}
				if err.Code != tt.wantCode {
					t.Fatalf("resolvePeerRef(%q) code = %v, want %v", tt.ref, err.Code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolvePeerRef(%q): %v", tt.ref, err)
			}
			if got != tt.want {
				t.Fatalf("resolvePeerRef(%q) = %v, want %v", tt.ref, got, tt.want)
			}
		})
	}
}

// TestDisconnectForgetsInboundBuckets checks that unpairing a peer is what
// clears its inbound quota. A link drop keeps the buckets so a reconnect cannot
// refresh them; unpairing starts the peer over.
func TestDisconnectForgetsInboundBuckets(t *testing.T) {
	p := &fakePairing{peers: []pairing.Peer{disconnectTestPeer(fp(2), "hostB")}}
	l := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, l))
	d := newDelivery(deliveryDeps{})
	tb.b.deps.Delivery = d

	for i := 0; i < inboundLimitBurst; i++ {
		if ok, _ := d.inbound.take(fp(2), "target"); !ok {
			t.Fatalf("take %d failed, want a full burst", i)
		}
	}
	if ok, _ := d.inbound.take(fp(2), "target"); ok {
		t.Fatal("take after the burst succeeded, want a refusal")
	}

	if _, err := tb.client.Disconnect(context.Background(), &localpb.DisconnectRequest{Broker: "hostB"}); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if ok, _ := d.inbound.take(fp(2), "target"); !ok {
		t.Fatal("unpairing the peer did not forget its buckets")
	}
}
