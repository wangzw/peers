package broker

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

func fingerprintBytes(b byte) []byte {
	f := fp(b)
	return f[:]
}

// TestPeerInfoDialerConnected checks a dialer's addresses from the acceptor and
// the card, its connected status and its role.
func TestPeerInfoDialerConnected(t *testing.T) {
	p := pairing.Peer{
		Fingerprint: fp(1),
		Alias:       "hostB",
		Role:        pairing.RoleDialer,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: "192.0.2.10:17443"},
			{Source: pairing.SourceAcceptor, Addr: "203.0.113.5:17443"},
			{Source: pairing.SourceCard, Addr: "203.0.113.5:17443"},
			{Source: pairing.SourceAcceptor, Addr: "10.0.0.5:17443"},
		},
	}
	got := peerBrokerInfo(p, PeerLinkStatus{Phase: LinkEstablished}, nil)
	want := &localpb.BrokerInfo{
		Alias:       "hostB",
		Fingerprint: fingerprintBytes(1),
		AdvertisedAddresses: []*localpb.AdvertisedAddress{
			{Kind: "direct", Addr: "203.0.113.5:17443"},
			{Kind: "direct", Addr: "10.0.0.5:17443"},
		},
		IsPeer:    true,
		Status:    localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
		Role:      localpb.PeerRole_PEER_ROLE_DIALER,
		LinkState: localpb.LinkState_LINK_STATE_ESTABLISHED,
	}
	if !proto.Equal(got, want) {
		t.Fatalf("peerBrokerInfo = %v, want %v", got, want)
	}

	t.Run("source card only", func(t *testing.T) {
		withCard := pairing.Peer{
			Fingerprint: fp(1),
			Alias:       "hostB",
			Role:        pairing.RoleDialer,
			Candidates: []pairing.Candidate{
				{Source: pairing.SourceCard, Addr: "198.51.100.7:17443"},
			},
		}
		got := peerBrokerInfo(withCard, PeerLinkStatus{Phase: LinkEstablished}, nil)
		if len(got.AdvertisedAddresses) != 0 {
			t.Fatalf("advertised addresses = %v, want none from a SourceCard candidate", got.AdvertisedAddresses)
		}
		if got.AdvertisedAddresses == nil {
			t.Fatal("AdvertisedAddresses is nil, want an empty slice")
		}
	})

	t.Run("card merges and dedupes", func(t *testing.T) {
		card := []config.AdvertisedAddr{
			{Kind: config.AdvertisedKindDirect, Addr: "203.0.113.5:17443"},
			{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.8:17443"},
		}
		got := peerBrokerInfo(p, PeerLinkStatus{Phase: LinkEstablished}, card)
		want := []*localpb.AdvertisedAddress{
			{Kind: "direct", Addr: "203.0.113.5:17443"},
			{Kind: "direct", Addr: "10.0.0.5:17443"},
			{Kind: "direct", Addr: "198.51.100.8:17443"},
		}
		if !proto.Equal(&localpb.BrokerInfo{AdvertisedAddresses: got.AdvertisedAddresses},
			&localpb.BrokerInfo{AdvertisedAddresses: want}) {
			t.Fatalf("advertised addresses = %v, want %v", got.AdvertisedAddresses, want)
		}
	})
}

// TestPeerInfoAcceptorWithoutCard checks that an acceptor without a card has no
// addresses and is not directly reachable.
func TestPeerInfoAcceptorWithoutCard(t *testing.T) {
	p := pairing.Peer{Fingerprint: fp(2), Alias: "hostC", Role: pairing.RoleAcceptor}
	got := peerBrokerInfo(p, PeerLinkStatus{Phase: LinkWaiting}, nil)
	want := &localpb.BrokerInfo{
		Alias:               "hostC",
		Fingerprint:         fingerprintBytes(2),
		AdvertisedAddresses: []*localpb.AdvertisedAddress{},
		IsPeer:              true,
		Status:              localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE,
		Role:                localpb.PeerRole_PEER_ROLE_ACCEPTOR,
		LinkState:           localpb.LinkState_LINK_STATE_WAITING,
	}
	if !proto.Equal(got, want) {
		t.Fatalf("peerBrokerInfo = %v, want %v", got, want)
	}
	if got.AdvertisedAddresses == nil {
		t.Fatal("AdvertisedAddresses is nil, want an empty slice")
	}
}

// TestPeerInfoAcceptorWithCard checks that an acceptor with a card uses the
// card's direct addresses and is connectable.
func TestPeerInfoAcceptorWithCard(t *testing.T) {
	p := pairing.Peer{
		Fingerprint: fp(3),
		Alias:       "hostD",
		Role:        pairing.RoleAcceptor,
		Candidates:  []pairing.Candidate{{Source: pairing.SourceAcceptor, Addr: "192.0.2.99:17443"}},
	}
	card := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.4:17443"},
		{Kind: "relay", Addr: "r:1"},
	}
	got := peerBrokerInfo(p, PeerLinkStatus{Phase: LinkWaiting}, card)
	want := []*localpb.AdvertisedAddress{{Kind: "direct", Addr: "198.51.100.4:17443"}}
	if !proto.Equal(&localpb.BrokerInfo{AdvertisedAddresses: got.AdvertisedAddresses},
		&localpb.BrokerInfo{AdvertisedAddresses: want}) {
		t.Fatalf("advertised addresses = %v, want %v", got.AdvertisedAddresses, want)
	}
	if got.Status != localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE {
		t.Fatalf("status = %v, want CONNECTABLE", got.Status)
	}
	if got.Role != localpb.PeerRole_PEER_ROLE_ACCEPTOR {
		t.Fatalf("role = %v, want ACCEPTOR", got.Role)
	}
}

// TestPeerInfoDialerBackoffConnectable checks that a dialer in backoff with a
// known address is connectable.
func TestPeerInfoDialerBackoffConnectable(t *testing.T) {
	p := pairing.Peer{
		Fingerprint: fp(4),
		Alias:       "hostB",
		Role:        pairing.RoleDialer,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourceAcceptor, Addr: "203.0.113.5:17443"},
		},
	}
	got := peerBrokerInfo(p, PeerLinkStatus{Phase: LinkBackoff}, nil)
	if got.Status != localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE {
		t.Fatalf("status = %v, want CONNECTABLE", got.Status)
	}
	if got.LinkState != localpb.LinkState_LINK_STATE_BACKOFF {
		t.Fatalf("link state = %v, want BACKOFF", got.LinkState)
	}
}

// TestPeerStatusDetailRejected checks that a not-a-peer rejection explains
// re-pairing and maps to REJECTED.
func TestPeerStatusDetailRejected(t *testing.T) {
	st := PeerLinkStatus{Phase: LinkRejected, Failure: FailureNotAPeer}
	detail := peerStatusDetail(st)
	if !strings.Contains(detail, "no longer recognizes this Broker") || !strings.Contains(detail, "pair again") {
		t.Fatalf("status detail = %q, want it to say the peer no longer recognizes this Broker and to pair again", detail)
	}
	if got := linkStateToProto(st.Phase); got != localpb.LinkState_LINK_STATE_REJECTED {
		t.Fatalf("link state = %v, want REJECTED", got)
	}
}

// TestPeerStatusDetailTable checks the status detail text for each failure
// reason.
func TestPeerStatusDetailTable(t *testing.T) {
	tests := []struct {
		name string
		st   PeerLinkStatus
		want string
	}{
		{
			name: "not a peer",
			st:   PeerLinkStatus{Failure: FailureNotAPeer},
			want: "the peer no longer recognizes this Broker as its peer (it was unpaired on that side); redialing has stopped; pair again to reconnect",
		},
		{
			name: "role mismatch",
			st:   PeerLinkStatus{Failure: FailureRoleMismatch},
			want: "both Brokers consider themselves the dialer for this pair; pair again to fix the roles",
		},
		{
			name: "version incompatible",
			st:   PeerLinkStatus{Failure: FailureVersionIncompatible},
			want: "link protocol version incompatible with this peer; upgrade the older Broker",
		},
		{
			name: "version incompatible with peer version",
			st:   PeerLinkStatus{Failure: FailureVersionIncompatible, Detail: "peer speaks link protocol version 2"},
			want: "link protocol version incompatible with this peer; upgrade the older Broker: peer speaks link protocol version 2",
		},
		{
			name: "inbound limit",
			st:   PeerLinkStatus{Phase: LinkBackoff, Failure: FailureInboundLimit},
			want: "the peer sent more state frames than this Broker's inbound limit allows on one connection; the link was closed and will reconnect after a backoff; check the audit log for the link_down record",
		},
		{
			name: "none",
			st:   PeerLinkStatus{Failure: FailureNone, Detail: "ignored when there is no failure"},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := peerStatusDetail(tt.st); got != tt.want {
				t.Fatalf("peerStatusDetail(%+v) = %q, want %q", tt.st, got, tt.want)
			}
		})
	}
}

// TestLinkStateToProtoTable checks the mapping from every link phase to the
// local API state.
func TestLinkStateToProtoTable(t *testing.T) {
	tests := []struct {
		phase LinkPhase
		want  localpb.LinkState
	}{
		{LinkNone, localpb.LinkState_LINK_STATE_UNSPECIFIED},
		{LinkDialing, localpb.LinkState_LINK_STATE_DIALING},
		{LinkWaiting, localpb.LinkState_LINK_STATE_WAITING},
		{LinkHandshaking, localpb.LinkState_LINK_STATE_HANDSHAKING},
		{LinkEstablished, localpb.LinkState_LINK_STATE_ESTABLISHED},
		{LinkBackoff, localpb.LinkState_LINK_STATE_BACKOFF},
		{LinkRejected, localpb.LinkState_LINK_STATE_REJECTED},
	}
	for _, tt := range tests {
		if got := linkStateToProto(tt.phase); got != tt.want {
			t.Fatalf("linkStateToProto(%v) = %v, want %v", tt.phase, got, tt.want)
		}
	}
}

// TestPeerViewEffectiveAlias checks the effective alias of a peer and of a
// non-peer.
func TestPeerViewEffectiveAlias(t *testing.T) {
	pr := &fakePairing{peers: []pairing.Peer{
		{Fingerprint: fp(2), Alias: "hostB", LocalAlias: "lab-b"},
	}}
	v := newPeerView(pr, &fakePeerLinks{}, nil)
	if got, ok := v.EffectiveAlias(fp(2)); !ok || got != "lab-b" {
		t.Fatalf("EffectiveAlias(peer) = %q, %v, want lab-b, true", got, ok)
	}
	if got, ok := v.EffectiveAlias(fp(3)); ok || got != "" {
		t.Fatalf("EffectiveAlias(non-peer) = %q, %v, want \"\", false", got, ok)
	}
}

// TestPeerViewInfosReadsCatalogEachTime checks that each listing reads the
// catalog, so a later catalog still supplies addresses.
func TestPeerViewInfosReadsCatalogEachTime(t *testing.T) {
	p := pairing.Peer{Fingerprint: fp(2), Alias: "hostC", Role: pairing.RoleAcceptor}
	pr := &fakePairing{peers: []pairing.Peer{p}}
	catalog := &fakeCatalog{cards: map[pairing.Fingerprint][]config.AdvertisedAddr{
		fp(2): {{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.4:17443"}},
	}}
	calls := 0
	v := newPeerViewFunc(pr, &fakePeerLinks{}, func() BrokerCatalog {
		calls++
		if calls == 1 {
			return nil
		}
		return catalog
	})

	first := v.Peers(context.Background())
	if len(first) != 1 {
		t.Fatalf("first listing has %d entries, want 1", len(first))
	}
	if n := len(first[0].AdvertisedAddresses); n != 0 {
		t.Fatalf("first listing has %d addresses, want 0 before the catalog exists", n)
	}

	second := v.Peers(context.Background())
	if len(second) != 1 {
		t.Fatalf("second listing has %d entries, want 1", len(second))
	}
	if len(second[0].AdvertisedAddresses) != 1 || second[0].AdvertisedAddresses[0].Addr != "198.51.100.4:17443" {
		t.Fatalf("second listing addresses = %v, want the card address", second[0].AdvertisedAddresses)
	}
}

// TestSortBrokerInfos checks that peers sort first, then alias, then fingerprint
// bytes.
func TestSortBrokerInfos(t *testing.T) {
	infos := []*localpb.BrokerInfo{
		{Alias: "zeta", Fingerprint: fingerprintBytes(9), IsPeer: false},
		{Alias: "bravo", Fingerprint: fingerprintBytes(3), IsPeer: true},
		{Alias: "alpha", Fingerprint: fingerprintBytes(5), IsPeer: true},
		{Alias: "alpha", Fingerprint: fingerprintBytes(2), IsPeer: true},
		{Alias: "aardvark", Fingerprint: fingerprintBytes(4), IsPeer: false},
	}
	sortBrokerInfos(infos)

	wantAliases := []string{"alpha", "alpha", "bravo", "aardvark", "zeta"}
	wantFingerprints := []byte{2, 5, 3, 4, 9}
	for i := range infos {
		if infos[i].Alias != wantAliases[i] || infos[i].Fingerprint[0] != wantFingerprints[i] {
			t.Fatalf("entry %d = %s/%x, want %s/%x; full order %v",
				i, infos[i].Alias, infos[i].Fingerprint[:1], wantAliases[i], wantFingerprints[i], aliasesOf(infos))
		}
	}
}

func aliasesOf(infos []*localpb.BrokerInfo) []string {
	out := make([]string, len(infos))
	for i, info := range infos {
		out[i] = info.GetAlias()
	}
	return out
}
