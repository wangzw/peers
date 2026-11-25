package directory

import (
	"bytes"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/pairing"
)

func newViewDirectory(t *testing.T, seed byte, now time.Time) (*Directory, *fakePeers, *fakeLinks, *fakeClock) {
	t.Helper()
	deps, ep := dirTestDeps(t, seed, t.TempDir(), newFakeNet(), nil)
	peers := deps.Peers.(*fakePeers)
	clk := newFakeClock(now)
	d, err := openWith(deps, clk, realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	t.Cleanup(d.Close)
	return d, peers, ep, clk
}

func advAddr(kind, addr string) *directorypb.AdvertisedAddress {
	return &directorypb.AdvertisedAddress{Kind: kind, Addr: addr}
}

func viewPut(t *testing.T, d *Directory, r *storedRecord) {
	t.Helper()
	d.mu.Lock()
	d.st.putOwn(r)
	d.mu.Unlock()
}

func viewCard(t *testing.T, d *Directory, seed byte, alias string, version uint64, expires time.Time, addrs ...*directorypb.AdvertisedAddress) {
	t.Helper()
	viewPut(t, d, mustSignRecord(t, seed, recKindCard, "", version, expires, recTestCard(alias, addrs...)))
}

func viewAnnouncement(t *testing.T, d *Directory, seed byte, name, description string, version uint64, expires time.Time) {
	t.Helper()
	viewPut(t, d, mustSignRecord(t, seed, recKindTopic, name, version, expires,
		&directorypb.TopicAnnouncement{Name: name, Description: description}))
}

func viewRevocation(t *testing.T, d *Directory, seed byte, kind directorypb.RecordKind, object string, version uint64, expires time.Time) {
	t.Helper()
	viewPut(t, d, mustSignRecord(t, seed, kind, object, version, expires,
		&directorypb.Revocation{RevokedKind: kind}))
}

func viewFind(t *testing.T, brokers []KnownBroker, fp Fingerprint) KnownBroker {
	t.Helper()
	for _, b := range brokers {
		if b.Fingerprint == fp {
			return b
		}
	}
	t.Fatalf("broker %v is not listed", fp)
	return KnownBroker{}
}

func viewHas(brokers []KnownBroker, fp Fingerprint) bool {
	for _, b := range brokers {
		if b.Fingerprint == fp {
			return true
		}
	}
	return false
}

// TestBrokersListsPeersAndCardHolders checks the address list of a dialer peer, an acceptor peer and a card holder.
func TestBrokersListsPeersAndCardHolders(t *testing.T) {
	now := dirTestNow()
	d, peers, _, _ := newViewDirectory(t, 1, now)
	fpD := newTestIdentity(2).Fingerprint()
	fpE := newTestIdentity(3).Fingerprint()
	fpX := newTestIdentity(4).Fingerprint()

	peers.Add(pairing.Peer{
		Fingerprint: fpD,
		Alias:       "alpha",
		Role:        pairing.RoleDialer,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourceAcceptor, Addr: "203.0.113.5:17443"},
			{Source: pairing.SourcePairing, Addr: "192.0.2.1:17443"},
			{Source: pairing.SourceCard, Addr: "198.51.100.7:17443"},
		},
	})
	peers.Add(pairing.Peer{
		Fingerprint: fpE,
		Alias:       "beta",
		Role:        pairing.RoleAcceptor,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourceAcceptor, Addr: "203.0.113.6:17443"},
			{Source: pairing.SourceCard, Addr: "198.51.100.14:17443"},
		},
	})
	viewCard(t, d, 4, "gamma", 1, now.Add(recordTTL), advAddr("direct", "198.51.100.6:17443"))

	got := d.Brokers()
	if len(got) != 3 {
		t.Fatalf("Brokers() returned %d entries, want 3", len(got))
	}
	wantOrder := []Fingerprint{fpD, fpE, fpX}
	for i, fp := range wantOrder {
		if got[i].Fingerprint != fp {
			t.Fatalf("entry %d is %v, want %v (aliases must sort ascending)", i, got[i].Fingerprint, fp)
		}
	}

	dEntry := viewFind(t, got, fpD)
	if !dEntry.IsPeer || dEntry.Role != pairing.RoleDialer {
		t.Errorf("dialer peer = peer:%v role:%v, want a dialer peer", dEntry.IsPeer, dEntry.Role)
	}
	wantD := []config.AdvertisedAddr{{Kind: "direct", Addr: "203.0.113.5:17443"}}
	if !reflect.DeepEqual(dEntry.Addresses, wantD) {
		t.Errorf("dialer peer addresses = %v, want only the acceptor-announced one %v", dEntry.Addresses, wantD)
	}

	eEntry := viewFind(t, got, fpE)
	if !eEntry.IsPeer || eEntry.Role != pairing.RoleAcceptor {
		t.Errorf("acceptor peer = peer:%v role:%v, want an acceptor peer", eEntry.IsPeer, eEntry.Role)
	}
	if eEntry.Addresses == nil || len(eEntry.Addresses) != 0 {
		t.Errorf("acceptor peer addresses = %v, want a non-nil empty slice", eEntry.Addresses)
	}

	xEntry := viewFind(t, got, fpX)
	if xEntry.IsPeer || xEntry.Role != pairing.RoleUnknown {
		t.Errorf("card holder = peer:%v role:%v, want a non-peer", xEntry.IsPeer, xEntry.Role)
	}
	wantX := []config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.6:17443"}}
	if !reflect.DeepEqual(xEntry.Addresses, wantX) {
		t.Errorf("card holder addresses = %v, want %v", xEntry.Addresses, wantX)
	}
	if xEntry.Link != LinkNone {
		t.Errorf("card holder link = %v, want LinkNone", xEntry.Link)
	}

	viewCard(t, d, 2, "alpha", 2, now.Add(recordTTL), advAddr("direct", "198.51.100.8:17443"))
	dEntry = viewFind(t, d.Brokers(), fpD)
	wantD = []config.AdvertisedAddr{
		{Kind: "direct", Addr: "203.0.113.5:17443"},
		{Kind: "direct", Addr: "198.51.100.8:17443"},
	}
	if !reflect.DeepEqual(dEntry.Addresses, wantD) {
		t.Errorf("dialer peer addresses after its card arrived = %v, want %v", dEntry.Addresses, wantD)
	}

	viewCard(t, d, 2, "alpha", 3, now.Add(recordTTL),
		advAddr("direct", "203.0.113.5:17443"), advAddr("direct", "198.51.100.8:17443"))
	dEntry = viewFind(t, d.Brokers(), fpD)
	wantD = []config.AdvertisedAddr{
		{Kind: "direct", Addr: "203.0.113.5:17443"},
		{Kind: "direct", Addr: "198.51.100.8:17443"},
	}
	if !reflect.DeepEqual(dEntry.Addresses, wantD) {
		t.Errorf("dialer peer addresses with a repeated card address = %v, want %v", dEntry.Addresses, wantD)
	}
}

// TestBrokersStatus checks connected, connectable and unreachable status and that unknown address kinds are skipped.
func TestBrokersStatus(t *testing.T) {
	now := dirTestNow()
	d, peers, ep, _ := newViewDirectory(t, 1, now)
	fpUp := newTestIdentity(2).Fingerprint()
	fpAddr := newTestIdentity(3).Fingerprint()
	fpNone := newTestIdentity(4).Fingerprint()
	fpUnknown := newTestIdentity(5).Fingerprint()

	peers.Add(pairing.Peer{Fingerprint: fpUp, Alias: "up", Role: pairing.RoleAcceptor})
	ep.SetStatus(fpUp, LinkView{Phase: LinkEstablished})
	peers.Add(pairing.Peer{
		Fingerprint: fpAddr,
		Alias:       "addr",
		Role:        pairing.RoleDialer,
		Candidates:  []pairing.Candidate{{Source: pairing.SourceAcceptor, Addr: "198.51.100.9:17443"}},
	})
	peers.Add(pairing.Peer{Fingerprint: fpNone, Alias: "none", Role: pairing.RoleAcceptor})
	viewCard(t, d, 5, "unknown", 1, now.Add(recordTTL), advAddr("relay", "198.51.100.10:17443"))

	got := d.Brokers()
	cases := []struct {
		name string
		fp   Fingerprint
		want BrokerStatus
	}{
		{"established without addresses", fpUp, StatusConnected},
		{"not established with an address", fpAddr, StatusConnectable},
		{"not established without addresses", fpNone, StatusUnreachable},
		{"card with only unknown address kinds", fpUnknown, StatusUnreachable},
	}
	for _, tc := range cases {
		entry := viewFind(t, got, tc.fp)
		if entry.Status != tc.want {
			t.Errorf("%s: status = %v, want %v", tc.name, entry.Status, tc.want)
		}
	}
	if entry := viewFind(t, got, fpUnknown); len(entry.Addresses) != 0 {
		t.Errorf("unknown address kinds leaked into the view: %v", entry.Addresses)
	}
}

// TestBrokersIsPeerFollowsPairing checks that isPeer follows the current peer table.
func TestBrokersIsPeerFollowsPairing(t *testing.T) {
	now := dirTestNow()
	d, peers, _, _ := newViewDirectory(t, 1, now)
	fpX := newTestIdentity(2).Fingerprint()
	fpY := newTestIdentity(3).Fingerprint()

	peers.Add(pairing.Peer{Fingerprint: fpX, Alias: "x", Role: pairing.RoleAcceptor})
	viewCard(t, d, 2, "x", 1, now.Add(recordTTL), advAddr("direct", "198.51.100.12:17443"))

	got := d.Brokers()
	if entry := viewFind(t, got, fpX); !entry.IsPeer {
		t.Fatal("a paired peer is not marked as a peer")
	}
	if viewHas(got, fpY) {
		t.Fatal("a Broker that is not a peer and holds no card is listed")
	}

	peers.Remove(fpX)
	got = d.Brokers()
	entry := viewFind(t, got, fpX)
	if entry.IsPeer {
		t.Fatal("a Broker removed from the peer table is still marked as a peer")
	}
	if entry.Alias != "x" {
		t.Fatalf("card holder alias = %q, want the card alias", entry.Alias)
	}
}

// TestBrokersExcludesSelfAndRevoked checks that the local card, revoked cards and expired cards are left out.
func TestBrokersExcludesSelfAndRevoked(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	expires := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	alive := newTestIdentity(2).Fingerprint()

	d, _, _, clk := newViewDirectory(t, 1, now)
	viewCard(t, d, 1, "self", 1, expires)
	viewCard(t, d, 2, "alive", 1, expires)
	viewCard(t, d, 3, "revoked", 2, expires)
	viewRevocation(t, d, 3, recKindCard, "", 3, expires)
	viewCard(t, d, 4, "expired", 1, now.Add(-time.Minute))

	got := d.Brokers()
	if len(got) != 1 || got[0].Fingerprint != alive {
		t.Fatalf("Brokers() = %v, want only the live foreign card", got)
	}

	clk.Advance(2 * 24 * time.Hour)
	if got := d.Brokers(); len(got) != 0 {
		t.Fatalf("Brokers() = %v on day 8, want nothing left", got)
	}
}

// TestBrokersPeerDetailEmpty checks that a peer entry reports its link phase and leaves Detail empty.
func TestBrokersPeerDetailEmpty(t *testing.T) {
	now := dirTestNow()
	d, peers, ep, _ := newViewDirectory(t, 1, now)
	fpRejected := newTestIdentity(2).Fingerprint()
	fpRole := newTestIdentity(3).Fingerprint()
	fpVersion := newTestIdentity(4).Fingerprint()
	fpCard := newTestIdentity(5).Fingerprint()

	peers.Add(pairing.Peer{Fingerprint: fpRejected, Alias: "rejected", Role: pairing.RoleAcceptor})
	peers.Add(pairing.Peer{Fingerprint: fpRole, Alias: "role", Role: pairing.RoleAcceptor})
	peers.Add(pairing.Peer{Fingerprint: fpVersion, Alias: "version", Role: pairing.RoleAcceptor})
	ep.SetStatus(fpRejected, LinkView{Phase: LinkRejected, Reason: FailureNotAPeer})
	ep.SetStatus(fpRole, LinkView{Phase: LinkBackoff, Reason: FailureRoleMismatch})
	ep.SetStatus(fpVersion, LinkView{Phase: LinkBackoff, Reason: FailureVersionIncompatible, PeerVersion: 2})
	viewCard(t, d, 5, "card", 1, now.Add(recordTTL), advAddr("direct", "198.51.100.13:17443"))

	got := d.Brokers()
	wantLink := map[Fingerprint]LinkPhase{
		fpRejected: LinkRejected,
		fpRole:     LinkBackoff,
		fpVersion:  LinkBackoff,
		fpCard:     LinkNone,
	}
	for fp, want := range wantLink {
		entry := viewFind(t, got, fp)
		if entry.Link != want {
			t.Errorf("link for %v = %v, want %v", fp, entry.Link, want)
		}
		if entry.Detail != "" {
			t.Errorf("detail for %v = %q, want empty", fp, entry.Detail)
		}
	}
}

// TestPublicTopicsMerge checks that same-name declarations merge into one entry and a revocation lowers the count.
func TestPublicTopicsMerge(t *testing.T) {
	now := dirTestNow()
	d, _, _, _ := newViewDirectory(t, 1, now)
	expires := now.Add(recordTTL)
	viewAnnouncement(t, d, 4, "infra-help", "a", 1, expires)
	viewAnnouncement(t, d, 5, "infra-help", "b", 2, expires)
	viewAnnouncement(t, d, 6, "ops", "", 1, expires)

	want := []PublicTopic{
		{Name: "infra-help", Description: "b", Publisher: newTestIdentity(5).Fingerprint(), Declarers: 2},
		{Name: "ops", Publisher: newTestIdentity(6).Fingerprint(), Declarers: 1},
	}
	if got := d.PublicTopics(); !reflect.DeepEqual(got, want) {
		t.Fatalf("PublicTopics() = %v, want %v", got, want)
	}

	viewRevocation(t, d, 4, recKindTopic, "infra-help", 3, expires)
	want[0].Declarers = 1
	if got := d.PublicTopics(); !reflect.DeepEqual(got, want) {
		t.Fatalf("PublicTopics() after one publisher revoked = %v, want %v", got, want)
	}
}

// TestPublicTopicsPrefersSelfAndPeerDescriptions checks that the local and peer tiers outrank a large version from another publisher.
func TestPublicTopicsPrefersSelfAndPeerDescriptions(t *testing.T) {
	now := dirTestNow()
	d, peers, _, _ := newViewDirectory(t, 1, now)
	expires := now.Add(recordTTL)
	self := newTestIdentity(1).Fingerprint()
	fpPeer := newTestIdentity(2).Fingerprint()
	fpOther := newTestIdentity(3).Fingerprint()
	fpOtherLow := newTestIdentity(4).Fingerprint()

	peers.Add(pairing.Peer{
		Fingerprint: fpPeer,
		Alias:       "peer-alias",
		LocalAlias:  "peer-local",
		Role:        pairing.RoleAcceptor,
	})

	viewAnnouncement(t, d, 1, "local-name", "A", 1, expires)
	viewAnnouncement(t, d, 3, "local-name", "B", math.MaxUint64, expires)
	viewAnnouncement(t, d, 2, "peer-name", "P", 1, expires)
	viewAnnouncement(t, d, 3, "peer-name", "Q", math.MaxUint64, expires)
	viewAnnouncement(t, d, 4, "other-name", "R", 1, expires)
	viewAnnouncement(t, d, 3, "other-name", "S", 2, expires)
	viewCard(t, d, 3, "other-card", 1, expires)

	want := []PublicTopic{
		{Name: "local-name", Description: "A", Publisher: self, PublisherAlias: "", Declarers: 2},
		{Name: "other-name", Description: "S", Publisher: fpOther, PublisherAlias: "other-card", Declarers: 2},
		{Name: "peer-name", Description: "P", Publisher: fpPeer, PublisherAlias: "peer-local", Declarers: 2},
	}
	if got := d.PublicTopics(); !reflect.DeepEqual(got, want) {
		t.Fatalf("PublicTopics() = %v, want %v", got, want)
	}

	viewAnnouncement(t, d, 4, "tie-name", "U", 5, expires)
	viewAnnouncement(t, d, 3, "tie-name", "V", 5, expires)
	first, firstDesc := fpOther, "V"
	if bytes.Compare(fpOtherLow[:], fpOther[:]) < 0 {
		first, firstDesc = fpOtherLow, "U"
	}
	entry := map[string]PublicTopic{}
	for _, topic := range d.PublicTopics() {
		entry[topic.Name] = topic
	}
	if got := entry["tie-name"]; got.Publisher != first || got.Description != firstDesc {
		t.Fatalf("tie-name = %+v, want the smaller fingerprint %v to supply description %q", got, first, firstDesc)
	}
}

// TestPublicTopicsSkipsInvalidDeclarations checks that malformed declarations are forwarded but left out of the view.
func TestPublicTopicsSkipsInvalidDeclarations(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	c := newReconcileNode(t, net, 2)
	p := newReconcileNode(t, net, 3)

	net.Connect(b.fp, c.fp)
	net.Connect(p.fp, b.fp)
	net.Settle(t)

	good := reconcileAnnouncement(t, 200, "good", "ok", 1)
	longDesc := reconcileAnnouncement(t, 201, "long", strings.Repeat("d", 60<<10), 1)
	newline := reconcileAnnouncement(t, 202, "bad\nname", "", 1)
	longName := reconcileAnnouncement(t, 203, strings.Repeat("n", 300), "", 1)
	recs := []*storedRecord{good, longDesc, newline, longName}
	vs := make([]*directorypb.RecordVersion, 0, len(recs))
	for _, r := range recs {
		reconcileInject(t, p, r)
		vs = append(vs, r.recordVersion())
	}
	p.d.broadcast(vs, Fingerprint{})
	reconcileAwait(t, net, "B and C to accept and forward the declarations", func() bool {
		for _, n := range []*reconcileNode{b, c} {
			for _, r := range recs {
				got, ok := reconcileLookup(n, r.id)
				if !ok || !bytes.Equal(got.raw, r.raw) {
					return false
				}
			}
		}
		return true
	})

	want := []PublicTopic{{
		Name:        "good",
		Description: "ok",
		Publisher:   newTestIdentity(200).Fingerprint(),
		Declarers:   1,
	}}
	if got := b.d.PublicTopics(); !reflect.DeepEqual(got, want) {
		t.Fatalf("PublicTopics() = %v, want only the well-formed declaration %v", got, want)
	}
}

// TestBrokersSkipsMalformedDirectAddresses checks that a "direct" entry that cannot be parsed is left out while the card stays usable.
func TestBrokersSkipsMalformedDirectAddresses(t *testing.T) {
	now := dirTestNow()
	d, _, _, _ := newViewDirectory(t, 1, now)
	fpX := newTestIdentity(2).Fingerprint()
	good := "198.51.100.4:17443"
	bad := "\x1b198.51.100.5:17443"
	viewCard(t, d, 2, "x", 1, now.Add(recordTTL),
		advAddr("direct", bad),
		advAddr("direct", "no-port"),
		advAddr("direct", good))

	entry := viewFind(t, d.Brokers(), fpX)
	want := []config.AdvertisedAddr{{Kind: "direct", Addr: good}}
	if !reflect.DeepEqual(entry.Addresses, want) {
		t.Fatalf("card holder addresses = %v, want only the well-formed entry %v", entry.Addresses, want)
	}
	if got := d.CardsByAddress(bad); got != nil {
		t.Errorf("CardsByAddress(%q) = %v, want nil", bad, got)
	}
	if got := d.CardsByAddress(good); !reflect.DeepEqual(got, []Fingerprint{fpX}) {
		t.Errorf("CardsByAddress(%q) = %v, want %v", good, got, fpX)
	}
}

func fingerprintsAscending(fps ...Fingerprint) []Fingerprint {
	sort.Slice(fps, func(i, j int) bool { return bytes.Compare(fps[i][:], fps[j][:]) < 0 })
	return fps
}

// TestCardsByAddress checks that every publisher advertising an address is returned and unknown or self addresses are not.
func TestCardsByAddress(t *testing.T) {
	now := dirTestNow()
	d, _, _, _ := newViewDirectory(t, 1, now)
	expires := now.Add(recordTTL)
	fpA := newTestIdentity(2).Fingerprint()
	fpB := newTestIdentity(3).Fingerprint()
	fpD := newTestIdentity(5).Fingerprint()
	shared := "198.51.100.4:17443"
	tie := "198.51.100.11:17443"

	viewCard(t, d, 2, "a", math.MaxUint64, expires, advAddr("direct", shared))
	viewCard(t, d, 3, "b", 7, expires, advAddr("direct", shared), advAddr("direct", tie))
	viewCard(t, d, 4, "c", 1, expires, advAddr("relay", "198.51.100.5:17443"))
	viewCard(t, d, 5, "d", 7, expires, advAddr("direct", tie))
	viewCard(t, d, 1, "self", 9, expires, advAddr("direct", "198.51.100.1:17443"))

	wantShared := fingerprintsAscending(fpA, fpB)
	if got := d.CardsByAddress(shared); !reflect.DeepEqual(got, wantShared) {
		t.Fatalf("CardsByAddress(%q) = %v, want every matching publisher %v", shared, got, wantShared)
	}
	for _, addr := range []string{
		"203.0.113.99:17443",
		"198.51.100.5:17443",
		"198.51.100.1:17443",
	} {
		if got := d.CardsByAddress(addr); got != nil {
			t.Errorf("CardsByAddress(%q) = %v, want nil", addr, got)
		}
	}

	wantTie := fingerprintsAscending(fpB, fpD)
	if got := d.CardsByAddress(tie); !reflect.DeepEqual(got, wantTie) {
		t.Fatalf("CardsByAddress(%q) = %v, want both publishers %v", tie, got, wantTie)
	}
}

// TestCardsByAddressCanonicalForm checks that host case, port leading zeros and IPv6 spelling do not hide a match.
func TestCardsByAddressCanonicalForm(t *testing.T) {
	now := dirTestNow()
	d, _, _, _ := newViewDirectory(t, 1, now)
	expires := now.Add(recordTTL)
	fpMixedCase := newTestIdentity(2).Fingerprint()
	fpIPv6 := newTestIdentity(3).Fingerprint()

	viewCard(t, d, 2, "mixed", 1, expires, advAddr("direct", "SERVICE.Example:017443"))
	viewCard(t, d, 3, "v6", 1, expires, advAddr("direct", "[2001:0DB8:0:0:0:0:0:1]:443"))

	for _, tc := range []struct {
		lookup string
		want   []Fingerprint
	}{
		{"service.example:17443", []Fingerprint{fpMixedCase}},
		{"Service.Example:17443", []Fingerprint{fpMixedCase}},
		{"[2001:db8::1]:443", []Fingerprint{fpIPv6}},
		{"[2001:0db8:0:0:0:0:0:1]:0443", []Fingerprint{fpIPv6}},
	} {
		if got := d.CardsByAddress(tc.lookup); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("CardsByAddress(%q) = %v, want %v", tc.lookup, got, tc.want)
		}
	}
}

type reentrantPeers struct {
	*fakePeers
	d    *Directory
	held chan struct{}
}

func (p *reentrantPeers) Peers() []pairing.Peer {
	p.d.PublicTopics()
	if !p.d.mu.TryLock() {
		p.held <- struct{}{}
	} else {
		p.d.mu.Unlock()
	}
	return p.fakePeers.Peers()
}

// TestViewsDoNotHoldLockDuringCallbacks checks that Brokers calls the peer table outside the directory lock even when the callback re-enters the directory.
func TestViewsDoNotHoldLockDuringCallbacks(t *testing.T) {
	now := dirTestNow()
	d, peers, _, _ := newViewDirectory(t, 1, now)
	peers.Add(pairing.Peer{Fingerprint: newTestIdentity(2).Fingerprint(), Alias: "p", Role: pairing.RoleAcceptor})
	rp := &reentrantPeers{fakePeers: peers, d: d, held: make(chan struct{}, 1)}
	d.peers = rp

	done := make(chan []KnownBroker, 1)
	go func() { done <- d.Brokers() }()
	select {
	case got := <-done:
		if len(got) != 1 {
			t.Fatalf("Brokers() returned %d entries, want 1", len(got))
		}
	case <-time.After(eventTimeout):
		t.Fatal("Brokers did not return before the event timeout: it holds the lock during the peer callback")
	}
	select {
	case <-rp.held:
		t.Fatal("Brokers called the peer table while holding the directory lock")
	default:
	}
}
