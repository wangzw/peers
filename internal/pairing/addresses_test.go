package pairing

import (
	"strconv"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

func direct(addr string) config.AdvertisedAddr {
	return config.AdvertisedAddr{Kind: config.AdvertisedKindDirect, Addr: addr}
}

// TestValidDialPortBounds pins the accepted port range and the per-byte digit
// check of validDialPort. The largest port is accepted and one past it is not.
// A sign is not a digit, so "+1" is rejected. Leading zeros stay decimal: every
// byte is a digit and Atoi reads "080" as 80, so it is accepted, which is the
// same rule connect_broker applies to its address.
func TestValidDialPortBounds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		port string
		want bool
	}{
		{"1", true},
		{"65535", true},
		{"65536", false},
		{"0", false},
		{"080", true},
		{"+1", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := validDialPort(tc.port); got != tc.want {
			t.Errorf("validDialPort(%q) = %v, want %v", tc.port, got, tc.want)
		}
	}
}

func TestCandidateMergeOrderAndDedup(t *testing.T) {
	t.Parallel()
	e := &peerEntry{
		role: RoleDialer,
		acceptor: []config.AdvertisedAddr{
			direct("a1:1"), direct("a2:2"),
		},
		card: []config.AdvertisedAddr{
			direct("a2:2"), direct("a3:3"),
		},
		pairingAddr: "a4:4",
	}
	got := mergeCandidateList(e)
	want := []Candidate{
		{Source: SourceAcceptor, Addr: "a1:1"},
		{Source: SourceAcceptor, Addr: "a2:2"},
		{Source: SourceCard, Addr: "a3:3"},
		{Source: SourcePairing, Addr: "a4:4"},
	}
	if len(got) != len(want) {
		t.Fatalf("merged candidates = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	addrs, last := mergeCandidates(e)
	if last != "" {
		t.Errorf("last connected = %q, want empty", last)
	}
	if len(addrs) != 4 || addrs[0] != "a1:1" || addrs[3] != "a4:4" {
		t.Errorf("addresses = %v", addrs)
	}
}

// TestRePairAddressStaysCardSourced covers a re-pair to an address the card
// already advertises: the table keeps the card source, drops the pairing
// source, and LastConnected still names the address.
func TestRePairAddressStaysCardSourced(t *testing.T) {
	t.Parallel()
	const addr = "10.0.0.1:17443"
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)

	first := peerTestEntry(fp, pub, "hostB", RoleDialer)
	first.acceptor = []config.AdvertisedAddr{direct("10.0.0.2:17443")}
	first.card = []config.AdvertisedAddr{direct(addr)}
	first.pairingAddr = addr
	if mustPut(t, table, first) {
		t.Fatal("the first pairing replaced an entry")
	}

	rePair := peerTestEntry(fp, pub, "hostB", RoleDialer)
	rePair.acceptor = []config.AdvertisedAddr{direct("10.0.0.9:17443")}
	rePair.pairingAddr = addr
	if !mustPut(t, table, rePair) {
		t.Fatal("the re-pairing did not replace the entry")
	}

	entry, ok := table.get(fp)
	if !ok {
		t.Fatal("the re-paired entry is missing from the table")
	}
	peer := entry.toPeer()
	want := []Candidate{
		{Source: SourceAcceptor, Addr: "10.0.0.9:17443"},
		{Source: SourceCard, Addr: addr},
	}
	if len(peer.Candidates) != len(want) {
		t.Fatalf("candidates = %v, want %v", peer.Candidates, want)
	}
	for i := range want {
		if peer.Candidates[i] != want[i] {
			t.Fatalf("candidate %d = %+v, want %+v", i, peer.Candidates[i], want[i])
		}
	}
	if peer.LastConnected != addr {
		t.Errorf("last connected = %q, want %q", peer.LastConnected, addr)
	}
}

func TestCandidatesSkipUnknownKinds(t *testing.T) {
	t.Parallel()
	e := &peerEntry{
		role: RoleDialer,
		acceptor: []config.AdvertisedAddr{
			{Kind: "relay", Addr: "relay:1"},
			direct("direct:1"),
		},
		card: []config.AdvertisedAddr{{Kind: "future", Addr: "future:1"}},
	}
	got := mergeCandidateList(e)
	if len(got) != 1 || got[0].Addr != "direct:1" {
		t.Fatalf("candidates = %v, want only the direct address", got)
	}
}

func TestCandidatesCapEight(t *testing.T) {
	t.Parallel()
	var acceptor, card []config.AdvertisedAddr
	for i := 0; i < 12; i++ {
		acceptor = append(acceptor, direct("acceptor-"+strconv.Itoa(i)+":1"))
		card = append(card, direct("card-"+strconv.Itoa(i)+":1"))
	}
	e := &peerEntry{role: RoleDialer, acceptor: capKnown(acceptor), card: capKnown(card)}
	addrs, _ := mergeCandidates(e)
	if len(addrs) != 16 {
		t.Fatalf("candidates = %d, want 16 (8 per source)", len(addrs))
	}
	for _, a := range addrs {
		if a == "acceptor-8:1" || a == "card-8:1" {
			t.Fatalf("candidate list kept a ninth entry: %q", a)
		}
	}
}

func TestCandidatesForAcceptorAreEmpty(t *testing.T) {
	t.Parallel()
	e := &peerEntry{
		role:          RoleAcceptor,
		acceptor:      []config.AdvertisedAddr{direct("a:1")},
		pairingAddr:   "b:2",
		lastConnected: "b:2",
	}
	if got := mergeCandidateList(e); got != nil {
		t.Fatalf("acceptor candidates = %v, want none", got)
	}
	if addrs, last := mergeCandidates(e); addrs != nil || last != "" {
		t.Fatalf("acceptor candidates = %v, %q", addrs, last)
	}
}

func TestREQBrokerPairing034_ObserveAcceptorAddressesReplacesSource(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "hostA", RoleDialer)
	entry.acceptor = []config.AdvertisedAddr{direct("old:1")}
	entry.card = []config.AdvertisedAddr{direct("card:2")}
	mustPut(t, table, entry)

	p := &Pairing{peers: table}
	p.ObserveAcceptorAddresses(fp, []config.AdvertisedAddr{direct("new:3")})
	got, _ := table.get(fp)
	if len(got.acceptor) != 1 || got.acceptor[0].Addr != "new:3" {
		t.Fatalf("acceptor source = %v, want [new:3]", got.acceptor)
	}
	if len(got.card) != 1 || got.card[0].Addr != "card:2" {
		t.Fatalf("card source changed: %v", got.card)
	}
	addrs, _ := p.Candidates(fp)
	if len(addrs) != 2 || addrs[0] != "new:3" || addrs[1] != "card:2" {
		t.Fatalf("Candidates = %v, want [new:3 card:2]", addrs)
	}
}

func TestREQBrokerPairing034_CandidatesUseUpdatedSource(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))
	p := &Pairing{peers: table}
	p.ObserveAcceptorAddresses(fp, []config.AdvertisedAddr{direct("one:1")})
	p.ObserveAcceptorAddresses(fp, []config.AdvertisedAddr{direct("two:2")})
	addrs, _ := p.Candidates(fp)
	if len(addrs) != 1 || addrs[0] != "two:2" {
		t.Fatalf("Candidates = %v, want only the latest source", addrs)
	}
}

func TestUpdateCardAddressesOnlyForDialerPeers(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpDialer, pubDialer := peerTestKey(t)
	fpAcceptor, pubAcceptor := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fpDialer, pubDialer, "dialer", RoleDialer))
	mustPut(t, table, peerTestEntry(fpAcceptor, pubAcceptor, "acceptor", RoleAcceptor))

	p := &Pairing{peers: table}
	p.UpdateCardAddresses(fpDialer, []config.AdvertisedAddr{direct("card:1")})
	p.UpdateCardAddresses(fpAcceptor, []config.AdvertisedAddr{direct("nope:1")})

	dialer, _ := table.get(fpDialer)
	if len(dialer.card) != 1 || dialer.card[0].Addr != "card:1" {
		t.Fatalf("dialer card = %v", dialer.card)
	}
	acceptor, _ := table.get(fpAcceptor)
	if len(acceptor.card) != 0 {
		t.Fatalf("acceptor card = %v, want none", acceptor.card)
	}
}

func TestObserveConnectedOnlyForDialerPeers(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpDialer, pubDialer := peerTestKey(t)
	fpAcceptor, pubAcceptor := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fpDialer, pubDialer, "dialer", RoleDialer))
	mustPut(t, table, peerTestEntry(fpAcceptor, pubAcceptor, "acceptor", RoleAcceptor))

	p := &Pairing{peers: table}
	p.ObserveConnected(fpDialer, "192.0.2.10:17443")
	p.ObserveConnected(fpAcceptor, "203.0.113.5:17443")

	dialer, _ := table.get(fpDialer)
	if dialer.lastConnected != "192.0.2.10:17443" {
		t.Fatalf("dialer last connected = %q", dialer.lastConnected)
	}
	acceptor, _ := table.get(fpAcceptor)
	if acceptor.lastConnected != "" {
		t.Fatalf("acceptor last connected = %q, want empty", acceptor.lastConnected)
	}
	if _, last := p.Candidates(fpDialer); last != "192.0.2.10:17443" {
		t.Fatalf("Candidates last = %q", last)
	}
}

func TestAddressUpdatePersistenceFailureKeepsOldValue(t *testing.T) {
	t.Parallel()
	table, _, ops := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "hostA", RoleDialer)
	entry.acceptor = []config.AdvertisedAddr{direct("old:1")}
	mustPut(t, table, entry)

	ops.set(true, false, false)
	p := &Pairing{peers: table}
	p.ObserveAcceptorAddresses(fp, []config.AdvertisedAddr{direct("new:2")})
	p.UpdateCardAddresses(fp, []config.AdvertisedAddr{direct("card:3")})
	p.ObserveConnected(fp, "new:2")

	got, _ := table.get(fp)
	if len(got.acceptor) != 1 || got.acceptor[0].Addr != "old:1" {
		t.Fatalf("acceptor source changed after failed persist: %v", got.acceptor)
	}
	if len(got.card) != 0 {
		t.Fatalf("card source changed after failed persist: %v", got.card)
	}
	if got.lastConnected != "" {
		t.Fatalf("last connected changed after failed persist: %q", got.lastConnected)
	}
}

func TestUpdateOnUnknownPeerIsIgnored(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, _ := peerTestKey(t)
	p := &Pairing{peers: table}
	p.ObserveAcceptorAddresses(fp, []config.AdvertisedAddr{direct("a:1")})
	p.UpdateCardAddresses(fp, []config.AdvertisedAddr{direct("b:2")})
	p.ObserveConnected(fp, "c:3")
	if _, ok := table.get(fp); ok {
		t.Fatalf("observing an unknown peer created an entry")
	}
}

func TestAddressSourceConstants(t *testing.T) {
	t.Parallel()
	cases := map[AddressSource]string{
		SourceAcceptor: "acceptor",
		SourceCard:     "card",
		SourcePairing:  "pairing",
	}
	for source, want := range cases {
		if got := source.String(); got != want {
			t.Errorf("AddressSource(%d).String() = %q, want %q", source, got, want)
		}
	}
}

func TestRoleString(t *testing.T) {
	t.Parallel()
	cases := map[Role]string{
		RoleUnknown:  "unknown",
		RoleDialer:   "dialer",
		RoleAcceptor: "acceptor",
	}
	for role, want := range cases {
		if got := role.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", role, got, want)
		}
	}
}
