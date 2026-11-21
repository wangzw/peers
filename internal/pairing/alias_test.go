package pairing

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

func aliasTestFingerprint(seed byte) Fingerprint {
	var fp Fingerprint
	for i := range fp {
		fp[i] = seed
	}
	return fp
}

func TestValidAlias(t *testing.T) {
	t.Parallel()
	valid := []string{"a", "A", "0", "host-1", "a.b_c-d", strings.Repeat("x", 24)}
	for _, s := range valid {
		if !validAlias(s) {
			t.Errorf("validAlias(%q) = false, want true", s)
		}
	}
	invalid := []string{"", strings.Repeat("x", 25), "a b", "a/b", "a:b", "é", "a\n"}
	for _, s := range invalid {
		if validAlias(s) {
			t.Errorf("validAlias(%q) = true, want false", s)
		}
	}
}

func TestSanitizeAliasPart(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"hostA":   "hostA",
		"a b/c":   "a-b-c",
		"a:b@c":   "a-b-c",
		"ümlaut":  "-mlaut",
		"keep._-": "keep._-",
	}
	for in, want := range cases {
		if got := sanitizeAliasPart(in); got != want {
			t.Errorf("sanitizeAliasPart(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestREQBrokerPairing037_LocalAliasMaxLengthAndCharset(t *testing.T) {
	t.Parallel()
	reported := []string{
		"hostA",
		"",
		"a very long self reported alias that certainly exceeds the limit",
		strings.Repeat("z", 100),
		"名字",
		"with spaces and /slashes",
	}
	fp := aliasTestFingerprint(0xab)
	for _, base := range reported {
		got := assignLocalAlias(base, fp, "own", map[string]struct{}{})
		if !validAlias(got) {
			t.Errorf("assignLocalAlias(%q) = %q, not a valid alias", base, got)
		}
		if len(got) > config.AliasMaxLen {
			t.Errorf("assignLocalAlias(%q) has length %d, want <= %d", base, len(got), config.AliasMaxLen)
		}
	}
}

func TestREQBrokerPairing037_LocalAliasTruncation(t *testing.T) {
	t.Parallel()
	fp := aliasTestFingerprint(0xab)
	got := assignLocalAlias(strings.Repeat("z", 24), fp, "", map[string]struct{}{})
	wantSuffix := "-abab"
	if !strings.HasSuffix(got, wantSuffix) {
		t.Fatalf("alias %q does not end with %q", got, wantSuffix)
	}
	if len(got) != config.AliasMaxLen {
		t.Fatalf("alias %q has length %d, want %d", got, len(got), config.AliasMaxLen)
	}
	if got != strings.Repeat("z", 19)+wantSuffix {
		t.Fatalf("alias = %q, want the base truncated to 19 characters plus the suffix", got)
	}
}

func TestAssignLocalAliasUsesSanitizedBaseAndPrefix(t *testing.T) {
	t.Parallel()
	fp := aliasTestFingerprint(0xab) // hex prefix "abab"
	cases := map[string]string{
		"hostA": "hostA-abab",
		"a b":   "a-b-abab",
		"":      "peer-abab",
	}
	for reported, want := range cases {
		if got := assignLocalAlias(reported, fp, "", map[string]struct{}{}); got != want {
			t.Errorf("assignLocalAlias(%q) = %q, want %q", reported, got, want)
		}
	}
}

func TestAssignLocalAliasAddsHexDigitsWhenTaken(t *testing.T) {
	t.Parallel()
	fp := aliasTestFingerprint(0xab)
	taken := map[string]struct{}{"hostA-abab": {}}
	got := assignLocalAlias("hostA", fp, "", taken)
	if got != "hostA-ababa" {
		t.Fatalf("assignLocalAlias = %q, want hostA-ababa", got)
	}
}

func TestAssignLocalAliasFallsBackToSequence(t *testing.T) {
	t.Parallel()
	fp := aliasTestFingerprint(0xab)
	hex := fingerprintHex(fp)
	taken := make(map[string]struct{})
	for digits := 4; digits+1 <= config.AliasMaxLen; digits++ {
		suffix := "-" + hex[:digits]
		taken[truncatePart("peer", config.AliasMaxLen-len(suffix))+suffix] = struct{}{}
	}
	got := assignLocalAlias("peer", fp, "", taken)
	if got != "peer-00" {
		t.Fatalf("assignLocalAlias = %q, want peer-00", got)
	}
}

func TestAssignLocalAliasAvoidsOwnAlias(t *testing.T) {
	t.Parallel()
	fp := aliasTestFingerprint(0xab)
	got := assignLocalAlias("hostA", fp, "hostA-abab", map[string]struct{}{})
	if got == "hostA-abab" {
		t.Fatalf("assignLocalAlias reused the Broker's own alias")
	}
	if !validAlias(got) {
		t.Fatalf("assignLocalAlias = %q, not valid", got)
	}
}

func TestAliasUniquenessAcrossOwnAliasAndPeers(t *testing.T) {
	t.Parallel()
	own := "own-alias"
	peers := []string{"peer-one", "peer-two"}
	locals := []string{"local-one", "local-two"}
	taken := map[string]struct{}{own: {}, peers[0]: {}, peers[1]: {}, locals[0]: {}, locals[1]: {}}
	for seed := byte(0); seed < 20; seed++ {
		got := assignLocalAlias("peer-one", aliasTestFingerprint(seed), own, taken)
		if !validAlias(got) {
			t.Fatalf("seed %d: invalid alias %q", seed, got)
		}
		if _, clash := taken[got]; clash {
			t.Fatalf("seed %d: alias %q collides with the taken set", seed, got)
		}
	}
}

func TestREQBrokerPairing024_AcceptorAssignsLocalAliasOnConflict(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpA, pubA := peerTestKey(t)
	fpB, pubB := peerTestKey(t)

	mustPut(t, table, peerTestEntry(fpA, pubA, "same", RoleAcceptor))
	mustPut(t, table, peerTestEntry(fpB, pubB, "other", RoleAcceptor))

	table.mu.Lock()
	alias, _, ok := table.assignLocalAliasLocked(fpB, "same", "own")
	table.mu.Unlock()
	if !ok {
		t.Fatalf("assignLocalAliasLocked returned no alias")
	}
	if alias == "same" || !validAlias(alias) {
		t.Fatalf("generated alias = %q", alias)
	}
	table.mu.Lock()
	owner, reserved := table.reservationOwnerLocked(alias)
	table.mu.Unlock()
	if !reserved || owner != fpB {
		t.Fatalf("generated alias %q was not reserved", alias)
	}
}

// TestResolveAliasSkipsInFlightReservation pins the rule that ResolveAlias
// resolves committed peers only. An alias reserved by a handshake that has not
// committed yet is not an Agent address: a model that received it would address
// a peer that may never exist.
func TestResolveAliasSkipsInFlightReservation(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	token, ok := table.reserveAlias("inflight", fp)
	if !ok {
		t.Fatalf("reserveAlias failed on an empty table")
	}
	p := &Pairing{peers: table}
	if got, ok := p.ResolveAlias("inflight"); ok {
		t.Fatalf("ResolveAlias during a reservation = %s, true; want false", fingerprintHex(got))
	}

	entry := peerTestEntry(fp, pub, "inflight", RoleDialer)
	if _, err := commitHandshakeForTest(p, entry, token); err != nil {
		t.Fatalf("commitHandshake: %v", err)
	}
	if got, ok := p.ResolveAlias("inflight"); !ok || got != fp {
		t.Fatalf("ResolveAlias after commit = %s, %v; want %s", fingerprintHex(got), ok, fingerprintHex(fp))
	}
}

func TestResolveAliasOnlyMatchesEffectiveAliases(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "Reported", RoleAcceptor)
	entry.localAlias = "local"
	mustPut(t, table, entry)
	p := &Pairing{peers: table}

	if got, ok := p.ResolveAlias("local"); !ok || got != fp {
		t.Fatalf("ResolveAlias(local) = %s, %v", fingerprintHex(got), ok)
	}
	if got, ok := p.ResolveAlias("Reported"); ok {
		t.Fatalf("ResolveAlias matched a shadowed self-reported alias: %s", fingerprintHex(got))
	}
	if _, ok := p.ResolveAlias("reported"); ok {
		t.Fatalf("ResolveAlias matched case-insensitively")
	}
}

func TestReservationRejectsCommittedAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpOwner, pubOwner := peerTestKey(t)
	fpOther, _ := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fpOwner, pubOwner, "taken", RoleAcceptor))

	if _, ok := table.reserveAlias("taken", fpOther); ok {
		t.Fatalf("reservation accepted an alias owned by another peer")
	}
	if _, ok := table.reserveAlias("taken", fpOwner); !ok {
		t.Fatalf("re-pairing peer could not reserve its own alias")
	}
}

func TestHandshakeReservationReleasedOnFailure(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpA, _ := peerTestKey(t)
	fpB, _ := peerTestKey(t)
	token, ok := table.reserveAlias("contested", fpA)
	if !ok {
		t.Fatalf("first reservation failed")
	}
	table.releaseToken(token)
	if _, ok := table.reserveAlias("contested", fpB); !ok {
		t.Fatalf("released alias could not be reserved by another handshake")
	}
}

func TestPutRechecksReservation(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpReserved, _ := peerTestKey(t)
	fpNew, pubNew := peerTestKey(t)
	if _, ok := table.reserveAlias("contested", fpReserved); !ok {
		t.Fatalf("reserveAlias failed")
	}
	entry := peerTestEntry(fpNew, pubNew, "contested", RoleDialer)
	_, err := table.put(entry)
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("put error = %v, want ErrNeedsLocalAlias", err)
	}
	if _, ok := table.get(fpNew); ok {
		t.Fatalf("conflicting entry was committed")
	}
}

// TestCommitHandshakeRequiresOwnReservation pins the reservation ownership
// recheck on the handshake commit path. Unlike put, commitHandshake must find
// the effective alias reserved by this very fingerprint: no reservation and a
// reservation owned by another fingerprint both fail with ErrNeedsLocalAlias and
// leave the table unchanged.
func TestCommitHandshakeRequiresOwnReservation(t *testing.T) {
	t.Parallel()

	t.Run("no reservation", func(t *testing.T) {
		t.Parallel()
		table, _, _ := newPeerTestTable(t)
		table.setOwnAlias("self")
		fp, pub := peerTestKey(t)
		entry := peerTestEntry(fp, pub, "contested", RoleDialer)

		_, _, _, err := table.commitHandshake(entry, 0)
		if !errors.Is(err, ErrNeedsLocalAlias) {
			t.Fatalf("commitHandshake error = %v, want ErrNeedsLocalAlias", err)
		}
		if _, ok := table.get(fp); ok {
			t.Fatal("a handshake committed without a reservation")
		}
		if got := len(table.reserved); got != 0 {
			t.Fatalf("%d reservations left behind by the failed commit", got)
		}
	})

	t.Run("foreign reservation", func(t *testing.T) {
		t.Parallel()
		table, _, _ := newPeerTestTable(t)
		table.setOwnAlias("self")
		fp, pub := peerTestKey(t)
		owner, _ := peerTestKey(t)
		if _, ok := table.reserveAlias("contested", owner); !ok {
			t.Fatal("could not reserve the alias for another fingerprint")
		}
		entry := peerTestEntry(fp, pub, "contested", RoleDialer)

		_, _, _, err := table.commitHandshake(entry, 0)
		if !errors.Is(err, ErrNeedsLocalAlias) {
			t.Fatalf("commitHandshake error = %v, want ErrNeedsLocalAlias", err)
		}
		if _, ok := table.get(fp); ok {
			t.Fatal("a handshake committed an alias reserved by another fingerprint")
		}
		table.mu.Lock()
		got, ok := table.reservationOwnerLocked("contested")
		table.mu.Unlock()
		if !ok || got != owner {
			t.Fatalf("reservation = %s, %v; want it still owned by %s", fingerprintHex(got), ok, fingerprintHex(owner))
		}
	})

	t.Run("cached own alias rejects the commit", func(t *testing.T) {
		t.Parallel()
		table, _, _ := newPeerTestTable(t)
		table.setOwnAlias("contested")
		fp, pub := peerTestKey(t)
		if _, ok := table.reserveAlias("contested", fp); !ok {
			t.Fatal("could not reserve the alias for this fingerprint")
		}
		entry := peerTestEntry(fp, pub, "contested", RoleDialer)

		_, _, _, err := table.commitHandshake(entry, 0)
		if !errors.Is(err, ErrNeedsLocalAlias) {
			t.Fatalf("commitHandshake with the cached own alias = %v, want ErrNeedsLocalAlias", err)
		}
		if _, ok := table.get(fp); ok {
			t.Fatal("a handshake committed the cached own alias")
		}
	})

	t.Run("own reservation commits", func(t *testing.T) {
		t.Parallel()
		table, path, _ := newPeerTestTable(t)
		table.setOwnAlias("self")
		fp, pub := peerTestKey(t)
		token, ok := table.reserveAlias("contested", fp)
		if !ok {
			t.Fatal("could not reserve the alias for this fingerprint")
		}
		entry := peerTestEntry(fp, pub, "contested", RoleDialer)

		if _, _, _, err := table.commitHandshake(entry, token); err != nil {
			t.Fatalf("commitHandshake with its own reservation: %v", err)
		}
		if got, ok := table.get(fp); !ok || got.effectiveAlias() != "contested" {
			t.Fatalf("committed entry = %+v, %v", got, ok)
		}
		table.mu.Lock()
		_, reserved := table.reservationOwnerLocked("contested")
		table.mu.Unlock()
		if reserved {
			t.Fatal("the reservation survived the commit")
		}
		reopened, err := loadPeerTable(path, realFileOps{})
		if err != nil {
			t.Fatalf("reload: %v", err)
		}
		if _, ok := reopened.get(fp); !ok {
			t.Fatal("the committed entry was not persisted")
		}
	})
}

// TestConcurrentSelfReportedAliasCollision has two peers report the same alias
// at the same time. Both observations must leave distinct effective aliases:
// one keeps the reported alias and the other gets a generated local alias.
func TestConcurrentSelfReportedAliasCollision(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpA, pubA := peerTestKey(t)
	fpB, pubB := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fpA, pubA, "first", RoleAcceptor))
	mustPut(t, table, peerTestEntry(fpB, pubB, "second", RoleAcceptor))
	table.setOwnAlias("own")

	var wg sync.WaitGroup
	results := make([][2]interface{}, 2)
	for i, fp := range []Fingerprint{fpA, fpB} {
		wg.Add(1)
		go func(i int, fp Fingerprint) {
			defer wg.Done()
			local, assigned, err := table.observeAlias(fp, "shared")
			results[i] = [2]interface{}{local, assigned}
			if err != nil {
				t.Errorf("observeAlias(%s): %v", fingerprintHex(fp), err)
			}
		}(i, fp)
	}
	wg.Wait()

	gotA, _ := table.get(fpA)
	gotB, _ := table.get(fpB)
	if gotA.effectiveAlias() == gotB.effectiveAlias() {
		t.Fatalf("both peers ended with effective alias %q", gotA.effectiveAlias())
	}
	if !validAlias(gotA.effectiveAlias()) || !validAlias(gotB.effectiveAlias()) {
		t.Fatalf("invalid effective aliases: %q, %q", gotA.effectiveAlias(), gotB.effectiveAlias())
	}
	assignedCount := 0
	for _, r := range results {
		if r[1].(bool) {
			assignedCount++
		}
	}
	if assignedCount != 1 {
		t.Fatalf("assigned local aliases = %d, want exactly 1", assignedCount)
	}
}

// TestREQBrokerPairing026_AliasChangeConflictKeepsOldAddress checks that an
// observed alias collision assigns a local alias, writes the audit event, and
// leaves the candidate addresses untouched.
func TestREQBrokerPairing026_AliasChangeConflictKeepsOldAddress(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpA, pubA := peerTestKey(t)
	fpB, pubB := peerTestKey(t)

	owner := peerTestEntry(fpA, pubA, "taken", RoleAcceptor)
	mustPut(t, table, owner)
	entry := peerTestEntry(fpB, pubB, "old", RoleDialer)
	entry.acceptor = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:17443"}}
	mustPut(t, table, entry)

	settings := &peerTestSettings{}
	p := &Pairing{peers: table, settings: settings}
	p.ObserveAlias(fpB, "taken")

	got, _ := table.get(fpB)
	if got.alias != "taken" {
		t.Errorf("self-reported alias = %q, want taken", got.alias)
	}
	if got.localAlias == "" || got.effectiveAlias() == "taken" {
		t.Errorf("no local alias assigned: %+v", got)
	}
	if len(got.acceptor) != 1 || got.acceptor[0].Addr != "192.0.2.10:17443" {
		t.Errorf("candidate addresses changed: %v", got.acceptor)
	}
	events := settings.recordedEvents()
	if len(events) != 1 || events[0].Event != config.EventLocalAliasAssigned {
		t.Fatalf("audit events = %+v, want one local_alias_assigned", events)
	}
	if events[0].Peer == nil || events[0].Peer.Fingerprint != [32]byte(fpB) {
		t.Fatalf("audit peer = %+v, want %s", events[0].Peer, fingerprintHex(fpB))
	}
}

// TestREQBrokerPairing026_ExistingLocalAliasUnaffected checks that a peer that
// already has a local alias keeps it when its self-reported alias changes.
func TestREQBrokerPairing026_ExistingLocalAliasUnaffected(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "old", RoleAcceptor)
	entry.localAlias = "stable-1234"
	mustPut(t, table, entry)

	settings := &peerTestSettings{}
	p := &Pairing{peers: table, settings: settings}
	p.ObserveAlias(fp, "renamed")

	got, _ := table.get(fp)
	if got.alias != "renamed" {
		t.Errorf("self-reported alias = %q, want renamed", got.alias)
	}
	if got.effectiveAlias() != "stable-1234" {
		t.Errorf("effective alias = %q, want stable-1234", got.effectiveAlias())
	}
	if events := settings.recordedEvents(); len(events) != 0 {
		t.Errorf("unexpected audit events: %+v", events)
	}
}

// TestObserveAliasUsesReportedAliasWhenFree covers the no-conflict branch.
func TestObserveAliasUsesReportedAliasWhenFree(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "old", RoleAcceptor))
	p := &Pairing{peers: table, settings: &peerTestSettings{}}
	p.ObserveAlias(fp, "new-name")
	got, _ := table.get(fp)
	if got.localAlias != "" || got.effectiveAlias() != "new-name" {
		t.Fatalf("entry = %+v, want the reported alias as effective", got)
	}
}

func TestObserveAliasPersistenceFailureKeepsOldValue(t *testing.T) {
	t.Parallel()
	table, _, ops := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "old", RoleAcceptor))

	ops.set(true, false, false)
	settings := &peerTestSettings{}
	p := &Pairing{peers: table, settings: settings}
	p.ObserveAlias(fp, "new-name")

	got, _ := table.get(fp)
	if got.alias != "old" {
		t.Fatalf("alias = %q after failed persist, want old", got.alias)
	}
	if events := settings.recordedEvents(); len(events) != 0 {
		t.Fatalf("audit written despite persistence failure: %+v", events)
	}
}

func TestObserveBeforeOpenIgnored(t *testing.T) {
	t.Parallel()
	var p Pairing
	fp := aliasTestFingerprint(1)
	// None of these may panic; they only log.
	p.ObserveAlias(fp, "x")
	p.ObserveConnected(fp, "192.0.2.10:1")
	p.ObserveAcceptorAddresses(fp, []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:1"}})
	p.UpdateCardAddresses(fp, []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:1"}})
}

func TestMakeLocalAliasAvoidsReserved(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp := aliasTestFingerprint(0xab) // hex prefix "abab"
	other, _ := peerTestKey(t)
	if _, ok := table.reserveAlias("buildbox-abab", other); !ok {
		t.Fatalf("reserveAlias failed")
	}
	table.mu.Lock()
	alias, ok := table.makeLocalAliasLocked(fp, "buildbox", "")
	table.mu.Unlock()
	if !ok {
		t.Fatalf("makeLocalAliasLocked failed")
	}
	if alias == "buildbox-abab" {
		t.Fatalf("generated alias reused a reserved alias")
	}
	if alias != "buildbox-ababa" {
		t.Fatalf("generated alias = %q, want buildbox-ababa", alias)
	}
}

func TestAssignLocalAliasInvalidSequenceDoesNotLoop(t *testing.T) {
	t.Parallel()
	fp := aliasTestFingerprint(0)
	taken := make(map[string]struct{})
	hex := fingerprintHex(fp)
	for digits := 4; digits+1 <= config.AliasMaxLen; digits++ {
		suffix := "-" + hex[:digits]
		taken[truncatePart("peer", config.AliasMaxLen-len(suffix))+suffix] = struct{}{}
	}
	for seq := 0; seq <= 99; seq++ {
		taken[fmt.Sprintf("peer-%02d", seq)] = struct{}{}
	}
	taken["peer-xx"] = struct{}{}
	got := assignLocalAlias("peer", fp, "", taken)
	if got != "" {
		t.Fatalf("assignLocalAlias = %q, want empty when the space is exhausted", got)
	}
}
