package pairing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// TestRetiredAliasNotReassigned reproduces the audit finding that an alias a
// peer gave up could be handed to another peer straight away, so an Agent
// address already in a model's hands would address a different Broker. After B
// renames from hostB to hostB2, C must not take over hostB; it gets a local
// alias instead and the assignment is audited.
func TestRetiredAliasNotReassigned(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	cFP, cPub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))
	mustPut(t, table, peerTestEntry(cFP, cPub, "hostC", RoleAcceptor))

	settings := &peerTestSettings{}
	p := &Pairing{peers: table, settings: settings}

	p.ObserveAlias(bFP, "hostB2")
	p.ObserveAlias(cFP, "hostB")

	if got := table.effectiveAlias(bFP); got != "hostB2" {
		t.Fatalf("B effective alias = %q, want hostB2", got)
	}
	if got, ok := table.resolveAlias("hostB"); ok {
		t.Fatalf("hostB resolves to %s after B gave it up, want no resolution", fingerprintHex(got))
	}
	cEntry, ok := table.get(cFP)
	if !ok {
		t.Fatal("C disappeared from the table")
	}
	if got := cEntry.effectiveAlias(); got == "hostB" {
		t.Fatal("C took over the alias B gave up")
	}
	if cEntry.localAlias == "" {
		t.Fatal("no local alias generated for C")
	}
	var audited bool
	for _, e := range settings.recordedEvents() {
		if e.Event == config.EventLocalAliasAssigned && e.Peer != nil && e.Peer.Fingerprint == [32]byte(cFP) {
			audited = true
		}
	}
	if !audited {
		t.Fatal("no local_alias_assigned audit event for C")
	}
}

// TestAliasConflictReportsRetiredAlias checks that a peer's tombstone is a
// conflict for the Broker's own alias, so it cannot adopt an alias another peer
// used before.
func TestAliasConflictReportsRetiredAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))

	settings := &peerTestSettings{}
	p := &Pairing{peers: table, settings: settings}
	p.ObserveAlias(bFP, "hostB2")

	if !p.AliasConflict("hostB") {
		t.Fatal(`AliasConflict("hostB") = false, want true for a retired alias`)
	}
}

// TestRetiredAliasReturnsToOwner checks that a peer may take back an alias it
// gave up: the alias leaves the tombstone list and resolves to that peer again.
func TestRetiredAliasReturnsToOwner(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	cFP, cPub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))
	mustPut(t, table, peerTestEntry(cFP, cPub, "hostC", RoleAcceptor))

	p := &Pairing{peers: table, settings: &peerTestSettings{}}
	p.ObserveAlias(bFP, "hostB2")
	p.ObserveAlias(cFP, "hostB")
	p.ObserveAlias(bFP, "hostB")

	if got, ok := table.resolveAlias("hostB"); !ok || got != bFP {
		t.Fatalf("hostB resolves to %s, %v, want B", fingerprintHex(got), ok)
	}
	bEntry, _ := table.get(bFP)
	if bEntry.retiresAlias("hostB") {
		t.Fatal("hostB is still retired after B took it back")
	}
	if !bEntry.retiresAlias("hostB2") {
		t.Fatalf("retired aliases = %v, want hostB2 after B left it", bEntry.retiredAliases)
	}
	if bEntry.effectiveAlias() != "hostB" {
		t.Fatalf("B effective alias = %q, want hostB", bEntry.effectiveAlias())
	}
}

// TestRetiredAliasPersistsAcrossReopen checks the on-disk field: the tombstone
// survives a reload and keeps another peer from taking the alias.
func TestRetiredAliasPersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	table, path, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	cFP, cPub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))
	mustPut(t, table, peerTestEntry(cFP, cPub, "hostC", RoleAcceptor))

	p := &Pairing{peers: table, settings: &peerTestSettings{}}
	p.ObserveAlias(bFP, "hostB2")

	file := readPeersFile(t, path)
	var stored []string
	for _, rec := range file.Peers {
		if rec.Fingerprint == fingerprintHex(bFP) {
			stored = rec.RetiredAliases
		}
	}
	if len(stored) != 1 || stored[0] != "hostB" {
		t.Fatalf("persisted retired_aliases = %v, want [hostB]", stored)
	}

	reopened, err := loadPeerTable(path, realFileOps{})
	if err != nil {
		t.Fatalf("loadPeerTable: %v", err)
	}
	entry, ok := reopened.get(bFP)
	if !ok {
		t.Fatal("B did not load")
	}
	if !entry.retiresAlias("hostB") {
		t.Fatalf("retired aliases after reload = %v, want hostB", entry.retiredAliases)
	}
	if got, ok := reopened.resolveAlias("hostB"); ok {
		t.Fatalf("hostB resolves to %s after reload, want no resolution", fingerprintHex(got))
	}
	if reopened.aliasAvailableLocked("hostB", cFP, "") {
		t.Fatal("C may take hostB after reload, want it still retired")
	}
}

// TestUnpairReleasesRetiredAlias checks that removing the peer removes its
// tombstones too, so another peer may use the alias afterwards.
func TestUnpairReleasesRetiredAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	p := &Pairing{peers: table, settings: &peerTestSettings{}}
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))
	p.ObserveAlias(bFP, "hostB2")
	if table.aliasAvailableLocked("hostB", Fingerprint{}, "") {
		t.Fatal("hostB available while B still holds its tombstone")
	}

	if err := p.Unpair(bFP); err != nil {
		t.Fatalf("Unpair: %v", err)
	}
	dFP, dPub := peerTestKey(t)
	if replaced := mustPut(t, table, peerTestEntry(dFP, dPub, "hostB", RoleAcceptor)); replaced {
		t.Fatal("put reported a replacement for a fresh peer")
	}
	if got, ok := table.resolveAlias("hostB"); !ok || got != dFP {
		t.Fatalf("hostB resolves to %s, %v after Unpair, want the new peer", fingerprintHex(got), ok)
	}
}

// TestInitiatorNeedsLocalAliasForRetiredAlias checks the pairing path: when the
// acceptor reports an alias the initiator's table retired, Connect refuses
// without a local alias instead of handing the address to a different Broker.
func TestInitiatorNeedsLocalAliasForRetiredAlias(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	seedFP, seedPub := hsKey(t)
	if _, err := initP.peers.put(peerEntry{
		fingerprint:    seedFP,
		publicKey:      seedPub,
		alias:          "seed",
		retiredAliases: []string{"beta"},
		role:           RoleAcceptor,
		pairedAt:       clk.Now(),
	}); err != nil {
		t.Fatalf("seed peer table: %v", err)
	}

	_, _, err, _ := hsPair(t, initP, accP, initTr, "STATIC-KEY", "", nil)
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("Connect with a retired alias = %v, want ErrNeedsLocalAlias", err)
	}
}

// TestAcceptorAssignsLocalAliasForRetiredAlias checks the accepting side: an
// initiator reporting an alias this table retired gets a local alias, and the
// assignment is audited.
func TestAcceptorAssignsLocalAliasForRetiredAlias(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	seedFP, seedPub := hsKey(t)
	if _, err := accP.peers.put(peerEntry{
		fingerprint:    seedFP,
		publicKey:      seedPub,
		alias:          "seed",
		retiredAliases: []string{"alpha"},
		role:           RoleAcceptor,
		pairedAt:       clk.Now(),
	}); err != nil {
		t.Fatalf("seed peer table: %v", err)
	}

	_, _, err, acceptErr := hsPair(t, initP, accP, initTr, "STATIC-KEY", "", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if acceptErr != nil {
		t.Fatalf("Accept: %v", acceptErr)
	}
	initFP := initP.identity.fingerprint()
	entry, ok := accP.peers.get(initFP)
	if !ok {
		t.Fatal("the initiator was not committed on the accepting side")
	}
	if entry.effectiveAlias() == "alpha" {
		t.Fatal("the acceptor used an alias it had retired")
	}
	if entry.localAlias == "" {
		t.Fatal("the acceptor did not assign a local alias")
	}
	var audited bool
	for _, e := range accS.recorded() {
		if e.Event == config.EventLocalAliasAssigned && e.Peer != nil && e.Peer.Fingerprint == [32]byte(initFP) {
			audited = true
		}
	}
	if !audited {
		t.Fatal("no local_alias_assigned audit event on the accepting side")
	}
}

// TestRetiredAliasCapFreezesLocalAlias checks the eighth-and-later rename: the
// ninth distinct effective alias freezes the current one as a local alias
// instead of adding a tombstone, and the assignment is audited.
func TestRetiredAliasCapFreezesLocalAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "host1", RoleAcceptor))
	settings := &peerTestSettings{}
	p := &Pairing{peers: table, settings: settings}

	for i := 2; i <= 9; i++ {
		p.ObserveAlias(fp, fmt.Sprintf("host%d", i))
	}
	entry, _ := table.get(fp)
	if len(entry.retiredAliases) != maxRetiredAliases {
		t.Fatalf("retired aliases = %d, want %d", len(entry.retiredAliases), maxRetiredAliases)
	}
	if entry.localAlias != "" {
		t.Fatalf("local alias = %q before the cap was reached, want none", entry.localAlias)
	}

	p.ObserveAlias(fp, "host10")
	entry, _ = table.get(fp)
	if entry.localAlias != "host9" {
		t.Fatalf("local alias = %q after the cap, want host9", entry.localAlias)
	}
	if got := entry.effectiveAlias(); got != "host9" {
		t.Fatalf("effective alias = %q after the cap, want the frozen host9", got)
	}
	if len(entry.retiredAliases) != maxRetiredAliases {
		t.Fatalf("retired aliases = %d after the cap, want %d", len(entry.retiredAliases), maxRetiredAliases)
	}
	events := settings.recordedEvents()
	if len(events) != 1 || events[0].Event != config.EventLocalAliasAssigned {
		t.Fatalf("audit events = %+v, want one local_alias_assigned", events)
	}
	if events[0].Peer == nil || events[0].Peer.Alias != "host9" {
		t.Fatalf("audit alias = %+v, want host9", events[0].Peer)
	}

	p.ObserveAlias(fp, "host11")
	entry, _ = table.get(fp)
	if got := entry.effectiveAlias(); got != "host9" {
		t.Fatalf("effective alias = %q after a further rename, want host9", got)
	}
	if len(entry.retiredAliases) != maxRetiredAliases {
		t.Fatalf("retired aliases = %d after a further rename, want %d", len(entry.retiredAliases), maxRetiredAliases)
	}
	if got := entry.alias; got != "host11" {
		t.Fatalf("stored self-reported alias = %q, want host11", got)
	}
}

// TestLoadRejectsInvalidRetiredAliases checks the load-time rules for
// tombstone fields: invalid names, too many, and collisions with an effective
// alias or another tombstone all reject the file as ErrStore.
func TestLoadRejectsInvalidRetiredAliases(t *testing.T) {
	t.Parallel()
	aFP, aPub := peerTestKey(t)
	bFP, bPub := peerTestKey(t)

	record := func(fp Fingerprint, pub []byte, alias string, retired []string) peerRecord {
		return peerRecord{
			Fingerprint:    fingerprintHex(fp),
			PublicKey:      base64Std(pub),
			Alias:          alias,
			RetiredAliases: retired,
			Role:           "acceptor",
			PairedAt:       time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		}
	}
	tooMany := make([]string, maxRetiredAliases+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("gone%d", i)
	}

	cases := []struct {
		name    string
		records []peerRecord
	}{
		{"another peer's effective alias", []peerRecord{
			record(aFP, aPub, "hostA", []string{"hostB"}),
			record(bFP, bPub, "hostB", nil),
		}},
		{"own effective alias", []peerRecord{
			record(aFP, aPub, "hostA", []string{"hostA"}),
		}},
		{"another peer's retired alias", []peerRecord{
			record(aFP, aPub, "hostA", []string{"gone"}),
			record(bFP, bPub, "hostB", []string{"gone"}),
		}},
		{"too many", []peerRecord{
			record(aFP, aPub, "hostA", tooMany),
		}},
		{"invalid alias", []peerRecord{
			record(aFP, aPub, "hostA", []string{"bad alias"}),
		}},
		{"duplicate in one list", []peerRecord{
			record(aFP, aPub, "hostA", []string{"gone", "gone"}),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, err := json.Marshal(peersFile{Version: peersFileVersion, Peers: tc.records})
			if err != nil {
				t.Fatalf("marshal fixture: %v", err)
			}
			path := filepath.Join(t.TempDir(), "peers.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			table, err := loadPeerTable(path, realFileOps{})
			if !errors.Is(err, ErrStore) {
				t.Fatalf("loadPeerTable = %v (table %v), want ErrStore", err, table)
			}
		})
	}
}

// TestRepairingKeepsRetiredAliases checks that overwriting an entry during
// re-pairing keeps the tombstones of the continuing relationship.
func TestRepairingKeepsRetiredAliases(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostB", RoleAcceptor))
	p := &Pairing{peers: table, settings: &peerTestSettings{}}
	p.ObserveAlias(fp, "hostB2")

	repaired := peerTestEntry(fp, pub, "hostB2", RoleAcceptor)
	mustPut(t, table, repaired)

	entry, _ := table.get(fp)
	if !entry.retiresAlias("hostB") {
		t.Fatalf("retired aliases = %v after re-pairing, want hostB kept", entry.retiredAliases)
	}
	if table.aliasAvailableLocked("hostB", Fingerprint{}, "") {
		t.Fatal("hostB available after re-pairing, want it still retired")
	}
}

// TestRepairingLocalAliasChangeRetiresOld checks that a re-pairing that adopts
// a new local alias retires the alias the peer answered to before.
func TestRepairingLocalAliasChangeRetiresOld(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	seed := peerTestEntry(fp, pub, "hostB", RoleDialer)
	seed.localAlias = "hostB-abcd"
	mustPut(t, table, seed)

	repaired := peerTestEntry(fp, pub, "hostB", RoleDialer)
	repaired.localAlias = "picked"
	mustPut(t, table, repaired)

	entry, _ := table.get(fp)
	if got := entry.effectiveAlias(); got != "picked" {
		t.Fatalf("effective alias = %q after re-pairing, want picked", got)
	}
	if !entry.retiresAlias("hostB-abcd") {
		t.Fatalf("retired aliases = %v, want the old local alias", entry.retiredAliases)
	}
}

// TestRepairingFreezesWhenRetiredAliasesFull checks that re-pairing cannot push
// a peer past the tombstone cap: it keeps the current effective alias instead.
func TestRepairingFreezesWhenRetiredAliasesFull(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	seed := peerTestEntry(fp, pub, "host9", RoleDialer)
	for i := 1; i <= maxRetiredAliases; i++ {
		seed.retiredAliases = append(seed.retiredAliases, fmt.Sprintf("host%d", i))
	}
	mustPut(t, table, seed)

	repaired := peerTestEntry(fp, pub, "host9", RoleDialer)
	repaired.localAlias = "picked"
	mustPut(t, table, repaired)

	entry, _ := table.get(fp)
	if got := entry.effectiveAlias(); got != "host9" {
		t.Fatalf("effective alias = %q after re-pairing at the cap, want host9 frozen", got)
	}
	if entry.localAlias != "host9" {
		t.Fatalf("local alias = %q after re-pairing at the cap, want host9", entry.localAlias)
	}
	if len(entry.retiredAliases) != maxRetiredAliases {
		t.Fatalf("retired aliases = %d, want %d", len(entry.retiredAliases), maxRetiredAliases)
	}
}

// TestAcceptorFreezeAuditsCommittedAlias checks that when re-pairing freezes
// the current effective alias because the tombstone cap is reached, the
// acceptor audits the alias it actually committed, not the one it reserved
// before PairDone.
func TestAcceptorFreezeAuditsCommittedAlias(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	initFP := initP.identity.fingerprint()
	seed := peerEntry{
		fingerprint: initFP,
		publicKey:   append([]byte(nil), initP.identity.publicKey()...),
		alias:       "host9",
		role:        RoleAcceptor,
		pairedAt:    clk.Now(),
	}
	for i := 1; i <= maxRetiredAliases; i++ {
		seed.retiredAliases = append(seed.retiredAliases, fmt.Sprintf("host%d", i))
	}
	if _, err := accP.peers.put(seed); err != nil {
		t.Fatalf("seed peer table: %v", err)
	}

	_, _, err, acceptErr := hsPair(t, initP, accP, initTr, "STATIC-KEY", "", func(_ context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if acceptErr != nil {
		t.Fatalf("Accept: %v", acceptErr)
	}

	entry, ok := accP.peers.get(initFP)
	if !ok {
		t.Fatal("the initiator was not committed on the accepting side")
	}
	if got := entry.effectiveAlias(); got != "host9" {
		t.Fatalf("effective alias = %q, want the frozen host9", got)
	}
	if entry.localAlias != "host9" {
		t.Fatalf("local alias = %q, want host9", entry.localAlias)
	}

	var succeeded, assigned bool
	for _, e := range accS.recorded() {
		if e.Peer == nil || e.Peer.Fingerprint != [32]byte(initFP) {
			continue
		}
		switch e.Event {
		case config.EventPairingSucceeded:
			succeeded = true
			if e.Peer.Alias != "host9" {
				t.Fatalf("pairing_succeeded alias = %q, want the committed host9", e.Peer.Alias)
			}
		case config.EventLocalAliasAssigned:
			assigned = true
			if e.Peer.Alias != "host9" {
				t.Fatalf("local_alias_assigned alias = %q, want the committed host9", e.Peer.Alias)
			}
		}
	}
	if !succeeded {
		t.Fatal("no pairing_succeeded audit event for the initiator")
	}
	if !assigned {
		t.Fatal("no local_alias_assigned audit event for the frozen alias")
	}
}

// TestInitiatorFreezeReplacesProvidedLocalAlias checks that a freeze during
// re-pairing replaces a caller-provided local alias with the current effective
// alias, that the returned Peer reports the alias actually committed, and that
// the initiator audits the frozen alias.
func TestInitiatorFreezeReplacesProvidedLocalAlias(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	accFP := accP.identity.fingerprint()
	seed := peerEntry{
		fingerprint: accFP,
		publicKey:   append([]byte(nil), accP.identity.publicKey()...),
		alias:       "host9",
		role:        RoleDialer,
		pairedAt:    clk.Now(),
	}
	for i := 1; i <= maxRetiredAliases; i++ {
		seed.retiredAliases = append(seed.retiredAliases, fmt.Sprintf("host%d", i))
	}
	if _, err := initP.peers.put(seed); err != nil {
		t.Fatalf("seed peer table: %v", err)
	}

	peer, _, err, acceptErr := hsPair(t, initP, accP, initTr, "STATIC-KEY", "picked", func(_ context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if acceptErr != nil {
		t.Fatalf("Accept: %v", acceptErr)
	}
	if peer.LocalAlias != "host9" {
		t.Fatalf("returned LocalAlias = %q, want the frozen host9", peer.LocalAlias)
	}
	entry, ok := initP.peers.get(accFP)
	if !ok {
		t.Fatal("the acceptor was not committed on the initiating side")
	}
	if got := entry.effectiveAlias(); got != "host9" {
		t.Fatalf("effective alias = %q, want the frozen host9", got)
	}

	var assigned bool
	for _, e := range initS.recorded() {
		if e.Peer == nil || e.Peer.Fingerprint != [32]byte(accFP) {
			continue
		}
		if e.Event != config.EventLocalAliasAssigned {
			continue
		}
		assigned = true
		if e.Peer.Alias != "host9" {
			t.Fatalf("local_alias_assigned alias = %q, want the frozen host9", e.Peer.Alias)
		}
	}
	if !assigned {
		t.Fatal("no local_alias_assigned audit event for the frozen alias")
	}
}

// TestPutRejectsRetiredAlias checks that the table's commit path refuses to
// give one peer an effective alias that another peer retired.
func TestPutRejectsRetiredAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	cFP, cPub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))
	p := &Pairing{peers: table, settings: &peerTestSettings{}}
	p.ObserveAlias(bFP, "hostB2")

	if _, err := table.put(peerTestEntry(cFP, cPub, "hostB", RoleAcceptor)); !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("put with another peer's retired alias = %v, want ErrNeedsLocalAlias", err)
	}
}

// TestEnsureReservationRejectsRetiredAlias checks the recheck a handshake
// performs when the alias it wants to move its reservation to has become
// another peer's tombstone between reserving and committing.
func TestEnsureReservationRejectsRetiredAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	cFP, _ := peerTestKey(t)
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))

	table.mu.Lock()
	defer table.mu.Unlock()
	token, ok := table.reserveAliasLocked("old", cFP)
	if !ok {
		t.Fatal("could not reserve the old alias")
	}
	// The alias the handshake wants to move to appears as a tombstone only after
	// the reservation was taken, which is the race ensureReservationLocked
	// rechecks against.
	table.peers[bFP].retiredAliases = append(table.peers[bFP].retiredAliases, "free")

	if err := table.ensureReservationLocked("free", token, cFP, ""); !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("ensureReservationLocked on a retired alias = %v, want ErrNeedsLocalAlias", err)
	}
}

// TestLocalAliasAvoidsAnotherPeersRetiredAlias checks that generating a local
// alias skips one that another peer retired, even though the generated name
// carries a fingerprint suffix.
func TestLocalAliasAvoidsAnotherPeersRetiredAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	bFP, bPub := peerTestKey(t)
	cFP, cPub := peerTestKey(t)
	dFP, dPub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(bFP, bPub, "hostB", RoleAcceptor))

	tombstone := "hostB-" + fingerprintHex(dFP)[:4]
	cEntry := peerTestEntry(cFP, cPub, "hostC", RoleAcceptor)
	cEntry.retiredAliases = []string{tombstone}
	mustPut(t, table, cEntry)
	mustPut(t, table, peerTestEntry(dFP, dPub, "hostD", RoleAcceptor))

	p := &Pairing{peers: table, settings: &peerTestSettings{}}
	p.ObserveAlias(dFP, "hostB")

	entry, _ := table.get(dFP)
	if entry.localAlias == "" {
		t.Fatal("no local alias generated for D")
	}
	if entry.localAlias == tombstone {
		t.Fatalf("local alias %q repeats another peer's retired alias", entry.localAlias)
	}
}

// TestRetiredAliasesOmittedFromFileWhenEmpty checks that a peer with no
// tombstones writes no retired_aliases field, keeping the file compatible with
// builds that do not know the field.
func TestRetiredAliasesOmittedFromFileWhenEmpty(t *testing.T) {
	t.Parallel()
	fp, pub := peerTestKey(t)
	data, err := marshalPeers(map[Fingerprint]*peerEntry{
		fp: func() *peerEntry {
			e := peerTestEntry(fp, pub, "hostA", RoleAcceptor)
			return &e
		}(),
	})
	if err != nil {
		t.Fatalf("marshalPeers: %v", err)
	}
	if strings.Contains(string(data), "retired_aliases") {
		t.Fatalf("peers.json contains retired_aliases for a peer without tombstones:\n%s", data)
	}
}
