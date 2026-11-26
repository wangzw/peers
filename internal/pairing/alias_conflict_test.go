package pairing

import "testing"

// TestAliasConflictReportsCommittedPeerAlias checks that an alias held by a
// committed peer is a conflict, and that an unused alias is not.
func TestAliasConflictReportsCommittedPeerAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	p := &Pairing{peers: table}

	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "taken", RoleAcceptor))

	if !p.AliasConflict("taken") {
		t.Fatal(`AliasConflict("taken") = false, want true for a committed peer`)
	}
	if p.AliasConflict("free") {
		t.Fatal(`AliasConflict("free") = true, want false for an unused alias`)
	}
}

// TestAliasConflictReportsInFlightReservation checks that an alias claimed by
// an in-flight handshake is a conflict until that handshake releases it.
func TestAliasConflictReportsInFlightReservation(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	p := &Pairing{peers: table}

	fp, _ := peerTestKey(t)
	token, ok := table.reserveAlias("inflight", fp)
	if !ok {
		t.Fatal(`reserveAlias("inflight") = false, want true`)
	}
	if !p.AliasConflict("inflight") {
		t.Fatal(`AliasConflict("inflight") = false, want true for an in-flight reservation`)
	}

	table.releaseToken(token)
	if p.AliasConflict("inflight") {
		t.Fatal(`AliasConflict("inflight") = true after releaseToken, want false`)
	}
}

// TestAliasConflictIgnoresOwnAlias checks that setting the Broker alias to its
// current value is not a conflict while a committed peer alias still is.
func TestAliasConflictIgnoresOwnAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	p := &Pairing{peers: table}

	p.SetOwnAlias("self")
	if p.AliasConflict("self") {
		t.Fatal(`AliasConflict("self") = true, want false for the Broker's own alias`)
	}

	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "other", RoleAcceptor))
	if !p.AliasConflict("other") {
		t.Fatal(`AliasConflict("other") = false, want true for a committed peer alongside SetOwnAlias`)
	}
}

// TestAliasConflictIgnoresEmptyAlias checks that the empty alias never reports
// a conflict.
func TestAliasConflictIgnoresEmptyAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	p := &Pairing{peers: table}

	if p.AliasConflict("") {
		t.Fatal(`AliasConflict("") = true, want false`)
	}
}
