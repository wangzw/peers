package broker

import (
	"context"
	"testing"
)

// TestCrossAccountOwnerAddressKeepsNoRef is the impersonation reproduction.
// An account that registered a name first keeps the ref-free address it had
// before a second account registered the same name; only the second account
// carries a ref. Before the fix both accounts carry one.
func TestCrossAccountOwnerAddressKeepsNoRef(t *testing.T) {
	f := newCollisionFixture(t)

	snap, ok := f.b.lookupAgent(f.victimID)
	if !ok {
		t.Fatal("the owner is not in the table")
	}
	if want := "worker@hostA"; snap.Address != want {
		t.Fatalf("the owner's address = %q, want the unchanged %q", snap.Address, want)
	}

	other, ok := f.b.lookupAgent(f.attackerID)
	if !ok {
		t.Fatal("the other account is not in the table")
	}
	_, ref, _, ok := ParseAddress(other.Address)
	if !ok {
		t.Fatalf("the other account's address %q does not parse", other.Address)
	}
	if ref != f.attackerRef || len(ref) != 12 {
		t.Fatalf("the other account's address %q carries ref %q, want its full 12-character %q",
			other.Address, ref, f.attackerRef)
	}
}

// TestCrossAccountOwnerSignatureKeepsNoRef checks the same rule on the
// signature of the owner's outgoing messages: the owner keeps the ref-free
// signature it had before the other account arrived.
func TestCrossAccountOwnerSignatureKeepsNoRef(t *testing.T) {
	f := newCollisionFixture(t)
	owner := f.agents.views[string(f.victimID)]

	if err := f.d.Send(context.Background(), owner, "observer@hostA", "hello"); err != nil {
		t.Fatalf("send from the owner: %v", err)
	}
	calls := f.inboxes[string(f.callerID)].Calls()
	if len(calls) != 1 {
		t.Fatalf("the observer received %d messages, want 1", len(calls))
	}
	if want := "worker@hostA"; calls[0].Sig != want {
		t.Fatalf("the owner's signature = %q, want the unchanged %q", calls[0].Sig, want)
	}
}

// TestCrossAccountMemberCarriesAccountAndRefRequired checks the roster entry a
// peer receives: the owner is not ref-required while the other account is, and
// each entry names the account that registered it.
func TestCrossAccountMemberCarriesAccountAndRefRequired(t *testing.T) {
	f := newCollisionFixture(t)

	owner, ok := f.tb.topics.Roster(f.victimID)
	if !ok {
		t.Fatal("no roster entry for the owner")
	}
	if owner.RefRequired {
		t.Fatal("the owner's roster entry is ref_required, want false")
	}
	if want := "acct1001"; owner.Account != want {
		t.Fatalf("the owner's roster account = %q, want %q", owner.Account, want)
	}

	other, ok := f.tb.topics.Roster(f.attackerID)
	if !ok {
		t.Fatal("no roster entry for the other account")
	}
	if !other.RefRequired {
		t.Fatal("the other account's roster entry is not ref_required, want true")
	}
	if want := "acct1002"; other.Account != want {
		t.Fatalf("the other account's roster account = %q, want %q", other.Account, want)
	}
}

// TestCrossAccountMemberAccountSanitized checks that a publisher whose account
// name is not a legal member name sends an empty account instead of one the
// receiver would reject, so the member entry itself still crosses the link.
func TestCrossAccountMemberAccountSanitized(t *testing.T) {
	tb := newTestBroker(t)
	b := tb.b
	id := testAgentID("eeeeee1")

	b.mu.Lock()
	insertCrossAccountAgent(b, id, "worker", 1001, 1)
	b.agents[id].caller.Username = "a@b"
	b.recordNameOwnerLocked("worker", 1001, 1001, tb.clock.Now())
	b.recomputeRefsLocked()
	b.mu.Unlock()

	roster, ok := tb.topics.Roster(id)
	if !ok {
		t.Fatal("no roster entry for the agent")
	}
	if roster.Account != "" {
		t.Fatalf("roster account = %q, want it cleared: a@b is not a legal member name", roster.Account)
	}
}

// TestCrossAccountSameAccountDuplicatesCarryRef narrows the duplicate-name
// requirement to one account: two Agents of the same account named alike both
// carry a ref.
func TestCrossAccountSameAccountDuplicatesCarryRef(t *testing.T) {
	tb := newTestBroker(t)
	b := tb.b
	first := testAgentID("dddddd1")
	second := testAgentID("dddddd2")

	b.mu.Lock()
	insertCrossAccountAgent(b, first, "worker", 1001, 1)
	insertCrossAccountAgent(b, second, "worker", 1001, 2)
	b.recordNameOwnerLocked("worker", 1001, 1001, tb.clock.Now())
	b.recomputeRefsLocked()
	b.mu.Unlock()

	for _, id := range []AgentID{first, second} {
		snap, ok := b.lookupAgent(id)
		if !ok {
			t.Fatalf("agent %s is not in the table", id)
		}
		_, ref, _, ok := ParseAddress(snap.Address)
		if !ok || ref == "" {
			t.Fatalf("same-account duplicate %s address = %q, want one carrying a ref", id, snap.Address)
		}
	}
}

// TestCrossAccountOwnershipTransferRemovesRef checks the 7-day release: once
// the departed owner's name is released, the account still holding it loses the
// ref, and its registration streams learn about the address change.
func TestCrossAccountOwnershipTransferRemovesRef(t *testing.T) {
	f := newCollisionFixture(t)
	removeNameOwnerAgent(f.b, f.victimID)
	f.agents.refresh(f.b, f.inboxes)

	// While the departed owner keeps the name, the other account stays pinned.
	snap, ok := f.b.lookupAgent(f.attackerID)
	if !ok {
		t.Fatal("the other account is not in the table")
	}
	if _, ref, _, ok := ParseAddress(snap.Address); !ok || len(ref) != 12 {
		t.Fatalf("after the owner leaves, the other account's address = %q, want a 12-character ref", snap.Address)
	}

	before := len(f.tb.topics.AgentChangedCalls())
	f.tb.clock.Advance(nameOwnerRetention)
	f.b.nameOwnerScan()

	snap, ok = f.b.lookupAgent(f.attackerID)
	if !ok {
		t.Fatal("the other account left the table")
	}
	if want := "worker@hostA"; snap.Address != want {
		t.Fatalf("after the transfer the address = %q, want %q", snap.Address, want)
	}
	if roster, ok := f.tb.topics.Roster(f.attackerID); !ok || roster.RefRequired {
		t.Fatalf("after the transfer the roster entry = %+v, want ref_required false", roster)
	}

	found := false
	for _, c := range f.tb.topics.AgentChangedCalls()[before:] {
		if c.Agent == f.attackerID {
			found = true
		}
	}
	if !found {
		t.Fatalf("no agent_changed was pushed for the other account after the transfer: %v",
			f.tb.topics.AgentChangedCalls()[before:])
	}
}
