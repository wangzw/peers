package broker

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// ownerResolveFixture is the interception scene: uid 1001 registered worker
// first and has left, uid 1002 still holds worker, and uid 1003 observes both
// in topic t1.
//
// The second account registered while the owner was still present, so the
// end state is the same as registering after the owner left: the name belongs
// to uid 1001 and uid 1002 carries a ref.
func ownerResolveFixture(t *testing.T) *collisionFixture {
	t.Helper()
	f := newCollisionFixture(t)
	removeNameOwnerAgent(f.b, f.victimID)
	f.refreshAgents()
	return f
}

// refreshAgents rebuilds the delivery's view of the table and the caller's
// snapshot after a direct change.
func (f *collisionFixture) refreshAgents() {
	f.agents.refresh(f.b, f.inboxes)
	if v, ok := f.agents.Lookup(string(f.callerID)); ok {
		f.caller = v
	}
}

// rejoinOwner puts the original owner back under the same name.
func (f *collisionFixture) rejoinOwner(t *testing.T) {
	t.Helper()
	f.b.mu.Lock()
	insertCrossAccountAgent(f.b, f.victimID, "worker", 1001, 1)
	f.b.recomputeRefsLocked()
	f.b.mu.Unlock()
	f.refreshAgents()
}

// TestCrossAccountBareNameDoesNotReachOtherAccount is the interception
// reproduction. The owner of "worker" has left, a second account holds the
// same name, and a third account sends to the bare address. Before the fix the
// message reaches the second account; after it the send reports not_found.
func TestCrossAccountBareNameDoesNotReachOtherAccount(t *testing.T) {
	f := ownerResolveFixture(t)

	err := f.send("worker@hostA", "stolen?")
	if err == nil {
		t.Fatal("a send to a name owned by a departed account succeeded, want not_found")
	}
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", got, err)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 0 {
		t.Fatalf("the other account received %d messages through the owner's bare name, want 0", got)
	}
}

// TestCrossAccountOwnerReturnsBareNameDelivers checks that a returning owner
// takes the bare name back: only the owner account's agent is selected and two
// visible same-name agents of different accounts are not ambiguous.
func TestCrossAccountOwnerReturnsBareNameDelivers(t *testing.T) {
	f := ownerResolveFixture(t)
	f.rejoinOwner(t)

	if err := f.send("worker@hostA", "welcome back"); err != nil {
		t.Fatalf("send to the returned owner: %v", err)
	}
	if got := len(f.inboxes[string(f.victimID)].Calls()); got != 1 {
		t.Fatalf("the owner received %d messages, want 1", got)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 0 {
		t.Fatalf("the other account received %d messages, want 0", got)
	}
}

// TestCrossAccountRenameHintStaysInOwnerAccount checks that the rename hint
// never points a caller at an agent of another account: a previous name owned
// by uid 1001 must not reveal the agent of uid 1002 that once used it.
func TestCrossAccountRenameHintStaysInOwnerAccount(t *testing.T) {
	f := ownerResolveFixture(t)

	f.b.mu.Lock()
	a := f.b.agents[f.attackerID]
	a.name = "newname"
	a.prevName = "worker"
	f.b.recomputeRefsLocked()
	f.b.mu.Unlock()
	f.refreshAgents()

	err := f.send("worker@hostA", "where did it go?")
	if err == nil {
		t.Fatal("a send to a previous name succeeded, want not_found")
	}
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", got, err)
	}
	if strings.Contains(err.Error(), "is now") {
		t.Fatalf("the refusal hints at another account's agent: %q", err)
	}
	if strings.Contains(err.Error(), string(f.attackerID[:12])) {
		t.Fatalf("the refusal leaks another account's agent: %q", err)
	}
}

// TestCrossAccountFullRefStillReachesHolder checks that the disambiguated
// address still reaches the account currently holding the name.
func TestCrossAccountFullRefStillReachesHolder(t *testing.T) {
	f := ownerResolveFixture(t)

	if err := f.send(fmt.Sprintf("worker [%s]@hostA", f.attackerRef), "hello"); err != nil {
		t.Fatalf("send to the holder's full ref: %v", err)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 1 {
		t.Fatalf("the holder received %d messages through its full ref, want 1", got)
	}
}

// findEventRecord returns the first log record whose event field equals want.
func findEventRecord(t *testing.T, logs *syncBuffer, want string) map[string]any {
	t.Helper()
	records := decodeLogLines(t, logs.String())
	for _, rec := range records {
		if rec["event"] == want {
			return rec
		}
	}
	t.Fatalf("no log record with event %q in %s", want, logs.String())
	return nil
}

// logUIDList reads a uid list from a decoded log record.
func logUIDList(t *testing.T, v any) []uint32 {
	t.Helper()
	raw, ok := v.([]any)
	if !ok {
		t.Fatalf("matched_uids = %#v, want a JSON array", v)
	}
	out := make([]uint32, 0, len(raw))
	for _, item := range raw {
		f, ok := item.(float64)
		if !ok {
			t.Fatalf("matched_uids item = %#v, want a number", item)
		}
		out = append(out, uint32(f))
	}
	return out
}

// TestCrossAccountBareNameRefusalLogged checks the delivery_refused_owner
// record: the name, the owner, the skipped account and the sender's ref, with
// no message text.
func TestCrossAccountBareNameRefusalLogged(t *testing.T) {
	f := ownerResolveFixture(t)

	err := f.send("worker@hostA", "stolen?")
	if err == nil {
		t.Fatal("a send to a name owned by a departed account succeeded, want not_found")
	}
	if !strings.Contains(err.Error(), `no agent named "worker" on hostA: the name belongs to another account`) {
		t.Fatalf("message = %q, want the ownership refusal text", err)
	}

	rec := findEventRecord(t, f.logs, EvDeliveryRefusedOwner)
	if rec["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", rec["level"])
	}
	if rec["name"] != "worker" {
		t.Fatalf("name = %v, want worker", rec["name"])
	}
	if uid, _ := rec["owner_uid"].(float64); uint32(uid) != 1001 {
		t.Fatalf("owner_uid = %v, want 1001", rec["owner_uid"])
	}
	if got, want := logUIDList(t, rec["matched_uids"]), []uint32{1002}; !reflect.DeepEqual(got, want) {
		t.Fatalf("matched_uids = %v, want %v", got, want)
	}
	if rec["from"] != f.caller.Ref {
		t.Fatalf("from = %v, want the caller ref %q", rec["from"], f.caller.Ref)
	}
	if strings.Contains(f.logs.String(), "stolen?") {
		t.Fatal("the refusal log records the message text")
	}
}

// TestOwnerVisibleOtherInvisibleSelectsOwner checks that an invisible
// same-name Agent of another account does not block the owner's visible one.
func TestOwnerVisibleOtherInvisibleSelectsOwner(t *testing.T) {
	cands := []candidate{
		{Name: "worker", UID: 1001, NameOwned: true, NameOwnerUID: 1001, Visible: true},
		{Name: "worker", UID: 1002, NameOwned: true, NameOwnerUID: 1001, Visible: false},
	}
	idx, err := pickTarget(parsedAddress{Name: "worker"}, cands, localRefMatch, "on this Broker")
	if err != nil {
		t.Fatalf("pickTarget: %v", err)
	}
	if idx != 0 {
		t.Fatalf("idx = %d, want the owner's candidate 0", idx)
	}
}

// TestOwnerInvisibleOtherVisibleIsNotVisible checks that the owner account
// having the name but not showing it keeps the not_visible result, even when
// another account has a visible same-name Agent.
func TestOwnerInvisibleOtherVisibleIsNotVisible(t *testing.T) {
	cands := []candidate{
		{Name: "worker", UID: 1001, NameOwned: true, NameOwnerUID: 1001, Visible: false},
		{Name: "worker", UID: 1002, NameOwned: true, NameOwnerUID: 1001, Visible: true},
	}
	_, err := pickTarget(parsedAddress{Name: "worker"}, cands, localRefMatch, "on this Broker")
	if err == nil {
		t.Fatal("pickTarget selected a target, want not_visible")
	}
	if got := err.Code; got != localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE {
		t.Fatalf("code = %v, want not_visible (message %q)", got, err.Message)
	}
}

// TestOwnerRefusalUIDsSortedDistinct checks the log detail: the skipped uids
// are ascending and appear once per account.
func TestOwnerRefusalUIDsSortedDistinct(t *testing.T) {
	cands := []candidate{
		{Name: "worker", UID: 1003, NameOwned: true, NameOwnerUID: 1001, Visible: true},
		{Name: "worker", UID: 1002, NameOwned: true, NameOwnerUID: 1001, Visible: true},
		{Name: "worker", UID: 1002, NameOwned: true, NameOwnerUID: 1001, Visible: true},
	}
	var refusal nameOwnerRefusal
	_, err := pickTargetRecording(parsedAddress{Name: "worker"}, cands, localRefMatch, "on this Broker", &refusal)
	if err == nil {
		t.Fatal("pickTargetRecording selected a target, want not_found")
	}
	if got := err.Code; got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found", got)
	}
	if !refusal.OwnerKnown || refusal.OwnerUID != 1001 {
		t.Fatalf("refusal = %+v, want owner 1001", refusal)
	}
	if refusal.Name != "worker" {
		t.Fatalf("refusal name = %q, want worker", refusal.Name)
	}
	if got, want := refusal.MatchedUIDs, []uint32{1002, 1003}; !reflect.DeepEqual(got, want) {
		t.Fatalf("matched uids = %v, want %v", got, want)
	}
}

// TestRemoteRefRequiredBareNameNotFound checks that a member whose publisher
// requires a ref is reachable only by its ref: the sender fails a by-name send
// before any request leaves this Broker.
func TestRemoteRefRequiredBareNameNotFound(t *testing.T) {
	members := []remoteMember{
		{Name: "worker", Ref: "abcdef123456", RefRequired: true},
		{Name: "solo", Ref: "123456"},
	}
	cands := remoteCandidates(members, func(remoteMember) bool { return true }, resolveTestAddr)

	_, err := pickTarget(parsedAddress{Name: "worker", Alias: "h"}, cands, remoteRefMatch, "on hostB")
	if err == nil {
		t.Fatal("a bare-name send to a ref-required member succeeded, want not_found")
	}
	if got := err.Code; got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", got, err.Message)
	}
	if !strings.Contains(err.Message, "on hostB: the name belongs to another account") {
		t.Fatalf("message = %q, want the ownership refusal text with the peer alias", err.Message)
	}
	if strings.Contains(err.Message, resolveTestAddr("worker", "abcdef123456", false)) {
		t.Fatalf("message = %q leaks the ref-required member's address", err.Message)
	}

	idx, err := pickTarget(parsedAddress{Name: "worker", Ref: "abcdef123456", Alias: "h"}, cands, remoteRefMatch, "on hostB")
	if err != nil {
		t.Fatalf("send to the ref-required member's ref: %v", err)
	}
	if idx != 0 {
		t.Fatalf("idx = %d, want the ref-required member 0", idx)
	}

	idx, err = pickTarget(parsedAddress{Name: "solo", Alias: "h"}, cands, remoteRefMatch, "on hostB")
	if err != nil {
		t.Fatalf("by-name send to an ordinary member: %v", err)
	}
	if idx != 1 {
		t.Fatalf("idx = %d, want the ordinary member 1", idx)
	}
}

// TestRemoteOwnerStaysRefFreeNextToRefRequiredCollider checks the remote
// address rule: a ref-required member of the same name must not add a ref to the
// owner's address, while it always carries one itself.
func TestRemoteOwnerStaysRefFreeNextToRefRequiredCollider(t *testing.T) {
	members := []remoteMember{
		{Name: "worker", Ref: "abcdef"},
		{Name: "worker", Ref: "abcdef123456", RefRequired: true},
	}
	cands := remoteCandidates(members, func(remoteMember) bool { return true }, resolveTestAddr)
	if len(cands) != 2 {
		t.Fatalf("candidates = %d, want 2", len(cands))
	}
	if got := cands[0].Address; got != "worker@h" {
		t.Fatalf("the owner's remote address = %q, want worker@h: a ref-required collider must not add a ref", got)
	}
	if got := cands[1].Address; got != "worker [abcdef123456]@h" {
		t.Fatalf("the ref-required member's address = %q, want it to carry its ref", got)
	}
}

// TestRemoteSenderRosterCountsOnlyPlainMembers checks the receiver's signature
// rule: a ref-required collider does not make the owner's name unique, so the
// owner's signature stays ref-free.
func TestRemoteSenderRosterCountsOnlyPlainMembers(t *testing.T) {
	members := []remoteMember{
		{Name: "worker", Ref: "abcdef"},
		{Name: "worker", Ref: "abcdef123456", RefRequired: true},
	}
	refs, found, refRequired := remoteSenderRoster(members, "worker", "abcdef")
	if !found {
		t.Fatal("the sender was not found in the roster")
	}
	if refRequired {
		t.Fatal("the owner's entry was reported as ref-required")
	}
	if got := len(refs["worker"]); got != 1 {
		t.Fatalf("plain refs for worker = %d, want 1: a ref-required collider must not be counted", got)
	}
}

// TestReceiveBareNameOwnedByAnotherAccount checks the receiver's own ownership
// check: a peer's by-name delivery to a name owned by a departed account is
// refused with not_found and recorded, and never reaches the other account.
func TestReceiveBareNameOwnedByAnotherAccount(t *testing.T) {
	other := testAgent("worker", "/tmp/peers-ownerresolve-1002.sock")
	other.UID = 1002
	other.NameOwned = true
	other.NameOwnerUID = 1001
	other.Ref = other.ID[:12]
	inbox := newRecordingInbox()
	other.Inbox = inbox

	f := newReceiveFixture(t, defaultReceiveSettings(), other)
	sender := remoteMember{Name: "sender", Ref: "1a2b3c"}
	f.member(sender, "t1")
	f.share(other.ID, "t1")
	f.keepReread(other.ID)

	// The request travels over the test link into the registered kind-400
	// handler, the way a peer's delivery request arrives.
	f.d.register()
	f.net.setUp(f.peer, f.self, true)
	frame := f.requestFrame(sender, agentView{Name: "worker"}, "hello", time.Second)
	reply, sent, err := f.net.end(f.peer).Request(context.Background(), f.self, frame)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if !sent {
		t.Fatal("Request sent = false, want true")
	}
	wantReceiveError(t, reply, localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
		`Not sent: no agent named "worker" on this Broker: the name belongs to another account`)
	if got := inbox.Count(); got != 0 {
		t.Fatalf("the other account received %d messages, want 0", got)
	}

	rec := findEventRecord(t, f.logs, EvDeliveryRefusedOwner)
	if uid, _ := rec["owner_uid"].(float64); uint32(uid) != 1001 {
		t.Fatalf("owner_uid = %v, want 1001", rec["owner_uid"])
	}
	if got, want := logUIDList(t, rec["matched_uids"]), []uint32{1002}; !reflect.DeepEqual(got, want) {
		t.Fatalf("matched_uids = %v, want %v", got, want)
	}
	if rec["from"] != f.peer.String() {
		t.Fatalf("from = %v, want the peer fingerprint %q", rec["from"], f.peer.String())
	}
}
