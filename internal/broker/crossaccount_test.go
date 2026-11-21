package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// The two inbox socket paths below were chosen offline so that the first six
// characters of SHA-256("session:" + path) are the same, ac7ffc. They were
// found by hashing paths of the form
// /run/user/1001/claude/agents/sess-%08x.sock in increasing order and keeping
// the first pair that shared a six-character prefix. The test does not search
// for a collision at run time; it only derives the IDs the Broker would derive.
const (
	collisionVictimPath   = "/run/user/1001/claude/agents/sess-00000af3.sock"
	collisionAttackerPath = "/run/user/1001/claude/agents/sess-00000e42.sock"
)

// crossAccountID derives an Agent ID the way the Broker derives it and fails if
// the path needed cleaning, so the fixture IDs match the fixture paths.
func crossAccountID(t *testing.T, path string) AgentID {
	t.Helper()
	id, cleaned, err := AgentIDFor(path)
	if err != nil {
		t.Fatalf("AgentIDFor(%q): %v", path, err)
	}
	if cleaned != path {
		t.Fatalf("AgentIDFor cleaned %q to %q, want the path unchanged", path, cleaned)
	}
	return id
}

// insertCrossAccountAgent inserts an agent directly so a test can build a
// multi-account table. The account name is derived from the uid, the way the
// peercred handshake fills it. Callers must hold Broker.mu.
func insertCrossAccountAgent(b *Broker, id AgentID, name string, uid uint32, seq uint64) {
	b.agents[id] = &agent{
		id:         id,
		name:       name,
		caller:     Caller{UID: uid, Username: fmt.Sprintf("acct%d", uid)},
		accountKey: uid,
		seq:        seq,
		public:     make(map[string]string),
		conns:      make(map[*regConn]struct{}),
	}
}

// crossAccountAgents is a localAgents view over the Broker's real table. Reread
// returns the stored view, so the fixture needs no session record on disk.
type crossAccountAgents struct {
	mu    sync.Mutex
	views map[string]agentView
}

func newCrossAccountAgents() *crossAccountAgents {
	return &crossAccountAgents{views: make(map[string]agentView)}
}

// refresh rebuilds the views from the Broker's current table and attaches the
// recording inbox of each Agent.
func (a *crossAccountAgents) refresh(b *Broker, inboxes map[string]*recordingInbox) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.views = make(map[string]agentView)
	for _, s := range b.allAgents() {
		v := viewOf(s)
		if in, ok := inboxes[v.ID]; ok {
			v.Inbox = in
		}
		a.views[v.ID] = v
	}
}

func (a *crossAccountAgents) Lookup(id string) (agentView, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.views[id]
	return v, ok
}

func (a *crossAccountAgents) All() []agentView {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]agentView, 0, len(a.views))
	for _, v := range a.views {
		out = append(out, v)
	}
	return out
}

func (a *crossAccountAgents) Reread(id string) (agentView, error) {
	v, ok := a.Lookup(id)
	if !ok {
		return agentView{}, errAgentGone
	}
	return v, nil
}

var _ localAgents = (*crossAccountAgents)(nil)

// newCollisionDelivery wires a delivery over the fixture table. Only local
// sends are used, so the peer table stays empty.
func newCollisionDelivery(agents *crossAccountAgents, topics *deliveryTopics, logs *syncBuffer) *delivery {
	var level slog.LevelVar
	level.Set(slog.LevelDebug)
	return newDelivery(deliveryDeps{
		Agents:   agents,
		Topics:   topics,
		Peers:    &deliveryPeers{aliases: make(map[string]peerID)},
		Settings: newFakeSettings(defaultSendSettings(), "hostA"),
		Log:      NewLogger(logs, &level),
	})
}

// collisionFixture is one system-mode agent table: an owner account and a
// second account whose derived ID collides with the owner's first six
// characters, plus a third account that sends.
type collisionFixture struct {
	tb           *testBroker
	b            *Broker
	victimID     AgentID
	attackerID   AgentID
	callerID     AgentID
	sharedPrefix string
	attackerRef  string
	inboxes      map[string]*recordingInbox
	agents       *crossAccountAgents
	logs         *syncBuffer
	d            *delivery
	caller       agentView
}

func newCollisionFixture(t *testing.T) *collisionFixture {
	t.Helper()
	tb := newTestBroker(t)
	b := tb.b

	f := &collisionFixture{
		tb:         tb,
		b:          b,
		victimID:   crossAccountID(t, collisionVictimPath),
		attackerID: crossAccountID(t, collisionAttackerPath),
		callerID:   crossAccountID(t, "/run/user/1003/claude/agents/observer.sock"),
		inboxes:    make(map[string]*recordingInbox),
		agents:     newCrossAccountAgents(),
		logs:       &syncBuffer{},
	}
	f.sharedPrefix = string(f.victimID[:6])
	if string(f.attackerID[:6]) != f.sharedPrefix {
		t.Fatalf("the fixture paths no longer share a six-character ID prefix: %q vs %q",
			f.victimID[:6], f.attackerID[:6])
	}
	if string(f.victimID[:7]) == string(f.attackerID[:7]) {
		t.Fatalf("the fixture paths share a seven-character prefix %q; the test needs them to differ after six",
			f.victimID[:7])
	}

	// The owner registers first. With the name still unique, its ref is the
	// six-character prefix a caller would have recorded.
	b.mu.Lock()
	insertCrossAccountAgent(b, f.victimID, "worker", 1001, 1)
	insertCrossAccountAgent(b, f.callerID, "observer", 1003, 2)
	b.recordNameOwnerLocked("worker", 1001, 1001, tb.clock.Now())
	b.recomputeRefsLocked()
	ownerRef := b.agents[f.victimID].ref
	b.mu.Unlock()
	if ownerRef != f.sharedPrefix {
		t.Fatalf("the owner's first ref = %q, want the six-character prefix %q", ownerRef, f.sharedPrefix)
	}

	// A second account registers the same name with the colliding path.
	b.mu.Lock()
	insertCrossAccountAgent(b, f.attackerID, "worker", 1002, 3)
	b.recomputeRefsLocked()
	f.attackerRef = b.agents[f.attackerID].ref
	ownerRefWithDuplicate := b.agents[f.victimID].ref
	b.mu.Unlock()

	// The owner keeps the existing shortest-prefix rule; only the other account
	// is pinned to 12 characters.
	wantOwnerRef := ComputeRefs([]AgentID{f.victimID, f.attackerID, f.callerID})[f.victimID]
	if ownerRefWithDuplicate != wantOwnerRef {
		t.Fatalf("the owner's ref with a duplicate name = %q, want the shortest unique prefix %q",
			ownerRefWithDuplicate, wantOwnerRef)
	}
	if len(ownerRefWithDuplicate) >= 12 {
		t.Fatalf("the owner's ref = %q, want it shorter than 12 characters", ownerRefWithDuplicate)
	}

	for _, id := range []AgentID{f.victimID, f.attackerID, f.callerID} {
		f.inboxes[string(id)] = newRecordingInbox()
	}
	topics := &deliveryTopics{
		local: map[string][]string{
			string(f.victimID):   {"t1"},
			string(f.attackerID): {"t1"},
			string(f.callerID):   {"t1"},
		},
		remote: make(map[peerID][]deliveryRemoteEntry),
	}
	f.d = newCollisionDelivery(f.agents, topics, f.logs)
	f.agents.refresh(b, f.inboxes)
	f.caller = f.agents.views[string(f.callerID)]
	return f
}

// send sends one message from the third account.
func (f *collisionFixture) send(to, text string) error {
	return f.d.Send(context.Background(), f.caller, to, text)
}

// setLocalAlias changes the alias this Broker uses for its own addresses, so a
// test can choose the rune budget the truncated-name cases work with.
func (f *collisionFixture) setLocalAlias(alias string) {
	f.d.deps.Settings.(*fakeSettings).SetLocalAlias(alias)
}

// TestCrossAccountCollidingRefDoesNotReachOtherAccount is the reproduction: a
// caller holds the owner's six-character ref, the owner leaves, and a second
// account has registered a socket path whose ID collides on those six
// characters. Before the fix the message reaches the second account; after it
// the send reports not_found.
func TestCrossAccountCollidingRefDoesNotReachOtherAccount(t *testing.T) {
	f := newCollisionFixture(t)

	// The owner leaves; its name still belongs to its account.
	removeNameOwnerAgent(f.b, f.victimID)
	f.agents.refresh(f.b, f.inboxes)

	err := f.send(fmt.Sprintf("worker [%s]@hostA", f.sharedPrefix), "stolen?")
	if err == nil {
		t.Fatal("a send to the departed owner's short ref succeeded, want not_found")
	}
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", got, err)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 0 {
		t.Fatalf("the other account received %d messages through the owner's short ref, want 0", got)
	}

	// The other account's own ref is the full 12 characters, and it still
	// reaches the account.
	if len(f.attackerRef) != 12 {
		t.Fatalf("the other account's ref = %q (%d characters), want 12", f.attackerRef, len(f.attackerRef))
	}
	if err := f.send(fmt.Sprintf("worker [%s]@hostA", f.attackerRef), "real"); err != nil {
		t.Fatalf("send to the full 12-character ref: %v", err)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 1 {
		t.Fatalf("the other account received %d messages through its full ref, want 1", got)
	}
}

// TestCrossAccountPrefixNameShortRefDoesNotReach checks the local half of the
// name-prefix bypass: an Agent named worker2 whose ID collides with the owner's
// short ref must not answer an address that names worker. Before the fix the
// name prefix matched and the message reached worker2.
func TestCrossAccountPrefixNameShortRefDoesNotReach(t *testing.T) {
	f := newCollisionFixture(t)

	// The other account's Agent answers to a name that only starts with worker.
	f.b.mu.Lock()
	f.b.agents[f.attackerID].name = "worker2"
	f.b.recomputeRefsLocked()
	f.b.mu.Unlock()
	removeNameOwnerAgent(f.b, f.victimID)
	f.agents.refresh(f.b, f.inboxes)

	err := f.send(fmt.Sprintf("worker [%s]@hostA", f.sharedPrefix), "stolen?")
	if err == nil {
		t.Fatal("a prefix-named Agent answered a short ref meant for worker")
	}
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", got, err)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 0 {
		t.Fatalf("the prefix-named Agent received %d messages, want 0", got)
	}
}

// TestCrossAccountNonOwnerRefIsTwelve checks the identifier length itself: the
// account that does not own the name carries 12 characters, while the owner
// keeps the existing shortest-prefix rule.
func TestCrossAccountNonOwnerRefIsTwelve(t *testing.T) {
	f := newCollisionFixture(t)
	if len(f.attackerRef) != 12 {
		t.Fatalf("the other account's ref = %q (%d characters), want 12", f.attackerRef, len(f.attackerRef))
	}
	if f.sharedPrefix != f.attackerRef[:6] {
		t.Fatalf("the other account's ref %q does not start with the shared prefix %q", f.attackerRef, f.sharedPrefix)
	}

	// While both hold the name, the address the other account shows carries the
	// full 12-character ref.
	snap, ok := f.b.lookupAgent(f.attackerID)
	if !ok {
		t.Fatal("the other account left the table")
	}
	_, ref, _, ok := ParseAddress(snap.Address)
	if !ok {
		t.Fatalf("the other account's address %q does not parse", snap.Address)
	}
	if len(ref) != 12 {
		t.Fatalf("the other account's address %q carries a %d-character ref, want 12", snap.Address, len(ref))
	}
}

// TestCrossAccountShortRefSelectsOwnerWhilePresent checks the other half of the
// rule: while the owner still holds the name, the shared short ref selects the
// owner and never the other account.
func TestCrossAccountShortRefSelectsOwnerWhilePresent(t *testing.T) {
	f := newCollisionFixture(t)

	if err := f.send(fmt.Sprintf("worker [%s]@hostA", f.sharedPrefix), "hello"); err != nil {
		t.Fatalf("send to the owner's short ref: %v", err)
	}
	if got := len(f.inboxes[string(f.victimID)].Calls()); got != 1 {
		t.Fatalf("the owner received %d messages, want 1", got)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 0 {
		t.Fatalf("the other account received %d messages while the owner was present, want 0", got)
	}
}

// TestCrossAccountSameAccountRefsUnchanged checks that a name held only by one
// account keeps the pre-change refs: two same-account Agents named alike use
// ComputeRefs and are not pinned.
func TestCrossAccountSameAccountRefsUnchanged(t *testing.T) {
	tb := newTestBroker(t)
	b := tb.b
	first := testAgentID("aaaaaa1")
	second := testAgentID("aaaaaa2")

	b.mu.Lock()
	insertCrossAccountAgent(b, first, "dup", 1001, 1)
	insertCrossAccountAgent(b, second, "dup", 1001, 2)
	b.recordNameOwnerLocked("dup", 1001, 1001, tb.clock.Now())
	b.recomputeRefsLocked()
	gotFirst := b.agents[first].ref
	gotSecond := b.agents[second].ref
	b.mu.Unlock()

	want := ComputeRefs([]AgentID{first, second})
	if gotFirst != want[first] || gotSecond != want[second] {
		t.Fatalf("refs = %q, %q, want the ComputeRefs values %q, %q",
			gotFirst, gotSecond, want[first], want[second])
	}
	if len(gotFirst) >= 12 || len(gotSecond) >= 12 {
		t.Fatalf("refs = %q, %q, want both shorter than 12: a same-account name must not be pinned",
			gotFirst, gotSecond)
	}
}

// TestCrossAccountTruncatedNameCannotAnswerShortRef is the truncation half of
// the bypass: a second account registers the owner's name plus filler, so its
// rendered address is truncated, and its socket path collides with the owner's
// six-character ref. The truncated rendering must not let it answer the owner's
// short ref, on this Broker or across a link, even when its own name starts
// with the name the caller wrote.
func TestCrossAccountTruncatedNameCannotAnswerShortRef(t *testing.T) {
	f := newCollisionFixture(t)
	longName := "worker" + strings.Repeat("x", 60)

	// The other account answers to a name whose address cannot show in full.
	f.b.mu.Lock()
	f.b.agents[f.attackerID].name = longName
	f.b.recomputeRefsLocked()
	f.b.mu.Unlock()
	removeNameOwnerAgent(f.b, f.victimID)
	f.agents.refresh(f.b, f.inboxes)

	// This Broker: the long name is not the owner's name, and a short ref must
	// not select it through its truncated address.
	if err := f.send(fmt.Sprintf("worker [%s]@hostA", f.sharedPrefix), "stolen?"); err == nil {
		t.Fatal("a truncated long name answered the owner's short ref")
	} else if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", got, err)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 0 {
		t.Fatalf("the truncated long name received %d messages, want 0", got)
	}

	// Across a link, a peer may spell the member with the colliding short ref
	// or with its full ref. The short ref may not select it at all, and a full
	// ref may not select it when the name written is not the truncated name the
	// member renders.
	for _, memberRef := range []string{f.sharedPrefix, f.attackerRef} {
		cands := remoteCandidates(
			[]remoteMember{{Name: longName, Ref: memberRef}},
			func(remoteMember) bool { return true }, resolveTestAddr)
		for _, query := range []parsedAddress{
			{Name: "worker", Ref: f.sharedPrefix, Alias: "h"},
			{Name: "worker", Ref: f.attackerRef, Alias: "h"},
		} {
			_, err := pickTarget(query, cands, remoteRefMatch, "on hostA")
			if err == nil {
				t.Fatalf("member ref %q, query %+v: the truncated long name was selected", memberRef, query)
			}
			if err.Code != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
				t.Fatalf("member ref %q, query %+v: code = %v, want not_found", memberRef, query, err.Code)
			}
		}
	}
}

// TestCrossAccountTruncatedNameShortRefAtAnyAlias checks that the
// alias-length-aware acceptance of a truncated name never applies to a ref
// shorter than twelve characters. An account registers the victim's name plus
// filler so that, at one legal alias length, its rendered address shortens to
// exactly the victim's name, and its socket path collides with the victim's
// six-character ref. A caller still holding that short ref must not reach the
// other account through the truncation.
func TestCrossAccountTruncatedNameShortRefAtAnyAlias(t *testing.T) {
	f := newCollisionFixture(t)
	// 18 alias bytes leave 30 runes for the name; the attacker's 50-rune name
	// therefore truncates to exactly the 30-rune victim name.
	const alias = "aaaaaaaaaaaaaaaaaa"
	f.setLocalAlias(alias)

	victimName := strings.Repeat("v", 30)
	attackerName := victimName + strings.Repeat("x", 20)

	f.b.mu.Lock()
	f.b.agents[f.victimID].name = victimName
	f.b.agents[f.attackerID].name = attackerName
	f.b.recordNameOwnerLocked(victimName, 1001, 1001, f.tb.clock.Now())
	f.b.recomputeRefsLocked()
	f.b.mu.Unlock()

	// The victim leaves; the caller keeps the six-character ref it held while
	// the victim was its account's only agent.
	removeNameOwnerAgent(f.b, f.victimID)
	f.agents.refresh(f.b, f.inboxes)

	f.b.mu.Lock()
	attackerRef := f.b.agents[f.attackerID].ref
	f.b.mu.Unlock()
	if len(attackerRef) != 12 {
		t.Fatalf("the attacker's ref = %q (%d characters), want the full 12 the truncation forces", attackerRef, len(attackerRef))
	}

	// The address the other account renders truncates to the victim's name.
	rendered := FormatAddress(attackerName, attackerRef, alias, true)
	renderedName, renderedRef, _, ok := ParseAddress(rendered)
	if !ok {
		t.Fatalf("the attacker's rendered address %q does not parse", rendered)
	}
	if renderedName != victimName || renderedRef != attackerRef {
		t.Fatalf("the attacker's rendered address %q has name %q ref %q, want name %q ref %q",
			rendered, renderedName, renderedRef, victimName, attackerRef)
	}
	if !strings.HasPrefix(attackerRef, f.sharedPrefix) {
		t.Fatalf("the attacker's ref %q does not start with the victim's six-character ref %q", attackerRef, f.sharedPrefix)
	}

	// A short ref paired with the truncated name must not select the attacker.
	err := f.send(fmt.Sprintf("%s [%s]@%s", victimName, f.sharedPrefix, alias), "stolen?")
	if err == nil {
		t.Fatal("a short ref selected the other account through the truncated name")
	}
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", got, err)
	}
	if got := len(f.inboxes[string(f.attackerID)].Calls()); got != 0 {
		t.Fatalf("the other account received %d messages, want 0", got)
	}
}

// TestReceiveTruncatedNameFromOtherAliasLength is the cross-link alias-length
// reproduction: the caller saw an address truncated under the other Broker's
// alias, and this Broker renders the same name under a different alias length,
// so the truncation points differ. A reply that carries the caller's spelling
// must reach the target. With equal alias lengths the two spellings coincide,
// which is the behaviour that already worked.
func TestReceiveTruncatedNameFromOtherAliasLength(t *testing.T) {
	const (
		// 3 alias bytes leave 45 runes for the name at the sending Broker.
		shortAlias = "abc"
		// 24 alias bytes leave 24 runes for the name at this Broker.
		longAlias = "abcdefghijklmnopqrstuvwx"
	)
	longName := strings.Repeat("n", 50)

	deliver := func(t *testing.T, senderAlias, receiverAlias string) {
		t.Helper()
		target := testAgent(longName, "/tmp/peers-alias-length.sock")
		target.Ref = target.ID[:12]
		inbox := newRecordingInbox()
		target.Inbox = inbox

		f := newReceiveFixture(t, defaultReceiveSettings(), target)
		f.peers.effective[f.peer] = receiverAlias
		sender := remoteMember{Name: "sender", Ref: "1a2b3c"}
		f.member(sender, "t1")
		f.share(target.ID, "t1")
		f.keepReread(target.ID)
		f.d.register()
		f.net.setUp(f.peer, f.self, true)

		addr := FormatAddress(longName, target.Ref, senderAlias, true)
		name, ref, _, ok := ParseAddress(addr)
		if !ok {
			t.Fatalf("the address the caller saw, %q, does not parse", addr)
		}
		if ref != target.Ref {
			t.Fatalf("the address the caller saw carries ref %q, want %q", ref, target.Ref)
		}
		frame := f.requestFrame(sender, agentView{Name: name, Ref: ref}, "hello", time.Second)
		reply, sent, err := f.net.end(f.peer).Request(context.Background(), f.self, frame)
		if err != nil {
			t.Fatalf("Request: %v", err)
		}
		if !sent {
			t.Fatal("Request sent = false, want true")
		}
		wantDelivered(t, reply)
		if got := inbox.Count(); got != 1 {
			t.Fatalf("the target received %d messages, want 1", got)
		}
	}

	t.Run("different alias lengths", func(t *testing.T) {
		deliver(t, shortAlias, longAlias)
	})
	t.Run("same alias length", func(t *testing.T) {
		deliver(t, longAlias, longAlias)
	})
}
