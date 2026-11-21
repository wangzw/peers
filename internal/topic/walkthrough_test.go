package topic

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// wlEndpoint is one side of a walkthrough: a Registry on its own fakeLink.
type wlEndpoint struct {
	r *Registry
	l *fakeLink
}

// wlNewEndpoint builds a Registry wired to a fresh fakeLink for self.
func wlNewEndpoint(t *testing.T, self link.Fingerprint) *wlEndpoint {
	t.Helper()
	l := newFakeLink(t, self)
	return &wlEndpoint{r: New(l, Options{Self: self}), l: l}
}

// introduce registers an agent with the registry. AgentChanged also introduces
// an agent that the registry has not seen before.
func (e *wlEndpoint) introduce(t *testing.T, id AgentID, name, ref string) {
	t.Helper()
	e.r.AgentChanged(id, name, "", ref, "idle")
}

// join adds an agent to a topic and fails the test on an error.
func (e *wlEndpoint) join(t *testing.T, id AgentID, name string) {
	t.Helper()
	if err := e.r.Join(id, name); err != nil {
		t.Fatalf("Join(%q, %q): %v", id, name, err)
	}
}

// wlMember returns the roster entry used by these tests: status idle, no
// previous name.
func wlMember(name, ref string) Member {
	return Member{Name: name, Ref: ref, Status: "idle"}
}

// wlCheckMembers compares remote member lists, treating nil and an empty list
// as equal.
func wlCheckMembers(t *testing.T, what string, got, want []Member) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %+v, want %+v", what, got, want)
	}
}

// wlCheckVisibleAgents compares visible agent lists, treating nil and an empty
// list as equal.
func wlCheckVisibleAgents(t *testing.T, what string, got, want []VisibleAgent) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %+v, want %+v", what, got, want)
	}
}

// wlHasMember reports whether ms contains an entry with the given name.
func wlHasMember(ms []Member, name string) bool {
	for _, m := range ms {
		if m.Name == name {
			return true
		}
	}
	return false
}

// wlCheckNoSecretRoster checks that A never sent B a Roster or RosterDelta
// under the digest of a private topic before the peer joined it.
func wlCheckNoSecretRoster(t *testing.T, frames []fakeFrame, d [32]byte) {
	t.Helper()
	for i, f := range frames {
		var got []byte
		switch f.Kind {
		case topicpb.KindRoster:
			got = decodeRoster(t, f).Digest
		case topicpb.KindRosterDelta:
			got = decodeDelta(t, f).Digest
		default:
			continue
		}
		if bytes.Equal(got, d[:]) {
			t.Fatalf("frame %d (kind %d) carries the private topic's roster", i, f.Kind)
		}
	}
}

// TestWalkthrough follows the seven steps of design-topic.md's walkthrough with
// two linked brokers. It covers REQ-topics-007 (same name on both brokers is
// the same topic), REQ-topics-009 (a private topic's name never leaves) and
// REQ-topics-014 (a roster only goes to a peer that has members).
func TestWalkthrough(t *testing.T) {
	const (
		repo   = "github.com/org/repo"
		secret = "secret-x"
	)
	salt := []byte("walkthrough-salt")

	a := wlNewEndpoint(t, fakeLinkFingerprint(1))
	b := wlNewEndpoint(t, fakeLinkFingerprint(2))

	apiWorker := AgentID("api-worker")
	frontend := AgentID("frontend")
	dbWorker := AgentID("db-worker")

	a.introduce(t, apiWorker, "api-worker", "aa0001")
	a.join(t, apiWorker, repo)
	a.join(t, apiWorker, secret)
	b.introduce(t, frontend, "frontend", "ff0001")
	b.join(t, frontend, repo)

	connect(a.l, b.l, salt)
	flushAll(t, a.l, b.l)

	// Steps 1 to 4: both sides send their digest sets, both intersections are
	// repo only, both exchange a roster for it, and api-worker sees frontend.
	node := AgentRef{Peer: b.l.self, Ref: "ff0001"}
	wlCheckVisibleAgents(t, "A.VisibleAgents(api-worker)", a.r.VisibleAgents(apiWorker), []VisibleAgent{{
		Agent:  node,
		Member: wlMember("frontend", "ff0001"),
		Shared: []string{repo},
	}})
	wlCheckMembers(t, "B.RemoteMembers(A)", b.r.RemoteMembers(a.l.self), []Member{
		wlMember("api-worker", "aa0001"),
	})

	// Before step 5 neither a roster nor a delta under the private topic's
	// digest crossed the link, and no frame carried the name itself.
	wlCheckNoSecretRoster(t, a.l.sentFrames(b.l.self), digest(salt, a.l.self, secret))
	for i, f := range a.l.sentFrames(b.l.self) {
		if bytes.Contains(f.Payload, []byte(secret)) {
			t.Fatalf("frame %d sent to B contains the private topic name", i)
		}
	}

	// Steps 5 and 6: db-worker joins the private topic on B; B re-sends its
	// digest set and sends the new roster, then A recomputes on receipt and
	// sends its own roster for the topic.
	b.introduce(t, dbWorker, "db-worker", "dd0001")
	b.join(t, dbWorker, secret)
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "A.RemoteMembers(B)", a.r.RemoteMembers(b.l.self), []Member{
		wlMember("db-worker", "dd0001"),
		wlMember("frontend", "ff0001"),
	})
	wlCheckVisibleAgents(t, "B.VisibleAgents(db-worker)", b.r.VisibleAgents(dbWorker), []VisibleAgent{{
		Agent:  AgentRef{Peer: a.l.self, Ref: "aa0001"},
		Member: wlMember("api-worker", "aa0001"),
		Shared: []string{secret},
	}})

	// Step 7: the link goes down and both sides forget the other.
	disconnect(a.l, b.l)
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "A.RemoteMembers(B) after disconnect", a.r.RemoteMembers(b.l.self), nil)
	wlCheckMembers(t, "B.RemoteMembers(A) after disconnect", b.r.RemoteMembers(a.l.self), nil)
	wlCheckVisibleAgents(t, "A.VisibleAgents(api-worker) after disconnect", a.r.VisibleAgents(apiWorker), nil)
}

// TestJoinNewNameNoApproval covers REQ-topics-002: joining a topic nobody has
// used succeeds, and two agents that joined the same new name see each other,
// on one broker and across brokers.
func TestJoinNewNameNoApproval(t *testing.T) {
	const name = "never-used-before"

	a := wlNewEndpoint(t, fakeLinkFingerprint(1))
	b := wlNewEndpoint(t, fakeLinkFingerprint(2))
	connect(a.l, b.l, []byte("salt"))
	flushAll(t, a.l, b.l)

	alpha := AgentID("alpha")
	bravo := AgentID("bravo")
	charlie := AgentID("charlie")

	a.introduce(t, alpha, "alpha", "a10002")
	if err := a.r.Join(alpha, name); err != nil {
		t.Fatalf("Join(%q, %q) on an unused name: %v", alpha, name, err)
	}
	a.introduce(t, bravo, "bravo", "b10002")
	a.join(t, bravo, name)

	shared, ok := a.r.Visible(alpha, AgentRef{ID: bravo})
	if !ok || !reflect.DeepEqual(shared, []string{name}) {
		t.Fatalf("A.Visible(alpha, {ID: bravo}) = (%v, %v), want ([%s], true)", shared, ok, name)
	}

	// Nobody is on B yet, so B joining the same name is the first member there.
	b.introduce(t, charlie, "charlie", "c10002")
	b.join(t, charlie, name)
	flushAll(t, a.l, b.l)

	if _, ok := a.r.Visible(alpha, AgentRef{Peer: b.l.self, Ref: "c10002"}); !ok {
		t.Fatal("alpha does not see charlie across brokers after both joined the new name")
	}
	if _, ok := b.r.Visible(charlie, AgentRef{Peer: a.l.self, Ref: "a10002"}); !ok {
		t.Fatal("charlie does not see alpha across brokers after both joined the new name")
	}
}

// TestLeaveAcrossBrokers covers REQ-topics-003: an agent that leaves a topic
// stops being visible, and the peer's view follows the remaining members.
func TestLeaveAcrossBrokers(t *testing.T) {
	const name = "t1"

	a := wlNewEndpoint(t, fakeLinkFingerprint(1))
	b := wlNewEndpoint(t, fakeLinkFingerprint(2))

	alpha := AgentID("alpha")
	bravo := AgentID("bravo")
	charlie := AgentID("charlie")

	a.introduce(t, alpha, "alpha", "a10002")
	a.join(t, alpha, name)
	a.introduce(t, bravo, "bravo", "b10002")
	a.join(t, bravo, name)
	b.introduce(t, charlie, "charlie", "c10002")
	b.join(t, charlie, name)

	connect(a.l, b.l, []byte("salt"))
	flushAll(t, a.l, b.l)

	if err := a.r.Leave(alpha, name); err != nil {
		t.Fatalf("Leave(%q, %q): %v", alpha, name, err)
	}
	flushAll(t, a.l, b.l)

	if _, ok := a.r.Visible(alpha, AgentRef{ID: bravo}); ok {
		t.Fatal("alpha still sees bravo after leaving the only shared topic")
	}
	wlCheckMembers(t, "B.RemoteMembers(A) after alpha left", b.r.RemoteMembers(a.l.self), []Member{
		wlMember("bravo", "b10002"),
	})

	if err := a.r.Leave(bravo, name); err != nil {
		t.Fatalf("Leave(%q, %q): %v", bravo, name, err)
	}
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "B.RemoteMembers(A) after both left", b.r.RemoteMembers(a.l.self), nil)
}

// TestLeaveAllAcrossBrokers covers REQ-topics-022: leaving everything removes
// every membership and the agent's information, and the peer stops seeing it
// while still seeing the other agents.
func TestLeaveAllAcrossBrokers(t *testing.T) {
	const (
		t1 = "t1"
		t2 = "t2"
	)

	a := wlNewEndpoint(t, fakeLinkFingerprint(1))
	b := wlNewEndpoint(t, fakeLinkFingerprint(2))

	alpha := AgentID("alpha")
	bravo := AgentID("bravo")
	charlie := AgentID("charlie")

	a.introduce(t, alpha, "alpha", "a10002")
	a.join(t, alpha, t1)
	a.join(t, alpha, t2)
	a.introduce(t, bravo, "bravo", "b10002")
	a.join(t, bravo, t2)
	b.introduce(t, charlie, "charlie", "c10002")
	b.join(t, charlie, t1)
	b.join(t, charlie, t2)

	connect(a.l, b.l, []byte("salt"))
	flushAll(t, a.l, b.l)

	a.r.LeaveAll(alpha)
	flushAll(t, a.l, b.l)

	got := b.r.RemoteMembers(a.l.self)
	if wlHasMember(got, "alpha") {
		t.Fatalf("B still sees alpha after LeaveAll: %+v", got)
	}
	wlCheckMembers(t, "B.RemoteMembers(A) after LeaveAll(alpha)", got, []Member{
		wlMember("bravo", "b10002"),
	})
	if topics := a.r.TopicsOf(alpha); topics != nil {
		t.Fatalf("A.TopicsOf(alpha) = %v after LeaveAll, want nil", topics)
	}
}

// TestAgentChangedAcrossBrokers covers REQ-agent-messaging-031 and
// REQ-agent-messaging-032: a rename and the eventual clearing of the previous
// name reach the peer's roster.
func TestAgentChangedAcrossBrokers(t *testing.T) {
	const name = "t1"

	a := wlNewEndpoint(t, fakeLinkFingerprint(1))
	b := wlNewEndpoint(t, fakeLinkFingerprint(2))

	alpha := AgentID("alpha")
	charlie := AgentID("charlie")

	a.introduce(t, alpha, "api-worker", "aa0001")
	a.join(t, alpha, name)
	b.introduce(t, charlie, "charlie", "c10002")
	b.join(t, charlie, name)

	connect(a.l, b.l, []byte("salt"))
	flushAll(t, a.l, b.l)

	// A rename carries the old name as the entry's previous name.
	a.r.AgentChanged(alpha, "api-v2", "api-worker", "aa0001", "busy")
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "B.RemoteMembers(A) after rename", b.r.RemoteMembers(a.l.self), []Member{{
		Name:     "api-v2",
		Ref:      "aa0001",
		Status:   "busy",
		PrevName: "api-worker",
	}})

	// Clearing the previous name is pushed as a change, not silently kept.
	a.r.AgentChanged(alpha, "api-v2", "", "aa0001", "busy")
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "B.RemoteMembers(A) after clearing the previous name", b.r.RemoteMembers(a.l.self), []Member{{
		Name:   "api-v2",
		Ref:    "aa0001",
		Status: "busy",
	}})
}

// TestDeltaOnlyToSharingPeersAcrossBrokers covers REQ-topics-014 with three
// brokers: a change on A reaches only the peer that shares the affected topic.
func TestDeltaOnlyToSharingPeersAcrossBrokers(t *testing.T) {
	a := wlNewEndpoint(t, fakeLinkFingerprint(1))
	b := wlNewEndpoint(t, fakeLinkFingerprint(2))
	c := wlNewEndpoint(t, fakeLinkFingerprint(3))

	alpha := AgentID("alpha")
	bravo := AgentID("bravo")
	charlie := AgentID("charlie")
	delta := AgentID("delta")

	a.introduce(t, alpha, "alpha", "a10002")
	a.join(t, alpha, "t1")
	a.introduce(t, bravo, "bravo", "b10002")
	a.join(t, bravo, "t1")
	b.introduce(t, charlie, "charlie", "c10002")
	b.join(t, charlie, "t1")
	c.introduce(t, delta, "delta", "ref-delta")
	c.join(t, delta, "t2")

	connect(a.l, b.l, []byte("salt-ab"))
	connect(a.l, c.l, []byte("salt-ac"))
	flushAll(t, a.l, b.l, c.l)

	wlCheckMembers(t, "C.RemoteMembers(A) before the change", c.r.RemoteMembers(a.l.self), nil)

	a.l.clearSent(b.l.self)
	a.l.clearSent(c.l.self)

	a.r.AgentChanged(alpha, "alpha", "", "a10002", "busy")
	flushAll(t, a.l, b.l, c.l)

	toB := a.l.sentFrames(b.l.self)
	if len(toB) != 1 || toB[0].Kind != topicpb.KindRosterDelta {
		t.Fatalf("A sent %d frames to B, want one RosterDelta", len(toB))
	}
	if got := a.l.sentFrames(c.l.self); len(got) != 0 {
		t.Fatalf("A sent %d frames to C, want none", len(got))
	}
	wlCheckMembers(t, "C.RemoteMembers(A) after the change", c.r.RemoteMembers(a.l.self), nil)
}
