package topic

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

type wlEndpoint struct {
	r *Registry
	l *fakeLink
}

func wlNewEndpoint(t *testing.T, self link.Fingerprint) *wlEndpoint {
	t.Helper()
	l := newFakeLink(t, self)
	return &wlEndpoint{r: New(l, Options{Self: self}), l: l}
}

func (e *wlEndpoint) introduce(t *testing.T, id AgentID, name, ref string) {
	t.Helper()
	e.r.AgentChanged(id, Member{Name: name, Ref: ref, Status: "idle"})
}

func (e *wlEndpoint) join(t *testing.T, id AgentID, name string) {
	t.Helper()
	if err := e.r.Join(id, name); err != nil {
		t.Fatalf("Join(%q, %q): %v", id, name, err)
	}
}

func wlMember(name, ref string) Member {
	return Member{Name: name, Ref: ref, Status: "idle"}
}

func wlCheckMembers(t *testing.T, what string, got, want []Member) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %+v, want %+v", what, got, want)
	}
}

func wlCheckVisibleAgents(t *testing.T, what string, got, want []VisibleAgent) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %+v, want %+v", what, got, want)
	}
}

func wlHasMember(ms []Member, name string) bool {
	for _, m := range ms {
		if m.Name == name {
			return true
		}
	}
	return false
}

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

// TestWalkthrough checks the digest exchange, a new membership and a disconnect
// between two linked Brokers.
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

	node := AgentRef{Peer: b.l.self, Ref: "ff0001"}
	wlCheckVisibleAgents(t, "A.VisibleAgents(api-worker)", a.r.VisibleAgents(apiWorker), []VisibleAgent{{
		Agent:  node,
		Member: wlMember("frontend", "ff0001"),
		Shared: []string{repo},
	}})
	wlCheckMembers(t, "B.RemoteMembers(A)", b.r.RemoteMembers(a.l.self), []Member{
		wlMember("api-worker", "aa0001"),
	})

	wlCheckNoSecretRoster(t, a.l.sentFrames(b.l.self), digest(salt, a.l.self, secret))
	for i, f := range a.l.sentFrames(b.l.self) {
		if bytes.Contains(f.Payload, []byte(secret)) {
			t.Fatalf("frame %d sent to B contains the private topic name", i)
		}
	}

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

	disconnect(a.l, b.l)
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "A.RemoteMembers(B) after disconnect", a.r.RemoteMembers(b.l.self), nil)
	wlCheckMembers(t, "B.RemoteMembers(A) after disconnect", b.r.RemoteMembers(a.l.self), nil)
	wlCheckVisibleAgents(t, "A.VisibleAgents(api-worker) after disconnect", a.r.VisibleAgents(apiWorker), nil)
}

// TestJoinNewNameNoApproval checks that joining a topic nobody has used succeeds
// and that two agents in the same new name see each other on one Broker and
// across Brokers.
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

// TestLeaveAcrossBrokers checks that an agent that leaves a topic stops being
// visible and that the peer's view follows the remaining members.
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

// TestLeaveAllAcrossBrokers checks that leaving everything removes every
// membership and the agent's information while the peer still sees the other
// agents.
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

// TestAgentChangedAcrossBrokers checks that a rename and the eventual clearing of
// the previous name reach the peer's roster.
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

	a.r.AgentChanged(alpha, Member{Name: "api-v2", Ref: "aa0001", Status: "busy", PrevName: "api-worker"})
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "B.RemoteMembers(A) after rename", b.r.RemoteMembers(a.l.self), []Member{{
		Name:     "api-v2",
		Ref:      "aa0001",
		Status:   "busy",
		PrevName: "api-worker",
	}})

	a.r.AgentChanged(alpha, Member{Name: "api-v2", Ref: "aa0001", Status: "busy"})
	flushAll(t, a.l, b.l)

	wlCheckMembers(t, "B.RemoteMembers(A) after clearing the previous name", b.r.RemoteMembers(a.l.self), []Member{{
		Name:   "api-v2",
		Ref:    "aa0001",
		Status: "busy",
	}})
}

// TestDeltaOnlyToSharingPeersAcrossBrokers checks that with three Brokers a
// change on A reaches only the peer that shares the affected topic.
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

	a.r.AgentChanged(alpha, Member{Name: "alpha", Ref: "a10002", Status: "busy"})
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
