package topic

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
	"google.golang.org/protobuf/proto"
)

// leakQueryText renders every query the broker could make for the given local
// agents and peers, so a test can assert that none of it mentions a name that
// must stay on its own Broker.
func leakQueryText(t *testing.T, r *Registry, locals []AgentID, peers []link.Fingerprint) string {
	t.Helper()
	var b strings.Builder
	for _, id := range locals {
		fmt.Fprintf(&b, "TopicsOf(%s) = %+v\n", id, r.TopicsOf(id))
		fmt.Fprintf(&b, "VisibleAgents(%s) = %+v\n", id, r.VisibleAgents(id))
	}
	for _, p := range peers {
		fmt.Fprintf(&b, "RemoteMembers(%s) = %+v\n", p, r.RemoteMembers(p))
	}
	return b.String()
}

// leakMustMarshal marshals an injected frame and fails the test on error.
func leakMustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal injected frame: %v", err)
	}
	return b
}

// leakAssertAbsent fails when text contains any of the strings.
func leakAssertAbsent(t *testing.T, where, text string, names ...string) {
	t.Helper()
	for _, name := range names {
		if strings.Contains(text, name) {
			t.Errorf("%s contains %q, which must not leave its Broker", where, name)
		}
	}
}

// TestPrivateTopicNameNeverSent covers REQ-topics-008 and REQ-topics-009: a
// topic name that is not declared public and not shared never appears in the
// frames one Broker sends to the other, in the other Broker's query results, or
// in its logs, even across joins, leaves, renames, status changes, reconnects
// and a dropped inbound frame.
func TestPrivateTopicNameNeverSent(t *testing.T) {
	const (
		secret = "secret-x"
		shared = "shared-t"
	)

	la := newFakeLink(t, fakeLinkFingerprint(0xA1))
	lb := newFakeLink(t, fakeLinkFingerprint(0xB2))
	ra, _ := newLinkRegistry(t, la)
	rb, bufB := newLinkRegistry(t, lb)

	const aID, cID = AgentID("agent-a"), AgentID("agent-c")
	if err := ra.Join(aID, secret); err != ErrUnknownAgent {
		t.Fatalf("Join before AgentChanged: err = %v, want %v", err, ErrUnknownAgent)
	}
	ra.AgentChanged(aID, "api-worker", "", "a10004", "idle")
	if err := ra.Join(aID, secret); err != nil {
		t.Fatalf("Join %q: %v", secret, err)
	}
	if err := ra.Join(aID, shared); err != nil {
		t.Fatalf("Join %q: %v", shared, err)
	}
	rb.AgentChanged(cID, "frontend", "", "c10004", "busy")
	if err := rb.Join(cID, shared); err != nil {
		t.Fatalf("Join %q: %v", shared, err)
	}

	salt1 := []byte("salt-1")
	connect(la, lb, salt1)
	flushAll(t, la, lb)

	// A full round of local changes: leave and rejoin the shared topic, then a
	// rename and a status change. Every frame they produce goes to B.
	if err := ra.Leave(aID, shared); err != nil {
		t.Fatalf("Leave %q: %v", shared, err)
	}
	if err := ra.Join(aID, shared); err != nil {
		t.Fatalf("rejoin %q: %v", shared, err)
	}
	ra.AgentChanged(aID, "api-v2", "api-worker", "a10004", "busy")
	ra.AgentChanged(aID, "api-v2", "", "a10004", "waiting")
	flushAll(t, la, lb)

	// A new connection with a new salt: the exchange starts over.
	disconnect(la, lb)
	flushAll(t, la, lb)
	salt2 := []byte("salt-2")
	connect(la, lb, salt2)
	flushAll(t, la, lb)

	// An invalid Roster (its digest is not in the peer's set) carrying the
	// private name. It must be dropped without the name reaching the log.
	unknown := digest(salt2, la.self, "unshared-topic")
	bad := &topicpb.Roster{
		Digest: unknown[:],
		Members: []*topicpb.Member{{
			Name:   secret,
			Ref:    "ref-x",
			Status: topicpb.AgentStatus_AGENT_STATUS_IDLE,
		}},
	}
	injectRaw(lb, la.self, topicpb.KindRoster, leakMustMarshal(t, bad))
	flushAll(t, la, lb)

	frames := la.sentFrames(lb.self)
	if len(frames) == 0 {
		t.Fatal("A sent no frames to B, the scenario did not exercise the wire")
	}
	for i, f := range frames {
		if bytes.Contains(f.Payload, []byte(secret)) {
			t.Errorf("frame %d (kind %d) sent to B contains %q", i, f.Kind, secret)
		}
	}

	// The scenario must be real: B knows A's agent under the shared topic.
	var names []string
	for _, m := range rb.RemoteMembers(la.self) {
		names = append(names, m.Name)
	}
	if len(names) == 0 {
		t.Fatal("B has no remote members from A, the scenario did not reach visibility")
	}

	leakAssertAbsent(t, "B queries", leakQueryText(t, rb, []AgentID{cID}, []link.Fingerprint{la.self}), secret)
	if log := bufB.String(); log == "" {
		t.Fatal("B logged nothing; the dropped frame did not exercise logging")
	} else {
		leakAssertAbsent(t, "B log", log, secret)
	}
}

// TestRosterOnlyToPeersWithMembers covers REQ-topics-014: the members of a topic
// reach a peer only after that peer has a member in the same topic.
func TestRosterOnlyToPeersWithMembers(t *testing.T) {
	const (
		shared = "t1"
		member = "api-worker"
	)

	la := newFakeLink(t, fakeLinkFingerprint(0xA3))
	lb := newFakeLink(t, fakeLinkFingerprint(0xB4))
	ra, _ := newLinkRegistry(t, la)
	rb, bufB := newLinkRegistry(t, lb)

	const aID, cID = AgentID("agent-a"), AgentID("agent-c")
	ra.AgentChanged(aID, member, "", "a10004", "idle")
	if err := ra.Join(aID, shared); err != nil {
		t.Fatalf("Join %q: %v", shared, err)
	}
	rb.AgentChanged(cID, "frontend", "", "c10004", "idle")

	connect(la, lb, []byte("salt"))
	flushAll(t, la, lb)

	before := la.sentFrames(lb.self)
	if len(before) == 0 {
		t.Fatal("A sent no frames to B on connect")
	}
	for i, f := range before {
		if bytes.Contains(f.Payload, []byte(member)) {
			t.Errorf("frame %d (kind %d) sent to B contains %q before B had a member in the topic", i, f.Kind, member)
		}
	}
	if got := rb.RemoteMembers(la.self); len(got) != 0 {
		t.Fatalf("B learned about A's members before joining a shared topic: %+v", got)
	}
	leakAssertAbsent(t, "B queries before joining", leakQueryText(t, rb, []AgentID{cID}, []link.Fingerprint{la.self}), member)
	leakAssertAbsent(t, "B log before joining", bufB.String(), member)

	// Once c joins the same topic the rosters flow both ways.
	if err := rb.Join(cID, shared); err != nil {
		t.Fatalf("Join %q on B: %v", shared, err)
	}
	flushAll(t, la, lb)

	if !leakHasRemote(t, rb.VisibleAgents(cID), member) {
		t.Errorf("B's VisibleAgents(%s) = %+v, want an agent named %q", cID, rb.VisibleAgents(cID), member)
	}
	if !leakHasRemote(t, ra.VisibleAgents(aID), "frontend") {
		t.Errorf("A's VisibleAgents(%s) = %+v, want an agent named %q", aID, ra.VisibleAgents(aID), "frontend")
	}
}

// leakHasRemote reports whether agents includes a remote agent whose member name
// is name.
func leakHasRemote(t *testing.T, agents []VisibleAgent, name string) bool {
	t.Helper()
	for _, a := range agents {
		if a.Agent.Peer != (link.Fingerprint{}) && a.Member.Name == name {
			return true
		}
	}
	return false
}

// TestLogsCarryNoNames covers the logging decision: neither Broker's logs carry
// a topic name, a member name, a previous name or a Ref used in the scenario,
// even while frames are dropped and connections come and go.
func TestLogsCarryNoNames(t *testing.T) {
	names := []string{
		"secret-alpha", "shared-beta",
		"api-worker", "frontend-controller", "api-v2",
		"api-legacy",
		"a10005", "a10006", "b10005",
	}

	la := newFakeLink(t, fakeLinkFingerprint(0xA5))
	lb := newFakeLink(t, fakeLinkFingerprint(0xB6))
	ra, bufA := newLinkRegistry(t, la)
	rb, bufB := newLinkRegistry(t, lb)

	const aID, cID = AgentID("agent-a"), AgentID("agent-c")
	ra.AgentChanged(aID, "api-worker", "", "a10005", "idle")
	if err := ra.Join(aID, "secret-alpha"); err != nil {
		t.Fatalf("Join secret-alpha: %v", err)
	}
	if err := ra.Join(aID, "shared-beta"); err != nil {
		t.Fatalf("Join shared-beta: %v", err)
	}
	rb.AgentChanged(cID, "frontend-controller", "", "b10005", "idle")
	if err := rb.Join(cID, "shared-beta"); err != nil {
		t.Fatalf("Join shared-beta on B: %v", err)
	}

	connect(la, lb, []byte("salt-1"))
	flushAll(t, la, lb)

	// A rename with a previous name and a new Ref, a status change, a leave and
	// rejoin: every path that pushes entries or digests.
	ra.AgentChanged(aID, "api-v2", "api-legacy", "a10006", "busy")
	ra.AgentChanged(aID, "api-v2", "api-legacy", "a10006", "waiting")
	if err := ra.Leave(aID, "shared-beta"); err != nil {
		t.Fatalf("Leave shared-beta: %v", err)
	}
	if err := ra.Join(aID, "shared-beta"); err != nil {
		t.Fatalf("rejoin shared-beta: %v", err)
	}
	flushAll(t, la, lb)

	disconnect(la, lb)
	flushAll(t, la, lb)
	connect(la, lb, []byte("salt-2"))
	flushAll(t, la, lb)

	// A frame that carries several of the names but is dropped because its
	// digest is not in the peer's set; the log must not echo its payload.
	unknown := digest([]byte("salt-2"), lb.self, "unshared-topic")
	bad := &topicpb.Roster{
		Digest: unknown[:],
		Members: []*topicpb.Member{{
			Name:         "frontend-controller",
			PreviousName: "api-legacy",
			Ref:          "b10005",
			Status:       topicpb.AgentStatus_AGENT_STATUS_BUSY,
		}},
	}
	injectRaw(lb, la.self, topicpb.KindRoster, leakMustMarshal(t, bad))
	flushAll(t, la, lb)

	// Sanity: the exchange reached B under the shared topic in both directions.
	if len(rb.RemoteMembers(la.self)) == 0 || len(ra.RemoteMembers(lb.self)) == 0 {
		t.Fatalf("the scenario did not reach visibility: A sees %+v, B sees %+v",
			ra.RemoteMembers(lb.self), rb.RemoteMembers(la.self))
	}

	leakAssertAbsent(t, "A log", bufA.String(), names...)
	if log := bufB.String(); log == "" {
		t.Fatal("B logged nothing; the dropped frame did not exercise logging")
	} else {
		leakAssertAbsent(t, "B log", log, names...)
	}
}
