package topic

import (
	"bytes"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// pubNewRegistry returns a Registry that uses fl as its transport and writes its
// logs to a buffer, without registering link callbacks.
func pubNewRegistry(t *testing.T, fl *fakeLink) (*Registry, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	return &Registry{
		t:     fl,
		log:   slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		self:  fl.self,
		local: newLocalTable(),
		peers: map[link.Fingerprint]*peerState{},
	}, buf
}

// pubLinkPeer simulates an established link to p whose peer has members in
// remoteTopics under salt, then clears the frames recorded so far.
func pubLinkPeer(t *testing.T, r *Registry, fl *fakeLink, p link.Fingerprint, salt []byte, remoteTopics ...string) {
	t.Helper()
	fl.up(p, salt)
	flushAll(t, fl)

	r.mu.Lock()
	defer r.mu.Unlock()
	ps := newPeerState(salt, r.self, p, r.local.topicNames())
	set := make(map[[32]byte]struct{}, len(remoteTopics))
	for _, name := range remoteTopics {
		set[digest(salt, p, name)] = struct{}{}
	}
	ps.setRemote(set)
	ps.recompute()
	r.peers[p] = ps
	fl.clearSent(p)
}

// pubDigestSet returns the digests this Broker sends for names under salt.
func pubDigestSet(self link.Fingerprint, salt []byte, names ...string) map[[32]byte]struct{} {
	set := make(map[[32]byte]struct{}, len(names))
	for _, name := range names {
		set[digest(salt, self, name)] = struct{}{}
	}
	return set
}

// pubCheckDigests checks that f is a TopicDigests carrying exactly want.
func pubCheckDigests(t *testing.T, f fakeFrame, want map[[32]byte]struct{}) {
	t.Helper()
	if f.Kind != topicpb.KindTopicDigests {
		t.Fatalf("frame kind = %d, want TopicDigests", f.Kind)
	}
	m := decodeDigests(t, f)
	if len(m.Digests) != len(want) {
		t.Fatalf("TopicDigests has %d digests, want %d", len(m.Digests), len(want))
	}
	for _, b := range m.Digests {
		d, err := parseDigest(b)
		if err != nil {
			t.Fatalf("TopicDigests contains a bad digest: %v", err)
		}
		if _, ok := want[d]; !ok {
			t.Errorf("TopicDigests contains an unexpected digest %x", d)
		}
	}
}

// pubCheckRoster checks that f is a Roster under d carrying exactly want.
func pubCheckRoster(t *testing.T, f fakeFrame, d [32]byte, want []Member) {
	t.Helper()
	if f.Kind != topicpb.KindRoster {
		t.Fatalf("frame kind = %d, want Roster", f.Kind)
	}
	m := decodeRoster(t, f)
	if !bytes.Equal(m.Digest, d[:]) {
		t.Fatalf("Roster digest = %x, want %x", m.Digest, d)
	}
	if got := pubMembers(m.Members); !slices.Equal(got, want) {
		t.Errorf("Roster members = %+v, want %+v", got, want)
	}
}

// pubCheckDelta checks that f is a RosterDelta under d with the given lists.
func pubCheckDelta(t *testing.T, f fakeFrame, d [32]byte, added []Member, removed []memberKey, changed []Member) {
	t.Helper()
	if f.Kind != topicpb.KindRosterDelta {
		t.Fatalf("frame kind = %d, want RosterDelta", f.Kind)
	}
	m := decodeDelta(t, f)
	if !bytes.Equal(m.Digest, d[:]) {
		t.Fatalf("RosterDelta digest = %x, want %x", m.Digest, d)
	}
	if got := pubMembers(m.Added); !slices.Equal(got, added) {
		t.Errorf("RosterDelta added = %+v, want %+v", got, added)
	}
	if got := pubKeys(m.Removed); !slices.Equal(got, removed) {
		t.Errorf("RosterDelta removed = %+v, want %+v", got, removed)
	}
	if got := pubMembers(m.Changed); !slices.Equal(got, changed) {
		t.Errorf("RosterDelta changed = %+v, want %+v", got, changed)
	}
}

func pubMembers(ps []*topicpb.Member) []Member {
	out := make([]Member, 0, len(ps))
	for _, p := range ps {
		out = append(out, memberFromWire(p))
	}
	return out
}

func pubKeys(ks []*topicpb.MemberKey) []memberKey {
	out := make([]memberKey, 0, len(ks))
	for _, k := range ks {
		out = append(out, memberKey{Name: k.Name, Ref: k.Ref})
	}
	return out
}

// TestJoinAndTopicsOf checks that joining records the membership and that an
// agent can belong to several topics at once.
// REQ-topics-001: an agent can join a topic by name.
// REQ-topics-004: an agent may be in more than one topic.
func TestJoinAndTopicsOf(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a AgentID = "a"

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	if err := r.Join(a, "release"); err != nil {
		t.Fatalf("Join(release): %v", err)
	}
	if err := r.Join(a, "t2"); err != nil {
		t.Fatalf("Join(t2): %v", err)
	}

	r.mu.RLock()
	got := r.local.topicsOf(a)
	r.mu.RUnlock()
	if want := []string{"release", "t2"}; !slices.Equal(got, want) {
		t.Errorf("topicsOf(a) = %v, want %v", got, want)
	}
}

// TestJoinFrameOrder checks the frames a join produces: a new topic sends
// TopicDigests and a Roster only to peers that also have members in it, while a
// second member of an existing topic sends a RosterDelta only to sharing peers.
// REQ-topics-002: joining an unused name succeeds without approval.
// REQ-topics-014: rosters go only to peers that also have members in the topic.
func TestJoinFrameOrder(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a, b AgentID = "a", "b"
	salt := []byte("salt-1")
	p, q := fakeLinkFingerprint(1), fakeLinkFingerprint(2)
	pubLinkPeer(t, r, fl, p, salt, "t9")
	pubLinkPeer(t, r, fl, q, salt)

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	if err := r.Join(a, "t9"); err != nil {
		t.Fatalf("Join(a, t9): %v", err)
	}

	pf := fl.sentFrames(p)
	if len(pf) != 2 {
		t.Fatalf("peer p received %d frames, want 2", len(pf))
	}
	pubCheckDigests(t, pf[0], pubDigestSet(r.self, salt, "t9"))
	pubCheckRoster(t, pf[1], digest(salt, r.self, "t9"), []Member{{Name: "api-worker", Ref: "aaaaaa"}})

	qf := fl.sentFrames(q)
	if len(qf) != 1 {
		t.Fatalf("peer q received %d frames, want only TopicDigests", len(qf))
	}
	pubCheckDigests(t, qf[0], pubDigestSet(r.self, salt, "t9"))

	fl.clearSent(p)
	fl.clearSent(q)
	r.AgentChanged(b, Member{Name: "frontend", Ref: "bbbbbb"})
	if err := r.Join(b, "t9"); err != nil {
		t.Fatalf("Join(b, t9): %v", err)
	}

	pf = fl.sentFrames(p)
	if len(pf) != 1 {
		t.Fatalf("peer p received %d frames for the second join, want 1", len(pf))
	}
	pubCheckDelta(t, pf[0], digest(salt, r.self, "t9"), []Member{{Name: "frontend", Ref: "bbbbbb"}}, nil, nil)
	if qf := fl.sentFrames(q); len(qf) != 0 {
		t.Errorf("peer q received %d frames for a topic it does not share, want 0", len(qf))
	}
}

// TestLeaveFrames checks that leaving a topic with members left sends a
// RosterDelta, and that the last leave sends TopicDigests instead.
// REQ-topics-003: an agent can leave a topic.
func TestLeaveFrames(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a, b AgentID = "a", "b"
	salt := []byte("salt-1")
	p := fakeLinkFingerprint(1)
	pubLinkPeer(t, r, fl, p, salt, "t1")

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	r.AgentChanged(b, Member{Name: "frontend", Ref: "bbbbbb"})
	if err := r.Join(a, "t1"); err != nil {
		t.Fatalf("Join(a, t1): %v", err)
	}
	if err := r.Join(b, "t1"); err != nil {
		t.Fatalf("Join(b, t1): %v", err)
	}
	fl.clearSent(p)

	if err := r.Leave(a, "t1"); err != nil {
		t.Fatalf("Leave(a, t1): %v", err)
	}
	pf := fl.sentFrames(p)
	if len(pf) != 1 {
		t.Fatalf("peer p received %d frames after Leave(a), want 1", len(pf))
	}
	pubCheckDelta(t, pf[0], digest(salt, r.self, "t1"), nil, []memberKey{{Name: "api-worker", Ref: "aaaaaa"}}, nil)

	fl.clearSent(p)
	if err := r.Leave(b, "t1"); err != nil {
		t.Fatalf("Leave(b, t1): %v", err)
	}
	pf = fl.sentFrames(p)
	if len(pf) != 1 {
		t.Fatalf("peer p received %d frames after the last leave, want only TopicDigests", len(pf))
	}
	pubCheckDigests(t, pf[0], pubDigestSet(r.self, salt))
}

// TestLeaveAllFrames checks the frames when an agent disappears: one
// TopicDigests for the emptied topics and a RosterDelta for each topic that
// still has members.
// REQ-topics-022: the memberships all disappear when the session ends.
func TestLeaveAllFrames(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a, b AgentID = "a", "b"
	salt := []byte("salt-1")
	p := fakeLinkFingerprint(1)
	pubLinkPeer(t, r, fl, p, salt, "t1", "t2")

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	r.AgentChanged(b, Member{Name: "frontend", Ref: "bbbbbb"})
	for _, step := range []struct {
		agent AgentID
		topic string
	}{{a, "t1"}, {a, "t2"}, {b, "t2"}} {
		if err := r.Join(step.agent, step.topic); err != nil {
			t.Fatalf("Join(%s, %s): %v", step.agent, step.topic, err)
		}
	}
	fl.clearSent(p)

	r.LeaveAll(a)

	pf := fl.sentFrames(p)
	if len(pf) != 2 {
		t.Fatalf("peer p received %d frames after LeaveAll, want 2", len(pf))
	}
	pubCheckDigests(t, pf[0], pubDigestSet(r.self, salt, "t2"))
	pubCheckDelta(t, pf[1], digest(salt, r.self, "t2"), nil, []memberKey{{Name: "api-worker", Ref: "aaaaaa"}}, nil)

	r.mu.RLock()
	topics := r.local.topicsOf(a)
	r.mu.RUnlock()
	if topics != nil {
		t.Errorf("topicsOf after LeaveAll = %v, want nil", topics)
	}
	if err := r.Join(a, "t1"); !errors.Is(err, ErrUnknownAgent) {
		t.Errorf("Join after LeaveAll: err = %v, want ErrUnknownAgent", err)
	}
}

// TestIdempotentAndInvalid checks that repeated joins and leaving an unjoined
// topic succeed silently, and that invalid names and unknown agents are
// rejected without frames.
func TestIdempotentAndInvalid(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a AgentID = "a"
	salt := []byte("salt-1")
	p := fakeLinkFingerprint(1)
	pubLinkPeer(t, r, fl, p, salt, "t1")

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	if err := r.Join(a, "t1"); err != nil {
		t.Fatalf("Join(a, t1): %v", err)
	}
	fl.clearSent(p)

	if err := r.Join(a, "t1"); err != nil {
		t.Errorf("repeated Join = %v, want nil", err)
	}
	if err := r.Leave(a, "t2"); err != nil {
		t.Errorf("Leave of an unjoined topic = %v, want nil", err)
	}
	if err := r.Join(a, ""); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("Join with an empty name = %v, want ErrInvalidTopic", err)
	}
	if err := r.Leave(a, "a\x00b"); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("Leave with a control character = %v, want ErrInvalidTopic", err)
	}
	if err := r.Join("nobody", "t1"); !errors.Is(err, ErrUnknownAgent) {
		t.Errorf("Join for an unknown agent = %v, want ErrUnknownAgent", err)
	}
	if err := r.Leave("nobody", "t1"); err != nil {
		t.Errorf("Leave for an unknown agent = %v, want nil", err)
	}

	if pf := fl.sentFrames(p); len(pf) != 0 {
		t.Errorf("peer p received %d frames from idempotent or invalid operations, want 0", len(pf))
	}
}

// TestAgentChangedFrames checks how a change of name, previous name, Ref and
// status is encoded.
// REQ-agent-messaging-031: an old name is carried to the peer.
// REQ-agent-messaging-032: previous names within ten minutes are listed.
func TestAgentChangedFrames(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a AgentID = "a"
	salt := []byte("salt-1")
	p := fakeLinkFingerprint(1)
	pubLinkPeer(t, r, fl, p, salt, "t1")

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	if err := r.Join(a, "t1"); err != nil {
		t.Fatalf("Join(a, t1): %v", err)
	}
	d := digest(salt, r.self, "t1")

	fl.clearSent(p)
	r.AgentChanged(a, Member{Name: "api-v2", Ref: "aaaaaa", Status: "busy", PrevName: "api-worker"})
	pf := fl.sentFrames(p)
	if len(pf) != 1 {
		t.Fatalf("rename produced %d frames, want 1", len(pf))
	}
	pubCheckDelta(t, pf[0], d,
		[]Member{{Name: "api-v2", Ref: "aaaaaa", Status: "busy", PrevName: "api-worker"}},
		[]memberKey{{Name: "api-worker", Ref: "aaaaaa"}}, nil)

	fl.clearSent(p)
	r.AgentChanged(a, Member{Name: "api-v2", Ref: "aaaaaa", Status: "busy"})
	pf = fl.sentFrames(p)
	if len(pf) != 1 {
		t.Fatalf("clearing the previous name produced %d frames, want 1", len(pf))
	}
	pubCheckDelta(t, pf[0], d, nil, nil,
		[]Member{{Name: "api-v2", Ref: "aaaaaa", Status: "busy"}})

	fl.clearSent(p)
	r.AgentChanged(a, Member{Name: "api-v2", Ref: "aaaaaa", Status: "idle"})
	pf = fl.sentFrames(p)
	if len(pf) != 1 {
		t.Fatalf("a status change produced %d frames, want 1", len(pf))
	}
	pubCheckDelta(t, pf[0], d, nil, nil,
		[]Member{{Name: "api-v2", Ref: "aaaaaa", Status: "idle"}})

	fl.clearSent(p)
	r.AgentChanged(a, Member{Name: "api-v2", Ref: "aaaaaaa", Status: "waiting"})
	pf = fl.sentFrames(p)
	if len(pf) != 1 {
		t.Fatalf("a Ref change produced %d frames, want 1", len(pf))
	}
	pubCheckDelta(t, pf[0], d,
		[]Member{{Name: "api-v2", Ref: "aaaaaaa", Status: "waiting"}},
		[]memberKey{{Name: "api-v2", Ref: "aaaaaa"}}, nil)

	fl.clearSent(p)
	r.AgentChanged(a, Member{Name: "api-v2", Ref: "aaaaaaa", Status: "waiting"})
	if pf := fl.sentFrames(p); len(pf) != 0 {
		t.Errorf("an unchanged AgentChanged produced %d frames, want 0", len(pf))
	}
}

// TestAgentChangedWithoutTopics checks that an introduced agent with no topic
// produces no frames.
func TestAgentChangedWithoutTopics(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a AgentID = "a"
	salt := []byte("salt-1")
	p := fakeLinkFingerprint(1)
	pubLinkPeer(t, r, fl, p, salt, "t1")

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	r.AgentChanged(a, Member{Name: "api-v2", Ref: "aaaaaa", Status: "busy", PrevName: "api-worker"})
	r.AgentChanged(a, Member{Name: "api-v3", Ref: "bbbbbb", Status: "idle", PrevName: "api-v2"})
	r.AgentChanged(a, Member{})

	if pf := fl.sentFrames(p); len(pf) != 0 {
		t.Errorf("AgentChanged for an agent without topics sent %d frames, want 0", len(pf))
	}
}

// TestDeltaOnlyToSharingPeers checks that an incremental change reaches only the
// peers that share the topic.
// REQ-topics-014: rosters go only to peers that also have members in the topic.
func TestDeltaOnlyToSharingPeers(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a, b AgentID = "a", "b"
	salt := []byte("salt-1")
	p, q := fakeLinkFingerprint(1), fakeLinkFingerprint(2)
	pubLinkPeer(t, r, fl, p, salt, "t1")
	pubLinkPeer(t, r, fl, q, salt, "t2")

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	r.AgentChanged(b, Member{Name: "frontend", Ref: "bbbbbb"})
	if err := r.Join(a, "t1"); err != nil {
		t.Fatalf("Join(a, t1): %v", err)
	}
	if err := r.Join(b, "t1"); err != nil {
		t.Fatalf("Join(b, t1): %v", err)
	}
	fl.clearSent(p)
	fl.clearSent(q)

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa", Status: "busy"})

	if pf := fl.sentFrames(p); len(pf) != 1 {
		t.Errorf("sharing peer p received %d frames, want 1", len(pf))
	}
	if qf := fl.sentFrames(q); len(qf) != 0 {
		t.Errorf("non-sharing peer q received %d frames, want 0", len(qf))
	}
}

// TestPeersVisitedInFingerprintOrder checks that one event reaches the peers in
// ascending fingerprint order.
func TestPeersVisitedInFingerprintOrder(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, _ := pubNewRegistry(t, fl)
	const a AgentID = "a"
	salt := []byte("salt-1")
	p1, p2, p3 := fakeLinkFingerprint(1), fakeLinkFingerprint(2), fakeLinkFingerprint(3)

	for _, p := range []link.Fingerprint{p3, p1, p2} {
		pubLinkPeer(t, r, fl, p, salt, "t1")
	}

	var order []link.Fingerprint
	fl.setSendHook(func(peer link.Fingerprint, _ link.Frame) {
		order = append(order, peer)
	})

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	if err := r.Join(a, "t1"); err != nil {
		t.Fatalf("Join(a, t1): %v", err)
	}

	want := []link.Fingerprint{p1, p1, p2, p2, p3, p3}
	if !slices.Equal(order, want) {
		t.Errorf("Send order = %v, want %v", order, want)
	}
}

// TestSendErrorIsTolerated checks that a failing Send is logged and does not
// stop the event from reaching the other peers.
func TestSendErrorIsTolerated(t *testing.T) {
	fl := newFakeLink(t, fakeLinkFingerprint(100))
	r, buf := pubNewRegistry(t, fl)
	const a AgentID = "a"
	salt := []byte("salt-1")
	p, q := fakeLinkFingerprint(1), fakeLinkFingerprint(2)
	pubLinkPeer(t, r, fl, p, salt, "t1")
	pubLinkPeer(t, r, fl, q, salt, "t1")

	r.AgentChanged(a, Member{Name: "api-worker", Ref: "aaaaaa"})
	fl.clearSent(p)
	fl.clearSent(q)
	fl.setFailSend(p, true)

	if err := r.Join(a, "t1"); err != nil {
		t.Fatalf("Join(a, t1): %v", err)
	}
	if qf := fl.sentFrames(q); len(qf) != 2 {
		t.Errorf("peer q received %d frames, want TopicDigests and Roster", len(qf))
	}
	if got := buf.String(); !strings.Contains(got, "topic.send_failed") {
		t.Errorf("logs do not mention topic.send_failed:\n%s", got)
	}
}
