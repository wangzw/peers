package topic

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// Compile-time check that the fake link satisfies the interface the registry
// consumes.
var _ Transport = (*fakeLink)(nil)

// syncBuffer is a bytes.Buffer safe for concurrent use by the fake link's
// scheduler goroutine and the test goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newLinkRegistry builds a Registry over fl that logs at debug level.
func newLinkRegistry(t *testing.T, fl *fakeLink) (*Registry, *syncBuffer) {
	t.Helper()
	buf := &syncBuffer{}
	r := New(fl, Options{Logger: slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), Self: fl.self})
	return r, buf
}

// seedLocal adds an agent and its memberships, bypassing Join, which belongs to
// another work item.
func seedLocal(t *testing.T, r *Registry, id AgentID, m Member, topics ...string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.local.setInfo(id, m)
	for _, name := range topics {
		if _, _, err := r.local.join(id, name); err != nil {
			t.Fatalf("join %q for %q: %v", name, id, err)
		}
	}
}

// injectRaw queues a frame with an arbitrary payload. fakeLink.inject marshals a
// message, so malformed bytes need this path.
func injectRaw(fl *fakeLink, peer link.Fingerprint, kind uint32, payload []byte) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	var gen uint64
	if st := fl.peers[peer]; st != nil {
		gen = st.gen
	}
	fl.queue = append(fl.queue, fakeEvent{
		kind:  fakeEventFrame,
		peer:  peer,
		gen:   gen,
		frame: link.Frame{Kind: link.FrameKind(kind), Payload: payload},
	})
	fl.cond.Broadcast()
}

// wireDigestsOf decodes a TopicDigests frame into fixed-size digests.
func wireDigestsOf(t *testing.T, f fakeFrame) [][32]byte {
	t.Helper()
	m := decodeDigests(t, f)
	out := make([][32]byte, 0, len(m.Digests))
	for _, b := range m.Digests {
		if len(b) != 32 {
			t.Fatalf("digest of %d bytes in TopicDigests", len(b))
		}
		var d [32]byte
		copy(d[:], b)
		out = append(out, d)
	}
	return out
}

// sortDigests sorts a digest list in place, ascending.
func sortDigests(ds [][32]byte) {
	slices.SortFunc(ds, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
}

func wantKinds(t *testing.T, frames []fakeFrame, kinds ...uint32) {
	t.Helper()
	if len(frames) != len(kinds) {
		t.Fatalf("got %d frames, want %d", len(frames), len(kinds))
	}
	for i, want := range kinds {
		if frames[i].Kind != want {
			t.Fatalf("frame %d has kind %d, want %d", i, frames[i].Kind, want)
		}
	}
}

// TestOnUpSendsDigests checks that a new link gets exactly one TopicDigests
// frame with the local set under the link's salt, empty set included.
func TestOnUpSendsDigests(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")

	t.Run("with topics", func(t *testing.T) {
		fl := newFakeLink(t, fakeLinkFingerprint(1))
		r, _ := newLinkRegistry(t, fl)
		seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t2", "t1")

		fl.up(p, salt)
		flushAll(t, fl)

		frames := fl.sentFrames(p)
		wantKinds(t, frames, topicpb.KindTopicDigests)
		want := [][32]byte{digest(salt, fl.self, "t1"), digest(salt, fl.self, "t2")}
		sortDigests(want)
		if got := wireDigestsOf(t, frames[0]); !slices.Equal(got, want) {
			t.Fatalf("digests = %x, want %x", got, want)
		}
	})

	t.Run("without topics", func(t *testing.T) {
		fl := newFakeLink(t, fakeLinkFingerprint(1))
		_, _ = newLinkRegistry(t, fl)

		fl.up(p, salt)
		flushAll(t, fl)

		frames := fl.sentFrames(p)
		wantKinds(t, frames, topicpb.KindTopicDigests)
		if got := wireDigestsOf(t, frames[0]); len(got) != 0 {
			t.Fatalf("digests = %x, want none", got)
		}
	})
}

// TestDigestsTriggerRosterForSharedOnly checks that the peer's digests start a
// Roster for the topics that entered the intersection and only for those, once.
// REQ-topics-014: the roster goes only to a peer with members in that topic.
func TestDigestsTriggerRosterForSharedOnly(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1", "t2")

	fl.up(p, salt)
	flushAll(t, fl)
	fl.clearSent(p)

	d1 := digest(salt, p, "t1")
	sent := digest(salt, fl.self, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1, digest(salt, p, "unrelated")}))
	flushAll(t, fl)

	frames := fl.sentFrames(p)
	wantKinds(t, frames, topicpb.KindRoster)
	roster := decodeRoster(t, frames[0])
	if !bytes.Equal(roster.Digest, sent[:]) {
		t.Fatalf("roster digest = %x, want %x", roster.Digest, sent)
	}
	if len(roster.Members) != 1 || roster.Members[0].Name != "api" {
		t.Fatalf("roster members = %v, want only api", roster.Members)
	}

	fl.clearSent(p)
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1, digest(salt, p, "unrelated")}))
	flushAll(t, fl)
	if frames := fl.sentFrames(p); len(frames) != 0 {
		t.Fatalf("same digest set produced %d frames, want none", len(frames))
	}
}

// TestRosterOutsideRemoteDigestsDropped checks that a Roster for a digest the
// peer never announced is discarded and its names never reach the log.
func TestRosterOutsideRemoteDigestsDropped(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)

	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "leaky-name", Ref: "aaaaaa"}},
	})
	flushAll(t, fl)

	if got := r.RemoteMembers(p); got != nil {
		t.Fatalf("RemoteMembers = %v, want nil", got)
	}
	logs := buf.String()
	if !strings.Contains(logs, "topic.frame_dropped") {
		t.Fatalf("log has no topic.frame_dropped: %s", logs)
	}
	if !strings.Contains(logs, `"reason":"unknown_digest"`) {
		t.Fatalf("log has no reason=unknown_digest: %s", logs)
	}
	if strings.Contains(logs, "leaky-name") {
		t.Fatalf("log leaks a member name: %s", logs)
	}
}

// TestRosterThenVisible checks that a roster entry under a shared digest makes a
// remote agent visible, and that disconnecting clears it.
// REQ-topics-005: visibility ends with the link.
func TestRosterThenVisible(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)

	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "frontend", Ref: "bbbbbb", Status: topicpb.AgentStatus_AGENT_STATUS_BUSY}},
	})
	flushAll(t, fl)

	shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"})
	if !ok || !slices.Equal(shared, []string{"t1"}) {
		t.Fatalf("Visible = (%v, %v), want ([t1], true)", shared, ok)
	}

	fl.down(p)
	flushAll(t, fl)

	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); ok || shared != nil {
		t.Fatalf("Visible after down = (%v, %v), want (nil, false)", shared, ok)
	}
	if got := r.RemoteMembers(p); got != nil {
		t.Fatalf("RemoteMembers after down = %v, want nil", got)
	}
}

// TestReconnectStartsOver checks that a re-established link uses a new salt and
// discards frames built from the old one.
func TestReconnectStartsOver(t *testing.T) {
	p := fakeLinkFingerprint(2)
	s1 := []byte("salt-1")
	s2 := []byte("salt-2")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, s1)
	flushAll(t, fl)
	d1 := digest(s1, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "frontend", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)
	if _, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); !ok {
		t.Fatal("remote agent not visible before the reconnect")
	}

	fl.down(p)
	flushAll(t, fl)
	fl.clearSent(p)

	fl.up(p, s2)
	flushAll(t, fl)

	d2 := digest(s2, p, "t1")
	sent2 := digest(s2, fl.self, "t1")
	if d1 == d2 {
		t.Fatal("the two salts produced the same digest")
	}
	frames := fl.sentFrames(p)
	wantKinds(t, frames, topicpb.KindTopicDigests)
	if got := wireDigestsOf(t, frames[0]); !slices.Equal(got, [][32]byte{sent2}) {
		t.Fatalf("digests after reconnect = %x, want [%x]", got, sent2)
	}

	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d2}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "frontend", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)

	if got := r.RemoteMembers(p); got != nil {
		t.Fatalf("old-salt roster was applied: %v", got)
	}
}

// TestFramesWithoutLinkIgnored checks that inbound frames without link state are
// dropped without side effects.
func TestFramesWithoutLinkIgnored(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "frontend", Ref: "bbbbbb"}},
	})
	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, []Member{{Name: "frontend", Ref: "bbbbbb"}}, nil, nil))
	flushAll(t, fl)

	if frames := fl.sentFrames(p); len(frames) != 0 {
		t.Fatalf("frames sent without a link: %d, want 0", len(frames))
	}
	if got := r.RemoteMembers(p); got != nil {
		t.Fatalf("RemoteMembers = %v, want nil", got)
	}
	if got := r.TopicsOf("a"); !slices.Equal(got, []string{"t1"}) {
		t.Fatalf("TopicsOf = %v, want [t1]", got)
	}
}

// TestMalformedFramesDropped checks that undecodable frames and a TopicDigests
// with a bad digest length are dropped while the previous state survives.
func TestMalformedFramesDropped(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "frontend", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)
	if _, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); !ok {
		t.Fatal("remote agent not visible before the malformed frames")
	}

	injectRaw(fl, p, topicpb.KindRoster, []byte{0xff, 0xff, 0xff, 0xff, 0xff})
	fl.inject(p, topicpb.KindTopicDigests, &topicpb.TopicDigests{Digests: [][]byte{make([]byte, 31)}})
	flushAll(t, fl)

	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); !ok || !slices.Equal(shared, []string{"t1"}) {
		t.Fatalf("Visible = (%v, %v), want ([t1], true)", shared, ok)
	}
	logs := buf.String()
	if !strings.Contains(logs, `"reason":"decode"`) {
		t.Fatalf("log has no reason=decode: %s", logs)
	}
	if !strings.Contains(logs, `"reason":"digest_length"`) {
		t.Fatalf("log has no reason=digest_length: %s", logs)
	}
}

// TestQueriesReturnCopies checks that mutating a query result never changes the
// registry.
func TestQueriesReturnCopies(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")
	seedLocal(t, r, "b", Member{Name: "worker", Ref: "cccccc"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "frontend", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)

	topics := r.TopicsOf("a")
	topics[0] = "mutated"
	if got := r.TopicsOf("a"); !slices.Equal(got, []string{"t1"}) {
		t.Fatalf("TopicsOf after mutation = %v, want [t1]", got)
	}

	agents := r.VisibleAgents("a")
	agents[0].Member.Name = "mutated"
	agents[0].Shared[0] = "mutated"
	again := r.VisibleAgents("a")
	if again[0].Member.Name != "worker" || !slices.Equal(again[0].Shared, []string{"t1"}) {
		t.Fatalf("VisibleAgents after mutation = %+v, want worker in [t1]", again[0])
	}

	remote := r.RemoteMembers(p)
	remote[0].Name = "mutated"
	if got := r.RemoteMembers(p); len(got) != 1 || got[0].Name != "frontend" {
		t.Fatalf("RemoteMembers after mutation = %v, want frontend", got)
	}
}

// testDigestList returns n distinct 32-byte digests.
func testDigestList(n int) [][32]byte {
	out := make([][32]byte, n)
	for i := range out {
		binary.BigEndian.PutUint32(out[i][28:], uint32(i+1))
	}
	return out
}

// testMemberWire returns n distinct wire members with valid names and Refs.
func testMemberWire(n int) []*topicpb.Member {
	out := make([]*topicpb.Member, n)
	for i := range out {
		out[i] = &topicpb.Member{Name: fmt.Sprintf("member%d", i), Ref: fmt.Sprintf("%06x", i)}
	}
	return out
}

// memberNames returns the names of members in order.
func memberNames(ms []Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Name
	}
	return out
}

// TestReenteringIntersectionDropsOldRoster reproduces the failure where a topic
// leaves the intersection and comes back: the entries saved before leaving must
// be gone, so the remote agent stays invisible until the peer sends a new
// Roster. The peer's digest set still contains the topic and no Delta arrives
// in this scenario, which is what made the stale entry visible.
func TestReenteringIntersectionDropsOldRoster(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "gone", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)
	if _, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); !ok {
		t.Fatal("remote agent not visible before the local agent left the topic")
	}

	if err := r.Leave("a", "t1"); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	flushAll(t, fl)

	// The peer still announces the digest (it has other members in the topic)
	// and sends no Delta while the topic is outside its intersection. Rejoining
	// must wait for a new Roster instead of resurrecting the old entry.
	if err := r.Join("a", "t1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	flushAll(t, fl)

	for _, m := range r.RemoteMembers(p) {
		if m.Ref == "bbbbbb" {
			t.Fatalf("stale roster entry came back after re-entering the intersection: %+v", m)
		}
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); ok || shared != nil {
		t.Fatalf("Visible = (%v, %v), want (nil, false)", shared, ok)
	}
}

// TestDeltaForUnknownDigestDropped checks that a RosterDelta for a digest the
// peer never announced is discarded with reason=unknown_digest, and that the
// member stays invisible even after the digest enters the intersection later.
func TestDeltaForUnknownDigestDropped(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1", "t2")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	d2 := digest(salt, p, "t2")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)

	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d2, []Member{{Name: "early", Ref: "bbbbbb"}}, nil, nil))
	flushAll(t, fl)

	if got := r.RemoteMembers(p); len(got) != 0 {
		t.Fatalf("RemoteMembers = %+v, want none", got)
	}
	logs := buf.String()
	if !strings.Contains(logs, "topic.frame_dropped") {
		t.Fatalf("log has no topic.frame_dropped: %s", logs)
	}
	if !strings.Contains(logs, `"reason":"unknown_digest"`) {
		t.Fatalf("log has no reason=unknown_digest: %s", logs)
	}

	// d2 enters the intersection only now; the dropped delta must not resurface.
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1, d2}))
	flushAll(t, fl)

	if got := r.RemoteMembers(p); len(got) != 0 {
		t.Fatalf("RemoteMembers after d2 became shared = %+v, want none", got)
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); ok || shared != nil {
		t.Fatalf("Visible = (%v, %v), want (nil, false)", shared, ok)
	}
}

// TestDeltaBadDigestLengthDropped checks that a RosterDelta whose digest is not
// 32 bytes is discarded with reason=unknown_digest.
func TestDeltaBadDigestLengthDropped(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)

	fl.inject(p, topicpb.KindRosterDelta, &topicpb.RosterDelta{
		Digest: make([]byte, 31),
		Added:  []*topicpb.Member{{Name: "short-digest", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)

	if got := r.RemoteMembers(p); len(got) != 0 {
		t.Fatalf("RemoteMembers = %+v, want none", got)
	}
	if logs := buf.String(); !strings.Contains(logs, `"reason":"unknown_digest"`) {
		t.Fatalf("log has no reason=unknown_digest: %s", logs)
	}
}

// TestDownDiscardsLinkState checks that a disconnect drops the whole link state:
// frames injected before a reconnect are ignored, the remote agents stay
// invisible, and a local change does not try to send on the dead link.
func TestDownDiscardsLinkState(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "gone", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)
	if _, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); !ok {
		t.Fatal("remote agent not visible before the disconnect")
	}

	fl.down(p)
	flushAll(t, fl)

	var sends atomic.Int32
	fl.setSendHook(func(peer link.Fingerprint, _ link.Frame) {
		if peer == p {
			sends.Add(1)
		}
	})

	// No onUp in between: these frames belong to the old connection.
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "late", Ref: "cccccc"}},
	})
	flushAll(t, fl)

	if got := r.RemoteMembers(p); got != nil {
		t.Fatalf("RemoteMembers = %+v, want nil", got)
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); ok || shared != nil {
		t.Fatalf("Visible = (%v, %v), want (nil, false)", shared, ok)
	}
	if got := sends.Load(); got != 0 {
		t.Fatalf("inbound frames after the disconnect produced %d sends, want 0", got)
	}

	// A new local topic must not be pushed to the disconnected peer.
	if err := r.Join("a", "t2"); err != nil {
		t.Fatalf("Join t2: %v", err)
	}
	flushAll(t, fl)
	if got := sends.Load(); got != 0 {
		t.Fatalf("Join after the disconnect sent %d frames to the dead link, want 0", got)
	}
}

// TestSecondOnUpStartsWithNewSalt checks that a second onUp without an onDown
// replaces the link state: the new TopicDigests uses the new salt and the old
// roster is gone.
func TestSecondOnUpStartsWithNewSalt(t *testing.T) {
	p := fakeLinkFingerprint(2)
	s1 := []byte("salt-1")
	s2 := []byte("salt-2")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, s1)
	flushAll(t, fl)
	d1 := digest(s1, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d1[:],
		Members: []*topicpb.Member{{Name: "gone", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)
	if _, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); !ok {
		t.Fatal("remote agent not visible before the second onUp")
	}

	fl.clearSent(p)
	fl.up(p, s2)
	flushAll(t, fl)

	d2 := digest(s2, p, "t1")
	sent2 := digest(s2, fl.self, "t1")
	if d1 == d2 {
		t.Fatal("the two salts produced the same digest")
	}
	frames := fl.sentFrames(p)
	wantKinds(t, frames, topicpb.KindTopicDigests)
	if got := wireDigestsOf(t, frames[0]); !slices.Equal(got, [][32]byte{sent2}) {
		t.Fatalf("digests after the second onUp = %x, want [%x]", got, sent2)
	}
	if got := r.RemoteMembers(p); got != nil {
		t.Fatalf("RemoteMembers = %+v, want nil", got)
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); ok || shared != nil {
		t.Fatalf("Visible = (%v, %v), want (nil, false)", shared, ok)
	}
}

// TestInboundMemberEntriesValidated checks that entries with a bad Ref or a bad
// name are dropped with a log line that does not repeat the member name, while
// valid entries in the same frame are kept.
func TestInboundMemberEntriesValidated(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)

	badNames := []string{
		"uppercase-ref", "short-ref", "long-ref", "nonhex-ref", "empty-ref", "empty-name",
		"with space", "with@at", "with*star", "with\nnewline",
	}
	members := []*topicpb.Member{
		{Name: "valid", Ref: "abcdef"},
		{Name: "名字", Ref: "0123456789ab"},
		{Name: "uppercase-ref", Ref: "ABCDEF"},
		{Name: "short-ref", Ref: "abcde"},
		{Name: "long-ref", Ref: "abcdefabcdef0"},
		{Name: "nonhex-ref", Ref: "zzzzzz"},
		{Name: "empty-ref"},
		{Ref: "aaaaaa"},
		{Name: "with space", Ref: "bbbbbb"},
		{Name: "with@at", Ref: "cccccc"},
		{Name: "with*star", Ref: "dddddd"},
		{Name: "with\nnewline", Ref: "eeeeee"},
	}
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d1[:], Members: members})
	flushAll(t, fl)

	// A Delta carries entries too; a bad one must be dropped the same way.
	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, []Member{{Name: "delta bad", Ref: "aaabbb"}}, nil, nil))
	flushAll(t, fl)

	got := memberNames(r.RemoteMembers(p))
	slices.Sort(got)
	want := []string{"valid", "名字"}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("RemoteMembers names = %v, want %v", got, want)
	}

	logs := buf.String()
	if !strings.Contains(logs, `"reason":"invalid_member"`) {
		t.Fatalf("log has no reason=invalid_member: %s", logs)
	}
	for _, name := range append(badNames, "delta bad") {
		if strings.Contains(logs, name) {
			t.Errorf("log leaks the rejected member name %q: %s", name, logs)
		}
	}
}

// TestMemberDropLogsOncePerFrame checks that all invalid entries of one frame
// produce a single topic.member_dropped line carrying the number dropped, and
// that the line names no member field.
func TestMemberDropLogsOncePerFrame(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)

	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d1[:], Members: []*topicpb.Member{
		{Name: "kept", Ref: "bbbbbb"},
		{Name: "bad one", Ref: "cccccc"},
		{Name: "bad two", Ref: "dddddd"},
	}})
	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1,
		[]Member{{Name: "bad three", Ref: "eeeeee"}},
		nil,
		[]Member{{Name: "bad four", Ref: "ABCDEF"}},
	))
	flushAll(t, fl)

	if got := strings.Count(buf.String(), "topic.member_dropped"); got != 2 {
		t.Fatalf("got %d topic.member_dropped lines, want one per frame (2): %s", got, buf.String())
	}
	if got := strings.Count(buf.String(), `"count":2`); got != 2 {
		t.Fatalf("got %d drop-count attrs, want 2: %s", got, buf.String())
	}
	for _, name := range []string{"bad one", "bad two", "bad three", "bad four"} {
		if strings.Contains(buf.String(), name) {
			t.Errorf("log leaks the rejected member name %q: %s", name, buf.String())
		}
	}
	if got := memberNames(r.RemoteMembers(p)); !slices.Equal(got, []string{"kept"}) {
		t.Fatalf("RemoteMembers names = %v, want [kept]", got)
	}
}

// TestPrevNameValidated checks that a non-empty PreviousName must follow the
// same name rules as Name: an entry with a bad one is dropped whole, while an
// empty PreviousName is allowed.
func TestPrevNameValidated(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)

	badPrevNames := []string{"old name", "old@at", "old*star", "old\nname"}
	members := []*topicpb.Member{
		{Name: "good", Ref: "bbbbbb", PreviousName: "old-good"},
		{Name: "empty-prev", Ref: "cccccc"},
		{Name: "bad-prev-space", Ref: "dddddd", PreviousName: badPrevNames[0]},
		{Name: "bad-prev-at", Ref: "eeeeee", PreviousName: badPrevNames[1]},
		{Name: "bad-prev-star", Ref: "ffffff", PreviousName: badPrevNames[2]},
		{Name: "bad-prev-control", Ref: "111111", PreviousName: badPrevNames[3]},
	}
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d1[:], Members: members})
	flushAll(t, fl)

	got := memberNames(r.RemoteMembers(p))
	slices.Sort(got)
	want := []string{"empty-prev", "good"}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("RemoteMembers names = %v, want %v", got, want)
	}

	logs := buf.String()
	if !strings.Contains(logs, `"reason":"invalid_member"`) {
		t.Fatalf("log has no reason=invalid_member: %s", logs)
	}
	for _, prev := range badPrevNames {
		if strings.Contains(logs, prev) {
			t.Errorf("log leaks the rejected previous name %q: %s", prev, logs)
		}
	}
}

// TestDeltaChangedEntryValidated checks that an invalid entry in the Changed
// list is dropped like one in Added: the member must not become visible, while
// a valid entry in the same frame is applied.
func TestDeltaChangedEntryValidated(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)

	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, nil, nil, []Member{
		{Name: "renamed", Ref: "bbbbbb"},
		{Name: "bad-changed", Ref: "ABCDEF"},
	}))
	flushAll(t, fl)

	if got := memberNames(r.RemoteMembers(p)); !slices.Equal(got, []string{"renamed"}) {
		t.Fatalf("RemoteMembers names = %v, want [renamed]", got)
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "ABCDEF"}); ok || shared != nil {
		t.Fatalf("Visible for the invalid Changed entry = (%v, %v), want (nil, false)", shared, ok)
	}
	logs := buf.String()
	if !strings.Contains(logs, `"reason":"invalid_member"`) {
		t.Fatalf("log has no reason=invalid_member: %s", logs)
	}
	if strings.Contains(logs, "bad-changed") {
		t.Fatalf("log leaks the rejected member name: %s", logs)
	}
}

// TestDeltaOutsideIntersectionNotStored checks that a RosterDelta for a digest
// the peer announced but that is not in the intersection is not saved, and that
// it does not resurface when the topic later enters the intersection.
func TestDeltaOutsideIntersectionNotStored(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	d2 := digest(salt, p, "t2")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1, d2}))
	flushAll(t, fl)

	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d2, []Member{{Name: "outside", Ref: "bbbbbb"}}, nil, nil))
	flushAll(t, fl)

	r.mu.RLock()
	_, stored := r.peers[p].rosters[d2]
	r.mu.RUnlock()
	if stored {
		t.Fatal("a RosterDelta for a digest outside the intersection was stored")
	}
	if got := r.RemoteMembers(p); len(got) != 0 {
		t.Fatalf("RemoteMembers = %+v, want none", got)
	}

	// t2 enters the intersection later; the discarded Delta must stay gone.
	if err := r.Join("a", "t2"); err != nil {
		t.Fatalf("Join t2: %v", err)
	}
	flushAll(t, fl)
	if got := r.RemoteMembers(p); len(got) != 0 {
		t.Fatalf("RemoteMembers after t2 became shared = %+v, want none", got)
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); ok || shared != nil {
		t.Fatalf("Visible = (%v, %v), want (nil, false)", shared, ok)
	}
}

// TestRosterOutsideIntersectionNotStored checks that a Roster for a digest the
// peer announced but that is not in the intersection is not saved, and that it
// does not resurface when the topic later enters the intersection.
func TestRosterOutsideIntersectionNotStored(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.up(p, salt)
	flushAll(t, fl)
	d1 := digest(salt, p, "t1")
	d2 := digest(salt, p, "t2")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1, d2}))
	flushAll(t, fl)

	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
		Digest:  d2[:],
		Members: []*topicpb.Member{{Name: "outside", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)

	r.mu.RLock()
	_, stored := r.peers[p].rosters[d2]
	r.mu.RUnlock()
	if stored {
		t.Fatal("a Roster for a digest outside the intersection was stored")
	}
	if got := r.RemoteMembers(p); len(got) != 0 {
		t.Fatalf("RemoteMembers = %+v, want none", got)
	}

	// t2 enters the intersection later; the discarded Roster must stay gone.
	if err := r.Join("a", "t2"); err != nil {
		t.Fatalf("Join t2: %v", err)
	}
	flushAll(t, fl)
	if got := r.RemoteMembers(p); len(got) != 0 {
		t.Fatalf("RemoteMembers after t2 became shared = %+v, want none", got)
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); ok || shared != nil {
		t.Fatalf("Visible = (%v, %v), want (nil, false)", shared, ok)
	}
}

// TestInboundFrameLimits checks the 4096-item caps on a digest set and on a
// roster or delta list: an oversized frame is dropped whole and logged, while a
// frame exactly at the cap is accepted.
func TestInboundFrameLimits(t *testing.T) {
	const limit = 4096

	newLinked := func(t *testing.T) (*fakeLink, *Registry, *syncBuffer, link.Fingerprint, [32]byte) {
		t.Helper()
		p := fakeLinkFingerprint(2)
		salt := []byte("salt")
		fl := newFakeLink(t, fakeLinkFingerprint(1))
		r, buf := newLinkRegistry(t, fl)
		seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")
		fl.up(p, salt)
		flushAll(t, fl)
		d1 := digest(salt, p, "t1")
		fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
		fl.inject(p, topicpb.KindRoster, &topicpb.Roster{
			Digest:  d1[:],
			Members: []*topicpb.Member{{Name: "anchor", Ref: "bbbbbb"}},
		})
		flushAll(t, fl)
		if got := memberNames(r.RemoteMembers(p)); !slices.Equal(got, []string{"anchor"}) {
			t.Fatalf("setup RemoteMembers = %v, want [anchor]", got)
		}
		return fl, r, buf, p, d1
	}

	t.Run("digest set over the limit", func(t *testing.T) {
		fl, r, buf, p, _ := newLinked(t)

		fl.inject(p, topicpb.KindTopicDigests, digestsMsg(testDigestList(limit+1)))
		flushAll(t, fl)

		if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "bbbbbb"}); !ok || !slices.Equal(shared, []string{"t1"}) {
			t.Fatalf("Visible = (%v, %v), want ([t1], true); the oversized frame must not replace the peer set", shared, ok)
		}
		if logs := buf.String(); !strings.Contains(logs, `"reason":"too_many_digests"`) {
			t.Fatalf("log has no reason=too_many_digests: %s", logs)
		}
	})

	t.Run("roster over the limit", func(t *testing.T) {
		fl, r, buf, p, d1 := newLinked(t)

		fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d1[:], Members: testMemberWire(limit + 1)})
		flushAll(t, fl)

		if got := memberNames(r.RemoteMembers(p)); !slices.Equal(got, []string{"anchor"}) {
			t.Fatalf("RemoteMembers = %v, want [anchor]; the oversized Roster must be dropped whole", got)
		}
		if logs := buf.String(); !strings.Contains(logs, `"reason":"too_many_members"`) {
			t.Fatalf("log has no reason=too_many_members: %s", logs)
		}
	})

	t.Run("delta over the limit", func(t *testing.T) {
		fl, r, buf, p, d1 := newLinked(t)

		added := make([]Member, limit+1)
		for i := range added {
			added[i] = Member{Name: fmt.Sprintf("member%d", i), Ref: fmt.Sprintf("%06x", i)}
		}
		fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, added, nil, nil))
		flushAll(t, fl)

		if got := memberNames(r.RemoteMembers(p)); !slices.Equal(got, []string{"anchor"}) {
			t.Fatalf("RemoteMembers = %v, want [anchor]; the oversized Delta must be dropped whole", got)
		}
		if logs := buf.String(); !strings.Contains(logs, `"reason":"too_many_members"`) {
			t.Fatalf("log has no reason=too_many_members: %s", logs)
		}
	})

	t.Run("delta changed over the limit", func(t *testing.T) {
		fl, r, buf, p, d1 := newLinked(t)

		changed := make([]Member, limit+1)
		for i := range changed {
			changed[i] = Member{Name: fmt.Sprintf("member%d", i), Ref: fmt.Sprintf("%06x", i)}
		}
		fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, nil, nil, changed))
		flushAll(t, fl)

		if got := memberNames(r.RemoteMembers(p)); !slices.Equal(got, []string{"anchor"}) {
			t.Fatalf("RemoteMembers = %v, want [anchor]; the oversized Delta must be dropped whole", got)
		}
		if logs := buf.String(); !strings.Contains(logs, `"reason":"too_many_members"`) {
			t.Fatalf("log has no reason=too_many_members: %s", logs)
		}
	})

	t.Run("delta removed over the limit", func(t *testing.T) {
		fl, r, buf, p, d1 := newLinked(t)

		removed := make([]memberKey, limit+1)
		for i := range removed {
			removed[i] = memberKey{Name: fmt.Sprintf("member%d", i), Ref: fmt.Sprintf("%06x", i)}
		}
		fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, nil, removed, nil))
		flushAll(t, fl)

		if got := memberNames(r.RemoteMembers(p)); !slices.Equal(got, []string{"anchor"}) {
			t.Fatalf("RemoteMembers = %v, want [anchor]; the oversized Delta must be dropped whole", got)
		}
		if logs := buf.String(); !strings.Contains(logs, `"reason":"too_many_members"`) {
			t.Fatalf("log has no reason=too_many_members: %s", logs)
		}
	})

	t.Run("roster at the limit", func(t *testing.T) {
		fl, r, _, p, d1 := newLinked(t)

		fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d1[:], Members: testMemberWire(limit)})
		flushAll(t, fl)

		if got := len(r.RemoteMembers(p)); got != limit {
			t.Fatalf("RemoteMembers has %d entries, want %d", got, limit)
		}
	})

	t.Run("delta changed at the limit", func(t *testing.T) {
		fl, r, buf, p, d1 := newLinked(t)

		changed := make([]Member, limit)
		for i := range changed {
			changed[i] = Member{Name: fmt.Sprintf("member%d", i), Ref: fmt.Sprintf("%06x", i)}
		}
		fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, nil, nil, changed))
		flushAll(t, fl)

		// The anchor entry stays; the frame's entries are all accepted.
		if got := len(r.RemoteMembers(p)); got != limit+1 {
			t.Fatalf("RemoteMembers has %d entries, want %d", got, limit+1)
		}
		if logs := buf.String(); strings.Contains(logs, `"reason":"too_many_members"`) {
			t.Fatalf("a Delta at the limit was rejected: %s", logs)
		}
	})

	t.Run("delta removed at the limit", func(t *testing.T) {
		fl, r, buf, p, d1 := newLinked(t)

		members := testMemberWire(limit)
		fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d1[:], Members: members})
		flushAll(t, fl)
		removed := make([]memberKey, len(members))
		for i, m := range members {
			removed[i] = memberKey{Name: m.Name, Ref: m.Ref}
		}
		fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d1, nil, removed, nil))
		flushAll(t, fl)

		if got := r.RemoteMembers(p); len(got) != 0 {
			t.Fatalf("RemoteMembers has %d entries, want none", len(got))
		}
		if logs := buf.String(); strings.Contains(logs, `"reason":"too_many_members"`) {
			t.Fatalf("a Delta at the limit was rejected: %s", logs)
		}
	})
}
