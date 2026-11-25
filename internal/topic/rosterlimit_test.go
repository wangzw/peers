package topic

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

func linkedRosterTest(t *testing.T, topics ...string) (*fakeLink, *Registry, *syncBuffer, [32]byte) {
	t.Helper()
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, topics...)

	fl.up(p, salt)
	flushAll(t, fl)
	digests := make([][32]byte, len(topics))
	for i, name := range topics {
		digests[i] = digest(salt, p, name)
	}
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg(digests))
	flushAll(t, fl)
	return fl, r, buf, p
}

func rosterMembersOf(n int) []*topicpb.Member {
	out := make([]*topicpb.Member, n)
	for i := range out {
		out[i] = &topicpb.Member{Name: fmt.Sprintf("m%d", i), Ref: fmt.Sprintf("%06x", i)}
	}
	return out
}

// TestRosterDeltaCumulativeLimit checks that a second frame adding another 4096
// entries under the same digest is dropped whole.
func TestRosterDeltaCumulativeLimit(t *testing.T) {
	const limit = 4096
	fl, r, buf, p := linkedRosterTest(t, "t1")
	d := digest([]byte("salt"), p, "t1")

	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d, memberList(rosterMembersOf(limit)), nil, nil))
	flushAll(t, fl)
	if got := rosterEntryCount(r, p); got != limit {
		t.Fatalf("after the first delta: %d entries, want %d", got, limit)
	}

	second := rosterMembersOf(limit)
	for i := range second {
		second[i].Ref = fmt.Sprintf("%06x", limit+i)
	}
	fl.inject(p, topicpb.KindRosterDelta, deltaMsg(d, memberList(second), nil, nil))
	flushAll(t, fl)

	if got := rosterEntryCount(r, p); got != limit {
		t.Fatalf("after the oversized delta: %d entries, want %d; the second frame must be dropped", got, limit)
	}
	if logs := buf.String(); !strings.Contains(logs, `"reason":"roster_full"`) {
		t.Fatalf("log has no reason=roster_full: %s", logs)
	}
}

// TestRosterMemberNameLength checks that a 129-byte name is dropped as an
// invalid entry while a 128-byte name is kept.
func TestRosterMemberNameLength(t *testing.T) {
	fl, r, buf, p := linkedRosterTest(t, "t1")
	d := digest([]byte("salt"), p, "t1")

	at := strings.Repeat("a", MaxMemberNameBytes)
	over := strings.Repeat("b", MaxMemberNameBytes+1)
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d[:], Members: []*topicpb.Member{
		{Name: at, Ref: "aaaaaa"},
		{Name: over, Ref: "bbbbbb"},
	}})
	flushAll(t, fl)

	got := memberNames(r.RemoteMembers(p))
	if len(got) != 1 || got[0] != at {
		t.Fatalf("RemoteMembers names = %v, want only the 128-byte name", got)
	}
	if logs := buf.String(); !strings.Contains(logs, `"reason":"invalid_member"`) {
		t.Fatalf("log has no reason=invalid_member: %s", logs)
	}
}

// TestPeerRosterCumulativeLimit checks that entries are bounded across every
// shared digest of one peer: the frame that would push the total past 16384 is
// dropped whole.
func TestPeerRosterCumulativeLimit(t *testing.T) {
	const (
		perDigest = 4096
		digests   = 5
	)
	topics := make([]string, digests)
	for i := range topics {
		topics[i] = fmt.Sprintf("t%d", i+1)
	}
	fl, r, buf, p := linkedRosterTest(t, topics...)

	for i := 0; i < digests; i++ {
		d := digest([]byte("salt"), p, topics[i])
		fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d[:], Members: rosterMembersOf(perDigest)})
		flushAll(t, fl)
	}

	if got := rosterEntryCount(r, p); got != 4*perDigest {
		t.Fatalf("%d entries after five rosters, want %d; the fifth must be dropped", got, 4*perDigest)
	}
	if logs := buf.String(); !strings.Contains(logs, `"reason":"peer_roster_full"`) {
		t.Fatalf("log has no reason=peer_roster_full: %s", logs)
	}
	if shared, ok := r.Visible("a", AgentRef{Peer: p, Ref: "000001"}); !ok || !slices.Equal(shared, topics[:4]) {
		t.Fatalf("Visible = (%v, %v), want the first four topics", shared, ok)
	}
}

// TestRosterResendSameDigestCountsReplacement checks that re-sending a full
// Roster for a digest that already has entries counts the new list in place of
// the old one rather than in addition to it.
func TestRosterResendSameDigestCountsReplacement(t *testing.T) {
	const perDigest = 4096
	topics := []string{"t1", "t2", "t3", "t4"}
	fl, r, _, p := linkedRosterTest(t, topics...)
	salt := []byte("salt")

	for _, name := range topics {
		d := digest(salt, p, name)
		fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d[:], Members: rosterMembersOf(perDigest)})
		flushAll(t, fl)
	}
	if got := rosterEntryCount(r, p); got != len(topics)*perDigest {
		t.Fatalf("%d entries after four full rosters, want %d", got, len(topics)*perDigest)
	}

	// A disjoint replacement keeps the total at the limit, so counting the old
	// list too would wrongly drop it.
	replacement := rosterMembersOf(perDigest)
	for i := range replacement {
		replacement[i].Ref = fmt.Sprintf("%06x", perDigest+i)
	}
	marker := fmt.Sprintf("%06x", 2*perDigest-1)
	d0 := digest(salt, p, topics[0])
	fl.inject(p, topicpb.KindRoster, &topicpb.Roster{Digest: d0[:], Members: replacement})
	flushAll(t, fl)

	found := false
	for _, m := range r.RemoteMembers(p) {
		if m.Ref == marker {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("re-sending a roster for a digest already at its limit was dropped; the replacement was not stored")
	}
}

func rosterEntryCount(r *Registry, peer link.Fingerprint) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ps := r.peers[peer]
	if ps == nil {
		return 0
	}
	n := 0
	for _, entries := range ps.rosters {
		n += len(entries)
	}
	return n
}

func memberList(ms []*topicpb.Member) []Member {
	out := make([]Member, len(ms))
	for i, m := range ms {
		out[i] = memberFromWire(m)
	}
	return out
}
