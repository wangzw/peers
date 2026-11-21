package topic

import (
	"bytes"
	"maps"
	"slices"
	"testing"

	"github.com/wangzw/peers/internal/link"
)

// peerTestFingerprints returns distinct self and peer fingerprints for tests
// that build a peerState directly.
func peerTestFingerprints() (self, peer link.Fingerprint) {
	return link.Fingerprint{1}, link.Fingerprint{2}
}

// peerTestDigestSet builds the digest set a peer would send for ds.
func peerTestDigestSet(ds [][32]byte) map[[32]byte]struct{} {
	set := make(map[[32]byte]struct{}, len(ds))
	for _, d := range ds {
		set[d] = struct{}{}
	}
	return set
}

// TestRecomputeEntering checks that the same topic name under the same salt is
// the only digest in the intersection, and that a second recompute reports
// nothing new.
// REQ-topics-007: the same non-public topic name on two Brokers is one topic.
func TestRecomputeEntering(t *testing.T) {
	salt := []byte("salt")
	self, peer := peerTestFingerprints()
	a := newPeerState(salt, self, peer, []string{"t1", "t2"})
	b := newPeerState(salt, peer, self, []string{"t1", "t3"})

	if got := a.localDigests(); !slices.IsSortedFunc(got, func(x, y [32]byte) int {
		return bytes.Compare(x[:], y[:])
	}) {
		t.Fatalf("localDigests is not ascending: %x", got)
	}

	a.setRemote(peerTestDigestSet(b.localDigests()))
	b.setRemote(peerTestDigestSet(a.localDigests()))

	// Each side compares in the direction of the other Broker's fingerprint,
	// so the two intersections hold different digests for the same topic.
	wantA := [][32]byte{digest(salt, peer, "t1")}
	wantB := [][32]byte{digest(salt, self, "t1")}
	if got := a.recompute(); !slices.Equal(got, wantA) {
		t.Errorf("a.recompute() = %x, want %x", got, wantA)
	}
	if got := b.recompute(); !slices.Equal(got, wantB) {
		t.Errorf("b.recompute() = %x, want %x", got, wantB)
	}
	if got := a.recompute(); len(got) != 0 {
		t.Errorf("second a.recompute() = %x, want none", got)
	}
	if got := b.recompute(); len(got) != 0 {
		t.Errorf("second b.recompute() = %x, want none", got)
	}
}

// TestSaltChangesDigests checks that a digest learned under one salt does not
// match the same topic under another salt.
func TestSaltChangesDigests(t *testing.T) {
	self, peer := peerTestFingerprints()
	d1 := digest([]byte("s1"), peer, "t1")
	d2 := digest([]byte("s2"), peer, "t1")
	if d1 == d2 {
		t.Fatal("the digest ignores the salt")
	}

	ps := newPeerState([]byte("s2"), self, peer, []string{"t1"})
	ps.setRemote(map[[32]byte]struct{}{d1: {}})
	if got := ps.recompute(); len(got) != 0 {
		t.Errorf("recompute() = %x, want none", got)
	}
	if len(ps.shared) != 0 {
		t.Errorf("shared = %x, want empty", ps.shared)
	}
}

// TestLeavingDropsRoster checks that leaving the intersection drops the digest
// from shared and discards its roster, and that re-entering reports the digest
// again but starts with no entries.
func TestLeavingDropsRoster(t *testing.T) {
	salt := []byte("salt")
	self, peer := peerTestFingerprints()
	ps := newPeerState(salt, self, peer, []string{"t1"})
	d := digest(salt, peer, "t1")
	ps.setRemote(map[[32]byte]struct{}{d: {}})
	if got := ps.recompute(); !slices.Equal(got, [][32]byte{d}) {
		t.Fatalf("recompute() = %x, want [%x]", got, d)
	}
	want := Member{Name: "peer", Ref: "r1", Status: "busy"}
	ps.applyRoster(d, []Member{want})

	ps.removeLocal("t1")
	if got := ps.recompute(); len(got) != 0 {
		t.Fatalf("recompute() after removeLocal = %x, want none", got)
	}
	if _, ok := ps.shared[d]; ok {
		t.Error("t1 is still in the intersection after removeLocal")
	}
	if _, ok := ps.send["t1"]; ok {
		t.Error("removeLocal kept the sent digest of t1")
	}
	if _, ok := ps.recv["t1"]; ok {
		t.Error("removeLocal kept the compared digest of t1")
	}
	if _, ok := ps.recvName[d]; ok {
		t.Error("removeLocal kept the reverse compared mapping of t1")
	}
	if entries, ok := ps.rosters[d]; ok {
		t.Errorf("roster entries = %+v, want the roster discarded when the digest left the intersection", entries)
	}

	ps.addLocal("t1")
	if got := ps.recompute(); !slices.Equal(got, [][32]byte{d}) {
		t.Errorf("recompute() after addLocal = %x, want [%x]", got, d)
	}
	if _, ok := ps.shared[d]; !ok {
		t.Error("t1 is not in the intersection after addLocal")
	}
	if entries, ok := ps.rosters[d]; ok {
		t.Errorf("roster entries = %+v after re-entering, want none until a new Roster arrives", entries)
	}
	ps.applyRoster(d, []Member{want})
	if got, ok := ps.rosters[d]["r1"]; !ok || got != want {
		t.Errorf("roster entry after a new Roster = %+v, %v, want %+v", got, ok, want)
	}
}

// TestApplyDeltaRules covers the receiver's rules for RosterDelta.
func TestApplyDeltaRules(t *testing.T) {
	salt := []byte("salt")
	self, peer := peerTestFingerprints()
	d := digest(salt, peer, "t1")

	tests := []struct {
		name    string
		initial []Member // nil means no Roster was ever applied
		added   []Member
		removed []memberKey
		changed []Member
		want    map[string]Member
	}{
		{
			name:    "a rename removes the old key and adds the new entry",
			initial: []Member{{Name: "old", Ref: "r1", Status: "idle"}},
			removed: []memberKey{{Name: "old", Ref: "r1"}},
			added:   []Member{{Name: "new", Ref: "r1", Status: "busy"}},
			want:    map[string]Member{"r1": {Name: "new", Ref: "r1", Status: "busy"}},
		},
		{
			name:    "a removal whose name does not match is ignored",
			initial: []Member{{Name: "old", Ref: "r1"}},
			removed: []memberKey{{Name: "other", Ref: "r1"}},
			want:    map[string]Member{"r1": {Name: "old", Ref: "r1"}},
		},
		{
			name:    "changed adds an unknown Ref",
			changed: []Member{{Name: "c", Ref: "r9", Status: "idle"}},
			want:    map[string]Member{"r9": {Name: "c", Ref: "r9", Status: "idle"}},
		},
		{
			name:  "an added entry with an empty Ref is skipped",
			added: []Member{{Name: "no-ref"}},
			want:  map[string]Member{},
		},
		{
			name:  "a delta applied before any roster starts empty",
			added: []Member{{Name: "early", Ref: "r2"}},
			want:  map[string]Member{"r2": {Name: "early", Ref: "r2"}},
		},
		{
			name:    "changed overwrites the entry of an existing Ref",
			initial: []Member{{Name: "here", Ref: "r3", Status: "idle"}},
			changed: []Member{{Name: "here", Ref: "r3", Status: "shell"}},
			want:    map[string]Member{"r3": {Name: "here", Ref: "r3", Status: "shell"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := newPeerState(salt, self, peer, []string{"t1"})
			if tt.initial != nil {
				ps.applyRoster(d, tt.initial)
			}
			ps.applyDelta(d, tt.added, tt.removed, tt.changed)
			if got := ps.rosters[d]; !maps.Equal(got, tt.want) {
				t.Errorf("entries = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestApplyRosterReplaces checks that a Roster replaces the whole list under its
// digest, and that entries with an empty Ref are skipped and a later duplicate
// Ref wins.
func TestApplyRosterReplaces(t *testing.T) {
	salt := []byte("salt")
	self, peer := peerTestFingerprints()
	d := digest(salt, peer, "t1")
	ps := newPeerState(salt, self, peer, []string{"t1"})

	ps.applyRoster(d, []Member{{Name: "a", Ref: "r1"}, {Name: "b", Ref: "r2"}})
	if got := len(ps.rosters[d]); got != 2 {
		t.Fatalf("entries after the first Roster = %d, want 2", got)
	}

	ps.applyRoster(d, []Member{{Name: "c", Ref: "r3"}})
	got := ps.rosters[d]
	if len(got) != 1 {
		t.Fatalf("entries after the second Roster = %+v, want only the new one", got)
	}
	if m, ok := got["r3"]; !ok || m.Name != "c" {
		t.Errorf("entry r3 = %+v, %v, want the new entry", m, ok)
	}

	ps.applyRoster(d, []Member{
		{Name: "first", Ref: "r4"},
		{Name: "second", Ref: "r4"},
		{Name: "no-ref"},
	})
	got = ps.rosters[d]
	if len(got) != 1 {
		t.Fatalf("entries = %+v, want one Ref-keyed entry", got)
	}
	if m, ok := got["r4"]; !ok || m.Name != "second" {
		t.Errorf("entry r4 = %+v, %v, want the later duplicate", m, ok)
	}
}
