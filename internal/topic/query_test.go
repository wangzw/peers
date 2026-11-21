package topic

import (
	"reflect"
	"slices"
	"testing"

	"github.com/wangzw/peers/internal/link"
)

// localSpec describes one agent for buildLocal.
type localSpec struct {
	Info   Member
	Topics []string
}

// buildLocal fills a localTable directly, without going through its methods.
func buildLocal(t *testing.T, agents map[AgentID]localSpec) *localTable {
	t.Helper()
	lt := newLocalTable()
	for id, spec := range agents {
		la := &localAgent{info: spec.Info, topics: map[string]struct{}{}}
		for _, name := range spec.Topics {
			la.topics[name] = struct{}{}
			members := lt.topics[name]
			if members == nil {
				members = map[AgentID]struct{}{}
				lt.topics[name] = members
			}
			members[id] = struct{}{}
		}
		lt.agents[id] = la
	}
	return lt
}

// buildPeer fills a peerState with the shared set and the rosters it is given.
// A roster topic outside the intersection is stored too, so tests can prove
// those entries stay invisible.
func buildPeer(t *testing.T, salt []byte, self, peer link.Fingerprint, localTopics, sharedTopics []string, rosters map[string][]Member) *peerState {
	t.Helper()
	ps := newPeerState(salt, self, peer, localTopics)
	for _, name := range sharedTopics {
		d := digest(salt, peer, name)
		ps.remote[d] = struct{}{}
		ps.shared[d] = struct{}{}
	}
	for name, members := range rosters {
		d := digest(salt, peer, name)
		byRef := ps.rosters[d]
		if byRef == nil {
			byRef = map[string]Member{}
			ps.rosters[d] = byRef
		}
		for _, member := range members {
			byRef[member.Ref] = member
		}
	}
	return ps
}

func queryFingerprint(b byte) link.Fingerprint {
	var f link.Fingerprint
	f[0] = b
	return f
}

// TestVisibleLocal checks that local agents are visible exactly when they share
// a topic.
// REQ-topics-005
func TestVisibleLocal(t *testing.T) {
	cases := []struct {
		name   string
		agents map[AgentID]localSpec
		other  AgentID
		want   []string
	}{
		{
			name: "same topic",
			agents: map[AgentID]localSpec{
				"a": {Info: Member{Name: "a"}, Topics: []string{"t1"}},
				"b": {Info: Member{Name: "b"}, Topics: []string{"t1"}},
			},
			other: "b",
			want:  []string{"t1"},
		},
		{
			name: "different topics",
			agents: map[AgentID]localSpec{
				"a": {Info: Member{Name: "a"}, Topics: []string{"t1"}},
				"c": {Info: Member{Name: "c"}, Topics: []string{"t2"}},
			},
			other: "c",
			want:  nil,
		},
		{
			name: "two shared topics ascending",
			agents: map[AgentID]localSpec{
				"a": {Info: Member{Name: "a"}, Topics: []string{"t2", "t1"}},
				"b": {Info: Member{Name: "b"}, Topics: []string{"t1", "t2"}},
			},
			other: "b",
			want:  []string{"t1", "t2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lt := buildLocal(t, tc.agents)
			got := visible(lt, nil, "a", AgentRef{ID: tc.other})
			if !slices.Equal(got, tc.want) {
				t.Errorf("visible(a, %q) = %v, want %v", tc.other, got, tc.want)
			}
		})
	}
}

// TestAgentWithoutTopicsInvisible checks that an agent in no topic is never
// listed and never sees anyone.
// REQ-topics-006
func TestAgentWithoutTopicsInvisible(t *testing.T) {
	lt := buildLocal(t, map[AgentID]localSpec{
		"a": {Info: Member{Name: "a"}, Topics: []string{"t1"}},
		"b": {Info: Member{Name: "b"}, Topics: []string{"t1"}},
		"z": {Info: Member{Name: "z"}},
	})

	if got := visible(lt, nil, "a", AgentRef{ID: "z"}); got != nil {
		t.Errorf("visible(a, z) = %v, want nil", got)
	}
	if got := visible(lt, nil, "z", AgentRef{ID: "a"}); got != nil {
		t.Errorf("visible(z, a) = %v, want nil", got)
	}
	got := visibleAgents(lt, nil, "a")
	if len(got) != 1 || got[0].Agent.ID != "b" {
		t.Errorf("visibleAgents(a) = %+v, want only b", got)
	}
	if got := visibleAgents(lt, nil, "z"); got != nil {
		t.Errorf("visibleAgents(z) = %+v, want nil", got)
	}
}

// TestVisibleRemoteOnlyInShared checks that a remote entry is visible only under
// a digest in the intersection and on a link we know.
// REQ-topics-005, REQ-topics-014
func TestVisibleRemoteOnlyInShared(t *testing.T) {
	salt := []byte("salt")
	p := queryFingerprint(1)
	lt := buildLocal(t, map[AgentID]localSpec{
		"a": {Info: Member{Name: "a"}, Topics: []string{"t1"}},
	})
	member := Member{Name: "frontend", Ref: "r1"}

	notShared := map[link.Fingerprint]*peerState{
		p: buildPeer(t, salt, queryFingerprint(100), p, []string{"t1"}, nil, map[string][]Member{"t1": {member}}),
	}
	if got := visible(lt, notShared, "a", AgentRef{Peer: p, Ref: "r1"}); got != nil {
		t.Errorf("visible over a digest outside the intersection = %v, want nil", got)
	}

	shared := map[link.Fingerprint]*peerState{
		p: buildPeer(t, salt, queryFingerprint(100), p, []string{"t1"}, []string{"t1"}, map[string][]Member{"t1": {member}}),
	}
	if got := visible(lt, shared, "a", AgentRef{Peer: p, Ref: "r1"}); !slices.Equal(got, []string{"t1"}) {
		t.Errorf("visible over a shared digest = %v, want [t1]", got)
	}

	unknown := queryFingerprint(9)
	if got := visible(lt, shared, "a", AgentRef{Peer: unknown, Ref: "r1"}); got != nil {
		t.Errorf("visible over an unknown link = %v, want nil", got)
	}
}

// TestVisibleSelf checks that an agent is visible to itself by the literal
// condition, but never listed among its own visible agents.
func TestVisibleSelf(t *testing.T) {
	lt := buildLocal(t, map[AgentID]localSpec{
		"a": {Info: Member{Name: "a"}, Topics: []string{"t2", "t1"}},
		"b": {Info: Member{Name: "b"}, Topics: []string{"t1"}},
	})

	if got := visible(lt, nil, "a", AgentRef{ID: "a"}); !slices.Equal(got, []string{"t1", "t2"}) {
		t.Errorf("visible(a, a) = %v, want [t1 t2]", got)
	}
	for _, va := range visibleAgents(lt, nil, "a") {
		if va.Agent.Peer.IsZero() && va.Agent.ID == "a" {
			t.Errorf("visibleAgents(a) includes a itself: %+v", va)
		}
	}
}

// TestVisibleAgentsOrderAndMerge checks the order of the result and that one
// remote agent in two shared topics appears once with a merged Shared slice.
func TestVisibleAgentsOrderAndMerge(t *testing.T) {
	salt := []byte("salt")
	p := queryFingerprint(1)
	q := queryFingerprint(2)

	lt := buildLocal(t, map[AgentID]localSpec{
		"a": {Info: Member{Name: "a"}, Topics: []string{"t1", "t2"}},
		"b": {Info: Member{Name: "b-id", Ref: "ref-b", Status: "idle"}, Topics: []string{"t1"}},
		"c": {Info: Member{Name: "c-id", Ref: "ref-c", Status: "busy"}, Topics: []string{"t2"}},
	})
	peers := map[link.Fingerprint]*peerState{
		p: buildPeer(t, salt, queryFingerprint(100), p, []string{"t1", "t2"}, []string{"t1", "t2"}, map[string][]Member{
			"t1": {
				{Name: "one", Ref: "r1", Status: "busy"},
				{Name: "two", Ref: "r2", Status: "idle"},
			},
			"t2": {
				{Name: "one", Ref: "r1", Status: "idle"},
			},
		}),
		q: buildPeer(t, salt, queryFingerprint(100), q, []string{"t1"}, []string{"t1"}, map[string][]Member{
			"t1": {{Name: "zero", Ref: "r0", Status: "shell"}},
		}),
	}

	want := []VisibleAgent{
		{Agent: AgentRef{ID: "b"}, Member: lt.agents["b"].info, Shared: []string{"t1"}},
		{Agent: AgentRef{ID: "c"}, Member: lt.agents["c"].info, Shared: []string{"t2"}},
		{Agent: AgentRef{Peer: p, Ref: "r1"}, Member: Member{Name: "one", Ref: "r1", Status: "busy"}, Shared: []string{"t1", "t2"}},
		{Agent: AgentRef{Peer: p, Ref: "r2"}, Member: Member{Name: "two", Ref: "r2", Status: "idle"}, Shared: []string{"t1"}},
		{Agent: AgentRef{Peer: q, Ref: "r0"}, Member: Member{Name: "zero", Ref: "r0", Status: "shell"}, Shared: []string{"t1"}},
	}
	if got := visibleAgents(lt, peers, "a"); !reflect.DeepEqual(got, want) {
		t.Errorf("visibleAgents(a) = %+v, want %+v", got, want)
	}
}

// TestRemoteMembersDedupe checks that remoteMembers deduplicates by Ref, keeps
// the first shared topic's entry, and drops entries outside the intersection.
func TestRemoteMembersDedupe(t *testing.T) {
	salt := []byte("salt")
	ps := buildPeer(t, salt, queryFingerprint(100), queryFingerprint(1), []string{"t1", "t2"}, []string{"t1", "t2"}, map[string][]Member{
		"t1": {
			{Name: "one", Ref: "r1", Status: "busy"},
			{Name: "two", Ref: "r2", Status: "idle"},
		},
		"t2": {
			{Name: "one", Ref: "r1", Status: "idle"},
		},
		// t3 is not in the intersection, so its entry must not appear.
		"t3": {{Name: "ghost", Ref: "r9", Status: "shell"}},
	})

	want := []Member{
		{Name: "one", Ref: "r1", Status: "busy"},
		{Name: "two", Ref: "r2", Status: "idle"},
	}
	if got := remoteMembers(ps); !reflect.DeepEqual(got, want) {
		t.Errorf("remoteMembers(ps) = %+v, want %+v", got, want)
	}
}
