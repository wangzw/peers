package topic

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/topic/topicpb"
)

func TestStatusMapping(t *testing.T) {
	for _, s := range []string{"idle", "busy", "waiting", "shell"} {
		if got := statusFromWire(statusToWire(s)); got != s {
			t.Errorf("statusFromWire(statusToWire(%q)) = %q, want %q", s, got, s)
		}
	}

	for _, s := range []string{"Idle", "", "sleeping"} {
		if got := statusToWire(s); got != topicpb.AgentStatus_AGENT_STATUS_UNSPECIFIED {
			t.Errorf("statusToWire(%q) = %v, want AGENT_STATUS_UNSPECIFIED", s, got)
		}
	}

	if got := statusFromWire(topicpb.AgentStatus_AGENT_STATUS_UNSPECIFIED); got != "" {
		t.Errorf("statusFromWire(UNSPECIFIED) = %q, want the empty string", got)
	}
	if got := statusFromWire(topicpb.AgentStatus(99)); got != "" {
		t.Errorf("statusFromWire(99) = %q, want the empty string", got)
	}
}

// TestMemberNameLengthLimit checks that a name or previous name over
// MaxMemberNameBytes bytes is rejected while one at the limit is kept.
func TestMemberNameLengthLimit(t *testing.T) {
	at := strings.Repeat("a", MaxMemberNameBytes)
	over := strings.Repeat("a", MaxMemberNameBytes+1)
	if !validMemberEntry(Member{Name: at, Ref: "abcdef"}) {
		t.Errorf("a %d-byte name was rejected", MaxMemberNameBytes)
	}
	if validMemberEntry(Member{Name: over, Ref: "abcdef"}) {
		t.Errorf("a %d-byte name was kept", MaxMemberNameBytes+1)
	}
	if validMemberEntry(Member{Name: "ok", Ref: "abcdef", PrevName: over}) {
		t.Errorf("a %d-byte previous name was kept", MaxMemberNameBytes+1)
	}
}

func TestMemberRoundTrip(t *testing.T) {
	members := []Member{
		{Name: "api-worker", Ref: "abc123", Status: "busy", PrevName: "api-v1", Account: "bob", RefRequired: true},
		{Name: "frontend", Ref: "def456", Status: "idle"},
	}
	for _, m := range members {
		if got := memberFromWire(memberToWire(m)); got != m {
			t.Errorf("memberFromWire(memberToWire(%+v)) = %+v, want %+v", m, got, m)
		}
	}
}

func TestMessagesAreSorted(t *testing.T) {
	mk := func(b byte) [32]byte {
		var d [32]byte
		d[len(d)-1] = b
		return d
	}
	same := func(a, b []byte) bool { return bytes.Equal(a, b) }

	t.Run("digests", func(t *testing.T) {
		d1, d2, d3 := mk(1), mk(2), mk(3)
		in := [][32]byte{d3, d1, d2}
		msg := digestsMsg(in)
		want := [][32]byte{d1, d2, d3}
		if len(msg.Digests) != len(want) {
			t.Fatalf("got %d digests, want %d", len(msg.Digests), len(want))
		}
		for i := range want {
			if !same(msg.Digests[i], want[i][:]) {
				t.Errorf("digest %d = %x, want %x", i, msg.Digests[i], want[i])
			}
		}
		in[0][len(in[0])-1] = 9 // the message must not alias the input
		if !same(msg.Digests[2], d3[:]) {
			t.Errorf("digestsMsg aliases its input: %x", msg.Digests[2])
		}
	})

	t.Run("roster", func(t *testing.T) {
		ms := []Member{
			{Name: "c", Ref: "cc"},
			{Name: "a", Ref: "aa"},
			{Name: "b", Ref: "bb"},
		}
		msg := rosterMsg(mk(1), ms)
		want := []string{"aa", "bb", "cc"}
		if len(msg.Members) != len(want) {
			t.Fatalf("got %d members, want %d", len(msg.Members), len(want))
		}
		for i, ref := range want {
			if msg.Members[i].Ref != ref {
				t.Errorf("member %d has Ref %q, want %q", i, msg.Members[i].Ref, ref)
			}
		}
	})

	t.Run("delta", func(t *testing.T) {
		msg := deltaMsg(mk(1),
			[]Member{{Name: "b", Ref: "bb"}, {Name: "a", Ref: "aa"}},
			[]memberKey{{Name: "z", Ref: "zz"}, {Name: "y", Ref: "yy"}},
			[]Member{{Name: "q", Ref: "qq"}, {Name: "p", Ref: "pp"}},
		)
		if len(msg.Added) != 2 || len(msg.Removed) != 2 || len(msg.Changed) != 2 {
			t.Fatalf("got Added=%d Removed=%d Changed=%d, want 2 each", len(msg.Added), len(msg.Removed), len(msg.Changed))
		}
		if got := []string{msg.Added[0].Ref, msg.Added[1].Ref}; got[0] != "aa" || got[1] != "bb" {
			t.Errorf("Added Refs = %v, want [aa bb]", got)
		}
		if got := []string{msg.Removed[0].Ref, msg.Removed[1].Ref}; got[0] != "yy" || got[1] != "zz" {
			t.Errorf("Removed Refs = %v, want [yy zz]", got)
		}
		if got := []string{msg.Changed[0].Ref, msg.Changed[1].Ref}; got[0] != "pp" || got[1] != "qq" {
			t.Errorf("Changed Refs = %v, want [pp qq]", got)
		}
	})
}

func TestParseDigests(t *testing.T) {
	var d [32]byte
	d[len(d)-1] = 7

	set, err := parseDigests(&topicpb.TopicDigests{Digests: [][]byte{d[:], d[:]}})
	if err != nil {
		t.Fatalf("parseDigests with valid digests: %v", err)
	}
	if len(set) != 1 {
		t.Errorf("got a set of %d digests, want 1", len(set))
	}
	if _, ok := set[d]; !ok {
		t.Errorf("the set does not contain the digest")
	}

	for _, n := range []int{31, 33} {
		set, err := parseDigests(&topicpb.TopicDigests{Digests: [][]byte{make([]byte, n)}})
		if !errors.Is(err, errBadDigest) {
			t.Errorf("parseDigests with a %d-byte digest: err = %v, want errBadDigest", n, err)
		}
		if set != nil {
			t.Errorf("parseDigests with a %d-byte digest returned a partial set", n)
		}
	}

	empty, err := parseDigests(&topicpb.TopicDigests{})
	if err != nil {
		t.Fatalf("parseDigests on an empty list: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("parseDigests on an empty list = %v, want an allocated empty set", empty)
	}
}
