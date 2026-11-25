package topic

import (
	"errors"
	"fmt"
	"testing"
)

func newLocalRegistry(t *testing.T) (*Registry, AgentID) {
	t.Helper()
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"})
	return r, "a"
}

// TestJoinTopicCountLimit checks that creating one topic past the local limit
// fails with ErrTopicLimit and leaves the table unchanged.
func TestJoinTopicCountLimit(t *testing.T) {
	r, id := newLocalRegistry(t)

	for i := 0; i < maxLocalTopics; i++ {
		if err := r.Join(id, fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("Join topic %d: %v", i, err)
		}
	}
	if err := r.Join(id, "one-too-many"); !errors.Is(err, ErrTopicLimit) {
		t.Fatalf("Join past the topic limit = %v, want ErrTopicLimit", err)
	}
	if got := len(r.TopicsOf(id)); got != maxLocalTopics {
		t.Fatalf("TopicsOf has %d topics, want %d", got, maxLocalTopics)
	}

	if err := r.Join(id, "t0"); err != nil {
		t.Fatalf("rejoin at the topic limit: %v", err)
	}
}

// TestJoinMemberLimit checks that adding one member past the local limit of a
// topic fails with ErrTopicLimit while a member already in it rejoins.
func TestJoinMemberLimit(t *testing.T) {
	r, _ := newLocalRegistry(t)

	for i := 0; i < maxInboundMembers; i++ {
		id := AgentID(fmt.Sprintf("agent-%05d", i))
		r.AgentChanged(id, Member{Name: fmt.Sprintf("m%d", i), Ref: fmt.Sprintf("%06x", i), Status: "idle"})
		if err := r.Join(id, "t1"); err != nil {
			t.Fatalf("Join member %d: %v", i, err)
		}
	}

	extra := AgentID("agent-extra")
	r.AgentChanged(extra, Member{Name: "extra", Ref: "fffffe", Status: "idle"})
	if err := r.Join(extra, "t1"); !errors.Is(err, ErrTopicLimit) {
		t.Fatalf("Join past the member limit = %v, want ErrTopicLimit", err)
	}
	if err := r.Join("agent-00000", "t1"); err != nil {
		t.Fatalf("rejoin a member at the limit: %v", err)
	}

	if got := r.TopicsOf(extra); len(got) != 0 {
		t.Fatalf("refused agent has topics %v, want none", got)
	}
}
