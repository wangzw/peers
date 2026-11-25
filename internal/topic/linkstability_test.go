package topic

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// A build without the local topic limit joins this many topics instead of
// looping forever.
const stableTestCap = 4097

func stablePeer(t *testing.T, agents int) (*fakeLink, *Registry, link.Fingerprint, int) {
	t.Helper()
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)

	r.AgentChanged("a0", Member{Name: "agent0", Ref: "a00001", Status: "idle"})
	n := 0
	for ; n < stableTestCap; n++ {
		err := r.Join("a0", fmt.Sprintf("t%d", n))
		if errors.Is(err, ErrTopicLimit) {
			break
		}
		if err != nil {
			t.Fatalf("Join topic %d: %v", n, err)
		}
	}
	for i := 1; i < agents; i++ {
		id := AgentID(fmt.Sprintf("a%d", i))
		r.AgentChanged(id, Member{Name: fmt.Sprintf("agent%d", i), Ref: fmt.Sprintf("a%05d", i), Status: "idle"})
		for j := 0; j < n; j++ {
			if err := r.Join(id, fmt.Sprintf("t%d", j)); err != nil {
				t.Fatalf("Join agent %d into topic %d: %v", i, j, err)
			}
		}
	}

	fl.up(p, salt)
	flushAll(t, fl)
	digs := make([][32]byte, 0, n)
	for i := 0; i < n; i++ {
		digs = append(digs, digest(salt, p, fmt.Sprintf("t%d", i)))
	}
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg(digs))
	flushAll(t, fl)
	return fl, r, p, n
}

// TestLinkStaysUpSharingTopicsToLimit checks that sharing topics up to the
// 512-topic local limit leaves the link up.
func TestLinkStaysUpSharingTopicsToLimit(t *testing.T) {
	fl, _, p, n := stablePeer(t, 1)

	if n > 512 {
		t.Fatalf("the Broker kept %d local topics, want at most 512", n)
	}
	if !fl.isUp(p) {
		t.Fatalf("the link went down with %d topics shared", n)
	}
}

// TestLinkStaysUpWhenAgentAtLimitRenames checks that repeated renames of an agent
// in many topics fold per digest and keep the link up while the peer is not
// reading.
func TestLinkStaysUpWhenAgentAtLimitRenames(t *testing.T) {
	fl, r, p, n := stablePeer(t, 1)

	fl.stall(p)
	prev := "agent0"
	for i := 0; i <= fakeSendQueueLimit/n+1; i++ {
		name := fmt.Sprintf("renamed-%d", i)
		r.AgentChanged("a0", Member{Name: name, Ref: "a00001", Status: "busy", PrevName: prev})
		prev = name
	}
	up := fl.resume(p)

	if !up {
		t.Fatalf("the link went down after renaming an agent in %d topics", n)
	}
}

// TestLinkStaysUpUnderConcurrentStatusChanges checks that concurrent status
// changes across many topics keep the link up while the peer is not reading.
func TestLinkStaysUpUnderConcurrentStatusChanges(t *testing.T) {
	const agents = 4
	fl, r, p, n := stablePeer(t, agents)

	fl.stall(p)
	statuses := []string{"busy", "waiting", "idle", "shell"}
	var wg sync.WaitGroup
	for i := 0; i < agents; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := AgentID(fmt.Sprintf("a%d", i))
			r.AgentChanged(id, Member{Name: fmt.Sprintf("agent%d", i), Ref: fmt.Sprintf("a%05d", i), Status: statuses[i]})
		}(i)
	}
	wg.Wait()
	up := fl.resume(p)

	if !up {
		t.Fatalf("the link went down while %d agents changed status across %d topics", agents, n)
	}
}
