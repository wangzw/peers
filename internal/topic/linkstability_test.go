package topic

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// stableTestCap bounds the join loop. A build without the local topic limit
// joins this many topics instead of looping forever, so the test still ends.
const stableTestCap = 4097

// stablePeer builds a registry whose first agent is in as many local topics as
// Join allows (up to stableTestCap), brings one fake peer up, and tells the peer
// to share every one of those topics. It returns the fake link, the registry,
// the peer fingerprint and the topic count.
func stablePeer(t *testing.T, agents int) (*fakeLink, *Registry, link.Fingerprint, int) {
	t.Helper()
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)

	r.AgentChanged("a0", "agent0", "", "a00001", "idle")
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
		r.AgentChanged(id, fmt.Sprintf("agent%d", i), "", fmt.Sprintf("a%05d", i), "idle")
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

// TestLinkStaysUpSharingTopicsToLimit shares topics up to the local limit and
// checks the link is established and stays so. The local topic count must stay
// within the 512-topic budget that keeps the TopicDigests frame inside what a
// peer accepts and one change from fanning out past the send queue.
func TestLinkStaysUpSharingTopicsToLimit(t *testing.T) {
	fl, _, p, n := stablePeer(t, 1)

	if n > 512 {
		t.Fatalf("the Broker kept %d local topics, want at most 512", n)
	}
	if !fl.isUp(p) {
		t.Fatalf("the link went down with %d topics shared", n)
	}
}

// TestLinkStaysUpWhenAgentAtLimitRenames renames an agent that is in the maximum
// number of topics several times while the peer has stopped reading. Each rename
// produces one frame per topic; the queue must fold the repeat for a digest, so
// the link must stay up instead of going down with a full send queue. The number
// of renames is chosen so that, without the fold, the frames would exceed the
// send queue even though a single rename alone fits under it.
func TestLinkStaysUpWhenAgentAtLimitRenames(t *testing.T) {
	fl, r, p, n := stablePeer(t, 1)

	fl.stall(p)
	prev := "agent0"
	for i := 0; i <= fakeSendQueueLimit/n+1; i++ {
		name := fmt.Sprintf("renamed-%d", i)
		r.AgentChanged("a0", name, prev, "a00001", "busy")
		prev = name
	}
	up := fl.resume(p)

	if !up {
		t.Fatalf("the link went down after renaming an agent in %d topics", n)
	}
}

// TestLinkStaysUpUnderConcurrentStatusChanges changes several agents' status at
// once while the peer has stopped reading. Every agent is in the same maximum
// set of topics, so each change would enqueue one frame per topic; the queue
// must keep one frame per digest across them, and the link must stay up.
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
			r.AgentChanged(id, fmt.Sprintf("agent%d", i), "", fmt.Sprintf("a%05d", i), statuses[i])
		}(i)
	}
	wg.Wait()
	up := fl.resume(p)

	if !up {
		t.Fatalf("the link went down while %d agents changed status across %d topics", agents, n)
	}
}
