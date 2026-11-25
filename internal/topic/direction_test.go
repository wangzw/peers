package topic

import (
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/topic/topicpb"
)

// TestEchoedDigestsRevealNothing checks that a peer echoing this Broker's own
// TopicDigests back enters no intersection and cannot forge a roster under them.
func TestEchoedDigestsRevealNothing(t *testing.T) {
	self := fakeLinkFingerprint(1)
	peer := fakeLinkFingerprint(2)
	fl := newFakeLink(t, self)
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "secret-x", "shared-t")

	salt := []byte(strings.Repeat("s", 64))
	fl.up(peer, salt)
	flushAll(t, fl)

	frames := fl.sentFrames(peer)
	if len(frames) != 1 || frames[0].Kind != topicpb.KindTopicDigests {
		t.Fatalf("on link up: got %d frames, want one TopicDigests", len(frames))
	}
	echo := decodeDigests(t, frames[0])
	fl.clearSent(peer)

	fl.inject(peer, topicpb.KindTopicDigests, echo)
	flushAll(t, fl)

	if got := fl.sentFrames(peer); len(got) != 0 {
		t.Fatalf("echoed digests produced %d frames, want none; a Roster reveals the topic", len(got))
	}

	fl.inject(peer, topicpb.KindRoster, &topicpb.Roster{
		Digest:  echo.Digests[0],
		Members: []*topicpb.Member{{Name: "intruder", Ref: "bbbbbb"}},
	})
	flushAll(t, fl)
	if got := r.RemoteMembers(peer); len(got) != 0 {
		t.Fatalf("RemoteMembers = %+v, want none after the forged Roster", got)
	}
	for _, va := range r.VisibleAgents("a") {
		if va.Agent.Peer == peer {
			t.Fatalf("VisibleAgents includes a forged remote member: %+v", va)
		}
	}
}
