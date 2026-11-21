package topic

import (
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/topic/topicpb"
)

// TestEchoedDigestsRevealNothing reproduces the audit finding behind
// REQ-topics-014: a peer that sends a Broker's own TopicDigests back verbatim
// must not enter the intersection. Before digests bound the sender's
// fingerprint the echo matched every local topic, so the Broker sent a Roster
// for each topic and accepted a forged Roster under the same digest.
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

	// The peer echoes the digests it received as its own set.
	fl.inject(peer, topicpb.KindTopicDigests, echo)
	flushAll(t, fl)

	if got := fl.sentFrames(peer); len(got) != 0 {
		t.Fatalf("echoed digests produced %d frames, want none; a Roster reveals the topic", len(got))
	}

	// A forged Roster under an echoed digest must stay invisible.
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
