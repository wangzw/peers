package topic

import (
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// TestOutgoingFramesUseLatestKeys checks the replacement key each topic frame
// uses: TopicDigests through SendLatest keyed by kind, and Roster and
// RosterDelta through SendLatestKey keyed by digest so a newer frame for the
// same digest replaces an older one still queued.
func TestOutgoingFramesUseLatestKeys(t *testing.T) {
	p := fakeLinkFingerprint(2)
	salt := []byte("salt")
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, _ := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")
	seedLocal(t, r, "b", Member{Name: "bee", Ref: "bbbbbb"})

	fl.up(p, salt)
	flushAll(t, fl)

	d1 := digest(salt, p, "t1")
	fl.inject(p, topicpb.KindTopicDigests, digestsMsg([][32]byte{d1}))
	flushAll(t, fl)
	fl.clearSent(p)

	// A new topic sends TopicDigests; joining an existing shared topic sends a
	// RosterDelta.
	if err := r.Join("a", "t2"); err != nil {
		t.Fatalf("Join t2: %v", err)
	}
	if err := r.Join("b", "t1"); err != nil {
		t.Fatalf("Join b to t1: %v", err)
	}
	flushAll(t, fl)

	frames := fl.sentFrames(p)
	var sawDigests, sawDelta bool
	for _, f := range frames {
		switch f.Kind {
		case topicpb.KindTopicDigests:
			sawDigests = true
			if !f.Latest || !strings.HasPrefix(f.Key, "k:") {
				t.Fatalf("TopicDigests left with key %q, want a kind key", f.Key)
			}
		case topicpb.KindRoster, topicpb.KindRosterDelta:
			if !f.Latest || !strings.HasPrefix(f.Key, "d:") {
				t.Fatalf("kind %d left with key %q, want a digest key", f.Kind, f.Key)
			}
			sawDelta = sawDelta || f.Kind == topicpb.KindRosterDelta
		}
	}
	if !sawDigests || !sawDelta {
		t.Fatalf("frames = %v, want both a TopicDigests and a RosterDelta", frames)
	}
}

// TestSendTooLargeLogsWarn checks that ErrFrameTooLarge is logged at Warn with
// its own event instead of the debug send-failure line.
func TestSendTooLargeLogsWarn(t *testing.T) {
	p := fakeLinkFingerprint(2)
	fl := newFakeLink(t, fakeLinkFingerprint(1))
	r, buf := newLinkRegistry(t, fl)
	seedLocal(t, r, "a", Member{Name: "api", Ref: "aaaaaa"}, "t1")

	fl.setSendError(link.FrameKind(topicpb.KindTopicDigests), link.ErrFrameTooLarge)
	fl.up(p, []byte("salt"))
	flushAll(t, fl)

	logs := buf.String()
	if !strings.Contains(logs, `"level":"WARN"`) || !strings.Contains(logs, "topic.frame_too_large") {
		t.Fatalf("log has no Warn topic.frame_too_large: %s", logs)
	}
	if !strings.Contains(logs, `"kind":200`) {
		t.Fatalf("log does not carry the frame kind: %s", logs)
	}
}
