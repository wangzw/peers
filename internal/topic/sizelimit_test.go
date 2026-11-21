package topic

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// wireSize returns the encoded size of a frame as the link transports it.
func wireSize(t *testing.T, kind uint32, m proto.Message) int {
	t.Helper()
	f, err := encodeFrame(kind, m)
	if err != nil {
		t.Fatalf("encode kind %d: %v", kind, err)
	}
	return proto.Size(&linkpb.Frame{Kind: f.Kind, Payload: f.Payload})
}

// TestOutboundLimitsFitLinkMessage checks that the largest TopicDigests and
// Roster this Broker may build, with names and previous names at the limit,
// still fit in one link message. It guards against a future limit increase that
// silently makes every such frame too large to send.
func TestOutboundLimitsFitLinkMessage(t *testing.T) {
	digests := wireSize(t, topicpb.KindTopicDigests, digestsMsg(testDigestList(maxInboundDigests)))
	if digests >= link.MaxMessageSize {
		t.Fatalf("a full TopicDigests encodes to %d bytes, want below %d", digests, link.MaxMessageSize)
	}

	name := strings.Repeat("a", MaxMemberNameBytes)
	prev := strings.Repeat("b", MaxMemberNameBytes)
	members := make([]*topicpb.Member, maxInboundMembers)
	for i := range members {
		members[i] = &topicpb.Member{Name: name, Ref: fmt.Sprintf("%012x", i), PreviousName: prev}
	}
	roster := wireSize(t, topicpb.KindRoster, &topicpb.Roster{Digest: make([]byte, 32), Members: members})
	if roster >= link.MaxMessageSize {
		t.Fatalf("a full Roster encodes to %d bytes, want below %d", roster, link.MaxMessageSize)
	}
}
