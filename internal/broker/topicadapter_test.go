package broker

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic"
)

// nopTopicTransport is the smallest topic.Transport: it never delivers a frame
// and never reports a link up or down.
type nopTopicTransport struct{}

var _ topic.Transport = nopTopicTransport{}

func (nopTopicTransport) Send(link.Fingerprint, link.Frame) error { return nil }

func (nopTopicTransport) HandleState(link.FrameKind, func(link.Fingerprint, link.Frame)) {}

func (nopTopicTransport) Subscribe(func(link.Fingerprint, []byte), func(link.Fingerprint)) {}

// TestTopicNameRulesMatch checks that the Broker rejects exactly the names
// topic.ValidateName rejects, so a name the Broker accepts never reaches the
// registry as an internal error.
func TestTopicNameRulesMatch(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "namer")
	ctx := topicCallContext(t)
	direct := regCallerContext(uint32(os.Geteuid()))

	names := []struct {
		name string
		wire bool // false when proto3 refuses the name while the client marshals
	}{
		{"", true},
		{"a", true},
		{strings.Repeat("a", 256), true},
		{strings.Repeat("a", 257), true},
		{"a\x00", true},
		{"a\u0085", true},
		{"\xff", false},
		{"hello world 🚀", true},
	}
	for _, tc := range names {
		wantInvalid := topic.ValidateName(tc.name) != nil
		if !tc.wire {
			if !wantInvalid {
				t.Fatalf("%q must stay in the invalid set for the in-process path", tc.name)
			}
			_, err := tb.b.JoinTopic(direct, &localpb.JoinTopicRequest{AgentId: id, Name: tc.name})
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("JoinTopic(%q) in process = %v, want InvalidArgument", tc.name, err)
			}
			continue
		}

		_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: tc.name})
		if wantInvalid {
			code, wire, _ := topicErrorDetail(t, err)
			if code != codes.InvalidArgument || wire != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
				t.Fatalf("JoinTopic(%q) = %v/%v, want InvalidArgument/INVALID_ARGUMENT", tc.name, code, wire)
			}
		} else if err != nil {
			t.Fatalf("JoinTopic(%q) = %v, want success", tc.name, err)
		}

		_, err = tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: id, Name: tc.name})
		if wantInvalid {
			code, wire, _ := topicErrorDetail(t, err)
			if code != codes.InvalidArgument || wire != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
				t.Fatalf("LeaveTopic(%q) = %v/%v, want InvalidArgument/INVALID_ARGUMENT", tc.name, code, wire)
			}
		} else if err != nil {
			t.Fatalf("LeaveTopic(%q) = %v, want success", tc.name, err)
		}
	}
}

// TestTopicAdapterJoinLeave checks that the adapter forwards memberships,
// roster entries and errors between the Broker's types and the registry's.
func TestTopicAdapterJoinLeave(t *testing.T) {
	r := topic.New(nopTopicTransport{}, topic.Options{Self: link.Fingerprint{1}})
	table := NewTopicTable(r)

	table.AgentChanged("ag1", "worker", "", "ref1", "idle")
	table.AgentChanged("ag2", "front", "old", "ref2", "busy")
	for _, agent := range []AgentID{"ag1", "ag2"} {
		if err := table.Join(agent, "t1"); err != nil {
			t.Fatalf("Join(%s, t1): %v", agent, err)
		}
	}
	if got, want := table.TopicsOf("ag1"), r.TopicsOf(topic.AgentID("ag1")); !slices.Equal(got, want) {
		t.Fatalf("TopicsOf(ag1) = %v, want %v", got, want)
	}
	if got, want := table.TopicsOf("ag1"), []string{"t1"}; !slices.Equal(got, want) {
		t.Fatalf("TopicsOf(ag1) = %v, want %v", got, want)
	}

	if err := table.Join("ag1", ""); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("Join with an empty name = %v, want ErrInvalidTopicName", err)
	}
	if err := table.Join("ghost", "t1"); !errors.Is(err, ErrUnknownAgent) {
		t.Fatalf("Join for an unknown agent = %v, want ErrUnknownAgent", err)
	}

	shared, ok := table.Visible("ag1", VisibleAgent{Local: "ag2"})
	if !ok || !slices.Equal(shared, []string{"t1"}) {
		t.Fatalf("Visible(ag1, ag2) = %v/%v, want [t1]/true", shared, ok)
	}
	if _, ok := table.Visible("ag1", VisibleAgent{Peer: [32]byte{1}, Member: Member{Ref: "ref2"}}); ok {
		t.Fatal("Visible against a peer with no link state reported a match")
	}
	if got := table.RemoteMembers([32]byte{1}); len(got) != 0 {
		t.Fatalf("RemoteMembers of an unconnected peer = %v, want none", got)
	}

	visible := table.VisibleAgents("ag1")
	if len(visible) != 1 {
		t.Fatalf("VisibleAgents(ag1) = %v, want one entry", visible)
	}
	got := visible[0]
	if got.Local != "ag2" || got.Peer != ([32]byte{}) {
		t.Fatalf("VisibleAgents(ag1)[0] = Local %q Peer %x, want ag2 and no peer", got.Local, got.Peer)
	}
	want := Member{Name: "front", Ref: "ref2", Status: "busy", PrevName: "old"}
	if got.Member != want {
		t.Fatalf("VisibleAgents(ag1)[0].Member = %+v, want %+v", got.Member, want)
	}
	if !slices.Equal(got.Shared, []string{"t1"}) {
		t.Fatalf("VisibleAgents(ag1)[0].Shared = %v, want [t1]", got.Shared)
	}

	if err := table.Leave("ag1", "t1"); err != nil {
		t.Fatalf("Leave(ag1, t1): %v", err)
	}
	if got := table.TopicsOf("ag1"); len(got) != 0 {
		t.Fatalf("TopicsOf(ag1) after Leave = %v, want none", got)
	}
	table.LeaveAll("ag2")
	if err := table.Join("ag2", "t1"); !errors.Is(err, ErrUnknownAgent) {
		t.Fatalf("Join after LeaveAll = %v, want ErrUnknownAgent", err)
	}
}
