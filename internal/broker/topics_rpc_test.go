package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

func topicCallContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	t.Cleanup(cancel)
	return ctx
}

func registerTopicAgent(t *testing.T, tb *testBroker, name string) (string, *regStream) {
	t.Helper()
	fake := fakesession.New(t, fakesession.Options{Name: name})
	rs := startRegistration(t, tb, fake)
	return rs.complete(eventTimeout).GetAgentId(), rs
}

func topicErrorDetail(t *testing.T, err error) (codes.Code, localpb.ErrorCode, string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a gRPC status", err)
	}
	for _, d := range st.Details() {
		if detail, ok := d.(*localpb.ErrorDetail); ok {
			return st.Code(), detail.GetCode(), detail.GetMessage()
		}
	}
	return st.Code(), localpb.ErrorCode_ERROR_CODE_UNSPECIFIED, st.Message()
}

func setDirectoryPublic(t *testing.T, d *fakeDirectory, public bool) {
	t.Helper()
	d.mu.Lock()
	d.public = public
	d.mu.Unlock()
}

func setDirectoryKnown(t *testing.T, d *fakeDirectory, known []PublicTopic) {
	t.Helper()
	d.mu.Lock()
	d.known = append([]PublicTopic(nil), known...)
	d.mu.Unlock()
}

func directoryCallsOf(d *fakeDirectory, method string) []directoryCall {
	var out []directoryCall
	for _, c := range d.Calls() {
		if c.Method == method {
			out = append(out, c)
		}
	}
	return out
}

type topicStepLog struct {
	mu    sync.Mutex
	steps []string
}

func (l *topicStepLog) add(step string) {
	l.mu.Lock()
	l.steps = append(l.steps, step)
	l.mu.Unlock()
}

func (l *topicStepLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.steps...)
}

type recordingDirectory struct {
	PublicTopicDirectory
	log *topicStepLog
}

func (d *recordingDirectory) DeclarePublicTopic(name, description string) error {
	d.log.add("declare:" + name)
	return d.PublicTopicDirectory.DeclarePublicTopic(name, description)
}

func (d *recordingDirectory) RetirePublicTopic(name string) {
	d.log.add("retire:" + name)
	d.PublicTopicDirectory.RetirePublicTopic(name)
}

type recordingTopics struct {
	TopicTable
	log *topicStepLog
}

func (t *recordingTopics) Join(agent AgentID, name string) error {
	t.log.add("join:" + name)
	return t.TopicTable.Join(agent, name)
}

func (t *recordingTopics) Leave(agent AgentID, name string) error {
	t.log.add("leave:" + name)
	return t.TopicTable.Leave(agent, name)
}

// TestJoinTopicQuotaExceeded checks that a join refused by the topic table's
// limit maps to quota_exceeded with a complete sentence.
func TestJoinTopicQuotaExceeded(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "quota")
	tb.topics.mu.Lock()
	tb.topics.joinErrs["t1"] = ErrTopicQuotaExceeded
	tb.topics.mu.Unlock()

	_, err := tb.client.JoinTopic(topicCallContext(t), &localpb.JoinTopicRequest{AgentId: id, Name: "t1"})
	code, wire, msg := topicErrorDetail(t, err)
	if code != codes.ResourceExhausted || wire != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Fatalf("JoinTopic = %v/%v, want ResourceExhausted/QUOTA_EXCEEDED", code, wire)
	}
	if msg == "" || !strings.HasSuffix(msg, ".") {
		t.Fatalf("message %q is not a complete sentence", msg)
	}
}

// TestJoinAndListTopics checks that joining lists the joined topics in order and
// no public topics.
func TestJoinAndListTopics(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "joiner")
	ctx := topicCallContext(t)

	resp, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: "t2"})
	if err != nil {
		t.Fatalf("JoinTopic(t2): %v", err)
	}
	if got := resp.GetTopics(); !equalStrings(got, []string{"t2"}) {
		t.Fatalf("topics after joining t2 = %v, want [t2]", got)
	}

	resp, err = tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: "t1"})
	if err != nil {
		t.Fatalf("JoinTopic(t1): %v", err)
	}
	if got := resp.GetTopics(); !equalStrings(got, []string{"t1", "t2"}) {
		t.Fatalf("topics after joining t1 = %v, want [t1 t2]", got)
	}

	list, err := tb.client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: id})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if got := list.GetJoinedTopics(); !equalStrings(got, []string{"t1", "t2"}) {
		t.Fatalf("joined_topics = %v, want [t1 t2]", got)
	}
	if n := len(list.GetPublicTopics()); n != 0 {
		t.Fatalf("public_topics = %v, want none", list.GetPublicTopics())
	}
}

// TestJoinFreshTopicNeedsNoApproval checks that an agent joining a fresh topic
// sees another agent that joined it without approval.
func TestJoinFreshTopicNeedsNoApproval(t *testing.T) {
	tb := newTestBroker(t)
	idA, _ := registerTopicAgent(t, tb, "alpha")
	idB, _ := registerTopicAgent(t, tb, "beta")
	ctx := topicCallContext(t)

	for _, id := range []string{idA, idB} {
		if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: "fresh"}); err != nil {
			t.Fatalf("JoinTopic(fresh) for %s: %v", id, err)
		}
	}

	visible := tb.topics.VisibleAgents(AgentID(idA))
	if len(visible) != 1 || visible[0].Local != AgentID(idB) {
		t.Fatalf("VisibleAgents(%s) = %+v, want only %s", idA, visible, idB)
	}
	if got := visible[0].Shared; !equalStrings(got, []string{"fresh"}) {
		t.Fatalf("shared topics = %v, want [fresh]", got)
	}
}

// TestLeaveTopic checks that leaving a topic removes only that membership.
func TestLeaveTopic(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "leaver")
	ctx := topicCallContext(t)

	for _, name := range []string{"t1", "t2"} {
		if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: name}); err != nil {
			t.Fatalf("JoinTopic(%s): %v", name, err)
		}
	}

	resp, err := tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: id, Name: "t1"})
	if err != nil {
		t.Fatalf("LeaveTopic(t1): %v", err)
	}
	if got := resp.GetTopics(); !equalStrings(got, []string{"t2"}) {
		t.Fatalf("topics after leaving t1 = %v, want [t2]", got)
	}

	list, err := tb.client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: id})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if got := list.GetJoinedTopics(); !equalStrings(got, []string{"t2"}) {
		t.Fatalf("joined_topics = %v, want [t2]", got)
	}
}

// TestJoinInvalidName checks that invalid names are refused without reaching the
// topic table.
func TestJoinInvalidName(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "namer")
	ctx := topicCallContext(t)

	// Invalid UTF-8 cannot cross the wire; proto3 rejects it while the client
	// marshals.
	bad := []string{"", strings.Repeat("a", 257), "\x00"}
	for _, name := range bad {
		_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: name})
		code, wire, _ := topicErrorDetail(t, err)
		if code != codes.InvalidArgument || wire != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
			t.Fatalf("JoinTopic(%q) = %v/%v, want InvalidArgument/INVALID_ARGUMENT", name, code, wire)
		}

		_, err = tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: id, Name: name})
		code, wire, _ = topicErrorDetail(t, err)
		if code != codes.InvalidArgument || wire != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
			t.Fatalf("LeaveTopic(%q) = %v/%v, want InvalidArgument/INVALID_ARGUMENT", name, code, wire)
		}
	}

	if n := len(tb.topics.JoinCalls()); n != 0 {
		t.Fatalf("Join calls for invalid names = %d, want 0", n)
	}
	if n := len(tb.topics.LeaveCalls()); n != 0 {
		t.Fatalf("Leave calls for invalid names = %d, want 0", n)
	}

	if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: strings.Repeat("b", 256)}); err != nil {
		t.Fatalf("JoinTopic(256-byte name): %v, want success", err)
	}
}

// TestTopicCallsRequireRegistration checks that topic calls with an unknown
// agent return not_registered.
func TestTopicCallsRequireRegistration(t *testing.T) {
	tb := newTestBroker(t)
	ctx := topicCallContext(t)
	unknown := string(testAgentID("unknown"))

	calls := []func() error{
		func() error {
			_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: unknown, Name: "t1"})
			return err
		},
		func() error {
			_, err := tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: unknown, Name: "t1"})
			return err
		},
		func() error {
			_, err := tb.client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: unknown})
			return err
		},
	}
	for i, call := range calls {
		code, wire, _ := topicErrorDetail(t, call())
		if code != codes.FailedPrecondition || wire != localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED {
			t.Fatalf("call %d with unknown agent = %v/%v, want FailedPrecondition/NOT_REGISTERED", i, code, wire)
		}
	}

	id, rs := registerTopicAgent(t, tb, "temporary")
	rs.cancel()
	waitForReg(t, "agent removal", func() bool { return tb.lockedAgentCount() == 0 })

	client := tb.newClient(t)
	_, err := client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: "t1"})
	code, wire, _ := topicErrorDetail(t, err)
	if code != codes.FailedPrecondition || wire != localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED {
		t.Fatalf("JoinTopic after disconnect = %v/%v, want FailedPrecondition/NOT_REGISTERED", code, wire)
	}
}

// TestJoinPublicDeclaresFirst checks that a public join declares the topic
// before joining it.
func TestJoinPublicDeclaresFirst(t *testing.T) {
	steps := &topicStepLog{}
	tb := newTestBroker(t, func(d *Deps) {
		d.Directory = &recordingDirectory{PublicTopicDirectory: d.Directory, log: steps}
		d.Topics = &recordingTopics{TopicTable: d.Topics, log: steps}
	})
	setDirectoryPublic(t, tb.dir, true)
	id, _ := registerTopicAgent(t, tb, "publisher")
	ctx := topicCallContext(t)

	if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
		AgentId: id, Name: "infra-help", IsPublic: true, Description: "help",
	}); err != nil {
		t.Fatalf("JoinTopic(infra-help, public): %v", err)
	}

	want := []string{"declare:infra-help", "join:infra-help"}
	if got := steps.snapshot(); !equalStrings(got, want) {
		t.Fatalf("steps = %v, want %v", got, want)
	}
}

// TestJoinPublicOnPrivateBrokerFails checks that a public join on a non-public
// Broker is refused and joins nothing.
func TestJoinPublicOnPrivateBrokerFails(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "private")
	ctx := topicCallContext(t)

	_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
		AgentId: id, Name: "infra-help", IsPublic: true, Description: "help",
	})
	code, wire, message := topicErrorDetail(t, err)
	if code != codes.InvalidArgument || wire != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Fatalf("public join on a private Broker = %v/%v, want InvalidArgument/INVALID_ARGUMENT", code, wire)
	}
	if !strings.Contains(message, "not public") {
		t.Fatalf("message = %q, want it to mention that the Broker is not public", message)
	}
	if n := len(tb.topics.JoinCalls()); n != 0 {
		t.Fatalf("Join calls = %d, want 0", n)
	}
	if n := len(tb.topics.TopicsOf(AgentID(id))); n != 0 {
		t.Fatalf("joined topics = %v, want none", tb.topics.TopicsOf(AgentID(id)))
	}
}

// TestJoinPublicWithNilDirectory checks that a public join without a directory is
// refused as a non-public Broker.
func TestJoinPublicWithNilDirectory(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) { d.Directory = nil })
	id, _ := registerTopicAgent(t, tb, "nodir")
	ctx := topicCallContext(t)

	_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
		AgentId: id, Name: "infra-help", IsPublic: true, Description: "help",
	})
	code, wire, message := topicErrorDetail(t, err)
	if code != codes.InvalidArgument || wire != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Fatalf("public join without a directory = %v/%v, want InvalidArgument/INVALID_ARGUMENT", code, wire)
	}
	if !strings.Contains(message, "not public") {
		t.Fatalf("message = %q, want it to mention that the Broker is not public", message)
	}

	list, err := tb.client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: id})
	if err != nil {
		t.Fatalf("ListTopics without a directory: %v", err)
	}
	if n := len(list.GetPublicTopics()); n != 0 {
		t.Fatalf("public_topics = %v, want none", list.GetPublicTopics())
	}
}

// TestRetireOnlyWhenLastPublicMemberLeaves checks that a public declaration is
// retired only when the last public member leaves or disconnects.
func TestRetireOnlyWhenLastPublicMemberLeaves(t *testing.T) {
	tb := newTestBroker(t)
	setDirectoryPublic(t, tb.dir, true)
	idA, _ := registerTopicAgent(t, tb, "alpha")
	idB, _ := registerTopicAgent(t, tb, "beta")
	ctx := topicCallContext(t)

	for _, id := range []string{idA, idB} {
		if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
			AgentId: id, Name: "infra-help", IsPublic: true, Description: "help",
		}); err != nil {
			t.Fatalf("public JoinTopic for %s: %v", id, err)
		}
	}

	tb.dir.ResetCalls()
	if _, err := tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: idA, Name: "infra-help"}); err != nil {
		t.Fatalf("LeaveTopic for %s: %v", idA, err)
	}
	if retired := directoryCallsOf(tb.dir, "retire"); len(retired) != 0 {
		t.Fatalf("retired after the first member left = %v, want none", retired)
	}

	if _, err := tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: idB, Name: "infra-help"}); err != nil {
		t.Fatalf("LeaveTopic for %s: %v", idB, err)
	}
	retired := directoryCallsOf(tb.dir, "retire")
	if len(retired) != 1 || retired[0].Name != "infra-help" {
		t.Fatalf("retire calls after the last member left = %v, want exactly infra-help", retired)
	}

	t.Run("last member disconnects", func(t *testing.T) {
		tb := newTestBroker(t)
		setDirectoryPublic(t, tb.dir, true)
		id, rs := registerTopicAgent(t, tb, "solo")
		ctx := topicCallContext(t)
		if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
			AgentId: id, Name: "infra-help", IsPublic: true, Description: "help",
		}); err != nil {
			t.Fatalf("public JoinTopic: %v", err)
		}

		tb.dir.ResetCalls()
		rs.cancel()
		waitForReg(t, "agent removal", func() bool { return tb.lockedAgentCount() == 0 })

		retired := directoryCallsOf(tb.dir, "retire")
		if len(retired) != 1 || retired[0].Name != "infra-help" {
			t.Fatalf("retire calls after the only member disconnected = %v, want exactly infra-help", retired)
		}
	})
}

// TestPrivateLeaveDoesNotRetire checks that a private member leaving does not
// retire a public declaration.
func TestPrivateLeaveDoesNotRetire(t *testing.T) {
	tb := newTestBroker(t)
	setDirectoryPublic(t, tb.dir, true)
	idA, _ := registerTopicAgent(t, tb, "alpha")
	idB, _ := registerTopicAgent(t, tb, "beta")
	ctx := topicCallContext(t)

	if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
		AgentId: idA, Name: "shared", IsPublic: true, Description: "shared topic",
	}); err != nil {
		t.Fatalf("public JoinTopic: %v", err)
	}
	if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: idB, Name: "shared"}); err != nil {
		t.Fatalf("private JoinTopic: %v", err)
	}

	tb.dir.ResetCalls()
	if _, err := tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: idB, Name: "shared"}); err != nil {
		t.Fatalf("private LeaveTopic: %v", err)
	}
	if retired := directoryCallsOf(tb.dir, "retire"); len(retired) != 0 {
		t.Fatalf("retired after a private member left = %v, want none", retired)
	}
}

// TestJoinFailureRollsBackDeclaration checks that a failed join after a public
// declaration retires the declaration.
func TestJoinFailureRollsBackDeclaration(t *testing.T) {
	tb := newTestBroker(t)
	setDirectoryPublic(t, tb.dir, true)
	tb.topics.FailJoin("boom", errors.New("boom"))
	id, _ := registerTopicAgent(t, tb, "rollback")
	ctx := topicCallContext(t)

	_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
		AgentId: id, Name: "boom", IsPublic: true, Description: "boom",
	})
	code, wire, _ := topicErrorDetail(t, err)
	if code != codes.Internal || wire != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Fatalf("public join after a failed Join = %v/%v, want Internal/INTERNAL", code, wire)
	}

	want := []directoryCall{
		{Method: "declare", Name: "boom", Description: "boom"},
		{Method: "retire", Name: "boom"},
	}
	got := tb.dir.Calls()
	if len(got) != len(want) {
		t.Fatalf("directory calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("directory calls = %v, want %v", got, want)
		}
	}
	if set := tb.b.publicTopicSet(); len(set) != 0 {
		t.Fatalf("publicTopicSet after the rollback = %v, want empty", set)
	}
}

// TestListTopicsSeparatesPublic checks that public topics list only those the
// caller has not joined.
func TestListTopicsSeparatesPublic(t *testing.T) {
	tb := newTestBroker(t)
	setDirectoryKnown(t, tb.dir, []PublicTopic{
		{Name: "b", Description: "db"},
		{Name: "a", Description: "da"},
	})
	id, _ := registerTopicAgent(t, tb, "lister")
	ctx := topicCallContext(t)

	if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: "a"}); err != nil {
		t.Fatalf("JoinTopic(a): %v", err)
	}

	list, err := tb.client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: id})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if got := list.GetJoinedTopics(); !equalStrings(got, []string{"a"}) {
		t.Fatalf("joined_topics = %v, want [a]", got)
	}
	public := list.GetPublicTopics()
	if len(public) != 1 || public[0].GetName() != "b" || public[0].GetDescription() != "db" {
		t.Fatalf("public_topics = %v, want only {b db}", public)
	}
}

// TestListTopicsReportsPublisher checks that a public topic carries the
// publisher fingerprint, alias and declarer count.
func TestListTopicsReportsPublisher(t *testing.T) {
	tb := newTestBroker(t)
	publisher := [32]byte{0xde, 0xad, 0xbe, 0xef}
	setDirectoryKnown(t, tb.dir, []PublicTopic{
		{Name: "a", Description: "da", Publisher: publisher, PublisherAlias: "alpha", Declarers: 3},
	})
	id, _ := registerTopicAgent(t, tb, "lister")
	list, err := tb.client.ListTopics(topicCallContext(t), &localpb.ListTopicsRequest{AgentId: id})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	public := list.GetPublicTopics()
	if len(public) != 1 {
		t.Fatalf("public_topics = %v, want one entry", public)
	}
	wantFP := "deadbeef" + strings.Repeat("00", 28)
	if got := public[0].GetPublisherFingerprint(); got != wantFP {
		t.Errorf("publisher_fingerprint = %q, want %q", got, wantFP)
	}
	if got := public[0].GetPublisherAlias(); got != "alpha" {
		t.Errorf("publisher_alias = %q, want alpha", got)
	}
	if got := public[0].GetDeclarers(); got != 3 {
		t.Errorf("declarers = %d, want 3", got)
	}
}

// TestPublicTopicSet checks that publicTopicSet holds one description per topic
// from the smaller agent ID and no private topics.
func TestPublicTopicSet(t *testing.T) {
	tb := newTestBroker(t)
	setDirectoryPublic(t, tb.dir, true)
	idA, _ := registerTopicAgent(t, tb, "alpha")
	idB, _ := registerTopicAgent(t, tb, "beta")
	ctx := topicCallContext(t)

	joins := []struct {
		id          string
		name        string
		isPublic    bool
		description string
	}{
		{idA, "x", true, "dx"},
		{idA, "y", true, "dy"},
		{idB, "x", true, "other"},
		{idB, "z", false, ""},
	}
	for _, j := range joins {
		if _, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{
			AgentId: j.id, Name: j.name, IsPublic: j.isPublic, Description: j.description,
		}); err != nil {
			t.Fatalf("JoinTopic(%s, public=%v): %v", j.name, j.isPublic, err)
		}
	}

	wantX := "dx"
	if idB < idA {
		wantX = "other"
	}
	set := tb.b.publicTopicSet()
	if len(set) != 2 {
		t.Fatalf("publicTopicSet = %v, want exactly x and y", set)
	}
	if set["x"] != wantX {
		t.Fatalf("publicTopicSet[x] = %q, want %q (the smaller agent ID)", set["x"], wantX)
	}
	if set["y"] != "dy" {
		t.Fatalf("publicTopicSet[y] = %q, want dy", set["y"])
	}
	if _, ok := set["z"]; ok {
		t.Fatalf("publicTopicSet contains the private topic z: %v", set)
	}

	if _, err := tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: idA, Name: "y"}); err != nil {
		t.Fatalf("LeaveTopic(y): %v", err)
	}
	if set := tb.b.publicTopicSet(); len(set) != 1 {
		t.Fatalf("publicTopicSet after leaving y = %v, want only x", set)
	}
}

// TestJoinUnknownAgentIsInternal checks that ErrUnknownAgent from the topic table
// maps to internal.
func TestJoinUnknownAgentIsInternal(t *testing.T) {
	tb := newTestBroker(t)
	tb.topics.FailJoin("t9", ErrUnknownAgent)
	id, _ := registerTopicAgent(t, tb, "stranger")
	ctx := topicCallContext(t)

	_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: "t9"})
	code, wire, _ := topicErrorDetail(t, err)
	if code != codes.Internal || wire != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Fatalf("JoinTopic after ErrUnknownAgent = %v/%v, want Internal/INTERNAL", code, wire)
	}
}

func joinWithRefill(t *testing.T, tb *testBroker, id, name string) error {
	t.Helper()
	tb.clock.Advance(time.Second)
	_, err := tb.client.JoinTopic(topicCallContext(t), &localpb.JoinTopicRequest{AgentId: id, Name: name})
	return err
}

// TestJoinTopicPerAccountLimit checks that a system Broker refuses a topic past
// the per-account limit and still lets the account rejoin one it already has.
func TestJoinTopicPerAccountLimit(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) { d.System = true })
	id, _ := registerTopicAgent(t, tb, "accountlimit")

	for i := 0; i < maxAccountTopics; i++ {
		if err := joinWithRefill(t, tb, id, fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("JoinTopic %d: %v", i, err)
		}
	}
	err := joinWithRefill(t, tb, id, "one-too-many")
	code, wire, message := topicErrorDetail(t, err)
	if code != codes.ResourceExhausted || wire != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Fatalf("JoinTopic past the account limit = %v/%v, want ResourceExhausted/QUOTA_EXCEEDED", code, wire)
	}
	if message == "" || !strings.HasSuffix(message, ".") {
		t.Fatalf("message %q is not a complete sentence", message)
	}

	if err := joinWithRefill(t, tb, id, "t0"); err != nil {
		t.Fatalf("rejoin an existing topic at the account limit: %v", err)
	}
}

// TestUserBrokerHasNoPerAccountLimit checks that a user Broker applies no
// per-account topic limit.
func TestUserBrokerHasNoPerAccountLimit(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "userlimit")

	for i := 0; i <= maxAccountTopics; i++ {
		if err := joinWithRefill(t, tb, id, fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("JoinTopic %d on a user Broker: %v", i, err)
		}
	}
}
