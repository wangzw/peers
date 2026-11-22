package mcpserver

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// topicRecorderStub is a topicRecorder that records the calls it receives.
type topicRecorderStub struct {
	remembered []rememberedTopic
	forgotten  []string
}

func (r *topicRecorderStub) Remember(t rememberedTopic)      { r.remembered = append(r.remembered, t) }
func (r *topicRecorderStub) Forget(name string)              { r.forgotten = append(r.forgotten, name) }
func (r *topicRecorderStub) lockTopic(context.Context) error { return nil }
func (r *topicRecorderStub) unlockTopic()                    {}

// newTopicsHandlers builds handlers whose status is PhaseConnected and whose
// client talks to a fresh fakelocal server. agentID may be empty to simulate a
// connected-but-not-registered session.
func newTopicsHandlers(t *testing.T, h fakelocal.Handlers, agentID string) (*handlers, *fakelocal.Server, *topicRecorderStub) {
	t.Helper()
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, h)

	cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing the fake Local service: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	rec := &topicRecorderStub{}
	st := &statusStore{}
	st.Update(func(s *Status) {
		s.Phase = PhaseConnected
		s.Client = localpb.NewLocalClient(cc)
		s.AgentID = agentID
		s.Address = "peer@hostA"
	})
	return &handlers{st: st, topics: rec}, srv, rec
}

// lastTopicCall returns the only unary request the fake server received, failing the
// test when there is not exactly one.
func lastTopicCall(t *testing.T, srv *fakelocal.Server) fakelocal.Call {
	t.Helper()
	calls := srv.Calls()
	if len(calls) != 1 {
		t.Fatalf("fake server received %d calls, want 1: %v", len(calls), calls)
	}
	return calls[0]
}

func TestJoinTopic(t *testing.T) {
	t.Run("forwards the request and remembers the topic", func(t *testing.T) {
		// REQ-topics-001, REQ-topics-010: join_topic forwards the name, the
		// public declaration and the description.
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{
			JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
				return &localpb.JoinTopicResponse{Topics: []string{"t", "u"}}, nil
			},
		}, "agent-1")

		out, err := h.JoinTopic(context.Background(), JoinTopicIn{Name: "t", Public: true, Description: "d"})
		if err != nil {
			t.Fatalf("JoinTopic: %v", err)
		}
		if want := []string{"t", "u"}; !reflect.DeepEqual(out.Topics, want) {
			t.Errorf("Topics = %v, want %v", out.Topics, want)
		}

		call := lastTopicCall(t, srv)
		if call.Method != "JoinTopic" {
			t.Fatalf("method = %q, want JoinTopic", call.Method)
		}
		req, ok := call.Request.(*localpb.JoinTopicRequest)
		if !ok {
			t.Fatalf("request type %T, want *localpb.JoinTopicRequest", call.Request)
		}
		if req.GetAgentId() != "agent-1" {
			t.Errorf("agent_id = %q, want agent-1", req.GetAgentId())
		}
		if req.GetName() != "t" {
			t.Errorf("name = %q, want t", req.GetName())
		}
		if !req.GetIsPublic() {
			t.Error("is_public = false, want true")
		}
		if req.GetDescription() != "d" {
			t.Errorf("description = %q, want d", req.GetDescription())
		}

		want := []rememberedTopic{{Name: "t", Public: true, Description: "d"}}
		if !reflect.DeepEqual(rec.remembered, want) {
			t.Errorf("remembered = %+v, want %+v", rec.remembered, want)
		}
	})

	t.Run("a failed public join is not remembered", func(t *testing.T) {
		// REQ-topics-023: a public declaration on a non-public Broker must fail
		// without being remembered, or the next registration would join it
		// silently.
		h, _, rec := newTopicsHandlers(t, fakelocal.Handlers{
			JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.InvalidArgument, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
					Message: "this Broker is not public; make it public first",
				})
			},
		}, "agent-1")

		_, err := h.JoinTopic(context.Background(), JoinTopicIn{Name: "t", Public: true, Description: "d"})
		if err == nil {
			t.Fatal("JoinTopic returned no error, want one")
		}
		if !strings.HasPrefix(err.Error(), "join_topic failed:") {
			t.Errorf("error = %q, want the prefix %q", err, "join_topic failed:")
		}
		if !strings.Contains(err.Error(), "this Broker is not public; make it public first") {
			t.Errorf("error = %q, want the Broker's message verbatim", err)
		}
		if len(rec.remembered) != 0 {
			t.Errorf("remembered = %+v, want nothing", rec.remembered)
		}
	})

	t.Run("not registered", func(t *testing.T) {
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{}, "")

		_, err := h.JoinTopic(context.Background(), JoinTopicIn{Name: "t"})
		if err == nil {
			t.Fatal("JoinTopic returned no error, want not_registered")
		}
		if !strings.Contains(err.Error(), "(code: not_registered)") {
			t.Errorf("error = %q, want code not_registered", err)
		}
		if calls := srv.Calls(); len(calls) != 0 {
			t.Errorf("fake server received %d calls, want none: %v", len(calls), calls)
		}
		if len(rec.remembered) != 0 {
			t.Errorf("remembered = %+v, want nothing", rec.remembered)
		}
	})
}

func TestLeaveTopic(t *testing.T) {
	t.Run("forwards the request and forgets the topic", func(t *testing.T) {
		// REQ-topics-003: leave_topic forwards the name and forgets the topic.
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{
			LeaveTopic: func(context.Context, *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
				return &localpb.LeaveTopicResponse{Topics: []string{"u"}}, nil
			},
		}, "agent-1")

		out, err := h.LeaveTopic(context.Background(), LeaveTopicIn{Name: "t"})
		if err != nil {
			t.Fatalf("LeaveTopic: %v", err)
		}
		if want := []string{"u"}; !reflect.DeepEqual(out.Topics, want) {
			t.Errorf("Topics = %v, want %v", out.Topics, want)
		}

		call := lastTopicCall(t, srv)
		if call.Method != "LeaveTopic" {
			t.Fatalf("method = %q, want LeaveTopic", call.Method)
		}
		req, ok := call.Request.(*localpb.LeaveTopicRequest)
		if !ok {
			t.Fatalf("request type %T, want *localpb.LeaveTopicRequest", call.Request)
		}
		if req.GetAgentId() != "agent-1" {
			t.Errorf("agent_id = %q, want agent-1", req.GetAgentId())
		}
		if req.GetName() != "t" {
			t.Errorf("name = %q, want t", req.GetName())
		}

		if want := []string{"t"}; !reflect.DeepEqual(rec.forgotten, want) {
			t.Errorf("forgotten = %v, want %v", rec.forgotten, want)
		}
	})

	t.Run("a failed leave is not forgotten", func(t *testing.T) {
		h, _, rec := newTopicsHandlers(t, fakelocal.Handlers{
			LeaveTopic: func(context.Context, *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.NotFound, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
					Message: "no such topic",
				})
			},
		}, "agent-1")

		_, err := h.LeaveTopic(context.Background(), LeaveTopicIn{Name: "t"})
		if err == nil {
			t.Fatal("LeaveTopic returned no error, want one")
		}
		if !strings.HasPrefix(err.Error(), "leave_topic failed:") {
			t.Errorf("error = %q, want the prefix %q", err, "leave_topic failed:")
		}
		if len(rec.forgotten) != 0 {
			t.Errorf("forgotten = %v, want nothing", rec.forgotten)
		}
	})

	t.Run("not registered", func(t *testing.T) {
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{}, "")

		_, err := h.LeaveTopic(context.Background(), LeaveTopicIn{Name: "t"})
		if err == nil {
			t.Fatal("LeaveTopic returned no error, want not_registered")
		}
		if !strings.Contains(err.Error(), "(code: not_registered)") {
			t.Errorf("error = %q, want code not_registered", err)
		}
		if calls := srv.Calls(); len(calls) != 0 {
			t.Errorf("fake server received %d calls, want none: %v", len(calls), calls)
		}
		if len(rec.forgotten) != 0 {
			t.Errorf("forgotten = %v, want nothing", rec.forgotten)
		}
	})
}

// mcpserver design line 119: a tool call returns instead of queueing while a
// topic change is stuck. The second topic call stops when its context ends,
// and a registration restore is not left waiting for the same lock.
func TestLeaveTopicStopsWaitingForTheTopicLock(t *testing.T) {
	sock := fakelocal.SocketPath(t)

	joinEntered := make(chan struct{})
	var enteredOnce sync.Once
	releaseJoin := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseJoin) }) }
	// Unblock the stuck join when the test fails, so its goroutine can exit.
	t.Cleanup(release)

	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if req.GetName() == "x" {
				enteredOnce.Do(func() { close(joinEntered) })
				<-releaseJoin
			}
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})
	client := topicsTestClient(t, sock)

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
	}
	r.onComplete(context.Background(), client, "agent-1", true) // the first registration joins nothing
	mem.Remember(rememberedTopic{Name: "a"})
	h := topicsRaceHandlers(client, mem)

	// A join_topic holds the topic lock while the Broker does not answer it.
	joinDone := make(chan error, 1)
	go func() {
		_, err := h.JoinTopic(context.Background(), JoinTopicIn{Name: "x"})
		joinDone <- err
	}()
	<-joinEntered

	// The second topic call stops when its context ends instead of queueing
	// on the lock.
	ctx, cancel := context.WithCancel(context.Background())
	leaveDone := make(chan error, 1)
	go func() {
		_, err := h.LeaveTopic(ctx, LeaveTopicIn{Name: "a"})
		leaveDone <- err
	}()
	cancel()
	select {
	case err := <-leaveDone:
		if err == nil {
			t.Fatal("leave_topic returned no error after its context ended")
		}
	case <-time.After(eventTimeout):
		t.Fatalf("leave_topic kept waiting for the topic lock after its context ended (%s)", eventTimeout)
	}
	// The join still holds the lock, so leave_topic must not have taken it. If
	// the join has already finished here, leave_topic ignored its context and
	// queued behind the lock until the Broker answered instead of ending on it.
	select {
	case err := <-joinDone:
		t.Fatalf("leave_topic took the topic lock while the join was still running (join_topic returned %v)", err)
	default:
	}

	// A registration that ends while it waits for the lock returns too, so a
	// reconnect is not dragged behind the stuck call.
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	restoreDone := make(chan struct{})
	go func() {
		defer close(restoreDone)
		r.onComplete(streamCtx, client, "agent-1", true)
	}()
	cancelStream()
	waitClosed(t, restoreDone, "the restore whose registration ended")

	// The Broker recovers and the next registration restores the memory.
	release()
	if err := waitErr(t, joinDone, "join_topic"); err != nil {
		t.Fatalf("join_topic: %v", err)
	}
	reconnectDone := make(chan struct{})
	go func() {
		defer close(reconnectDone)
		r.onComplete(context.Background(), client, "agent-1", true)
	}()
	waitClosed(t, reconnectDone, "the restore after the Broker recovered")
}

// defaultTopicSession returns a sessionTopics function that reports names.
func defaultTopicSession(names ...string) func(context.Context) []string {
	return func(context.Context) []string { return names }
}

func TestJoinDefaultTopics(t *testing.T) {
	t.Run("joins every default topic with the declaration and remembers each", func(t *testing.T) {
		// REQ-topics-026: a nameless join_topic joins each default topic and
		// applies public and description to every one of them.
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{
			JoinTopic: func(_ context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
				return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
			},
		}, "agent-1")
		h.sessionTopics = defaultTopicSession("a", "b")

		out, err := h.JoinTopic(context.Background(), JoinTopicIn{Public: true, Description: "d"})
		if err != nil {
			t.Fatalf("JoinTopic: %v", err)
		}
		// The reply is the one from the last join.
		if want := []string{"b"}; !reflect.DeepEqual(out.Topics, want) {
			t.Errorf("Topics = %v, want %v", out.Topics, want)
		}

		calls := srv.Calls()
		if len(calls) != 2 {
			t.Fatalf("fake server received %d calls, want 2: %v", len(calls), calls)
		}
		for i, name := range []string{"a", "b"} {
			req, ok := calls[i].Request.(*localpb.JoinTopicRequest)
			if !ok {
				t.Fatalf("call %d request type %T, want *localpb.JoinTopicRequest", i, calls[i].Request)
			}
			if req.GetName() != name {
				t.Errorf("call %d name = %q, want %q", i, req.GetName(), name)
			}
			if !req.GetIsPublic() || req.GetDescription() != "d" {
				t.Errorf("call %d is_public = %v, description = %q; want true, d",
					i, req.GetIsPublic(), req.GetDescription())
			}
			if req.GetAgentId() != "agent-1" {
				t.Errorf("call %d agent_id = %q, want agent-1", i, req.GetAgentId())
			}
		}
		want := []rememberedTopic{
			{Name: "a", Public: true, Description: "d"},
			{Name: "b", Public: true, Description: "d"},
		}
		if !reflect.DeepEqual(rec.remembered, want) {
			t.Errorf("remembered = %+v, want %+v", rec.remembered, want)
		}
	})

	t.Run("stops at the first failure and keeps what was joined", func(t *testing.T) {
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{
			JoinTopic: func(_ context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
				if req.GetName() == "b" {
					return nil, fakelocal.ErrorStatus(codes.InvalidArgument, &localpb.ErrorDetail{
						Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
						Message: "bad topic",
					})
				}
				return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
			},
		}, "agent-1")
		h.sessionTopics = defaultTopicSession("a", "b", "c")

		_, err := h.JoinTopic(context.Background(), JoinTopicIn{})
		if err == nil {
			t.Fatal("JoinTopic returned no error, want one")
		}
		if !strings.Contains(err.Error(), "bad topic") {
			t.Errorf("error = %q, want the Broker's message", err)
		}
		// The error names the topic that failed and the topics already joined.
		if !strings.Contains(err.Error(), `joined "a"`) {
			t.Errorf("error = %q, want the topics already joined", err)
		}
		if !strings.Contains(err.Error(), `joining "b" failed`) {
			t.Errorf("error = %q, want the failing topic b", err)
		}
		if calls := srv.Calls(); len(calls) != 2 {
			t.Errorf("fake server received %d calls, want 2 (stop at b)", len(calls))
		}
		if want := []rememberedTopic{{Name: "a"}}; !reflect.DeepEqual(rec.remembered, want) {
			t.Errorf("remembered = %+v, want %+v", rec.remembered, want)
		}
	})

	t.Run("no default topic", func(t *testing.T) {
		// REQ-topics-026: a session without a default topic fails the call.
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{}, "agent-1")
		h.sessionTopics = defaultTopicSession()

		_, err := h.JoinTopic(context.Background(), JoinTopicIn{})
		if err == nil {
			t.Fatal("JoinTopic returned no error, want invalid_argument")
		}
		if !strings.Contains(err.Error(), "(code: invalid_argument)") {
			t.Errorf("error = %q, want code invalid_argument", err)
		}
		if !strings.Contains(err.Error(), "default topic") {
			t.Errorf("error = %q, want the default-topic explanation", err)
		}
		if calls := srv.Calls(); len(calls) != 0 {
			t.Errorf("fake server received %d calls, want none: %v", len(calls), calls)
		}
		if len(rec.remembered) != 0 {
			t.Errorf("remembered = %+v, want nothing", rec.remembered)
		}
	})

	t.Run("preflight runs before the default-topic check", func(t *testing.T) {
		h, srv, _ := newTopicsHandlers(t, fakelocal.Handlers{}, "")
		h.sessionTopics = defaultTopicSession()

		_, err := h.JoinTopic(context.Background(), JoinTopicIn{})
		if err == nil {
			t.Fatal("JoinTopic returned no error, want not_registered")
		}
		if !strings.Contains(err.Error(), "(code: not_registered)") {
			t.Errorf("error = %q, want code not_registered", err)
		}
		if calls := srv.Calls(); len(calls) != 0 {
			t.Errorf("fake server received %d calls, want none: %v", len(calls), calls)
		}
	})

	t.Run("a whitespace-only name is passed to the Broker", func(t *testing.T) {
		h, srv, _ := newTopicsHandlers(t, fakelocal.Handlers{
			JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
				return &localpb.JoinTopicResponse{Topics: []string{"x"}}, nil
			},
		}, "agent-1")
		h.sessionTopics = defaultTopicSession("a")

		if _, err := h.JoinTopic(context.Background(), JoinTopicIn{Name: " "}); err != nil {
			t.Fatalf("JoinTopic: %v", err)
		}
		calls := srv.Calls()
		if len(calls) != 1 {
			t.Fatalf("fake server received %d calls, want 1", len(calls))
		}
		req := calls[0].Request.(*localpb.JoinTopicRequest)
		if req.GetName() != " " {
			t.Errorf("name = %q, want a single space", req.GetName())
		}
	})
}

func TestLeaveDefaultTopics(t *testing.T) {
	t.Run("leaves the joined defaults and forgets every default", func(t *testing.T) {
		// REQ-topics-027: every default topic leaves the memory, joined or not,
		// so a later registration does not restore it.
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{
			ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
				return &localpb.ListTopicsResponse{JoinedTopics: []string{"a", "x"}}, nil
			},
			LeaveTopic: func(_ context.Context, req *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
				return &localpb.LeaveTopicResponse{Topics: []string{"x"}}, nil
			},
		}, "agent-1")
		h.sessionTopics = defaultTopicSession("a", "b")

		out, err := h.LeaveTopic(context.Background(), LeaveTopicIn{})
		if err != nil {
			t.Fatalf("LeaveTopic: %v", err)
		}
		if want := []string{"x"}; !reflect.DeepEqual(out.Topics, want) {
			t.Errorf("Topics = %v, want %v", out.Topics, want)
		}

		calls := srv.Calls()
		if len(calls) != 2 {
			t.Fatalf("fake server received %d calls, want ListTopics then LeaveTopic: %v", len(calls), calls)
		}
		if calls[0].Method != "ListTopics" || calls[1].Method != "LeaveTopic" {
			t.Fatalf("call methods = %q, %q; want ListTopics, LeaveTopic", calls[0].Method, calls[1].Method)
		}
		req := calls[1].Request.(*localpb.LeaveTopicRequest)
		if req.GetName() != "a" {
			t.Errorf("left name = %q, want a", req.GetName())
		}
		if want := []string{"a", "b"}; !reflect.DeepEqual(rec.forgotten, want) {
			t.Errorf("forgotten = %v, want %v", rec.forgotten, want)
		}
	})

	t.Run("a default topic the session has not joined is only forgotten", func(t *testing.T) {
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{
			ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
				return &localpb.ListTopicsResponse{JoinedTopics: []string{"x"}}, nil
			},
		}, "agent-1")
		h.sessionTopics = defaultTopicSession("a", "b")

		out, err := h.LeaveTopic(context.Background(), LeaveTopicIn{})
		if err != nil {
			t.Fatalf("LeaveTopic: %v", err)
		}
		// Nothing had to be left, so the listing is returned.
		if want := []string{"x"}; !reflect.DeepEqual(out.Topics, want) {
			t.Errorf("Topics = %v, want %v", out.Topics, want)
		}
		if calls := srv.Calls(); len(calls) != 1 || calls[0].Method != "ListTopics" {
			t.Errorf("calls = %v, want only ListTopics", calls)
		}
		if want := []string{"a", "b"}; !reflect.DeepEqual(rec.forgotten, want) {
			t.Errorf("forgotten = %v, want %v", rec.forgotten, want)
		}
	})

	t.Run("a failed leave stops and forgets only what was left", func(t *testing.T) {
		h, _, rec := newTopicsHandlers(t, fakelocal.Handlers{
			ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
				return &localpb.ListTopicsResponse{JoinedTopics: []string{"a", "b"}}, nil
			},
			LeaveTopic: func(_ context.Context, req *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
				if req.GetName() == "b" {
					return nil, fakelocal.ErrorStatus(codes.NotFound, &localpb.ErrorDetail{
						Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
						Message: "no such topic",
					})
				}
				return &localpb.LeaveTopicResponse{Topics: []string{"b"}}, nil
			},
		}, "agent-1")
		h.sessionTopics = defaultTopicSession("a", "b")

		_, err := h.LeaveTopic(context.Background(), LeaveTopicIn{})
		if err == nil {
			t.Fatal("LeaveTopic returned no error, want one")
		}
		if !strings.Contains(err.Error(), "no such topic") {
			t.Errorf("error = %q, want the Broker's message", err)
		}
		// The error names the topic that failed and the topics already left.
		if !strings.Contains(err.Error(), `left "a"`) {
			t.Errorf("error = %q, want the topics already left", err)
		}
		if !strings.Contains(err.Error(), `leaving "b" failed`) {
			t.Errorf("error = %q, want the failing topic b", err)
		}
		if want := []string{"a"}; !reflect.DeepEqual(rec.forgotten, want) {
			t.Errorf("forgotten = %v, want %v", rec.forgotten, want)
		}
	})

	t.Run("no default topic", func(t *testing.T) {
		h, srv, rec := newTopicsHandlers(t, fakelocal.Handlers{}, "agent-1")
		h.sessionTopics = defaultTopicSession()

		_, err := h.LeaveTopic(context.Background(), LeaveTopicIn{})
		if err == nil {
			t.Fatal("LeaveTopic returned no error, want invalid_argument")
		}
		if !strings.Contains(err.Error(), "(code: invalid_argument)") {
			t.Errorf("error = %q, want code invalid_argument", err)
		}
		if calls := srv.Calls(); len(calls) != 0 {
			t.Errorf("fake server received %d calls, want none: %v", len(calls), calls)
		}
		if len(rec.forgotten) != 0 {
			t.Errorf("forgotten = %v, want nothing", rec.forgotten)
		}
	})
}

// A nameless leave only affects the current membership and memory; a later
// nameless join joins the default topics again.
func TestNamelessLeaveThenJoin(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(_ context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
		LeaveTopic: func(context.Context, *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
			return &localpb.LeaveTopicResponse{}, nil
		},
		ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
			return &localpb.ListTopicsResponse{JoinedTopics: []string{"a"}}, nil
		},
	})
	mem := &topicMemory{}
	h := topicsRaceHandlers(topicsTestClient(t, sock), mem)
	h.sessionTopics = defaultTopicSession("a")

	if _, err := h.LeaveTopic(context.Background(), LeaveTopicIn{}); err != nil {
		t.Fatalf("LeaveTopic: %v", err)
	}
	if got := mem.List(); len(got) != 0 {
		t.Fatalf("memory after the nameless leave = %+v, want empty", got)
	}

	if _, err := h.JoinTopic(context.Background(), JoinTopicIn{}); err != nil {
		t.Fatalf("JoinTopic after the leave: %v", err)
	}
	if got := mem.List(); len(got) != 1 || got[0].Name != "a" {
		t.Fatalf("memory after the nameless join = %+v, want [a]", got)
	}
	if names := topicNames(topicsJoinRequests(srv)); len(names) != 1 || names[0] != "a" {
		t.Fatalf("JoinTopic requests = %v, want [a]", names)
	}
}

func TestListTopics(t *testing.T) {
	t.Run("forwards both groups", func(t *testing.T) {
		// REQ-topics-013: list_topics returns the joined and the public groups.
		h, srv, _ := newTopicsHandlers(t, fakelocal.Handlers{
			ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
				return &localpb.ListTopicsResponse{
					JoinedTopics: []string{"a", "b"},
					PublicTopics: []*localpb.PublicTopic{
						{Name: "p", Description: "public one"},
						{Name: "q"},
					},
				}, nil
			},
		}, "agent-1")

		out, err := h.ListTopics(context.Background(), NoArgs{})
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if want := []string{"a", "b"}; !reflect.DeepEqual(out.Joined, want) {
			t.Errorf("Joined = %v, want %v", out.Joined, want)
		}
		want := []PublicTopicOut{{Name: "p", Description: "public one"}, {Name: "q"}}
		if !reflect.DeepEqual(out.Public, want) {
			t.Errorf("Public = %+v, want %+v", out.Public, want)
		}

		call := lastTopicCall(t, srv)
		if call.Method != "ListTopics" {
			t.Fatalf("method = %q, want ListTopics", call.Method)
		}
		req, ok := call.Request.(*localpb.ListTopicsRequest)
		if !ok {
			t.Fatalf("request type %T, want *localpb.ListTopicsRequest", call.Request)
		}
		if req.GetAgentId() != "agent-1" {
			t.Errorf("agent_id = %q, want agent-1", req.GetAgentId())
		}
	})

	t.Run("nil groups become empty arrays", func(t *testing.T) {
		h, _, _ := newTopicsHandlers(t, fakelocal.Handlers{
			ListTopics: func(context.Context, *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
				return &localpb.ListTopicsResponse{}, nil
			},
		}, "agent-1")

		out, err := h.ListTopics(context.Background(), NoArgs{})
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if out.Joined == nil || len(out.Joined) != 0 {
			t.Errorf("Joined = %#v, want a non-nil empty slice", out.Joined)
		}
		if out.Public == nil || len(out.Public) != 0 {
			t.Errorf("Public = %#v, want a non-nil empty slice", out.Public)
		}
	})

	t.Run("not registered", func(t *testing.T) {
		h, srv, _ := newTopicsHandlers(t, fakelocal.Handlers{}, "")

		_, err := h.ListTopics(context.Background(), NoArgs{})
		if err == nil {
			t.Fatal("ListTopics returned no error, want not_registered")
		}
		if !strings.Contains(err.Error(), "(code: not_registered)") {
			t.Errorf("error = %q, want code not_registered", err)
		}
		if calls := srv.Calls(); len(calls) != 0 {
			t.Errorf("fake server received %d calls, want none: %v", len(calls), calls)
		}
	})

	t.Run("a Broker that does not answer does not hold the call open", func(t *testing.T) {
		// design-mcpserver: every Broker call one topic tool makes is bounded by
		// topicRPCTimeout. list_topics must return a tool error instead of
		// waiting for a Broker that never answers.
		h, _, _ := newTopicsHandlers(t, fakelocal.Handlers{
			ListTopics: func(ctx context.Context, _ *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
				<-ctx.Done()
				return nil, fakelocal.ErrorStatus(codes.DeadlineExceeded, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_INTERNAL,
					Message: "the Broker did not answer",
				})
			},
		}, "agent-1")

		// The backstop is the test's own bound, not the product's: on the
		// unfixed code the fake never returns and this fires.
		const backstop = topicRPCTimeout + 5*time.Second
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			_, err := h.ListTopics(context.Background(), NoArgs{})
			done <- err
		}()
		select {
		case err := <-done:
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("ListTopics succeeded although the Broker never answered")
			}
			if elapsed < topicRPCTimeout-time.Second {
				t.Fatalf("ListTopics returned after %s, want about %s", elapsed, topicRPCTimeout)
			}
			if !strings.Contains(err.Error(), "(code: internal)") {
				t.Errorf("error = %q, want a tool error carrying code internal", err)
			}
		case <-time.After(backstop):
			t.Fatalf("ListTopics did not return within %s although topicRPCTimeout is %s; the call is not bounded", backstop, topicRPCTimeout)
		}
	})
}
