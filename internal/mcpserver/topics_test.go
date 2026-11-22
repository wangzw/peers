package mcpserver

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// topicsTestClient dials a fake Local service without going through conn.go, so
// this work item depends only on the fake service and the state types.
func topicsTestClient(t *testing.T, sock string) localpb.LocalClient {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("creating the gRPC client: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return localpb.NewLocalClient(cc)
}

// topicsJoinRequests returns the JoinTopic requests the fake service received,
// in arrival order.
func topicsJoinRequests(srv *fakelocal.Server) []*localpb.JoinTopicRequest {
	var out []*localpb.JoinTopicRequest
	for _, call := range srv.Calls() {
		if call.Method == "JoinTopic" {
			out = append(out, call.Request.(*localpb.JoinTopicRequest))
		}
	}
	return out
}

// topicsLogger returns a logger that discards its output; the restorer only
// logs when it drops a topic.
func topicsLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestTopicMemory(t *testing.T) {
	var m topicMemory
	m.Remember(rememberedTopic{Name: "a", Description: "one"})
	m.Remember(rememberedTopic{Name: "b"})
	m.Remember(rememberedTopic{Name: "a", Public: true, Description: "two"})

	want := []rememberedTopic{
		{Name: "a", Public: true, Description: "two"},
		{Name: "b"},
	}
	if got := m.List(); !reflect.DeepEqual(got, want) {
		t.Fatalf("List() = %+v, want %+v", got, want)
	}

	// List must return a copy: mutating it must not change the memory.
	got := m.List()
	got[0].Name = "changed"
	if m.List()[0].Name != "a" {
		t.Fatalf("List() returned the internal slice")
	}

	m.Forget("a")
	if got := m.List(); !reflect.DeepEqual(got, []rememberedTopic{{Name: "b"}}) {
		t.Fatalf("after Forget(a): %+v", got)
	}
	m.Forget("missing") // must not panic
}

// REQ-topics-019: default topics are joined without a public declaration.
func TestRestorerFirstRegistration(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	r := &restorer{
		mem: &topicMemory{},
		defaults: func(ctx context.Context, brokerAutoJoin bool) defaultLookup {
			return defaultLookup{topics: []string{"a", "b"}, settled: true}
		},
		log: topicsLogger(),
	}
	r.onComplete(context.Background(), topicsTestClient(t, sock), "agent-1", true)

	reqs := topicsJoinRequests(srv)
	if len(reqs) != 2 {
		t.Fatalf("got %d JoinTopic calls, want 2", len(reqs))
	}
	for i, name := range []string{"a", "b"} {
		req := reqs[i]
		if req.GetAgentId() != "agent-1" || req.GetName() != name {
			t.Errorf("request %d = {agent_id: %q, name: %q}, want {agent-1, %s}",
				i, req.GetAgentId(), req.GetName(), name)
		}
		if req.GetIsPublic() {
			t.Errorf("request %d for %q declared the topic public", i, name)
		}
	}
}

// The first registration and nameless topic calls read one computed result, so
// a project configuration or origin that changes afterwards does not change
// which topics a nameless call acts on.
func TestRestorerAndNamelessCallsShareTheDefaultList(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	current := []string{"team"}
	reads := 0
	session := &sessionTopicList{compute: func(context.Context) defaultLookup {
		reads++
		return defaultLookup{topics: append([]string{}, current...), settled: true}
	}}
	r := &restorer{
		mem: &topicMemory{},
		defaults: func(ctx context.Context, autoJoin bool) defaultLookup {
			res := session.lookup(ctx)
			if !autoJoin || res.projectAutoJoinOff {
				res.topics = nil
			}
			return res
		},
		log: topicsLogger(),
	}
	client := topicsTestClient(t, sock)
	r.onComplete(context.Background(), client, "agent-1", true)
	if reqs := topicsJoinRequests(srv); len(reqs) != 1 || reqs[0].GetName() != "team" {
		t.Fatalf("first registration joined %v, want [team]", topicNames(reqs))
	}

	// The configuration changes, but the session keeps the list it registered
	// with.
	current = []string{"other"}
	if got := session.get(context.Background()); !reflect.DeepEqual(got, []string{"team"}) {
		t.Fatalf("nameless call list = %v, want the registration's [team]", got)
	}
	if reads != 1 {
		t.Fatalf("the lookup ran %d times, want once", reads)
	}
}

// REQ-topics-020: the default-topic callback sees the auto_join flag the Broker
// reported in registration_complete.
func TestRestorerDefaultsSeeAutoJoin(t *testing.T) {
	for _, autoJoin := range []bool{true, false} {
		t.Run(fmt.Sprintf("auto_join=%v", autoJoin), func(t *testing.T) {
			var got bool
			r := &restorer{
				mem: &topicMemory{},
				defaults: func(ctx context.Context, brokerAutoJoin bool) defaultLookup {
					got = brokerAutoJoin
					return defaultLookup{settled: true}
				},
				log: topicsLogger(),
			}
			// defaults returns nothing, so no RPC reaches the nil client.
			r.onComplete(context.Background(), nil, "agent-1", autoJoin)
			if got != autoJoin {
				t.Fatalf("defaults saw brokerAutoJoin=%v, want %v", got, autoJoin)
			}
		})
	}
}

// A first registration that is interrupted while the defaults are computed must
// not mark them as done: the lookup ran with a cancelled context (the git call
// then fails), so the memory holds nothing and the next registration has to
// compute the defaults again.
func TestRestorerInterruptedFirstRegistrationKeepsDefaults(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})
	mem := &topicMemory{}
	defaultCalls := 0
	r := &restorer{
		mem: mem,
		defaults: func(ctx context.Context, autoJoin bool) defaultLookup {
			defaultCalls++
			if ctx.Err() != nil {
				// What the lookup returns when the git command is cancelled.
				return defaultLookup{settled: false}
			}
			return defaultLookup{topics: []string{"a"}, settled: true}
		},
		log: topicsLogger(),
	}
	client := topicsTestClient(t, sock)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	r.onComplete(cancelled, client, "agent-1", true)

	if got := mem.List(); len(got) != 0 {
		t.Fatalf("memory after the interrupted registration = %+v, want empty", got)
	}

	r.onComplete(context.Background(), client, "agent-1", true)

	reqs := topicsJoinRequests(srv)
	if len(reqs) != 1 || reqs[0].GetName() != "a" {
		t.Fatalf("the second registration joined %v, want [a]; defaults called %d times",
			topicNames(reqs), defaultCalls)
	}
}

// topicNames returns the names of the JoinTopic requests, for error messages.
func topicNames(reqs []*localpb.JoinTopicRequest) []string {
	out := make([]string, len(reqs))
	for i, req := range reqs {
		out[i] = req.GetName()
	}
	return out
}

// REQ-topics-022, REQ-broker-config-037: a later registration restores the
// memory, including the public declaration and description, and does not join
// the default topics again.
func TestRestorerSecondRegistrationRestoresMemory(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	mem := &topicMemory{}
	defaultsCalls := 0
	r := &restorer{
		mem: mem,
		defaults: func(ctx context.Context, brokerAutoJoin bool) defaultLookup {
			defaultsCalls++
			return defaultLookup{topics: []string{"a"}, settled: true}
		},
		log: topicsLogger(),
	}
	client := topicsTestClient(t, sock)

	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Forget("a")
	mem.Remember(rememberedTopic{Name: "x", Public: true, Description: "d"})

	before := len(topicsJoinRequests(srv))
	r.onComplete(context.Background(), client, "agent-1", true)

	reqs := topicsJoinRequests(srv)[before:]
	if len(reqs) != 1 {
		t.Fatalf("second registration sent %d JoinTopic calls, want only x", len(reqs))
	}
	req := reqs[0]
	if req.GetName() != "x" || !req.GetIsPublic() || req.GetDescription() != "d" {
		t.Fatalf("restored request = {name: %q, public: %v, description: %q}, want {x, true, d}",
			req.GetName(), req.GetIsPublic(), req.GetDescription())
	}
	if defaultsCalls != 1 {
		t.Fatalf("defaults called %d times, want once", defaultsCalls)
	}
}

// mcpserver design line 108: a public topic the Broker rejects is joined again
// without the public declaration and stays remembered as private.
func TestRestorerPublicFallback(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if req.GetName() == "x" && req.GetIsPublic() {
				return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
					Message: "broker is not public",
				})
			}
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(ctx context.Context, brokerAutoJoin bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
	}
	client := topicsTestClient(t, sock)

	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "x", Public: true, Description: "d"})
	r.onComplete(context.Background(), client, "agent-1", true)

	reqs := topicsJoinRequests(srv)
	if len(reqs) != 2 {
		t.Fatalf("got %d JoinTopic calls, want 2 (public, then private)", len(reqs))
	}
	if !reqs[0].GetIsPublic() || reqs[1].GetIsPublic() {
		t.Fatalf("is_public sequence = %v, %v; want true, false", reqs[0].GetIsPublic(), reqs[1].GetIsPublic())
	}
	if reqs[1].GetName() != "x" || reqs[1].GetDescription() != "d" {
		t.Fatalf("fallback request = {name: %q, description: %q}, want {x, d}",
			reqs[1].GetName(), reqs[1].GetDescription())
	}
	if got := mem.List(); len(got) != 1 || got[0].Name != "x" || got[0].Public {
		t.Fatalf("memory after fallback = %+v, want x remembered as private", got)
	}
}

// A topic the Broker rejects with an ErrorDetail and which cannot be joined
// privately either is dropped from the memory.
func TestRestorerForgetsTopicOnDetailError(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if req.GetName() == "y" {
				return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
					Message: "unknown topic",
				})
			}
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(ctx context.Context, brokerAutoJoin bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
	}
	client := topicsTestClient(t, sock)

	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "y"})
	r.onComplete(context.Background(), client, "agent-1", true)

	if got := mem.List(); len(got) != 0 {
		t.Fatalf("memory after a rejected join = %+v, want empty", got)
	}
	if reqs := topicsJoinRequests(srv); len(reqs) != 1 {
		t.Fatalf("got %d JoinTopic calls, want 1 (a private topic is not retried)", len(reqs))
	}
}

// An error without an ErrorDetail means the connection broke; the entry stays
// for the next registration.
func TestRestorerKeepsTopicOnConnectionError(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if req.GetName() == "z" {
				return nil, status.Error(codes.Unavailable, "connection lost")
			}
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(ctx context.Context, brokerAutoJoin bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
	}
	client := topicsTestClient(t, sock)

	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "z"})
	r.onComplete(context.Background(), client, "agent-1", true)

	if got := mem.List(); len(got) != 1 || got[0].Name != "z" {
		t.Fatalf("memory after a connection error = %+v, want z kept", got)
	}
}

// topicsRaceHandlers builds handlers whose session is registered and whose
// topic recorder is the same memory the restorer uses, so the tool calls and
// the restore contend for the same serialization.
func topicsRaceHandlers(client localpb.LocalClient, mem *topicMemory) *handlers {
	st := &statusStore{}
	st.Update(func(s *Status) {
		s.Phase = PhaseConnected
		s.Client = client
		s.AgentID = "agent-1"
		s.Address = "peer@hostA"
	})
	return &handlers{st: st, topics: mem, log: topicsLogger()}
}

// waitClosed waits up to eventTimeout for done to be closed.
func waitClosed(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatalf("%s did not finish within %s", what, eventTimeout)
	}
}

// waitErr waits up to eventTimeout for an error from a call goroutine.
func waitErr(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(eventTimeout):
		t.Fatalf("%s did not return within %s", what, eventTimeout)
		return nil
	}
}

// A leave_topic that runs while a restore is stuck joining a must not be
// undone by the restore joining the left topic from its old snapshot. The tool
// and the restore serialize their Broker call and memory write through the
// same lock, so whatever order they take once the stuck join returns, b is
// left in the end.
func TestRestorerLeaveRace(t *testing.T) {
	sock := fakelocal.SocketPath(t)

	var mu sync.Mutex
	joined := map[string]bool{}
	var order []string
	add := func(name string) {
		if !joined[name] {
			joined[name] = true
			order = append(order, name)
		}
	}
	remove := func(name string) {
		if !joined[name] {
			return
		}
		delete(joined, name)
		for i, n := range order {
			if n == name {
				order = append(order[:i], order[i+1:]...)
				return
			}
		}
	}
	current := func() []string { return append([]string{}, order...) }

	bJoinEntered := make(chan struct{})
	releaseB := make(chan struct{})

	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			// The restore joins a at once and blocks on b, holding the topic
			// lock while the leave below is issued.
			if req.GetName() == "b" {
				close(bJoinEntered)
				<-releaseB
			}
			mu.Lock()
			add(req.GetName())
			topics := current()
			mu.Unlock()
			return &localpb.JoinTopicResponse{Topics: topics}, nil
		},
		LeaveTopic: func(ctx context.Context, req *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
			// A real Broker applies the leave when it receives the request.
			mu.Lock()
			remove(req.GetName())
			topics := current()
			mu.Unlock()
			return &localpb.LeaveTopicResponse{Topics: topics}, nil
		},
		ListTopics: func(ctx context.Context, req *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
			mu.Lock()
			topics := current()
			mu.Unlock()
			return &localpb.ListTopicsResponse{JoinedTopics: topics}, nil
		},
	})
	client := topicsTestClient(t, sock)

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
	}
	// The first registration joins nothing; the second restores a and b in
	// that order.
	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "a"})
	mem.Remember(rememberedTopic{Name: "b"})
	h := topicsRaceHandlers(client, mem)

	restoreDone := make(chan struct{})
	go func() {
		defer close(restoreDone)
		r.onComplete(context.Background(), client, "agent-1", true)
	}()
	<-bJoinEntered

	leaveDone := make(chan error, 1)
	go func() {
		_, err := h.LeaveTopic(context.Background(), LeaveTopicIn{Name: "b"})
		leaveDone <- err
	}()
	close(releaseB)

	waitClosed(t, restoreDone, "the restore")
	if err := waitErr(t, leaveDone, "leave_topic"); err != nil {
		t.Fatalf("leave_topic: %v", err)
	}

	mu.Lock()
	isMember := joined["b"]
	mu.Unlock()
	if isMember {
		t.Fatal("the restore rejoined b after leave_topic removed it")
	}

	out, err := h.ListTopics(context.Background(), NoArgs{})
	if err != nil {
		t.Fatalf("list_topics: %v", err)
	}
	for _, name := range out.Joined {
		if name == "b" {
			t.Fatalf("list_topics still lists b: %v", out.Joined)
		}
	}
}

// A join_topic that changes a topic while the restore is stuck joining it must
// keep its new description; the restore's own retry must not overwrite it.
func TestRestorerJoinRace(t *testing.T) {
	sock := fakelocal.SocketPath(t)

	publicEntered := make(chan struct{})
	releasePublic := make(chan struct{})
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if req.GetName() == "a" && req.GetIsPublic() {
				close(publicEntered)
				<-releasePublic
				return nil, fakelocal.ErrorStatus(codes.InvalidArgument, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
					Message: "this Broker is not public; make it public first",
				})
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
	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "a", Public: true, Description: "old"})
	h := topicsRaceHandlers(client, mem)

	restoreDone := make(chan struct{})
	go func() {
		defer close(restoreDone)
		r.onComplete(context.Background(), client, "agent-1", true)
	}()
	<-publicEntered

	joinDone := make(chan error, 1)
	go func() {
		_, err := h.JoinTopic(context.Background(), JoinTopicIn{Name: "a", Description: "new"})
		joinDone <- err
	}()

	// The restore holds the lock from before the public join until after its
	// private retry, so the join below can only run once the retry is done.
	close(releasePublic)

	waitClosed(t, restoreDone, "the restore")
	if err := waitErr(t, joinDone, "join_topic"); err != nil {
		t.Fatalf("join_topic: %v", err)
	}

	var got rememberedTopic
	found := false
	for _, topic := range mem.List() {
		if topic.Name == "a" {
			got = topic
			found = true
		}
	}
	if !found {
		t.Fatal("the memory no longer holds a")
	}
	if got.Description != "new" {
		t.Fatalf("remembered description = %q, want %q", got.Description, "new")
	}
}

// A restore whose Broker call never answers releases the topic lock after its
// per-topic bound, so a later registration is not stuck behind it.
func TestRestoreReleasesTheTopicLockAfterItsBound(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	var calls atomic.Int32
	entered := make(chan struct{})
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if calls.Add(1) == 1 {
				close(entered)
				// The first call is never answered until its context ends.
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})
	client := topicsTestClient(t, sock)

	mem := &topicMemory{}
	r := &restorer{
		mem:        mem,
		defaults:   func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:        topicsLogger(),
		rpcTimeout: 100 * time.Millisecond,
	}
	r.onComplete(context.Background(), client, "agent-1", true) // the first registration joins nothing
	mem.Remember(rememberedTopic{Name: "a"})

	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		r.onComplete(streamCtx, client, "agent-1", true)
	}()
	<-entered

	// The Broker answers again; a registration that follows must not wait for
	// the wedged restore forever.
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		r.onComplete(context.Background(), client, "agent-1", true)
	}()
	waitClosed(t, secondDone, "the restore after the Broker recovered")
	waitClosed(t, firstDone, "the wedged restore")
}

// Remember, Forget and List may run while a registration restore is in flight;
// the race detector must not report a data race.
func TestTopicMemoryConcurrentAccess(t *testing.T) {
	mem := &topicMemory{}
	sock := fakelocal.SocketPath(t)
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})
	client := topicsTestClient(t, sock)
	r := &restorer{
		mem: mem,
		defaults: func(ctx context.Context, brokerAutoJoin bool) defaultLookup {
			return defaultLookup{topics: []string{"a", "b"}, settled: true}
		},
		log: topicsLogger(),
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.onComplete(context.Background(), client, "agent-1", true)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			mem.Remember(rememberedTopic{Name: fmt.Sprintf("t%d", j%7), Public: j%2 == 0})
			mem.Forget(fmt.Sprintf("t%d", j%5))
			_ = mem.List()
		}
	}()
	wg.Wait()
}

// topicClock is the part of a fake clock the fake topic limiter reads.
type topicClock interface {
	Now() time.Time
}

// fakeTopicLimiter mirrors the Broker's per-account topic-change token bucket:
// a burst of 64 and 4 tokens per second, the values of topicRateBurst and
// topicRatePerSecond in internal/broker. One limiter shared by several sessions
// models one account's bucket: the Broker keys it by uid, not by session.
type fakeTopicLimiter struct {
	mu     sync.Mutex
	clock  topicClock
	tokens float64
	last   time.Time
}

func newFakeTopicLimiter(clock topicClock) *fakeTopicLimiter {
	return &fakeTopicLimiter{clock: clock, tokens: 64, last: clock.Now()}
}

func (l *fakeTopicLimiter) allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	if elapsed := now.Sub(l.last).Seconds(); elapsed > 0 {
		l.tokens = min(float64(64), l.tokens+elapsed*4)
	}
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

// restoreClock is a fake clock that moves only when its driver moves it, and
// whose waits block until the clock reaches their deadline. Concurrent waits
// therefore share elapsed time the way real time works: sessions waiting
// together for 250ms see 250ms pass once, not once per session. A clock that
// let each wait advance itself would hand every session its own token and hide
// the contention these tests exercise.
type restoreClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*restoreWaiter
	wake    chan struct{}
	changed chan struct{}
}

// restoreWaiter is one blocked wait, removed by the wait itself on return.
type restoreWaiter struct {
	deadline time.Time
}

func newRestoreClock() *restoreClock {
	return &restoreClock{
		now:     time.Unix(0, 0),
		wake:    make(chan struct{}),
		changed: make(chan struct{}, 1),
	}
}

func (c *restoreClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// wait blocks until the clock reaches now+d or ctx ends. The driver moves the
// clock; a wait never moves it itself.
func (c *restoreClock) wait(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	w := &restoreWaiter{deadline: c.now.Add(d)}
	c.waiters = append(c.waiters, w)
	c.mu.Unlock()
	c.signal()

	defer func() {
		c.mu.Lock()
		for i, other := range c.waiters {
			if other == w {
				c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
				break
			}
		}
		c.mu.Unlock()
		c.signal()
	}()

	for {
		c.mu.Lock()
		now, wake := c.now, c.wake
		c.mu.Unlock()
		if !now.Before(w.deadline) {
			return nil
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// signal wakes the driver after a waiter appears or leaves.
func (c *restoreClock) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// advanceTo moves the clock to t if that is later and wakes every wait.
func (c *restoreClock) advanceTo(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !t.After(c.now) {
		return
	}
	c.now = t
	close(c.wake)
	c.wake = make(chan struct{})
}

// drive moves the clock to the earliest future deadline of any waiter until
// done is closed. It never sleeps: it wakes whenever a waiter appears or
// leaves, so time passes only while some restore is waiting.
func (c *restoreClock) drive(done <-chan struct{}) {
	for {
		c.mu.Lock()
		now := c.now
		var next time.Time
		for _, w := range c.waiters {
			if w.deadline.After(now) && (next.IsZero() || w.deadline.Before(next)) {
				next = w.deadline
			}
		}
		c.mu.Unlock()
		if !next.IsZero() {
			c.advanceTo(next)
			continue
		}
		select {
		case <-done:
			return
		case <-c.changed:
		}
	}
}

// rateLimitQuotaError is the detail the Broker returns when an account changes
// topics faster than its token bucket refills.
func rateLimitQuotaError() error {
	return fakelocal.ErrorStatus(codes.ResourceExhausted, &localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
		Message: "Too many topic changes. Try again in a moment.",
	})
}

// quantityLimitQuotaError is the detail the Broker returns when an account has
// reached its topic count limit.
func quantityLimitQuotaError() error {
	return fakelocal.ErrorStatus(codes.ResourceExhausted, &localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
		Message: "This account has reached the limit on topics.",
	})
}

// A session that remembers more topics than the Broker lets it join in one
// burst must lose none of them: a rate-limit refusal clears after the bucket
// refills, so the restore retries it during the same registration. Before the
// fix the refused topics were forgotten and only 64 of the 100 came back.
func TestRestorerRestoresTopicsThroughRateLimit(t *testing.T) {
	const remembered = 100
	sock := fakelocal.SocketPath(t)
	clock := &fakeClock{now: time.Unix(0, 0)}
	limiter := newFakeTopicLimiter(clock)

	var mu sync.Mutex
	joined := map[string]bool{}
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if !limiter.allow() {
				return nil, rateLimitQuotaError()
			}
			mu.Lock()
			joined[req.GetName()] = true
			mu.Unlock()
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait: func(ctx context.Context, d time.Duration) error {
			clock.Advance(d)
			return nil
		},
	}
	client := topicsTestClient(t, sock)
	r.onComplete(context.Background(), client, "agent-1", true)
	for i := 0; i < remembered; i++ {
		mem.Remember(rememberedTopic{Name: fmt.Sprintf("t%03d", i)})
	}
	r.onComplete(context.Background(), client, "agent-1", true)

	if got := len(mem.List()); got != remembered {
		t.Fatalf("after one registration the memory holds %d topics, want all %d", got, remembered)
	}
	mu.Lock()
	missing := 0
	for i := 0; i < remembered; i++ {
		if !joined[fmt.Sprintf("t%03d", i)] {
			missing++
		}
	}
	mu.Unlock()
	if missing != 0 {
		t.Fatalf("%d of the %d topics were never joined", missing, remembered)
	}
}

// A Broker keys its topic-change token bucket by account, so every session of
// an account shares one bucket. After a Broker restart all of them restore
// their topics at once. A rate refusal clears as the bucket refills, so the
// restore has to keep retrying while the connection stays up instead of giving
// up after a fixed number of attempts: under contention a bounded retry loses
// the topics that keep losing the race for a token.
func TestRestorerRestoresTopicsAcrossSessionsSharingOneBucket(t *testing.T) {
	const sessions = 3
	const remembered = 100

	sock := fakelocal.SocketPath(t)
	clock := newRestoreClock()
	limiter := newFakeTopicLimiter(clock)

	var mu sync.Mutex
	joined := map[string]map[string]bool{}
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if !limiter.allow() {
				return nil, rateLimitQuotaError()
			}
			mu.Lock()
			if joined[req.GetAgentId()] == nil {
				joined[req.GetAgentId()] = map[string]bool{}
			}
			joined[req.GetAgentId()][req.GetName()] = true
			mu.Unlock()
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})
	client := topicsTestClient(t, sock)

	stopDrive, waitDrive := startRestoreClock(t, clock)

	var sessionsWG sync.WaitGroup
	for s := 0; s < sessions; s++ {
		agent := fmt.Sprintf("agent-%d", s)
		mem := &topicMemory{}
		for i := 0; i < remembered; i++ {
			mem.Remember(rememberedTopic{Name: fmt.Sprintf("t%03d", i)})
		}
		r := &restorer{
			mem:      mem,
			defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
			log:      topicsLogger(),
			wait:     clock.wait,
		}
		sessionsWG.Add(1)
		go func() {
			defer sessionsWG.Done()
			r.onComplete(context.Background(), client, agent, true)
		}()
	}

	restored := make(chan struct{})
	go func() {
		sessionsWG.Wait()
		close(restored)
	}()
	select {
	case <-restored:
	case <-time.After(eventTimeout):
		stopDrive()
		t.Fatalf("the sessions did not finish restoring within %s", eventTimeout)
	}
	stopDrive()
	waitDrive()

	mu.Lock()
	missing := 0
	for s := 0; s < sessions; s++ {
		agent := fmt.Sprintf("agent-%d", s)
		for i := 0; i < remembered; i++ {
			if !joined[agent][fmt.Sprintf("t%03d", i)] {
				missing++
			}
		}
	}
	mu.Unlock()
	if missing != 0 {
		t.Fatalf("%d of the %d topics across %d sessions were never restored", missing, sessions*remembered, sessions)
	}
}

// startRestoreClock starts a driver for clock and returns a stop function and
// a wait function. Both must be called before the test ends.
func startRestoreClock(t *testing.T, clock *restoreClock) (stop func(), wait func()) {
	t.Helper()
	done := make(chan struct{})
	var once sync.Once
	stop = func() { once.Do(func() { close(done) }) }
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		clock.drive(done)
	}()
	t.Cleanup(func() {
		stop()
		wg.Wait()
	})
	return stop, wg.Wait
}

// startQuantityRefusingBroker starts a fake Broker whose JoinTopic always
// refuses with the topic count limit and which signals once on the attempt
// numbered threshold.
func startQuantityRefusingBroker(t *testing.T, sock string, threshold int32) (*atomic.Int32, <-chan struct{}) {
	t.Helper()
	var attempts atomic.Int32
	enough := make(chan struct{}, 1)
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if n := attempts.Add(1); n == threshold {
				enough <- struct{}{}
			}
			return nil, quantityLimitQuotaError()
		},
	})
	return &attempts, enough
}

// waitForRetriesOrStop waits until the restore has retried a quota-refused
// topic past any small fixed cap, or fails when it stops first.
func waitForRetriesOrStop(t *testing.T, enough, stopped <-chan struct{}, attempts *atomic.Int32) {
	t.Helper()
	select {
	case <-enough:
	case <-stopped:
		t.Fatalf("the restore stopped after %d attempts, want it to keep retrying", attempts.Load())
	case <-time.After(eventTimeout):
		t.Fatalf("the restore made no further attempts within %s (after %d)", eventTimeout, attempts.Load())
	}
}

// A quota refusal can be the account's topic count limit rather than a rate
// limit. The restore keeps retrying it with a growing backoff and keeps the
// entry, so when another topic leaves and frees a slot the refused topic joins
// by itself instead of waiting for the next registration.
func TestRestorerRetriesQuantityRefusalUntilASlotFrees(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	clock := newRestoreClock()

	var mu sync.Mutex
	open := false
	joined := false
	var attempts atomic.Int32
	enough := make(chan struct{}, 1)
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			mu.Lock()
			isOpen := open
			if isOpen {
				joined = true
			}
			mu.Unlock()
			if n := attempts.Add(1); n == 5 {
				enough <- struct{}{}
			}
			if !isOpen {
				return nil, quantityLimitQuotaError()
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
		wait:     clock.wait,
	}
	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "over"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		r.onComplete(ctx, client, "agent-1", true)
	}()
	stopDrive, waitDrive := startRestoreClock(t, clock)

	waitForRetriesOrStop(t, enough, stopped, &attempts)

	if got := mem.List(); len(got) != 1 || got[0].Name != "over" {
		t.Fatalf("memory while the quota stands = %+v, want over kept", got)
	}

	mu.Lock()
	open = true
	mu.Unlock()

	select {
	case <-stopped:
	case <-time.After(eventTimeout):
		stopDrive()
		t.Fatal("the restore did not finish after a slot was freed")
	}
	stopDrive()
	waitDrive()

	mu.Lock()
	didJoin := joined
	mu.Unlock()
	if !didJoin {
		t.Fatal("the topic never joined after a slot was freed")
	}
	if got := mem.List(); len(got) != 1 || got[0].Name != "over" {
		t.Fatalf("memory after the topic was restored = %+v, want over", got)
	}
}

// When the connection ends while a quota-refused topic is backing off, the
// retries stop and the entry stays for the next registration.
func TestRestorerKeepsTopicWhenConnectionEndsDuringQuotaRetry(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	clock := newRestoreClock()
	attempts, enough := startQuantityRefusingBroker(t, sock, 4)
	client := topicsTestClient(t, sock)

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait:     clock.wait,
	}
	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "over"})

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		r.onComplete(ctx, client, "agent-1", true)
	}()
	stopDrive, waitDrive := startRestoreClock(t, clock)

	waitForRetriesOrStop(t, enough, stopped, attempts)

	cancel()
	select {
	case <-stopped:
	case <-time.After(eventTimeout):
		stopDrive()
		t.Fatal("the restore kept retrying after the connection ended")
	}
	stopDrive()
	waitDrive()

	if got := mem.List(); len(got) != 1 || got[0].Name != "over" {
		t.Fatalf("memory after the connection ended = %+v, want over kept", got)
	}
}

// The retry loop runs in the goroutine that owns the registration. When the
// connection ends that goroutine has to return instead of leaking on a pending
// timer: the fake clock never moves by itself, so only the ended connection can
// release the wait.
func TestRestorerRetryGoroutineExitsWhenConnectionEnds(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	clock := newRestoreClock()
	attempts, enough := startQuantityRefusingBroker(t, sock, 4)
	client := topicsTestClient(t, sock)

	r := &restorer{
		mem:      &topicMemory{},
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait:     clock.wait,
	}
	r.mem.Remember(rememberedTopic{Name: "over"})

	ctx, cancel := context.WithCancel(context.Background())
	var retryWG sync.WaitGroup
	retryWG.Add(1)
	stopped := make(chan struct{})
	go func() {
		defer retryWG.Done()
		defer close(stopped)
		r.onComplete(ctx, client, "agent-1", true)
	}()
	stopDrive, waitDrive := startRestoreClock(t, clock)

	waitForRetriesOrStop(t, enough, stopped, attempts)

	cancel()
	exited := make(chan struct{})
	go func() {
		retryWG.Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(eventTimeout):
		stopDrive()
		t.Fatal("the retry goroutine did not exit after the connection ended")
	}
	stopDrive()
	waitDrive()
}

// Jitter must not push a wait outside the documented range: at most 30 seconds
// and at least 250 milliseconds, even when the scaled value would cross either
// bound. In the middle of the range it keeps the [0.5x, 1.5x) envelope.
func TestTopicRestoreJitterClamps(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if got := topicRestoreJitter(topicRestoreBackoffMax); got > topicRestoreBackoffMax {
			t.Fatalf("jitter(%v) = %v, want at most %v", topicRestoreBackoffMax, got, topicRestoreBackoffMax)
		}
		if got := topicRestoreJitter(topicRestoreBackoffInitial); got < topicRestoreBackoffInitial {
			t.Fatalf("jitter(%v) = %v, want at least %v", topicRestoreBackoffInitial, got, topicRestoreBackoffInitial)
		}
	}
	for i := 0; i < 1000; i++ {
		base := 2 * time.Second
		if got := topicRestoreJitter(base); got < base/2 || got > 3*base/2 {
			t.Fatalf("jitter(%v) = %v, want within [%v, %v]", base, got, base/2, 3*base/2)
		}
	}
}

// A topic the Broker never lets in must not hold up the topics behind it: a
// quota refusal defers that topic to the next pass, and the topics that fit are
// restored in the first pass. Reverting to an in-place retry of the refused
// topic leaves the others unjoined and the wait below times out.
func TestRestorerQuotaRefusalDoesNotBlockLaterTopics(t *testing.T) {
	const remembered = 100
	sock := fakelocal.SocketPath(t)
	clock := newRestoreClock()

	var mu sync.Mutex
	joined := map[string]bool{}
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if req.GetName() == "t000" {
				return nil, quantityLimitQuotaError()
			}
			mu.Lock()
			joined[req.GetName()] = true
			mu.Unlock()
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})
	client := topicsTestClient(t, sock)

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait:     clock.wait,
	}
	r.onComplete(context.Background(), client, "agent-1", true)
	for i := 0; i < remembered; i++ {
		mem.Remember(rememberedTopic{Name: fmt.Sprintf("t%03d", i)})
	}

	stopDrive, waitDrive := startRestoreClock(t, clock)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		r.onComplete(ctx, client, "agent-1", true)
	}()

	regWaitUntil(t, "the 99 topics that fit to be joined", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(joined) == remembered-1
	})

	cancel()
	select {
	case <-stopped:
	case <-time.After(eventTimeout):
		stopDrive()
		t.Fatal("the restore did not stop after the registration context ended")
	}
	stopDrive()
	waitDrive()

	if got := len(mem.List()); got != remembered {
		t.Fatalf("the memory holds %d topics after the refusal, want all %d", got, remembered)
	}
}

// A join refused with NOT_REGISTERED says the agent identity is gone, not that
// the topic is invalid. The restore stops and keeps the entry so the next
// registration can restore it, instead of forgetting the topic.
func TestRestorerKeepsTopicWhenRegistrationGone(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	var attempts atomic.Int32
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			attempts.Add(1)
			return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED,
				Message: `agent "agent-1" is not registered for this account`,
			})
		},
	})
	mem := &topicMemory{}
	mem.Remember(rememberedTopic{Name: "kept"})
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait:     func(context.Context, time.Duration) error { return nil },
	}
	client := topicsTestClient(t, sock)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.onComplete(context.Background(), client, "agent-1", true)
	}()
	waitClosed(t, done, "the restore after NOT_REGISTERED")

	if got := mem.List(); len(got) != 1 || got[0].Name != "kept" {
		t.Fatalf("memory after a NOT_REGISTERED refusal = %+v, want kept", got)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("the restore made %d attempts after NOT_REGISTERED, want exactly one", got)
	}
}

// A leave_topic that runs while a quota-refused topic waits for its next pass
// stops the retry and keeps the topic out of the deferred set: the restore does
// not join it again after the user left it.
func TestRestorerStopsRetryingAfterLeaveTopic(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	clock := newRestoreClock()
	var attempts atomic.Int32
	firstAttempt := make(chan struct{}, 1)
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(context.Context, *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			attempts.Add(1)
			select {
			case firstAttempt <- struct{}{}:
			default:
			}
			return nil, quantityLimitQuotaError()
		},
		LeaveTopic: func(context.Context, *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
			return &localpb.LeaveTopicResponse{}, nil
		},
	})
	client := topicsTestClient(t, sock)

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait:     clock.wait,
	}
	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "a"})
	h := topicsRaceHandlers(client, mem)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.onComplete(context.Background(), client, "agent-1", true)
	}()
	select {
	case <-firstAttempt:
	case <-time.After(eventTimeout):
		t.Fatalf("the restore did not attempt to join a within %s", eventTimeout)
	}

	// The restore is now waiting for its next pass; the leave lands meanwhile.
	if _, err := h.LeaveTopic(context.Background(), LeaveTopicIn{Name: "a"}); err != nil {
		t.Fatalf("leave_topic: %v", err)
	}
	before := attempts.Load()

	stopDrive, waitDrive := startRestoreClock(t, clock)
	waitClosed(t, done, "the restore after the topic was left")
	stopDrive()
	waitDrive()

	if got := attempts.Load(); got != before {
		t.Fatalf("the restore joined the left topic %d more times, want none", got-before)
	}
	if got := mem.List(); len(got) != 0 {
		t.Fatalf("memory after leave_topic = %+v, want empty", got)
	}
}

// The restore belongs to one registration, not to the Register stream. A
// not_registered event revokes the registration: the retry stops and keeps its
// topics, and the next registration_complete on the same stream restores them
// at once instead of waiting for the old retry to time out.
func TestRestorerStopsOnRevocationAndRestoresOnReRegister(t *testing.T) {
	const (
		stateQuota   = "quota"
		stateRevoked = "revoked"
		stateOK      = "ok"
	)
	var mu sync.Mutex
	state := stateQuota
	successfulJoins := 0
	attempted := make(chan struct{}, 1)

	handlers := fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			mu.Lock()
			current := state
			mu.Unlock()
			select {
			case attempted <- struct{}{}:
			default:
			}
			switch current {
			case stateQuota:
				return nil, quantityLimitQuotaError()
			case stateRevoked:
				return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED,
					Message: `agent "agent-1" is not registered for this account`,
				})
			default:
				mu.Lock()
				successfulJoins++
				mu.Unlock()
				return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
			}
		},
	}
	mem := &topicMemory{}
	mem.Remember(rememberedTopic{Name: "kept"})
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		// A short real wait keeps the retry paced; the registration context
		// ends it at once when the registration is revoked.
		wait: func(ctx context.Context, _ time.Duration) error {
			select {
			case <-time.After(2 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	run := startRegRunOn(t, handlers, &localpb.RegisterRequest{}, r.onComplete)

	if err := run.conn.Complete("agent-1", "me@h", false); err != nil {
		t.Fatalf("sending the first registration_complete: %v", err)
	}
	select {
	case <-attempted:
	case <-time.After(eventTimeout):
		t.Fatalf("the restore did not attempt a join within %s", eventTimeout)
	}

	// Revoke the registration on the stream and make the Broker refuse the live
	// identity as well, as a deregistering Broker does.
	mu.Lock()
	state = stateRevoked
	mu.Unlock()
	if err := run.conn.NotRegistered(localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND); err != nil {
		t.Fatalf("sending not_registered: %v", err)
	}
	regWaitUntil(t, "the status to show the revoked registration", func() bool {
		return run.st.Get().NotRegistered == localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND
	})

	// The entry must survive the revocation window instead of being forgotten
	// by a late join refused with NOT_REGISTERED.
	regConsistently(t, "the topic to stay remembered after the revocation", 200*time.Millisecond, func() bool {
		list := mem.List()
		return len(list) == 1 && list[0].Name == "kept"
	})

	// Re-registering on the same stream must restore at once.
	mu.Lock()
	state = stateOK
	mu.Unlock()
	if err := run.conn.Complete("agent-1", "me@h", false); err != nil {
		t.Fatalf("sending the second registration_complete: %v", err)
	}
	regWaitUntil(t, "the topic to be restored by the new registration", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return successfulJoins > 0
	})
	if got := mem.List(); len(got) != 1 || got[0].Name != "kept" {
		t.Fatalf("memory after the re-registration = %+v, want kept", got)
	}

	run.conn.End()
	if err := run.wait(t); err == nil {
		t.Fatal("runRegistration returned nil after the stream ended")
	}
}

// A newer registration_complete replaces the registration on the same stream
// even when no not_registered event arrives. The restore of the replaced
// registration must stop while it sits in a backoff wait, so the new
// registration restores at once instead of waiting for that backoff. The fake
// clock never moves, so a successful join below can only come from the
// cancelled wait, never from elapsed time.
func TestRestorerReplacementStopsBackoffWithoutRevocation(t *testing.T) {
	var mu sync.Mutex
	refuse := true
	attempted := make(chan struct{}, 1)
	joined := make(chan string, 1)
	handlers := fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			select {
			case attempted <- struct{}{}:
			default:
			}
			mu.Lock()
			refusing := refuse
			mu.Unlock()
			if refusing {
				return nil, quantityLimitQuotaError()
			}
			select {
			case joined <- req.GetAgentId():
			default:
			}
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	}
	mem := &topicMemory{}
	mem.Remember(rememberedTopic{Name: "kept"})
	clock := newRestoreClock()
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait:     clock.wait,
	}
	run := startRegRunOn(t, handlers, &localpb.RegisterRequest{}, r.onComplete)

	if err := run.conn.Complete("agent-1", "me@h", false); err != nil {
		t.Fatalf("sending the first registration_complete: %v", err)
	}
	waitClosed(t, attempted, "the first restore attempt")

	// The refused restore is now parked in its backoff. The clock is never
	// advanced, so only stopping the replaced registration can release the wait.
	regWaitUntil(t, "the refused restore to park in its backoff wait", func() bool {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		return len(clock.waiters) > 0
	})

	mu.Lock()
	refuse = false
	mu.Unlock()
	if err := run.conn.Complete("agent-2", "me@h2", false); err != nil {
		t.Fatalf("sending the second registration_complete: %v", err)
	}

	// Deliberately no timeout: with the replaced registration's context
	// cancelled the join lands at once; without that cancellation the old
	// restore holds the serial slot in its backoff forever, and go test
	// -timeout reports the hang.
	if got := <-joined; got != "agent-2" {
		t.Fatalf("the successful join used agent %q, want the new registration agent-2", got)
	}
	if got := mem.List(); len(got) != 1 || got[0].Name != "kept" {
		t.Fatalf("memory after the replacement = %+v, want kept", got)
	}

	run.conn.End()
	if err := run.wait(t); err == nil {
		t.Fatal("runRegistration returned nil after the stream ended")
	}
}

// A topic the Broker rejects for a reason other than a quota is not retried and
// is dropped from the memory, so a permanently rejected name stops costing a
// restore on every registration.
func TestRestorerForgetsDeterministicRejection(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	var calls atomic.Int32
	fakelocal.Start(t, sock, fakelocal.Handlers{
		JoinTopic: func(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
			if req.GetName() == "hidden" {
				calls.Add(1)
				return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
					Message: "this topic is not visible to you",
				})
			}
			return &localpb.JoinTopicResponse{Topics: []string{req.GetName()}}, nil
		},
	})

	mem := &topicMemory{}
	r := &restorer{
		mem:      mem,
		defaults: func(context.Context, bool) defaultLookup { return defaultLookup{settled: true} },
		log:      topicsLogger(),
		wait:     func(context.Context, time.Duration) error { return nil },
	}
	client := topicsTestClient(t, sock)
	r.onComplete(context.Background(), client, "agent-1", true)
	mem.Remember(rememberedTopic{Name: "hidden"})
	r.onComplete(context.Background(), client, "agent-1", true)

	if got := mem.List(); len(got) != 0 {
		t.Fatalf("memory after a deterministic rejection = %+v, want empty", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("the determinant rejection was retried %d times, want one attempt", got-1)
	}
}
