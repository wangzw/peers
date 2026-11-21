package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// overflowEventName is the runtime log event written when a registration
// stream's downstream backlog exceeds its limit. It is spelled out here so the
// test still compiles against the version that has no such event.
const overflowEventName = "register_events_overflow"

// maxQueuedEvents is the documented downstream backlog limit of one
// registration stream.
const maxQueuedEvents = 64

func changedResponse(id AgentID, address string) *localpb.RegisterResponse {
	return &localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_AgentChanged{
			AgentChanged: &localpb.AgentChangedEvent{
				AgentId:      string(id),
				AgentAddress: address,
			},
		},
	}
}

func completeResponse(id AgentID) *localpb.RegisterResponse {
	return &localpb.RegisterResponse{
		Event: &localpb.RegisterResponse_RegistrationComplete{
			RegistrationComplete: &localpb.RegistrationComplete{AgentId: string(id)},
		},
	}
}

func problemResponse() *localpb.RegisterResponse {
	return notRegisteredResponse(localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND, "")
}

// queueAddresses returns the agent addresses of the queued agent_changed
// events, oldest first.
func queueAddresses(q *eventQueue) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []string
	for _, item := range q.items {
		if changed := item.GetAgentChanged(); changed != nil {
			out = append(out, changed.GetAgentAddress())
		}
	}
	return out
}

// TestEventQueueCoalescesAgentChanges pins the merge rule: a pushed
// agent_changed replaces an unsent agent_changed for the same agent in place,
// and changes for different agents stay separate.
func TestEventQueueCoalescesAgentChanges(t *testing.T) {
	q := newEventQueue()
	first := testAgentID("aaaaaa1")
	second := testAgentID("bbbbbb1")

	q.push(changedResponse(first, "one@hostA"))
	q.push(changedResponse(first, "two@hostA"))
	if n := queueLen(q); n != 1 {
		t.Fatalf("queued events after two changes for one agent = %d, want 1", n)
	}
	if got := queueAddresses(q); len(got) != 1 || got[0] != "two@hostA" {
		t.Fatalf("queued addresses = %v, want only two@hostA", got)
	}

	q.push(changedResponse(second, "other@hostA"))
	if n := queueLen(q); n != 2 {
		t.Fatalf("queued events after a change for another agent = %d, want 2", n)
	}

	q.push(changedResponse(second, "other2@hostA"))
	if n := queueLen(q); n != 2 {
		t.Fatalf("queued events after a second change for another agent = %d, want 2", n)
	}
	if got := queueAddresses(q); len(got) != 2 || got[0] != "two@hostA" || got[1] != "other2@hostA" {
		t.Fatalf("queued addresses = %v, want [two@hostA other2@hostA]", got)
	}
}

// TestEventQueueKeepsOtherEvents pins that merging touches only a trailing
// agent_changed: a registration_complete directly before one survives, and
// other events keep their order.
func TestEventQueueKeepsOtherEvents(t *testing.T) {
	q := newEventQueue()
	first := testAgentID("aaaaaa2")

	q.push(completeResponse(first))
	q.push(changedResponse(first, "renamed@hostA"))
	q.push(changedResponse(first, "renamed-again@hostA"))

	if n := queueLen(q); n != 2 {
		t.Fatalf("queued events = %d, want the completion plus one merged change", n)
	}
	q.mu.Lock()
	head := q.items[0]
	q.mu.Unlock()
	if head.GetRegistrationComplete() == nil {
		t.Fatalf("queued head = %v, want registration_complete", head)
	}

	q.push(problemResponse())
	if n := queueLen(q); n != 3 {
		t.Fatalf("queued events after a not_registered = %d, want 3", n)
	}
}

// TestEventQueueCapsBacklog pins that the queue refuses events past its limit
// instead of growing with everything the client has not read.
func TestEventQueueCapsBacklog(t *testing.T) {
	q := newEventQueue()
	for i := 0; i < 100; i++ {
		q.push(problemResponse())
	}
	if n := queueLen(q); n > maxQueuedEvents {
		t.Fatalf("queued events = %d, want at most %d", n, maxQueuedEvents)
	}
}

// stalledRegisterStream is a Local_RegisterServer whose Send blocks until the
// test releases it, so events pushed while one Send is in flight pile up in the
// registration's downstream queue. Recv returns the first frame and then waits
// for the context, like a client that keeps its side of the stream open.
type stalledRegisterStream struct {
	grpc.ServerStream

	ctx         context.Context
	first       *localpb.RegisterRequest
	sendStarted chan struct{}
	sendRelease chan struct{}
}

func (s *stalledRegisterStream) Context() context.Context { return s.ctx }

func (s *stalledRegisterStream) Recv() (*localpb.RegisterRequest, error) {
	if s.first != nil {
		req := s.first
		s.first = nil
		return req, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *stalledRegisterStream) Send(*localpb.RegisterResponse) error {
	select {
	case s.sendStarted <- struct{}{}:
	default:
	}
	select {
	case <-s.sendRelease:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// stalledRegistration is one Register handler running on a stream whose first
// Send blocks, so later events pile up in its downstream queue.
type stalledRegistration struct {
	tb         *testBroker
	stream     *stalledRegisterStream
	conn       *regConn
	handlerErr chan error
}

// startStalledRegistration serves Register on a stream whose first Send blocks
// until the test closes sendRelease or the stream context ends, and waits for
// the registration to attach. The Register goroutine cancels the stream
// context once the handler returns, which is what gRPC does when an RPC ends:
// a Send blocked in the transport then returns.
func startStalledRegistration(t *testing.T, name string) (*stalledRegistration, AgentID) {
	t.Helper()
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: name})
	id := mustAgentID(t, fake.SocketPath())

	base, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx := peer.NewContext(base, &peer.Peer{
		AuthInfo: &callerAuthInfo{caller: Caller{UID: uint32(os.Geteuid())}},
	})
	r := &stalledRegistration{
		tb: tb,
		stream: &stalledRegisterStream{
			ctx: ctx,
			first: &localpb.RegisterRequest{
				InboxSocketPath: fake.SocketPath(),
				ClaudeConfigDir: fake.ConfigDir(),
			},
			sendStarted: make(chan struct{}, 1),
			sendRelease: make(chan struct{}),
		},
		handlerErr: make(chan error, 1),
	}
	go func() {
		err := tb.b.Register(r.stream)
		cancel()
		r.handlerErr <- err
	}()

	select {
	case <-r.stream.sendStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("the registration_complete event was not sent")
	}

	waitForReg(t, "the registration to attach", func() bool {
		tb.b.mu.Lock()
		defer tb.b.mu.Unlock()
		for _, a := range tb.b.agents {
			for c := range a.conns {
				r.conn = c
				return true
			}
		}
		return false
	})
	return r, id
}

// fillRegistrationBacklog queues maxQueuedEvents events plus one more, which
// the queue refuses. The events name no agent, so only the connection itself
// can name the overflow.
func fillRegistrationBacklog(r *stalledRegistration) {
	for i := 0; i < maxQueuedEvents; i++ {
		r.conn.out.push(problemResponse())
	}
	r.conn.out.push(problemResponse())
}

// assertOverflowLog checks the single overflow warning: it is a WARN for this
// account that names the connection's agent.
func assertOverflowLog(t *testing.T, tb *testBroker, id AgentID) {
	t.Helper()
	var overflow map[string]any
	for _, rec := range tb.logLines(t) {
		if rec["event"] == overflowEventName {
			overflow = rec
		}
	}
	if overflow == nil {
		t.Fatalf("no %s record in the runtime log", overflowEventName)
	}
	if got := overflow["uid"]; got != float64(os.Geteuid()) {
		t.Fatalf("log uid = %v, want %d", got, os.Geteuid())
	}
	if got := overflow["agent"]; got != shortID(id) {
		t.Fatalf("log agent = %v, want %q", got, shortID(id))
	}
	if got := overflow["level"]; got != "WARN" {
		t.Fatalf("log level = %v, want WARN", got)
	}
}

// TestRegisterEndsStreamWhenEventBacklogExceedsLimit pins the behaviour of an
// unread registration stream: once more than maxQueuedEvents events wait for
// the sender, the Broker ends the stream with quota_exceeded and records one
// warning, instead of keeping the backlog in memory without bound. The test
// never releases the blocked Send: the handler must return without waiting for
// a client that stopped reading.
func TestRegisterEndsStreamWhenEventBacklogExceedsLimit(t *testing.T) {
	r, id := startStalledRegistration(t, "backlog")

	// The sender is stuck in its first Send, so these events stay queued.
	fillRegistrationBacklog(r)

	select {
	case err := <-r.handlerErr:
		if err == nil {
			t.Fatal("Register returned nil, want quota_exceeded")
		}
		st, _ := status.FromError(err)
		if st.Code() != codes.ResourceExhausted {
			t.Fatalf("status code = %v (%v), want ResourceExhausted", st.Code(), err)
		}
		var detail *localpb.ErrorDetail
		for _, d := range st.Details() {
			if ed, ok := d.(*localpb.ErrorDetail); ok {
				detail = ed
			}
		}
		if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
			t.Fatalf("error code = %v, want quota_exceeded", detail.GetCode())
		}
		want := "cannot send registration events: more than 64 are waiting to be read"
		if got := detail.GetMessage(); got != want {
			t.Fatalf("message = %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Register did not end after the event backlog exceeded its limit")
	}
	if got := r.tb.sessions.ReleaseCount(); got != 1 {
		t.Fatalf("session backend released %d times on overflow, want 1", got)
	}

	assertOverflowLog(t, r.tb, id)
}

// TestRegisterOverflowLogNamesTheAgent pins the warning's agent field when the
// refused event names no agent. Releasing the blocked Send here lets a handler
// that waits for its sender still finish, so this test isolates the log field.
func TestRegisterOverflowLogNamesTheAgent(t *testing.T) {
	r, id := startStalledRegistration(t, "backlog-log")

	fillRegistrationBacklog(r)
	close(r.stream.sendRelease)

	select {
	case err := <-r.handlerErr:
		if err == nil {
			t.Fatal("Register returned nil, want quota_exceeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Register did not end after the event backlog exceeded its limit")
	}

	assertOverflowLog(t, r.tb, id)
}
