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

const overflowEventName = "register_events_overflow"

// maxQueuedEvents is the documented backlog limit, written here so a change to
// registrationEventLimit fails this test.
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

// TestEventQueueCoalescesAgentChanges checks that an agent_changed replaces an
// unsent one for the same agent while different agents stay separate.
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

// TestEventQueueKeepsOtherEvents checks that merging touches only a trailing
// agent_changed, leaving other events in order.
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

// TestEventQueueCapsBacklog checks that the queue refuses events past its limit
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
// test releases it, so events pushed while a Send is in flight pile up in the
// downstream queue.
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

type stalledRegistration struct {
	tb         *testBroker
	stream     *stalledRegisterStream
	conn       *regConn
	handlerErr chan error
}

// startStalledRegistration serves Register on a stream whose first Send blocks,
// and waits for the registration to attach. The Register goroutine cancels the
// stream context when the handler returns, as gRPC does when an RPC ends.
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
	case <-time.After(eventTimeout):
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

// fillRegistrationBacklog queues maxQueuedEvents events plus one more, which the
// queue refuses; the events name no agent, so only the connection can name the
// overflow.
func fillRegistrationBacklog(r *stalledRegistration) {
	for i := 0; i < maxQueuedEvents; i++ {
		r.conn.out.push(problemResponse())
	}
	r.conn.out.push(problemResponse())
}

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

// TestRegisterEndsStreamWhenEventBacklogExceedsLimit checks that an unread
// stream ends with quota_exceeded and one warning once its backlog passes the
// limit, without waiting for the blocked sender.
func TestRegisterEndsStreamWhenEventBacklogExceedsLimit(t *testing.T) {
	r, id := startStalledRegistration(t, "backlog")

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
	case <-time.After(eventTimeout):
		t.Fatal("Register did not end after the event backlog exceeded its limit")
	}
	if got := r.tb.sessions.ReleaseCount(); got != 1 {
		t.Fatalf("session backend released %d times on overflow, want 1", got)
	}

	assertOverflowLog(t, r.tb, id)
}

// TestRegisterOverflowLogNamesTheAgent checks that the overflow warning names the
// connection's agent when the refused event names no agent.
func TestRegisterOverflowLogNamesTheAgent(t *testing.T) {
	r, id := startStalledRegistration(t, "backlog-log")

	fillRegistrationBacklog(r)
	close(r.stream.sendRelease)

	select {
	case err := <-r.handlerErr:
		if err == nil {
			t.Fatal("Register returned nil, want quota_exceeded")
		}
	case <-time.After(eventTimeout):
		t.Fatal("Register did not end after the event backlog exceeded its limit")
	}

	assertOverflowLog(t, r.tb, id)
}
