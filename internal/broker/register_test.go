package broker

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// regStream is one open Register stream with a pump that is the only Recv
// caller, so tests can bound each wait independently.
type regStream struct {
	t      *testing.T
	stream localpb.Local_RegisterClient
	cancel context.CancelFunc
	events chan *localpb.RegisterResponse
	errs   chan error
}

func startRegistration(t *testing.T, tb *testBroker, fake *fakesession.Fake) *regStream {
	t.Helper()
	return startRegistrationAt(t, tb.client, fake.ConfigDir(), fake.SocketPath())
}

func startRegistrationAt(t *testing.T, client localpb.LocalClient, configDir, socketPath string) *regStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Register(ctx)
	if err != nil {
		cancel()
		t.Fatalf("Register: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{
		InboxSocketPath: socketPath,
		ClaudeConfigDir: configDir,
	}); err != nil {
		cancel()
		t.Fatalf("send the first registration frame: %v", err)
	}
	rs := &regStream{
		t:      t,
		stream: stream,
		cancel: cancel,
		events: make(chan *localpb.RegisterResponse, 16),
		errs:   make(chan error, 1),
	}
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case rs.errs <- err:
				default:
				}
				return
			}
			select {
			case rs.events <- resp:
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(cancel)
	return rs
}

func (rs *regStream) next(d time.Duration) *localpb.RegisterResponse {
	rs.t.Helper()
	select {
	case resp := <-rs.events:
		return resp
	case err := <-rs.errs:
		rs.t.Fatalf("registration stream ended: %v", err)
	case <-time.After(d):
		rs.t.Fatalf("no registration event within %s", d)
	}
	return nil
}

func (rs *regStream) complete(d time.Duration) *localpb.RegistrationComplete {
	rs.t.Helper()
	resp := rs.next(d)
	complete := resp.GetRegistrationComplete()
	if complete == nil {
		rs.t.Fatalf("registration event = %v, want registration_complete", resp)
	}
	return complete
}

func (rs *regStream) problemEvent(d time.Duration) *localpb.RegistrationProblem {
	rs.t.Helper()
	resp := rs.next(d)
	problem := resp.GetNotRegistered()
	if problem == nil {
		rs.t.Fatalf("registration event = %v, want not_registered", resp)
	}
	return problem
}

func (rs *regStream) problem(d time.Duration) localpb.NotRegisteredReason {
	rs.t.Helper()
	return rs.problemEvent(d).GetReason()
}

// expectNoEvent fails when the stream yields an event within d. The window is
// the assertion and is deliberately short: a negative window can only miss an
// event under load, never fail a correct run, so it is not a flake source.
func (rs *regStream) expectNoEvent(d time.Duration) {
	rs.t.Helper()
	select {
	case resp := <-rs.events:
		rs.t.Fatalf("unexpected registration event %v", resp)
	case err := <-rs.errs:
		rs.t.Fatalf("registration stream ended unexpectedly: %v", err)
	case <-time.After(d):
	}
}

func (rs *regStream) closeErr(d time.Duration) error {
	rs.t.Helper()
	select {
	case err := <-rs.errs:
		return err
	case resp := <-rs.events:
		rs.t.Fatalf("unexpected registration event %v", resp)
	case <-time.After(d):
		rs.t.Fatalf("registration stream did not end within %s", d)
	}
	return nil
}

func waitForReg(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ap-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func mustAgentID(t *testing.T, socketPath string) AgentID {
	t.Helper()
	id, _, err := AgentIDFor(socketPath)
	if err != nil {
		t.Fatalf("AgentIDFor(%q): %v", socketPath, err)
	}
	return id
}

func errorCode(err error) localpb.ErrorCode {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
}

func (tb *testBroker) lockedWaitingCount() int {
	tb.b.mu.Lock()
	defer tb.b.mu.Unlock()
	return len(tb.b.waiting)
}

func (tb *testBroker) lockedWatchCount() int {
	tb.b.mu.Lock()
	defer tb.b.mu.Unlock()
	return len(tb.b.watches)
}

func (tb *testBroker) lockedAgentCount() int {
	tb.b.mu.Lock()
	defer tb.b.mu.Unlock()
	return len(tb.b.agents)
}

func (tb *testBroker) lockedConnCount(id AgentID) int {
	tb.b.mu.Lock()
	defer tb.b.mu.Unlock()
	a, ok := tb.b.agents[id]
	if !ok {
		return 0
	}
	return len(a.conns)
}

func drainNotify(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func TestRegisterCompletes(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	rs := startRegistration(t, tb, fake)

	complete := rs.complete(eventTimeout)
	if complete.GetAgentId() != string(mustAgentID(t, fake.SocketPath())) {
		t.Fatalf("agent_id = %q, want the ID of %q", complete.GetAgentId(), fake.SocketPath())
	}
	if complete.GetAgentAddress() != "api-worker@hostA" {
		t.Fatalf("agent_address = %q, want api-worker@hostA", complete.GetAgentAddress())
	}
	if !complete.GetAutoJoin() {
		t.Fatal("auto_join = false, want true")
	}
	if n := len(tb.topics.AgentChangedCalls()); n != 1 {
		t.Fatalf("AgentChanged calls = %d, want 1", n)
	}
}

func TestRegisterIntroducesAgentBeforeComplete(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "joiner"})
	rs := startRegistration(t, tb, fake)
	complete := rs.complete(eventTimeout)

	if err := tb.topics.Join(AgentID(complete.GetAgentId()), "t1"); err != nil {
		t.Fatalf("Join right after registration_complete: %v, want nil", err)
	}

	t.Run("unknown agent", func(t *testing.T) {
		if err := tb.topics.Join(testAgentID("cccccc0"), "t1"); !errors.Is(err, ErrUnknownAgent) {
			t.Fatalf("Join for an agent that never registered = %v, want ErrUnknownAgent", err)
		}
	})
}

func TestRegisterHoldsOneAcquire(t *testing.T) {
	t.Run("one acquire per stream", func(t *testing.T) {
		tb := newTestBroker(t)
		fake := fakesession.New(t, fakesession.Options{Name: "holder"})
		rs := startRegistration(t, tb, fake)
		rs.complete(eventTimeout)

		uids := tb.sessions.AcquiredUIDs()
		if len(uids) != 1 {
			t.Fatalf("Acquire calls = %d, want 1", len(uids))
		}
		if uids[0] != uint32(os.Geteuid()) {
			t.Fatalf("Acquire uid = %d, want %d", uids[0], os.Geteuid())
		}

		rs.cancel()
		waitForReg(t, "the stream to release its backend", func() bool {
			return tb.sessions.ReleaseCount() == 1
		})
	})

	t.Run("acquire failure", func(t *testing.T) {
		sessions := &fakeSessions{acquireErr: errors.New("no helper process")}
		tb := newTestBroker(t, func(d *Deps) { d.Sessions = sessions })
		fake := fakesession.New(t, fakesession.Options{Name: "homeless"})
		rs := startRegistration(t, tb, fake)

		err := rs.closeErr(eventTimeout)
		if status.Code(err) != codes.Internal {
			t.Fatalf("register error = %v (code %v), want Internal", err, status.Code(err))
		}
	})
}

func TestRegisterNormalizesName(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: " API Worker "})
	rs := startRegistration(t, tb, fake)

	complete := rs.complete(eventTimeout)
	if complete.GetAgentAddress() != "api-worker@hostA" {
		t.Fatalf("agent_address = %q, want api-worker@hostA", complete.GetAgentAddress())
	}
}

func TestRegisterAutoJoinFromConfig(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.setValue("topics.auto_join", config.Value{Kind: config.KindBool, Bool: false})

	fake := fakesession.New(t, fakesession.Options{Name: "manual"})
	rs := startRegistration(t, tb, fake)
	complete := rs.complete(eventTimeout)
	if complete.GetAutoJoin() {
		t.Fatal("auto_join = true, want false")
	}
}

func TestRegisterEmptySocketPathReportsAtOnce(t *testing.T) {
	tb := newTestBroker(t)
	rs := startRegistrationAt(t, tb.client, shortTempDir(t), "")

	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("reason = %v, want socket_path_missing", reason)
	}
}

func TestRegisterInvalidSocketPathWaits(t *testing.T) {
	for _, socketPath := range []string{"rel/s.sock", "/tmp/../s.sock"} {
		t.Run(socketPath, func(t *testing.T) {
			tb := newTestBroker(t)
			rs := startRegistrationAt(t, tb.client, shortTempDir(t), socketPath)

			waitForReg(t, "the connection to wait", func() bool {
				return tb.lockedWaitingCount() == 1
			})
			rs.expectNoEvent(300 * time.Millisecond)

			tb.clock.Advance(10 * time.Second)
			if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
				t.Fatalf("reason = %v, want session_record_not_found", reason)
			}

			// The stream stays open: another call on the same connection works.
			if _, err := tb.client.Hello(context.Background(), &localpb.HelloRequest{ProtocolVersion: 1}); err != nil {
				t.Fatalf("Hello after not_registered: %v", err)
			}
			if n := tb.lockedWatchCount(); n != 0 {
				t.Fatalf("watches = %d, want 0 for an impossible socket path", n)
			}
		})
	}
}

func TestRegisterWaitsThenReportsMissingRecord(t *testing.T) {
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	tb := newTestBroker(t)
	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	tb.clock.Advance(10 * time.Second)
	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND {
		t.Fatalf("reason = %v, want session_record_not_found", reason)
	}

	fakesession.New(t, fakesession.Options{ConfigDir: dir, SocketPath: socketPath, Name: "late"})
	complete := rs.complete(eventTimeout)
	if complete.GetAgentAddress() != "late@hostA" {
		t.Fatalf("agent_address = %q, want late@hostA", complete.GetAgentAddress())
	}
}

func TestRegisterUnusableName(t *testing.T) {
	for _, name := range []string{"a@b", "*x", "", "\"<>\""} {
		t.Run(name, func(t *testing.T) {
			tb := newTestBroker(t)
			fake := fakesession.New(t, fakesession.Options{Name: name})
			rs := startRegistration(t, tb, fake)

			waitForReg(t, "the connection to wait", func() bool {
				return tb.lockedWaitingCount() == 1
			})
			tb.clock.Advance(10 * time.Second)
			if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE {
				t.Fatalf("reason = %v, want name_unusable", reason)
			}
			if calls := tb.topics.Calls(); len(calls) != 0 {
				t.Fatalf("topic calls before the name became usable = %v, want none", calls)
			}

			fake.SetName("ab")
			complete := rs.complete(eventTimeout)
			if complete.GetAgentAddress() != "ab@hostA" {
				t.Fatalf("agent_address = %q, want ab@hostA", complete.GetAgentAddress())
			}
		})
	}
}

// TestRegisterUnusableNameDetail checks that a not_registered event with the
// NAME_UNUSABLE reason names what made the name unusable, not just the enum.
func TestRegisterUnusableNameDetail(t *testing.T) {
	tests := []struct {
		name   string
		detail string
	}{
		{"", "the session name is empty"},
		{"a@b", `the session name contains "@"`},
		{"*x", `the session name contains "*"`},
		{"\"<>\"", "the session name has no characters a receiver can read"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := newTestBroker(t)
			fake := fakesession.New(t, fakesession.Options{Name: tt.name})
			rs := startRegistration(t, tb, fake)

			waitForReg(t, "the connection to wait", func() bool {
				return tb.lockedWaitingCount() == 1
			})
			tb.clock.Advance(10 * time.Second)
			problem := rs.problemEvent(eventTimeout)
			if problem.GetReason() != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE {
				t.Fatalf("reason = %v, want name_unusable", problem.GetReason())
			}
			if got := problem.GetDetail(); got != tt.detail {
				t.Fatalf("detail = %q, want %q", got, tt.detail)
			}
		})
	}
}

// blockingLookupBackend makes the first Lookup wait until release is closed, so
// a test can fire the wait timer while the first attempt is still inside the
// registry lookup.
type blockingLookupBackend struct {
	SessionBackend
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingLookupBackend) Lookup(ctx context.Context, configDir, socketPath string) (inbox.Session, error) {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	return b.SessionBackend.Lookup(ctx, configDir, socketPath)
}

// blockingSessions hands out one blockingLookupBackend wrapping the real one.
type blockingSessions struct {
	SessionRouter
	backend *blockingLookupBackend
}

func (s *blockingSessions) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := s.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	s.backend.SessionBackend = base
	return s.backend, release, nil
}

// TestRegisterWaitTimerCarriesFirstAttemptReason pins the ordering between the
// first attempt and the wait timer: when the timer fires before the attempt has
// stored a reason, the eventual report must still carry the reason the attempt
// found, never the zero NotRegisteredReason.
func TestRegisterWaitTimerCarriesFirstAttemptReason(t *testing.T) {
	blocked := &blockingLookupBackend{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = &blockingSessions{SessionRouter: d.Sessions, backend: blocked}
	})
	fake := fakesession.New(t, fakesession.Options{Name: "a@b"})
	rs := startRegistration(t, tb, fake)

	select {
	case <-blocked.started:
	case <-time.After(eventTimeout):
		t.Fatal("the first attempt did not reach the registry lookup")
	}

	// The wait timer fires while the first attempt is still inside the lookup.
	// It may not report a reason that no attempt has established.
	tb.clock.Advance(10 * time.Second)
	close(blocked.release)

	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE {
		t.Fatalf("reason = %v, want name_unusable", reason)
	}
}

func TestRegisterSocketNotOwned(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) {
		d.Geteuid = func() int { return 4242 }
		d.ReadPeerCred = func(conn net.Conn) (Caller, error) {
			caller, err := ReadPeerCred(conn)
			caller.UID = 4242
			return caller, err
		}
	})
	fake := fakesession.New(t, fakesession.Options{Name: "not-mine"})
	rs := startRegistration(t, tb, fake)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	tb.clock.Advance(10 * time.Second)
	if reason := rs.problem(eventTimeout); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED {
		t.Fatalf("reason = %v, want socket_not_owned", reason)
	}
}

// TestRegisterSameSocketSharesAgent checks that several connections on one
// socket path share one agent.
//
// REQ-agent-messaging-035: a sub-agent's connection joins its parent's agent.
// REQ-topics-022: the last connection closing removes the agent and its topic
// membership.
func TestRegisterSameSocketSharesAgent(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "shared"})

	first := startRegistration(t, tb, fake)
	firstComplete := first.complete(eventTimeout)
	second := startRegistrationAt(t, tb.newClient(t), fake.ConfigDir(), fake.SocketPath())
	secondComplete := second.complete(eventTimeout)

	if firstComplete.GetAgentId() != secondComplete.GetAgentId() {
		t.Fatalf("agent ids differ: %q and %q", firstComplete.GetAgentId(), secondComplete.GetAgentId())
	}
	id := mustAgentID(t, fake.SocketPath())
	waitForReg(t, "both connections on one agent", func() bool {
		return tb.lockedAgentCount() == 1 && tb.lockedConnCount(id) == 2
	})

	drainNotify(tb.topics.notify)

	first.cancel()
	waitForReg(t, "the first connection to be released", func() bool {
		return tb.sessions.ReleaseCount() == 1
	})
	if n := tb.lockedAgentCount(); n != 1 {
		t.Fatalf("agents after closing one of two connections = %d, want 1", n)
	}
	if calls := tb.topics.LeaveAllCalls(); len(calls) != 0 {
		t.Fatalf("LeaveAll calls after one connection closed = %d, want 0", len(calls))
	}

	second.cancel()
	select {
	case <-tb.topics.notify:
	case <-time.After(eventTimeout):
		t.Fatal("no topic call after the last connection closed")
	}
	waitForReg(t, "the agent to be unregistered", func() bool {
		return tb.lockedAgentCount() == 0
	})
	if calls := tb.topics.LeaveAllCalls(); len(calls) != 1 {
		t.Fatalf("LeaveAll calls = %d, want 1", len(calls))
	}
}

// REQ-agent-messaging-034: disabling the MCP server unregisters the agent, and
// re-enabling it on the same socket path gets the same identity.
func TestReRegisterGetsSameID(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "again"})

	first := startRegistration(t, tb, fake)
	firstComplete := first.complete(eventTimeout)
	id := AgentID(firstComplete.GetAgentId())

	if err := tb.topics.Join(id, "t1"); err != nil {
		t.Fatalf("Join: %v", err)
	}
	first.cancel()
	waitForReg(t, "the first registration to end", func() bool {
		return tb.lockedAgentCount() == 0 && tb.sessions.ReleaseCount() == 1
	})
	if topics := tb.topics.TopicsOf(id); len(topics) != 0 {
		t.Fatalf("topics after unregistration = %v, want none", topics)
	}

	second := startRegistration(t, tb, fake)
	secondComplete := second.complete(eventTimeout)
	if secondComplete.GetAgentId() != string(id) {
		t.Fatalf("agent_id after re-registration = %q, want %q", secondComplete.GetAgentId(), id)
	}
	if topics := tb.topics.TopicsOf(id); len(topics) != 0 {
		t.Fatalf("topics on the fresh agent = %v, want none", topics)
	}
}

func TestRegisterIgnoresLaterUpstreamFrames(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "first"})
	rs := startRegistration(t, tb, fake)
	firstComplete := rs.complete(eventTimeout)

	if err := rs.stream.Send(&localpb.RegisterRequest{
		InboxSocketPath: "/tmp/another.sock",
		ClaudeConfigDir: fake.ConfigDir(),
	}); err != nil {
		t.Fatalf("send a later frame: %v", err)
	}

	fake.SetStatus("busy")
	select {
	case <-tb.topics.notify:
	case <-time.After(eventTimeout):
		t.Fatal("the status change did not reach the topic table")
	}

	if n := tb.lockedAgentCount(); n != 1 {
		t.Fatalf("agents = %d, want 1", n)
	}
	if _, ok := tb.b.lookupAgent(mustAgentID(t, fake.SocketPath())); !ok {
		t.Fatal("the original agent disappeared")
	}
	if id := mustAgentID(t, fake.SocketPath()); id != AgentID(firstComplete.GetAgentId()) {
		t.Fatalf("agent id changed to %s, want %s", id, firstComplete.GetAgentId())
	}
}

// REQ-agent-messaging-036: a headless session registers like any other; fields
// such as kind do not affect registration.
func TestRegisterHeadlessSession(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{
		Name:  "headless",
		Extra: map[string]any{"kind": "background", "entrypoint": "sdk-cli"},
	})
	rs := startRegistration(t, tb, fake)

	complete := rs.complete(eventTimeout)
	if complete.GetAgentAddress() != "headless@hostA" {
		t.Fatalf("agent_address = %q, want headless@hostA", complete.GetAgentAddress())
	}
}

// REQ-agent-messaging-012: a session that ends leaves the agent list within
// five seconds (the wait below allows three), and
// REQ-broker-config-014: the Broker keeps serving with an empty agent table.
func TestBrokerKeepsRunningWithEmptyTable(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "transient"})
	rs := startRegistration(t, tb, fake)
	rs.complete(eventTimeout)
	rs.cancel()

	waitForReg(t, "the agent table to empty", func() bool {
		return tb.lockedAgentCount() == 0
	})
	if _, err := tb.client.Hello(context.Background(), &localpb.HelloRequest{ProtocolVersion: 1}); err != nil {
		t.Fatalf("Hello with an empty agent table: %v", err)
	}
}

func TestWaitingConnReleasesWatch(t *testing.T) {
	dir := shortTempDir(t)
	socketPath := filepath.Join(dir, "s.sock")
	tb := newTestBroker(t)
	rs := startRegistrationAt(t, tb.client, dir, socketPath)

	waitForReg(t, "the watch to be taken", func() bool {
		return tb.lockedWatchCount() == 1
	})
	rs.cancel()
	waitForReg(t, "the watch to be released", func() bool {
		return tb.lockedWatchCount() == 0
	})
}

func TestConcurrentRegisterAndDisconnect(t *testing.T) {
	const n = 20
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "crowd"})

	streams := make([]*regStream, n)
	for i := range streams {
		streams[i] = startRegistration(t, tb, fake)
	}
	for _, rs := range streams {
		rs.complete(eventTimeout)
	}

	id := mustAgentID(t, fake.SocketPath())
	waitForReg(t, "all connections to attach", func() bool {
		return tb.lockedAgentCount() == 1 && tb.lockedConnCount(id) == n
	})

	for _, rs := range streams {
		rs.cancel()
	}
	waitForReg(t, "all connections to disconnect", func() bool {
		return tb.lockedAgentCount() == 0 && tb.sessions.ReleaseCount() == n
	})
	if calls := tb.topics.LeaveAllCalls(); len(calls) != 1 {
		t.Fatalf("LeaveAll calls = %d, want 1", len(calls))
	}
}
