package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// loopTestLogger discards everything; the loop only logs when recovering from
// a panic or when a restoration fails.
func loopTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// loopWaitLimit bounds every wait for an observable loop event. Each wait ends
// on an event (a Register stream, a status change, or a fake-clock request),
// not on elapsed time, so the bound is a safety net that turns a stuck loop
// into a test failure rather than a hang. It is helloTimeout plus the package
// eventTimeout: a Hello that hangs reaches helloTimeout, and the test then has
// the whole event fallback before its own bound, so a loaded runner cannot fail
// both at once. The fallback keeps the failure report about the loop instead of
// racing the product's own timeout.
const loopWaitLimit = helloTimeout + eventTimeout

// loopClock replaces time.After in the loop. Every wait is recorded, and the
// test drives it with advance or autoAdvance. An advance that arrives before
// the wait is remembered and fires the next wait at once. It also closes a
// changed channel whenever a wait is recorded, so a test can wait for that
// event instead of polling the recorded waits on a wall-clock deadline.
type loopClock struct {
	mu      sync.Mutex
	waits   []time.Duration
	pending chan time.Time
	queued  int
	auto    bool
	changed chan struct{}
}

func newLoopClock() *loopClock {
	return &loopClock{changed: make(chan struct{})}
}

func (c *loopClock) after(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waits = append(c.waits, d)
	ch := make(chan time.Time, 1)
	switch {
	case c.queued > 0:
		c.queued--
		ch <- time.Now()
	case c.auto:
		ch <- time.Now()
	default:
		c.pending = ch
	}
	close(c.changed)
	c.changed = make(chan struct{})
	return ch
}

// advance releases the pending wait, or queues one tick for the next wait.
func (c *loopClock) advance() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending != nil {
		c.pending <- time.Now()
		c.pending = nil
		return
	}
	c.queued++
}

// autoAdvance releases the pending wait and every later wait as soon as the
// loop requests it. A test that only needs the loop to reconnect uses this
// instead of sequencing every wait, so it never has to observe the loop's
// internal progress before it can let the loop proceed.
func (c *loopClock) autoAdvance() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.auto = true
	if c.pending != nil {
		c.pending <- time.Now()
		c.pending = nil
	}
}

func (c *loopClock) durations() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.waits))
	copy(out, c.waits)
	return out
}

// loopWaitCount waits until the loop has requested n waits and returns them.
// It ends on the clock's notification, not on a poll interval.
func loopWaitCount(t *testing.T, c *loopClock, n int) []time.Duration {
	t.Helper()
	deadline := time.NewTimer(loopWaitLimit)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		if len(c.waits) >= n {
			out := append([]time.Duration(nil), c.waits...)
			c.mu.Unlock()
			return out
		}
		changed := c.changed
		c.mu.Unlock()

		select {
		case <-changed:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d backoff waits; saw %v", n, c.durations())
			return nil
		}
	}
}

// loopWaitStatus polls the status store until cond holds.
func loopWaitStatus(t *testing.T, st *statusStore, what string, cond func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(loopWaitLimit)
	for time.Now().Before(deadline) {
		s := st.Get()
		if cond(s) {
			return s
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; last status %+v", what, st.Get())
	return Status{}
}

// loopNextRegister waits for the next Register stream on srv.
func loopNextRegister(t *testing.T, srv *fakelocal.Server) *fakelocal.RegisterConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), loopWaitLimit)
	defer cancel()
	conn, err := srv.NextRegister(ctx)
	if err != nil {
		t.Fatalf("waiting for a Register stream: %v", err)
	}
	return conn
}

// loopAwaitString waits for one value on ch.
func loopAwaitString(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(loopWaitLimit):
		t.Fatalf("timed out waiting for %s", what)
		return ""
	}
}

// loopCountCalls counts the unary calls recorded under method.
func loopCountCalls(srv *fakelocal.Server, method string) int {
	n := 0
	for _, c := range srv.Calls() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// loopWaitCalls waits until calls() reaches want.
func loopWaitCalls(t *testing.T, calls func() int, want int, what string) {
	t.Helper()
	deadline := time.Now().Add(loopWaitLimit)
	for time.Now().Before(deadline) {
		if calls() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to be called %d times; saw %d", what, want, calls())
}

// loopRetryPaused asserts that, with the injected timer held, the loop has
// made exactly one call to what and is blocked on the clock. The first request
// for a timer is the barrier: reaching it means the loop made its one call and
// then blocked, and a loop that retried without waiting would never get there.
// Waiting for the barrier instead of sleeping keeps the assertion free of a
// fixed settle window. It then releases the first wait and checks that the
// following call happens, so the pause is a wait rather than a dead end.
func loopRetryPaused(t *testing.T, calls func() int, what string, clock *loopClock) {
	t.Helper()
	waits := loopWaitCount(t, clock, 1)
	if got := calls(); got != 1 {
		t.Fatalf("%s calls when the loop paused on the timer = %d, want 1", what, got)
	}
	if waits[0] != 250*time.Millisecond {
		t.Fatalf("first wait = %v, want 250ms", waits[0])
	}
	clock.advance()
	loopWaitCalls(t, calls, 2, what)
}

// loopBaseDeps builds dependencies that use the real dial, handshake and
// registration implementation. Every test stubs the selection, and each may
// replace the clock, onComplete, register or waitNotReady.
func loopBaseDeps(st *statusStore, clock *loopClock) loopDeps {
	return loopDeps{
		probe:        probeSocket,
		dial:         dialLocal,
		hello:        hello,
		waitNotReady: waitNotReady,
		version:      localpb.ProtocolVersion,
		register:     runRegistration,
		first:        &localpb.RegisterRequest{InboxSocketPath: "/s", ClaudeConfigDir: "/c"},
		st:           st,
		onComplete:   func(context.Context, localpb.LocalClient, string, bool) {},
		after:        clock.after,
		log:          loopTestLogger(),
	}
}

// loopStart runs runLoop in the background. It returns a stop function that
// cancels the loop and waits for it, also registered as a cleanup.
func loopStart(t *testing.T, deps loopDeps) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runLoop(ctx, deps)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(loopWaitLimit):
				t.Errorf("runLoop did not return within %v of cancelling its context", loopWaitLimit)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// loopClosingSocket listens on path and closes every accepted connection at
// once. This is the shape of a refusal at grpc-go's server handshake: the
// connection is closed before any RPC frame, so the client gets a transport
// error and its ClientConn never becomes READY, unlike a Hello the handler
// answers with a status of its own.
func loopClosingSocket(t *testing.T, path string) {
	t.Helper()
	lis, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listening on %s: %v", path, err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
}

// TestRunLoop exercises the background connection loop against a fake Local
// service and against raw sockets.
func TestRunLoop(t *testing.T) {
	t.Run("user reconnects to the same Broker and registers again", func(t *testing.T) {
		// REQ-broker-config-037, REQ-topics-022
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})
		completed := make(chan string, 8)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		deps.onComplete = func(_ context.Context, _ localpb.LocalClient, agentID string, _ bool) {
			completed <- agentID
		}
		stop := loopStart(t, deps)

		first := loopNextRegister(t, srv)
		if got := first.First.GetInboxSocketPath(); got != "/s" {
			t.Fatalf("first frame inbox_socket_path = %q, want %q", got, "/s")
		}
		if err := first.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		if got := loopAwaitString(t, completed, "the first completion"); got != "id1" {
			t.Fatalf("onComplete agent ID = %q, want id1", got)
		}

		srv.Stop()

		// Start the replacement and let the fake clock release the reconnect
		// wait without first waiting for the loop to request that wait. The
		// clock fires a wait as soon as it is asked for, so the only real-time
		// wait below is for the Register stream itself.
		replacement := fakelocal.Start(t, path, fakelocal.Handlers{})
		clock.autoAdvance()

		second := loopNextRegister(t, replacement)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("reconnect waits before the second Register = %v, want the first one to be 250ms", waits)
		}
		if got, want := second.First.GetInboxSocketPath(), first.First.GetInboxSocketPath(); got != want {
			t.Errorf("second first frame inbox_socket_path = %q, want %q", got, want)
		}
		if got, want := second.First.GetClaudeConfigDir(), first.First.GetClaudeConfigDir(); got != want {
			t.Errorf("second first frame claude_config_dir = %q, want %q", got, want)
		}
		if err := second.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending the second registration_complete: %v", err)
		}
		if got := loopAwaitString(t, completed, "the second completion"); got != "id1" {
			t.Fatalf("second onComplete agent ID = %q, want id1", got)
		}
		stop()
	})

	t.Run("a reconnect retries after a transient dial failure", func(t *testing.T) {
		// A loaded runner can fail the first dial after a reconnect wait. The
		// test must release the following wait as well instead of assuming one
		// attempt is enough; otherwise the Register stream is still correct
		// but the test times out waiting for it.
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})
		completed := make(chan string, 8)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		var dials atomic.Int32
		baseDial := deps.dial
		deps.dial = func(path string) (*grpc.ClientConn, localpb.LocalClient, error) {
			if dials.Add(1) == 2 {
				return nil, nil, errors.New("transient dial failure")
			}
			return baseDial(path)
		}
		deps.onComplete = func(_ context.Context, _ localpb.LocalClient, agentID string, _ bool) {
			completed <- agentID
		}
		stop := loopStart(t, deps)

		first := loopNextRegister(t, srv)
		if err := first.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		if got := loopAwaitString(t, completed, "the first completion"); got != "id1" {
			t.Fatalf("onComplete agent ID = %q, want id1", got)
		}

		srv.Stop()
		replacement := fakelocal.Start(t, path, fakelocal.Handlers{})
		clock.autoAdvance()

		second := loopNextRegister(t, replacement)
		if waits := clock.durations(); len(waits) < 2 ||
			waits[0] != 250*time.Millisecond || waits[1] != 500*time.Millisecond {
			t.Fatalf("waits before the second Register = %v, want 250ms then 500ms", waits)
		}
		if err := second.Complete("id2", "me@h", false); err != nil {
			t.Fatalf("sending the second registration_complete: %v", err)
		}
		stop()
	})

	t.Run("user reconnect never selects a different Broker", func(t *testing.T) {
		// REQ-broker-config-037
		userPath := fakelocal.SocketPath(t)
		systemPath := fakelocal.SocketPath(t)
		userSrv := fakelocal.Start(t, userPath, fakelocal.Handlers{})
		systemSrv := fakelocal.Start(t, systemPath, fakelocal.Handlers{})
		var selectCalls atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			selectCalls.Add(1)
			return target{Path: userPath, Kind: kindUser}, nil
		}
		stop := loopStart(t, deps)

		first := loopNextRegister(t, userSrv)
		if err := first.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		userSrv.Stop()

		replacement := fakelocal.Start(t, userPath, fakelocal.Handlers{})
		clock.autoAdvance()

		second := loopNextRegister(t, replacement)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("reconnect waits before the second Register = %v, want the first one to be 250ms", waits)
		}
		if err := second.Complete("id2", "me@h", false); err != nil {
			t.Fatalf("sending the second registration_complete: %v", err)
		}

		// The second registration is the later event that proves the loop never
		// fell through to the system Broker or selected again.
		if calls := systemSrv.Calls(); len(calls) != 0 {
			t.Errorf("the system Broker received %d calls, want none: %v", len(calls), calls)
		}
		if got := selectCalls.Load(); got != 1 {
			t.Errorf("selection ran %d times, want once at startup", got)
		}
		stop()
	})

	t.Run("system Broker is retried, never replaced", func(t *testing.T) {
		// design:108
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})
		var selectCalls atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			selectCalls.Add(1)
			return target{Path: path, Kind: kindSystem}, nil
		}
		stop := loopStart(t, deps)

		first := loopNextRegister(t, srv)
		if err := first.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		srv.Stop()

		replacement := fakelocal.Start(t, path, fakelocal.Handlers{})
		clock.autoAdvance()

		second := loopNextRegister(t, replacement)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("reconnect waits before the second Register = %v, want the first one to be 250ms", waits)
		}
		if got, want := second.First.GetInboxSocketPath(), first.First.GetInboxSocketPath(); got != want {
			t.Errorf("first frame after the restart = %q, want %q", got, want)
		}
		if got := selectCalls.Load(); got != 1 {
			t.Errorf("selection ran %d times, want once at startup", got)
		}
		stop()
	})

	t.Run("a test-named Broker unavailable then up", func(t *testing.T) {
		// REQ-agent-messaging-038
		path := fakelocal.SocketPath(t)
		var selectCalls atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			if selectCalls.Add(1) == 1 {
				return target{}, fmt.Errorf("the socket %s named by the test hook is unavailable (missing)", path)
			}
			return target{Path: path, Kind: kindTest}, nil
		}
		stop := loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseUnavailable", func(s Status) bool {
			return s.Phase == PhaseUnavailable
		})
		if !strings.Contains(status.Detail, path) {
			t.Fatalf("Detail = %q, want it to name %s", status.Detail, path)
		}
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})
		clock.autoAdvance()

		conn := loopNextRegister(t, srv)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("reconnect waits before the Register = %v, want the first one to be 250ms", waits)
		}
		if err := conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		loopWaitStatus(t, st, "the registered agent", func(s Status) bool { return s.AgentID == "id1" })
		stop()
	})

	t.Run("no available Broker enters PhaseNoBroker and retries the selection", func(t *testing.T) {
		// REQ-broker-config-038
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})
		var selectCalls atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			if selectCalls.Add(1) == 1 {
				return target{}, errNoBroker
			}
			return target{Path: path, Kind: kindUser}, nil
		}
		stop := loopStart(t, deps)

		loopWaitStatus(t, st, "PhaseNoBroker", func(s Status) bool {
			return s.Phase == PhaseNoBroker
		})

		// The retry uses the connection backoff; there is no separate launch
		// backoff to wait through.
		clock.autoAdvance()

		conn := loopNextRegister(t, srv)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("first wait after no Broker = %v, want 250ms", waits)
		}
		if err := conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		if got := selectCalls.Load(); got != 2 {
			t.Errorf("selection ran %d times, want 2", got)
		}
		stop()
	})

	t.Run("a user Broker whose socket is gone reports no Broker", func(t *testing.T) {
		// REQ-broker-config-037: the tool error is the same no-Broker text as a
		// session that never found one.
		path := fakelocal.SocketPath(t)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		deps.probe = func(context.Context, string) probeResult { return probeMissing }
		loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseNoBroker", func(s Status) bool {
			return s.Phase == PhaseNoBroker
		})
		if status.Detail != "" {
			t.Fatalf("Detail = %q, want the general no-Broker text", status.Detail)
		}
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for a gone user Broker")
		}
		for _, want := range []string{"no Broker is available", "ask the administrator", "peers service install", "peers service run"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %q, want it to contain %q", err, want)
			}
		}
		waits := loopWaitCount(t, clock, 1)
		if waits[0] != 250*time.Millisecond {
			t.Fatalf("first reconnect wait = %v, want 250ms", waits[0])
		}
	})

	t.Run("a system Broker that fails reports no system Broker", func(t *testing.T) {
		// REQ-broker-config-037: a session that was using the system Broker
		// must be sent to the administrator, not to a user Broker.
		path := fakelocal.SocketPath(t)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindSystem}, nil
		}
		loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseNoBroker", func(s Status) bool {
			return s.Phase == PhaseNoBroker
		})
		if status.Detail != textNoSystemBroker {
			t.Fatalf("Detail = %q, want %q", status.Detail, textNoSystemBroker)
		}
		if strings.Contains(status.Detail, "peers service install") {
			t.Fatalf("Detail = %q, must not send the user to a user service", status.Detail)
		}
		// A socket that is missing keeps the wording for no Broker, so the
		// refusal text a running Broker produces cannot leak here.
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for a missing system socket")
		}
		if !strings.Contains(err.Error(), "ask the administrator to start it") {
			t.Fatalf("error = %q, want the no-Broker wording", err)
		}
		if strings.Contains(err.Error(), textBrokerRefusedConnection) {
			t.Fatalf("error = %q, want no refusal wording for a missing socket", err)
		}
		waits := loopWaitCount(t, clock, 1)
		if waits[0] != 250*time.Millisecond {
			t.Fatalf("first reconnect wait = %v, want 250ms", waits[0])
		}
	})

	t.Run("a registration refused by the Broker passes its sentence to the model", func(t *testing.T) {
		// The account is at its registration limit while the Broker is
		// running, so the tool error quotes the Broker and never sends the
		// reader to start a Broker that is already running.
		path := fakelocal.SocketPath(t)
		fakelocal.Start(t, path, fakelocal.Handlers{})
		const brokerText = "cannot register: account alice already has 1 open registrations"

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindSystem}, nil
		}
		deps.register = func(context.Context, localpb.LocalClient, *localpb.RegisterRequest,
			*statusStore, completeFunc, *slog.Logger) error {
			return fakelocal.ErrorStatus(codes.ResourceExhausted, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
				Message: brokerText,
			})
		}
		loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseBrokerRejected", func(s Status) bool {
			return s.Phase == PhaseBrokerRejected
		})
		if status.RejectCode != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
			t.Fatalf("RejectCode = %v, want QUOTA_EXCEEDED", status.RejectCode)
		}
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for a refused registration")
		}
		want := "list_agents failed: " + brokerText + " (code: quota_exceeded)"
		if got := err.Error(); got != want {
			t.Fatalf("error = %q, want %q", got, want)
		}
		if strings.Contains(err.Error(), "ask the administrator to start it") {
			t.Fatalf("error = %q, want no advice to start a running Broker", err)
		}
	})

	t.Run("a system Broker that closes the handshake is reported as running", func(t *testing.T) {
		// A connection limit enforced during the credential handshake closes
		// the connection before any ErrorDetail can travel, so the text names
		// a likely cause without claiming the Broker is gone. A listener that
		// accepts and closes reproduces that path: the ClientConn never
		// becomes READY, unlike a Hello the handler answers with a status.
		path := fakelocal.SocketPath(t)
		loopClosingSocket(t, path)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindSystem}, nil
		}
		loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseBrokerRejected", func(s Status) bool {
			return s.Phase == PhaseBrokerRejected
		})
		const refusedText = "the Broker is running but refused this connection; this account or the whole host may have reached its connection limit, so retry shortly."
		if status.Detail != refusedText {
			t.Fatalf("Detail = %q, want %q", status.Detail, refusedText)
		}
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for a refused handshake")
		}
		want := "list_agents failed: " + refusedText + " (code: internal)"
		if got := err.Error(); got != want {
			t.Fatalf("error = %q, want %q", got, want)
		}
		for _, unwanted := range []string{"ask the administrator to start it", "no Broker is available"} {
			if strings.Contains(err.Error(), unwanted) {
				t.Fatalf("error = %q, want no %q", err, unwanted)
			}
		}
	})

	t.Run("a user Broker whose socket refuses connections reports no Broker", func(t *testing.T) {
		// REQ-broker-config-037: a refused connection means no Broker is
		// listening, so the tool error must say how to start one. Only a socket
		// that answers and then fails is reported as unavailable.
		path := fakelocal.SocketPath(t)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		deps.probe = func(context.Context, string) probeResult { return probeRefused }
		loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseNoBroker", func(s Status) bool {
			return s.Phase == PhaseNoBroker
		})
		if status.Detail != "" {
			t.Fatalf("Detail = %q, want the general no-Broker text", status.Detail)
		}
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for a refused user socket")
		}
		for _, want := range []string{"no Broker is available", "peers service install", "peers service run"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %q, want it to contain %q", err, want)
			}
		}
		waits := loopWaitCount(t, clock, 1)
		if waits[0] != 250*time.Millisecond {
			t.Fatalf("first reconnect wait = %v, want 250ms", waits[0])
		}
	})

	// A Hello failure only becomes the refusal text when the probe confirms a
	// socket answers and the handshake itself was closed. A probe that reports
	// another account serving the socket, a denied connection, or any other
	// failure is not a refusal: the phase and text stay what they were before
	// the refusal text existed, with the original error.
	t.Run("a Hello failure without a confirmed socket keeps the unavailable text", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			probe probeResult
		}{
			{"the socket is served by another account", probeWrongPeer},
			{"the account may not connect", probeDenied},
			{"the probe failed another way", probeOther},
		} {
			probe := tc.probe
			t.Run(tc.name, func(t *testing.T) {
				path := fakelocal.SocketPath(t)
				const helloErr = "the Broker rejected the handshake"

				st := &statusStore{}
				clock := newLoopClock()
				deps := loopBaseDeps(st, clock)
				deps.selectTarget = func(context.Context) (target, error) {
					return target{Path: path, Kind: kindUser}, nil
				}
				deps.probe = func(context.Context, string) probeResult { return probe }
				deps.hello = func(context.Context, localpb.LocalClient, uint32) (helloResult, error) {
					return helloResult{}, errors.New(helloErr)
				}
				loopStart(t, deps)

				status := loopWaitStatus(t, st, "PhaseUnavailable", func(s Status) bool {
					return s.Phase == PhaseUnavailable
				})
				want := fmt.Sprintf("the Broker at %s is unavailable: %s", path, helloErr)
				if status.Detail != want {
					t.Fatalf("Detail = %q, want %q", status.Detail, want)
				}
				h := &handlers{st: st}
				_, err := h.preflight("list_agents", callGeneric, false)
				if err == nil {
					t.Fatal("preflight returned nil for a Hello failure")
				}
				if !strings.Contains(err.Error(), helloErr) {
					t.Fatalf("error = %q, want it to carry the original error", err)
				}
				if strings.Contains(err.Error(), "refused this connection") {
					t.Fatalf("error = %q, want no refusal text", err)
				}
			})
		}
	})

	t.Run("version mismatch waits for the Broker instead of polling", func(t *testing.T) {
		// REQ-broker-config-016
		path := fakelocal.SocketPath(t)
		var brokerVersion atomic.Uint32
		brokerVersion.Store(2)
		handlers := fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return &localpb.HelloResponse{ProtocolVersion: brokerVersion.Load()}, nil
			},
		}
		srv := fakelocal.Start(t, path, handlers)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		stop := loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseVersionMismatch", func(s Status) bool {
			return s.Phase == PhaseVersionMismatch
		})
		if !strings.Contains(status.Remedy, "restart the Broker") {
			t.Fatalf("Remedy = %q, want it to say restart the Broker", status.Remedy)
		}
		if !strings.Contains(status.Remedy, "peers service install") || !strings.Contains(status.Remedy, "peers service run") {
			t.Fatalf("Remedy = %q, want it to name both ways to start a Broker", status.Remedy)
		}
		if !strings.HasPrefix(status.Detail, "the running Broker speaks local protocol version 2") {
			t.Fatalf("Detail = %q, want both versions", status.Detail)
		}

		if waits := clock.durations(); len(waits) != 0 {
			t.Fatalf("waits during the version mismatch = %v, want none", waits)
		}
		// The version-mismatch status is the later event: the loop has not
		// opened a Register stream.
		if got := loopCountCalls(srv, "Hello"); got != 1 {
			t.Fatalf("Hello calls at the version mismatch = %d, want 1 (no polling)", got)
		}
		if got := loopCountCalls(srv, "Register"); got != 0 {
			t.Fatalf("Register streams at the version mismatch = %d, want 0", got)
		}

		brokerVersion.Store(1)
		srv.Stop()
		srv = fakelocal.Start(t, path, handlers)

		// The Broker leaving READY is still a failed attempt: reconnect waits
		// one connection backoff before dialing again.
		clock.autoAdvance()

		conn := loopNextRegister(t, srv)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("reconnect waits before the Register = %v, want the first one to be 250ms", waits)
		}
		if err := conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		if got := loopCountCalls(srv, "Hello"); got != 1 {
			t.Fatalf("Hello calls on the replacement listener = %d, want 1", got)
		}
		if got := loopCountCalls(srv, "Register"); got != 1 {
			t.Fatalf("Register streams = %d after the second registration, want 1 (the post-mismatch attempt only)", got)
		}
		stop()
	})

	t.Run("version mismatch of the system Broker tells the user to ask the administrator", func(t *testing.T) {
		// REQ-broker-config-016
		path := fakelocal.SocketPath(t)
		var brokerVersion atomic.Uint32
		brokerVersion.Store(2)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return &localpb.HelloResponse{ProtocolVersion: brokerVersion.Load()}, nil
			},
		})

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindSystem}, nil
		}
		loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseVersionMismatch", func(s Status) bool {
			return s.Phase == PhaseVersionMismatch
		})
		if !strings.Contains(status.Remedy, "administrator") || !strings.Contains(status.Remedy, "restart the Broker") {
			t.Fatalf("Remedy = %q, want the administrator sentence", status.Remedy)
		}

		brokerVersion.Store(1)
		srv.Restart()

		clock.autoAdvance()
		conn := loopNextRegister(t, srv)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("waits before reconnecting to the system Broker = %v, want the first one to be 250ms", waits)
		}
		if err := conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
	})

	t.Run("version mismatch of a test-named Broker names the socket in the remedy", func(t *testing.T) {
		// REQ-broker-config-016
		path := fakelocal.SocketPath(t)
		fakelocal.Start(t, path, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return &localpb.HelloResponse{ProtocolVersion: 2}, nil
			},
		})

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindTest}, nil
		}
		loopStart(t, deps)

		status := loopWaitStatus(t, st, "PhaseVersionMismatch", func(s Status) bool {
			return s.Phase == PhaseVersionMismatch
		})
		if !strings.Contains(status.Remedy, path) {
			t.Fatalf("Remedy = %q, want it to name the socket %q", status.Remedy, path)
		}
		if !strings.Contains(status.Remedy, "restart the Broker") {
			t.Fatalf("Remedy = %q, want it to say restart the Broker", status.Remedy)
		}
	})

	t.Run("a test-named Broker is only retried after a disconnect", func(t *testing.T) {
		// A socket named by the test hook is retried as it is, not replaced by
		// another selection.
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindTest}, nil
		}
		loopStart(t, deps)

		first := loopNextRegister(t, srv)
		if err := first.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		loopWaitStatus(t, st, "the registered agent", func(s Status) bool { return s.AgentID == "id1" })
		srv.Stop()

		for i, want := range []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second} {
			waits := loopWaitCount(t, clock, i+1)
			if waits[i] != want {
				t.Fatalf("wait %d = %v, want %v", i+1, waits[i], want)
			}
			if i < 2 {
				clock.advance()
			}
		}
		replacement := fakelocal.Start(t, path, fakelocal.Handlers{})
		clock.advance()

		second := loopNextRegister(t, replacement)
		if err := second.Complete("id2", "me@h", false); err != nil {
			t.Fatalf("sending the second registration_complete: %v", err)
		}
	})

	t.Run("a Hello failure on a user Broker waits before retrying", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return nil, status.Error(codes.Unavailable, "the Broker is shutting down")
			},
		})

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		loopStart(t, deps)

		loopRetryPaused(t, func() int { return loopCountCalls(srv, "Hello") }, "Hello", clock)
		// The handler answered Hello with a status, so the connection stayed
		// usable and this is not a handshake refusal; the phase and text stay
		// what a user Broker whose Hello keeps failing reported before.
		if got := st.Get().Phase; got != PhaseUnavailable {
			t.Fatalf("Phase while Hello keeps failing = %v, want PhaseUnavailable", got)
		}
	})

	t.Run("a Register failure right after Hello waits before retrying", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		fakelocal.Start(t, path, fakelocal.Handlers{})
		var registers atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		deps.register = func(context.Context, localpb.LocalClient, *localpb.RegisterRequest,
			*statusStore, completeFunc, *slog.Logger) error {
			registers.Add(1)
			return errors.New("the Register stream was rejected at once")
		}
		loopStart(t, deps)

		loopRetryPaused(t, func() int { return int(registers.Load()) }, "Register", clock)
	})

	t.Run("a version mismatch on a user Broker that disconnects waits before retrying", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return &localpb.HelloResponse{ProtocolVersion: 2}, nil
			},
		})

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		// The Broker answers Hello and then drops the connection, which is what
		// waitNotReady observes in production.
		deps.waitNotReady = func(context.Context, *grpc.ClientConn) {}
		loopStart(t, deps)

		loopRetryPaused(t, func() int { return loopCountCalls(srv, "Hello") }, "Hello", clock)
	})

	t.Run("a panic in one iteration does not end the loop", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})
		var selectCalls atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			if selectCalls.Add(1) == 1 {
				panic("boom")
			}
			return target{Path: path, Kind: kindUser}, nil
		}
		loopStart(t, deps)

		clock.autoAdvance()

		conn := loopNextRegister(t, srv)
		if waits := clock.durations(); len(waits) == 0 || waits[0] != 250*time.Millisecond {
			t.Fatalf("waits after the panic = %v, want the first one to be 250ms", waits)
		}
		if err := conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
	})

	t.Run("cancelling the context closes the Register stream and returns", func(t *testing.T) {
		// REQ-agent-messaging-034
		path := fakelocal.SocketPath(t)
		srv := fakelocal.Start(t, path, fakelocal.Handlers{})

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			runLoop(ctx, deps)
		}()

		conn := loopNextRegister(t, srv)
		if err := conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		loopWaitStatus(t, st, "the registered agent", func(s Status) bool { return s.AgentID == "id1" })

		cancel()
		select {
		case <-done:
		case <-time.After(loopWaitLimit):
			t.Fatalf("runLoop did not return within %v of cancelling its context", loopWaitLimit)
		}
		select {
		case <-conn.Done():
		case <-time.After(loopWaitLimit):
			t.Fatal("the Register stream stayed open after the context was cancelled")
		}
	})

	t.Run("the connection backoff doubles up to 5s", func(t *testing.T) {
		path := fakelocal.SocketPath(t)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindSystem}, nil
		}
		loopStart(t, deps)

		want := []time.Duration{
			250 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			2 * time.Second,
			4 * time.Second,
			5 * time.Second,
			5 * time.Second,
		}
		for i, w := range want {
			waits := loopWaitCount(t, clock, i+1)
			if waits[i] != w {
				t.Fatalf("wait %d = %v, want %v", i+1, waits[i], w)
			}
			if i < len(want)-1 {
				clock.advance()
			}
		}
	})
}
