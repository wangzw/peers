package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
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

func loopTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

const loopWaitLimit = helloTimeout + eventTimeout

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

func loopCountCalls(srv *fakelocal.Server, method string) int {
	n := 0
	for _, c := range srv.Calls() {
		if c.Method == method {
			n++
		}
	}
	return n
}

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

func loopBaseDeps(st *statusStore, clock *loopClock) loopDeps {
	return loopDeps{
		probe:        inspectSocket,
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

// loopClosingSocket accepts and closes every connection, reproducing a refusal
// at grpc-go's server handshake.
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

		if calls := systemSrv.Calls(); len(calls) != 0 {
			t.Errorf("the system Broker received %d calls, want none: %v", len(calls), calls)
		}
		if got := selectCalls.Load(); got != 1 {
			t.Errorf("selection ran %d times, want once at startup", got)
		}
		stop()
	})

	t.Run("system Broker is retried, never replaced", func(t *testing.T) {
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

	t.Run("a peer mismatch at selection reads the same as at reconnection", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		const wrongUID = 4242
		wantDetail := peerMismatchError(path, kindUser, wrongUID).Error()

		selecting := &statusStore{}
		selectDeps := loopBaseDeps(selecting, newLoopClock())
		selectDeps.selectTarget = func(context.Context) (target, error) {
			return target{}, peerMismatchError(path, kindUser, wrongUID)
		}
		stopSelecting := loopStart(t, selectDeps)
		selection := loopWaitStatus(t, selecting, "PhaseUnavailable", func(s Status) bool {
			return s.Phase == PhaseUnavailable
		})
		stopSelecting()

		reconnecting := &statusStore{}
		reconnectDeps := loopBaseDeps(reconnecting, newLoopClock())
		reconnectDeps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		reconnectDeps.probe = func(context.Context, string) probeInfo {
			return probeInfo{Outcome: probeWrongPeer, PeerUID: wrongUID, PeerKnown: true}
		}
		reconnectDeps.hello = func(context.Context, localpb.LocalClient, uint32) (helloResult, error) {
			return helloResult{}, errors.New("the Broker rejected the handshake")
		}
		loopStart(t, reconnectDeps)
		reconnection := loopWaitStatus(t, reconnecting, "PhaseUnavailable", func(s Status) bool {
			return s.Phase == PhaseUnavailable
		})

		if selection.Detail != wantDetail || reconnection.Detail != wantDetail {
			t.Fatalf("selection Detail = %q, reconnection Detail = %q, want both %q",
				selection.Detail, reconnection.Detail, wantDetail)
		}
		if strings.Contains(selection.Detail, "the Broker is unavailable") {
			t.Fatalf("selection Detail = %q, want no unavailable wrapper", selection.Detail)
		}
	})

	t.Run("a system peer mismatch at selection still says how to start a Broker", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		const wrongUID = 4242
		wantMismatch := peerMismatchError(path, kindSystem, wrongUID).Error()

		st := &statusStore{}
		deps := loopBaseDeps(st, newLoopClock())
		deps.selectTarget = func(context.Context) (target, error) {
			return target{}, peerMismatchError(path, kindSystem, wrongUID)
		}
		stop := loopStart(t, deps)
		status := loopWaitStatus(t, st, "PhaseUnavailable", func(s Status) bool {
			return s.Phase == PhaseUnavailable
		})
		stop()

		if !strings.Contains(status.Detail, wantMismatch) {
			t.Fatalf("Detail = %q, want it to contain %q", status.Detail, wantMismatch)
		}
		if !strings.Contains(status.Detail, textNoBroker) {
			t.Fatalf("Detail = %q, want the how-to-start text %q", status.Detail, textNoBroker)
		}
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for a system peer mismatch at selection")
		}
		wantErr := "list_agents failed: " + status.Detail + " (code: internal)"
		if got := err.Error(); got != wantErr {
			t.Fatalf("error = %q, want %q", got, wantErr)
		}
	})

	t.Run("a user Broker whose socket is gone reports no Broker", func(t *testing.T) {
		path := fakelocal.SocketPath(t)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		deps.probe = func(context.Context, string) probeInfo { return probeInfo{Outcome: probeMissing} }
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

	t.Run("a system Broker whose socket is gone or refusing keeps the no-Broker text", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			probe probeResult
		}{
			{"missing", probeMissing},
			{"refusing connections", probeRefused},
		} {
			probe := tc.probe
			t.Run(tc.name, func(t *testing.T) {
				path := fakelocal.SocketPath(t)

				st := &statusStore{}
				clock := newLoopClock()
				deps := loopBaseDeps(st, clock)
				deps.selectTarget = func(context.Context) (target, error) {
					return target{Path: path, Kind: kindSystem}, nil
				}
				deps.probe = func(context.Context, string) probeInfo { return probeInfo{Outcome: probe} }
				deps.hello = func(context.Context, localpb.LocalClient, uint32) (helloResult, error) {
					return helloResult{}, errors.New("the socket did not answer")
				}
				loopStart(t, deps)

				status := loopWaitStatus(t, st, "PhaseNoBroker", func(s Status) bool {
					return s.Phase == PhaseNoBroker
				})
				if status.Detail != textNoSystemBroker {
					t.Fatalf("Detail = %q, want %q", status.Detail, textNoSystemBroker)
				}
				h := &handlers{st: st}
				_, err := h.preflight("list_agents", callGeneric, false)
				if err == nil {
					t.Fatal("preflight returned nil for a system socket that is gone")
				}
				want := "list_agents failed: " + textNoSystemBroker + " (code: internal)"
				if got := err.Error(); got != want {
					t.Fatalf("error = %q, want %q", got, want)
				}
				if strings.Contains(err.Error(), "running but is not answering") {
					t.Fatalf("error = %q, want no running-Broker text", err)
				}
			})
		}
	})

	t.Run("a system Broker that answers Hello with an error is reported as running", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		const helloErr = "the Broker is busy"
		helloRPCErr := status.Error(codes.Unavailable, helloErr)
		fakelocal.Start(t, path, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return nil, helloRPCErr
			},
		})
		var selectCalls atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			selectCalls.Add(1)
			return target{Path: path, Kind: kindSystem}, nil
		}
		loopStart(t, deps)

		snapshot := loopWaitStatus(t, st, "PhaseUnavailable", func(s Status) bool {
			return s.Phase == PhaseUnavailable
		})
		want := "the system Broker is running but is not answering: " + helloRPCErr.Error() +
			"; this session retries in the background, and if this persists, ask the administrator to check it."
		if snapshot.Detail != want {
			t.Fatalf("Detail = %q, want %q", snapshot.Detail, want)
		}
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for an answering system Broker")
		}
		wantErr := "list_agents failed: " + want + " (code: internal)"
		if got := err.Error(); got != wantErr {
			t.Fatalf("error = %q, want %q", got, wantErr)
		}
		for _, unwanted := range []string{
			"no Broker is available",
			"ask the administrator to start it",
			"the system Broker is unavailable",
		} {
			if strings.Contains(err.Error(), unwanted) {
				t.Fatalf("error = %q, want no %q", err, unwanted)
			}
		}
		if waits := loopWaitCount(t, clock, 1); waits[0] != 250*time.Millisecond {
			t.Fatalf("first retry wait = %v, want 250ms", waits[0])
		}
		if got := selectCalls.Load(); got != 1 {
			t.Errorf("selection ran %d times, want once while the same Broker is retried", got)
		}
	})

	t.Run("a system Broker whose registration stream ends without an error is reported as running", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		fakelocal.Start(t, path, fakelocal.Handlers{})
		var selectCalls atomic.Int32

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			selectCalls.Add(1)
			return target{Path: path, Kind: kindSystem}, nil
		}
		deps.register = func(context.Context, localpb.LocalClient, *localpb.RegisterRequest,
			*statusStore, completeFunc, *slog.Logger) error {
			return errors.New("the Register stream ended")
		}
		loopStart(t, deps)

		snapshot := loopWaitStatus(t, st, "PhaseUnavailable", func(s Status) bool {
			return s.Phase == PhaseUnavailable
		})
		want := "the system Broker is running but is not answering: " + loopLostText +
			"; this session retries in the background, and if this persists, ask the administrator to check it."
		if snapshot.Detail != want {
			t.Fatalf("Detail = %q, want %q", snapshot.Detail, want)
		}
		h := &handlers{st: st}
		_, err := h.preflight("list_agents", callGeneric, false)
		if err == nil {
			t.Fatal("preflight returned nil for an answering system Broker")
		}
		wantErr := "list_agents failed: " + want + " (code: internal)"
		if got := err.Error(); got != wantErr {
			t.Fatalf("error = %q, want %q", got, wantErr)
		}
		if strings.Contains(err.Error(), "ask the administrator to start it") {
			t.Fatalf("error = %q, want no advice to start a running Broker", err)
		}
		if waits := loopWaitCount(t, clock, 1); waits[0] != 250*time.Millisecond {
			t.Fatalf("first retry wait = %v, want 250ms", waits[0])
		}
		if got := selectCalls.Load(); got != 1 {
			t.Errorf("selection ran %d times, want once while the same Broker is retried", got)
		}
	})

	t.Run("a registration refused by the Broker passes its sentence to the model", func(t *testing.T) {
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
		path := fakelocal.SocketPath(t)

		st := &statusStore{}
		clock := newLoopClock()
		deps := loopBaseDeps(st, clock)
		deps.selectTarget = func(context.Context) (target, error) {
			return target{Path: path, Kind: kindUser}, nil
		}
		deps.probe = func(context.Context, string) probeInfo { return probeInfo{Outcome: probeRefused} }
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

	t.Run("a Hello failure is classified by what the probe found", func(t *testing.T) {
		const (
			helloErr = "the Broker rejected the handshake"
			wrongUID = 4242
		)
		unavailable := "the Broker is unavailable: " + helloErr + "; retrying in the background."
		cases := []struct {
			name    string
			kind    targetKind
			probe   probeResult
			peerUID uint32
			want    func(path string) string
		}{
			{
				name:    "user socket served by another account",
				kind:    kindUser,
				probe:   probeWrongPeer,
				peerUID: wrongUID,
				want: func(path string) string {
					return fmt.Sprintf("the user Broker socket %s is served by uid %d, not this account (%d)",
						path, wrongUID, os.Geteuid())
				},
			},
			{
				name:  "user socket the account may not connect to",
				kind:  kindUser,
				probe: probeDenied,
				want:  func(string) string { return unavailable },
			},
			{
				name:  "user socket whose probe failed another way",
				kind:  kindUser,
				probe: probeOther,
				want:  func(string) string { return unavailable },
			},
			{
				name:    "system socket served by another account",
				kind:    kindSystem,
				probe:   probeWrongPeer,
				peerUID: wrongUID,
				want: func(path string) string {
					return fmt.Sprintf("the system Broker socket %s is served by uid %d, not root", path, wrongUID)
				},
			},
			{
				name:  "system socket the account may not connect to",
				kind:  kindSystem,
				probe: probeDenied,
				want: func(path string) string {
					return fmt.Sprintf("cannot reach the system Broker at %s: %s; this session retries in the background.",
						path, helloErr)
				},
			},
			{
				name:  "system socket whose probe failed another way",
				kind:  kindSystem,
				probe: probeOther,
				want: func(path string) string {
					return fmt.Sprintf("cannot reach the system Broker at %s: %s; this session retries in the background.",
						path, helloErr)
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				path := fakelocal.SocketPath(t)

				st := &statusStore{}
				clock := newLoopClock()
				deps := loopBaseDeps(st, clock)
				deps.selectTarget = func(context.Context) (target, error) {
					return target{Path: path, Kind: tc.kind}, nil
				}
				deps.probe = func(context.Context, string) probeInfo {
					return probeInfo{Outcome: tc.probe, PeerUID: tc.peerUID, PeerKnown: tc.peerUID != 0}
				}
				deps.hello = func(context.Context, localpb.LocalClient, uint32) (helloResult, error) {
					return helloResult{}, errors.New(helloErr)
				}
				loopStart(t, deps)

				status := loopWaitStatus(t, st, "PhaseUnavailable", func(s Status) bool {
					return s.Phase == PhaseUnavailable
				})
				want := tc.want(path)
				if status.Detail != want {
					t.Fatalf("Detail = %q, want %q", status.Detail, want)
				}
				h := &handlers{st: st}
				_, err := h.preflight("list_agents", callGeneric, false)
				if err == nil {
					t.Fatal("preflight returned nil for a Hello failure")
				}
				wantErr := "list_agents failed: " + want + " (code: internal)"
				if got := err.Error(); got != wantErr {
					t.Fatalf("error = %q, want %q", got, wantErr)
				}
				switch tc.probe {
				case probeWrongPeer:
					if !strings.Contains(err.Error(), fmt.Sprintf("uid %d", wrongUID)) {
						t.Fatalf("error = %q, want the answering account's uid", err)
					}
					if strings.Contains(err.Error(), "running but is not answering") {
						t.Fatalf("error = %q, want no running-Broker claim for a peer mismatch", err)
					}
				case probeDenied, probeOther:
					if strings.Contains(err.Error(), "running but is not answering") {
						t.Fatalf("error = %q, want no running-Broker claim", err)
					}
					if tc.kind == kindSystem {
						for _, unwanted := range []string{"no Broker is available", "ask the administrator to start it"} {
							if strings.Contains(err.Error(), unwanted) {
								t.Fatalf("error = %q, want no %q", err, unwanted)
							}
						}
					} else {
						if got := strings.Count(err.Error(), "is unavailable"); got != 1 {
							t.Fatalf("error = %q has %d \"is unavailable\" clauses, want 1", err, got)
						}
						if strings.Contains(err.Error(), "refused this connection") {
							t.Fatalf("error = %q, want no refusal text", err)
						}
					}
				}
			})
		}
	})

	t.Run("version mismatch waits for the Broker instead of polling", func(t *testing.T) {
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
		if got := loopCountCalls(srv, "Hello"); got != 1 {
			t.Fatalf("Hello calls at the version mismatch = %d, want 1 (no polling)", got)
		}
		if got := loopCountCalls(srv, "Register"); got != 0 {
			t.Fatalf("Register streams at the version mismatch = %d, want 0", got)
		}

		brokerVersion.Store(1)
		srv.Stop()
		srv = fakelocal.Start(t, path, handlers)

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
