package mcpserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// regClient dials the fake Local service for the registration tests.
func regClient(t *testing.T, path string) localpb.LocalClient {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+path,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithIdleTimeout(0))
	if err != nil {
		t.Fatalf("dialing %s: %v", path, err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return localpb.NewLocalClient(cc)
}

type regRun struct {
	conn   *fakelocal.RegisterConn
	errc   chan error
	st     *statusStore
	cancel context.CancelFunc
}

// startRegRun starts runRegistration in the background and waits for its first
// Register frame to arrive at the fake Broker.
func startRegRun(t *testing.T, first *localpb.RegisterRequest, onComplete completeFunc) *regRun {
	t.Helper()
	return startRegRunOn(t, fakelocal.Handlers{}, first, onComplete)
}

// startRegRunOn is startRegRun with the fake Broker's handlers supplied by the
// caller, for tests that need the Broker side to answer topic calls.
func startRegRunOn(t *testing.T, h fakelocal.Handlers, first *localpb.RegisterRequest, onComplete completeFunc) *regRun {
	t.Helper()
	srv := fakelocal.Start(t, fakelocal.SocketPath(t), h)
	client := regClient(t, srv.Path())
	st := &statusStore{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errc := make(chan error, 1)
	go func() {
		errc <- runRegistration(ctx, client, first, st, onComplete, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	regCtx, regCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer regCancel()
	conn, err := srv.NextRegister(regCtx)
	if err != nil {
		t.Fatalf("waiting for the Register stream: %v", err)
	}
	return &regRun{conn: conn, errc: errc, st: st, cancel: cancel}
}

// wait waits for runRegistration to return.
func (r *regRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.errc:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("runRegistration did not return within 3s")
		return nil
	}
}

// regWaitUntil polls cond until it holds or the deadline passes.
func regWaitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// regConsistently asserts that cond holds throughout a bounded window.
func regConsistently(t *testing.T, what string, window time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("%s did not hold throughout %s", what, window)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRunRegistration(t *testing.T) {
	t.Run("sends the first frame verbatim", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			first *localpb.RegisterRequest
		}{
			{"values", &localpb.RegisterRequest{InboxSocketPath: "/s", ClaudeConfigDir: "/c"}},
			{"empty", &localpb.RegisterRequest{}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				run := startRegRun(t, tc.first, func(context.Context, localpb.LocalClient, string, bool) {})
				if got, want := run.conn.First.GetInboxSocketPath(), tc.first.GetInboxSocketPath(); got != want {
					t.Errorf("inbox_socket_path = %q, want %q", got, want)
				}
				if got, want := run.conn.First.GetClaudeConfigDir(), tc.first.GetClaudeConfigDir(); got != want {
					t.Errorf("claude_config_dir = %q, want %q", got, want)
				}
				run.conn.End()
				if err := run.wait(t); err == nil {
					t.Fatal("runRegistration returned nil after the stream ended")
				}
			})
		}
	})

	t.Run("completion updates the status and calls onComplete", func(t *testing.T) {
		type completion struct {
			agentID  string
			autoJoin bool
		}
		calls := make(chan completion, 4)
		run := startRegRun(t, &localpb.RegisterRequest{}, func(_ context.Context, _ localpb.LocalClient, agentID string, autoJoin bool) {
			calls <- completion{agentID: agentID, autoJoin: autoJoin}
		})

		if err := run.conn.Complete("id1", "me@h", true); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		select {
		case got := <-calls:
			if got.agentID != "id1" || !got.autoJoin {
				t.Fatalf("onComplete got (%q, %v), want (id1, true)", got.agentID, got.autoJoin)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete was not called within 3s")
		}
		st := run.st.Get()
		if st.AgentID != "id1" || st.Address != "me@h" {
			t.Fatalf("status = (%q, %q), want (id1, me@h)", st.AgentID, st.Address)
		}
		if st.NotRegistered != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_UNSPECIFIED {
			t.Fatalf("NotRegistered = %v, want UNSPECIFIED", st.NotRegistered)
		}

		run.conn.End()
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
	})

	t.Run("a not_registered event revokes without ending the stream", func(t *testing.T) {
		calls := make(chan string, 4)
		run := startRegRun(t, &localpb.RegisterRequest{}, func(_ context.Context, _ localpb.LocalClient, agentID string, _ bool) {
			calls <- agentID
		})

		if err := run.conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		select {
		case <-calls:
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete was not called within 3s")
		}

		if err := run.conn.NotRegistered(localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE); err != nil {
			t.Fatalf("sending not_registered: %v", err)
		}
		regWaitUntil(t, "the status to show NAME_UNUSABLE", func() bool {
			st := run.st.Get()
			return st.AgentID == "" && st.NotRegistered == localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE
		})
		select {
		case <-run.conn.Done():
			t.Fatal("the Register stream ended after not_registered")
		default:
		}

		if err := run.conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending the second registration_complete: %v", err)
		}
		select {
		case got := <-calls:
			if got != "id1" {
				t.Fatalf("second onComplete got %q, want id1", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete was not called again after the second completion")
		}

		run.conn.End()
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
	})

	t.Run("a broker_degraded not_registered records the detail and keeps the stream open", func(t *testing.T) {
		calls := make(chan string, 4)
		run := startRegRun(t, &localpb.RegisterRequest{}, func(_ context.Context, _ localpb.LocalClient, agentID string, _ bool) {
			calls <- agentID
		})

		if err := run.conn.Send(&localpb.RegisterResponse{
			Event: &localpb.RegisterResponse_NotRegistered{
				NotRegistered: &localpb.RegistrationProblem{
					Reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED,
					Detail: "config parse failed",
				},
			},
		}); err != nil {
			t.Fatalf("sending not_registered: %v", err)
		}
		regWaitUntil(t, "the degraded reason", func() bool {
			st := run.st.Get()
			return st.AgentID == "" &&
				st.NotRegistered == localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED &&
				st.NotRegisteredDetail == "config parse failed"
		})
		// The stream must stay open after the event. Wait out a short window
		// instead of sampling once, so a close that follows the event is still
		// caught.
		regConsistently(t, "the Register stream stays open after the degraded not_registered", 100*time.Millisecond, func() bool {
			select {
			case <-run.conn.Done():
				return false
			default:
				return true
			}
		})

		// A later completion clears the degraded state, as it does any other
		// not_registered reason.
		if err := run.conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		select {
		case got := <-calls:
			if got != "id1" {
				t.Fatalf("onComplete got %q, want id1", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete was not called within 3s")
		}
		if st := run.st.Get(); st.NotRegistered != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_UNSPECIFIED || st.NotRegisteredDetail != "" {
			t.Fatalf("status after re-registration = (%v, %q), want UNSPECIFIED and no detail", st.NotRegistered, st.NotRegisteredDetail)
		}

		run.conn.End()
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
	})

	t.Run("agent_changed updates the address of the current agent only", func(t *testing.T) {
		calls := make(chan struct{}, 4)
		run := startRegRun(t, &localpb.RegisterRequest{}, func(context.Context, localpb.LocalClient, string, bool) {
			calls <- struct{}{}
		})
		if err := run.conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		select {
		case <-calls:
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete was not called within 3s")
		}

		if err := run.conn.Send(&localpb.RegisterResponse{
			Event: &localpb.RegisterResponse_AgentChanged{
				AgentChanged: &localpb.AgentChangedEvent{AgentId: "id1", AgentAddress: "new@h"},
			},
		}); err != nil {
			t.Fatalf("sending agent_changed: %v", err)
		}
		regWaitUntil(t, "the address to change to new@h", func() bool {
			return run.st.Get().Address == "new@h"
		})

		if err := run.conn.Send(&localpb.RegisterResponse{
			Event: &localpb.RegisterResponse_AgentChanged{
				AgentChanged: &localpb.AgentChangedEvent{AgentId: "other", AgentAddress: "x@h"},
			},
		}); err != nil {
			t.Fatalf("sending a foreign agent_changed: %v", err)
		}
		deadline := time.Now().Add(150 * time.Millisecond)
		for time.Now().Before(deadline) {
			if got := run.st.Get().Address; got != "new@h" {
				t.Fatalf("a foreign agent_changed changed the address to %q", got)
			}
			time.Sleep(5 * time.Millisecond)
		}

		run.conn.End()
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
	})

	t.Run("stream end clears the registration state", func(t *testing.T) {
		calls := make(chan struct{}, 1)
		run := startRegRun(t, &localpb.RegisterRequest{}, func(context.Context, localpb.LocalClient, string, bool) {
			calls <- struct{}{}
		})
		if err := run.conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		select {
		case <-calls:
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete was not called within 3s")
		}

		run.conn.End()
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
		st := run.st.Get()
		if st.AgentID != "" || st.Address != "" {
			t.Fatalf("status = (%q, %q), want both empty", st.AgentID, st.Address)
		}
		if st.NotRegistered != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_UNSPECIFIED {
			t.Fatalf("NotRegistered = %v, want UNSPECIFIED", st.NotRegistered)
		}
	})

	t.Run("cancelling ctx closes the stream", func(t *testing.T) {
		run := startRegRun(t, &localpb.RegisterRequest{}, func(context.Context, localpb.LocalClient, string, bool) {})
		run.cancel()
		select {
		case err := <-run.errc:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("runRegistration returned %v, want context.Canceled", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("runRegistration did not return after ctx was cancelled")
		}
		select {
		case <-run.conn.Done():
		case <-time.After(3 * time.Second):
			t.Fatal("the Broker did not see the Register stream close within 3s")
		}
	})

	t.Run("waits for onComplete and cancels its context", func(t *testing.T) {
		started := make(chan struct{})
		ctxDone := make(chan struct{})
		run := startRegRun(t, &localpb.RegisterRequest{}, func(ctx context.Context, _ localpb.LocalClient, _ string, _ bool) {
			close(started)
			<-ctx.Done()
			close(ctxDone)
		})
		if err := run.conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete did not start within 3s")
		}

		run.conn.End()
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
		// runRegistration waits for onComplete, so its context must already be
		// cancelled when it returns.
		select {
		case <-ctxDone:
		default:
			t.Fatal("runRegistration returned before onComplete finished")
		}
	})

	t.Run("a lost stream reports unavailable before clearing the identity", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		run := startRegRun(t, &localpb.RegisterRequest{}, func(context.Context, localpb.LocalClient, string, bool) {
			close(started)
			<-release
		})
		run.st.Update(func(s *Status) { s.Phase = PhaseConnected })

		if err := run.conn.Complete("id1", "me@h", false); err != nil {
			t.Fatalf("sending registration_complete: %v", err)
		}
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("onComplete did not start within 3s")
		}

		run.conn.End()

		// While onComplete is still blocked, the status must already say the
		// connection is gone. A tool call in this window must be told to retry,
		// not that the session is merely unregistered.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && run.st.Get().Phase != PhaseUnavailable {
			time.Sleep(2 * time.Millisecond)
		}
		st := run.st.Get()
		if st.Phase != PhaseUnavailable || st.Detail != loopLostText {
			t.Fatalf("status during the window = %+v, want PhaseUnavailable with %q", st, loopLostText)
		}
		h := &handlers{st: run.st}
		if _, err := h.preflight("list_agents", callGeneric, true); err == nil ||
			!strings.Contains(err.Error(), loopLostText) ||
			!strings.Contains(err.Error(), "(code: internal)") {
			t.Fatalf("preflight during the window = %v, want the reconnecting error", err)
		}

		close(release)
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
		if st := run.st.Get(); st.AgentID != "" || st.Address != "" {
			t.Fatalf("status after the stream ended = (%q, %q), want both empty", st.AgentID, st.Address)
		}
	})

	t.Run("a completion arriving while the first runs does not run after cancellation", func(t *testing.T) {
		// The exact interleaving this guards against is covered directly by
		// TestRunCompletionDoesNotRunAfterCancellation; this subtest checks the
		// wiring: a second completion that waited for the first one must not
		// run once the stream is cancelled.
		for i := 0; i < 5; i++ {
			started := make(chan struct{})
			release := make(chan struct{})
			var mu sync.Mutex
			secondCalls := 0
			run := startRegRun(t, &localpb.RegisterRequest{}, func(_ context.Context, _ localpb.LocalClient, agentID string, _ bool) {
				if agentID == "id1" {
					close(started)
					<-release
					return
				}
				mu.Lock()
				secondCalls++
				mu.Unlock()
			})

			if err := run.conn.Complete("id1", "one@h", false); err != nil {
				t.Fatalf("iteration %d: sending the first registration_complete: %v", i, err)
			}
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatalf("iteration %d: the first onComplete did not start within 3s", i)
			}
			if err := run.conn.Complete("id2", "two@h", false); err != nil {
				t.Fatalf("iteration %d: sending the second registration_complete: %v", i, err)
			}
			regWaitUntil(t, "the second completion to be processed", func() bool {
				return run.st.Get().AgentID == "id2"
			})

			run.cancel()
			close(release)
			if err := run.wait(t); err == nil {
				t.Fatalf("iteration %d: runRegistration returned nil after ctx was cancelled", i)
			}
			mu.Lock()
			got := secondCalls
			mu.Unlock()
			if got != 0 {
				t.Fatalf("iteration %d: the second onComplete ran after the context was cancelled", i)
			}
		}
	})

	t.Run("serializes back-to-back completions", func(t *testing.T) {
		started := make(chan string, 4)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
		var mu sync.Mutex
		active, maxActive := 0, 0
		var order []string
		run := startRegRun(t, &localpb.RegisterRequest{}, func(ctx context.Context, _ localpb.LocalClient, agentID string, _ bool) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			order = append(order, agentID+":start")
			mu.Unlock()
			started <- agentID
			select {
			case <-release:
			case <-ctx.Done():
			}
			mu.Lock()
			active--
			order = append(order, agentID+":end")
			mu.Unlock()
		})
		t.Cleanup(releaseAll)

		if err := run.conn.Complete("id1", "one@h", false); err != nil {
			t.Fatalf("sending the first registration_complete: %v", err)
		}
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("the first onComplete did not start within 3s")
		}
		if err := run.conn.Complete("id2", "two@h", false); err != nil {
			t.Fatalf("sending the second registration_complete: %v", err)
		}
		// The second completion is applied before the first one is released, so
		// the order log below is a fair record of what the server did with it.
		regWaitUntil(t, "the second registration completion", func() bool {
			return run.st.Get().AgentID == "id2"
		})
		releaseAll()
		select {
		case got := <-started:
			if got != "id2" {
				t.Fatalf("second onComplete got %q, want id2", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("the second onComplete did not start after the first returned")
		}

		run.conn.End()
		if err := run.wait(t); err == nil {
			t.Fatal("runRegistration returned nil after the stream ended")
		}
		mu.Lock()
		defer mu.Unlock()
		if maxActive > 1 {
			t.Fatalf("%d onComplete calls ran at once, want at most one", maxActive)
		}
		// The second completion must not have started before the first returned.
		// Both ran, so their recorded order answers that without a sleep.
		firstEnd, secondStart := -1, -1
		for i, event := range order {
			switch event {
			case "id1:end":
				if firstEnd < 0 {
					firstEnd = i
				}
			case "id2:start":
				if secondStart < 0 {
					secondStart = i
				}
			}
		}
		if firstEnd < 0 || secondStart < 0 || firstEnd > secondStart {
			t.Fatalf("completion order = %v, want id1:end before id2:start", order)
		}
	})
}

// TestRunCompletionDoesNotRunAfterCancellation checks the dispatch decision
// directly: when the serial slot is free and the stream context is already
// done, both select cases are ready, and the cancellation must win. The
// registration stream can reach this state when the previous completion frees
// the slot at the same moment the connection is cancelled.
func TestRunCompletionDoesNotRunAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	serial := make(chan struct{}, 1)

	for i := 0; i < 100; i++ {
		called := false
		runCompletion(ctx, serial, func() { called = true })
		if called {
			t.Fatalf("iteration %d: the completion ran after its context was cancelled", i)
		}
	}
}
