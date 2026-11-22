//go:build e2e

package e2e

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
	"github.com/wangzw/peers/test/e2e/harness"
)

// registrationStallSetupBound is the fallback for the setup of the stall cases
// that is not a tool call: waiting for the session's Register stream.
const registrationStallSetupBound = harness.EventTimeout

// TestRegistrationListAgentsAbandonsAStalledCall constrains the harness's poll
// bound: the fake Broker accepts the session's registration and then never
// answers ListAgents. A list_agents call made through a wait must give up at
// ToolCallPollBound and report a retryable state. When the call used the
// harness's sixty-second call bound instead, one stalled call outlived the wait
// and failed the test from inside it with a transport error.
func TestRegistrationListAgentsAbandonsAStalledCall(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := fakelocal.Start(t, fakelocal.SocketPath(t), fakelocal.Handlers{
		// The Broker accepts the request and never answers it until the call is
		// cancelled or the test releases it.
		ListAgents: func(ctx context.Context, _ *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &localpb.ListAgentsResponse{}, nil
		},
		Send: func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error) {
			return &localpb.SendResponse{}, nil
		},
	})
	s, _ := registrationStalledSession(t, srv, "stalled", "agent-under-test", "stalled@test")

	// send_message needs the agent identity, so its success proves the MCP
	// server applied registration_complete. list_agents would prove it too, but
	// it is the call under test. The probe is not read-only, so it goes through
	// CallUntilOK: only the tool's own error is retried, never a transport error
	// that may already have delivered the message.
	s.CallUntilOK(t, harness.ToolCallWaitBound, "send_message", map[string]any{"to": "someone@test", "message": "hi"})

	start := time.Now()
	_, state := registrationListAgentsInWait(t, s, start.Add(harness.ToolCallPollBound), nil)
	elapsed := time.Since(start)

	if state == "" {
		t.Fatal("list_agents succeeded although the Broker never answered")
	}
	if !strings.Contains(state, "deadline exceeded") {
		t.Fatalf("the stalled call did not report its bound: %s", state)
	}
	if limit := 2 * harness.ToolCallPollBound; elapsed > limit {
		t.Fatalf("the stalled call returned after %s, want at most %s: %s", elapsed, limit, state)
	}
}

// TestWaitRegisteredToleratesASlowUpdateCheck checks the poll bound against the
// update hint and against a short caller bound. The hint runs inside the same
// tools/call after the handler and may spend almost updateStatusTimeout, three
// seconds, querying the Broker; with a poll bound equal to that timeout every
// call would be cut off while the hint was answering. The fake Broker here
// spends 2.9 seconds in UpdateStatus and another second in ListAgents, so the
// call lasts more than three seconds but stays well inside ToolCallPollBound.
// The test passes the project-wide three-second wait bound on purpose: a wait
// with a short bound must lift it to ToolCallWaitBound, or PollCall clamps the
// first call to the time the wait has left and the call is cut before its hint
// finishes. WaitRegistered must still pass.
func TestWaitRegisteredToleratesASlowUpdateCheck(t *testing.T) {
	const (
		updateCheckDelay = 2900 * time.Millisecond
		handlerDelay     = time.Second
	)
	srv := fakelocal.Start(t, fakelocal.SocketPath(t), fakelocal.Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			time.Sleep(handlerDelay)
			return &localpb.ListAgentsResponse{}, nil
		},
		UpdateStatus: func(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
			time.Sleep(updateCheckDelay)
			return &localpb.UpdateStatusResponse{}, nil
		},
	})
	s, _ := registrationStalledSession(t, srv, "slow-update", "slow-agent", "slow@test")

	start := time.Now()
	harness.WaitRegistered(t, s, harness.EventTimeout)
	elapsed := time.Since(start)
	if elapsed < handlerDelay+updateCheckDelay {
		t.Fatalf("WaitRegistered returned after %s, want at least the %s update check and %s handler delays; the call did not exercise the hint", elapsed, updateCheckDelay, handlerDelay)
	}
	t.Logf("WaitRegistered passed after %s with a %s update check and a three-second caller bound", elapsed, updateCheckDelay)
}

// TestWaitRegisteredRetriesAStalledCall checks that one stalled list_agents call
// is retried instead of failing the wait. The fake Broker's first ListAgents
// request blocks until its context is cancelled, which is the poll bound; the
// next request answers. WaitRegistered must pass, with two Broker calls
// recorded, which shows the first call was abandoned and made again.
func TestWaitRegisteredRetriesAStalledCall(t *testing.T) {
	var calls atomic.Int32
	srv := fakelocal.Start(t, fakelocal.SocketPath(t), fakelocal.Handlers{
		ListAgents: func(ctx context.Context, _ *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			if calls.Add(1) == 1 {
				<-ctx.Done()
				return nil, status.Error(codes.DeadlineExceeded, "the first ListAgents request stalled")
			}
			return &localpb.ListAgentsResponse{}, nil
		},
	})
	s, _ := registrationStalledSession(t, srv, "stalled-then-registered", "retry-agent", "retry@test")

	harness.WaitRegistered(t, s, harness.ToolCallWaitBound)

	if got := calls.Load(); got < 2 {
		t.Fatalf("the Broker answered %d ListAgents calls, want the stalled one retried", got)
	}
}

// TestRegistrationRejectedRetriesAStalledCall checks the same retry through the
// unusable-name wait. The rejected session's first list_agents call reaches the
// Broker and stalls until the poll bound; the fake Broker then reports the name
// unusable on the open Register stream, and the next poll observes the
// not_registered result. The observer proves that no agent is listed for the
// name.
func TestRegistrationRejectedRetriesAStalledCall(t *testing.T) {
	const detail = `the session name contains "@"`
	var (
		mu      sync.Mutex
		starReg *fakelocal.RegisterConn
		stalled atomic.Bool
	)
	srv := fakelocal.Start(t, fakelocal.SocketPath(t), fakelocal.Handlers{
		ListAgents: func(ctx context.Context, req *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			if req.GetAgentId() != "star-agent" {
				return &localpb.ListAgentsResponse{}, nil
			}
			if !stalled.CompareAndSwap(false, true) {
				return &localpb.ListAgentsResponse{}, nil
			}
			<-ctx.Done()
			mu.Lock()
			conn := starReg
			mu.Unlock()
			if conn != nil {
				_ = conn.Send(&localpb.RegisterResponse{
					Event: &localpb.RegisterResponse_NotRegistered{
						NotRegistered: &localpb.RegistrationProblem{
							Reason: localpb.NotRegisteredReason_NOT_REGISTERED_REASON_NAME_UNUSABLE,
							Detail: detail,
						},
					},
				})
			}
			return nil, status.Error(codes.DeadlineExceeded, "the first list_agents call stalled")
		},
	})
	observer, _ := registrationStalledSession(t, srv, "observer", "observer-agent", "observer@test")
	harness.WaitRegistered(t, observer, harness.ToolCallWaitBound)

	star, conn := registrationStalledSession(t, srv, "a@b", "star-agent", "a@b@test")
	mu.Lock()
	starReg = conn
	mu.Unlock()

	registrationRejected(t, star, observer, "a@b", detail, func(addr string) bool {
		return strings.Contains(addr, "a@b")
	})
}

// registrationStalledSession starts a session whose MCP server talks to srv,
// waits for its Register stream and completes the registration as agentID at
// address. The session ends with the test.
func registrationStalledSession(t *testing.T, srv *fakelocal.Server, name, agentID, address string) (*harness.Session, *fakelocal.RegisterConn) {
	t.Helper()
	host := harness.NewLocalHost(t, harness.LocalOptions{Name: "stalled-" + name})
	s := host.StartSession(t, harness.SessionSpec{
		Name:     name,
		StartMCP: true,
		Env:      map[string]string{"PEERS_TEST_SOCKET": srv.Path()},
	})
	t.Cleanup(func() { s.End(t) })

	ctx, cancel := context.WithTimeout(context.Background(), registrationStallSetupBound)
	defer cancel()
	conn, err := srv.NextRegister(ctx)
	if err != nil {
		t.Fatalf("waiting for the session's Register stream: %v", err)
	}
	if err := conn.Complete(agentID, address, false); err != nil {
		t.Fatalf("completing registration: %v", err)
	}
	return s, conn
}
