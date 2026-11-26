package mcpserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

const wantHint = "peers 0.1.1 is available (this Broker runs 0.1.0). " +
	"To upgrade, the user can ask you to call update_broker or run `peers update` in a terminal."

func connectHintClient(t *testing.T, ts toolSet, source updateStatusSource, timeout time.Duration) *mcp.ClientSession {
	t.Helper()
	return connectHintClientAt(t, ts, source, timeout, nil)
}

func connectHintClientAt(t *testing.T, ts toolSet, source updateStatusSource, timeout time.Duration, now func() time.Time) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := newMCPServer("0.1.0", ts, log)
	server.AddReceivingMiddleware((&updateHint{
		source:          source,
		timeout:         timeout,
		log:             log,
		fallbackCurrent: "0.1.0",
		now:             now,
	}).middleware)

	ct, st := mcp.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() {
		_ = cs.Close()
		_ = ss.Wait()
	})
	return cs
}

func contentTexts(res *mcp.CallToolResult) []string {
	var texts []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			texts = append(texts, tc.Text)
		}
	}
	return texts
}

func hasHint(res *mcp.CallToolResult) bool {
	for _, text := range contentTexts(res) {
		if text == wantHint {
			return true
		}
	}
	return false
}

func countingSource(calls *atomic.Int32, st updateStatus) updateStatusSource {
	return func(context.Context) (updateStatus, error) {
		calls.Add(1)
		return st, nil
	}
}

// TestUpdateHintFirstToolCallOnly checks that the hint is appended to the first
// tool call only.
func TestUpdateHintFirstToolCallOnly(t *testing.T) {
	var calls atomic.Int32
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{Self: "me@host"}, nil
		},
		listTopics: func(context.Context, NoArgs) (ListTopicsOut, error) {
			return ListTopicsOut{Joined: []string{"t"}}, nil
		},
	}
	cs := connectHintClient(t, ts, countingSource(&calls, updateStatus{
		CurrentVersion:   "0.1.0",
		AvailableVersion: "0.1.1",
	}), updateStatusTimeout)

	first := callTool(t, cs, "list_agents", nil)
	texts := contentTexts(first)
	if len(texts) < 2 {
		t.Fatalf("first result content = %q, want the tool text and the hint", texts)
	}
	if got := texts[len(texts)-1]; got != wantHint {
		t.Errorf("last content = %q, want %q", got, wantHint)
	}

	second := callTool(t, cs, "list_topics", nil)
	if hasHint(second) {
		t.Errorf("second result carries the hint: %q", contentTexts(second))
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("UpdateStatus called %d times, want 1", got)
	}
}

// TestUpdateHintNoAvailableVersion checks that no hint is appended without an
// available version.
func TestUpdateHintNoAvailableVersion(t *testing.T) {
	var calls atomic.Int32
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{}, nil
		},
	}
	cs := connectHintClient(t, ts, countingSource(&calls, updateStatus{
		CurrentVersion: "0.1.0",
	}), updateStatusTimeout)

	res := callTool(t, cs, "list_agents", nil)
	if hasHint(res) {
		t.Errorf("result carries a hint without an available version: %q", contentTexts(res))
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("UpdateStatus called %d times, want 1", got)
	}
}

// TestUpdateHintUnimplementedIsAFailedQuery checks that Unimplemented leaves the
// tool result untouched and the next call retries.
func TestUpdateHintUnimplementedIsAFailedQuery(t *testing.T) {
	var calls atomic.Int32
	source := func(context.Context) (updateStatus, error) {
		calls.Add(1)
		return updateStatus{}, status.Error(codes.Unimplemented, "unknown method")
	}
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{Self: "me@host"}, nil
		},
	}
	cs := connectHintClient(t, ts, source, updateStatusTimeout)

	for i := 0; i < 2; i++ {
		res := callTool(t, cs, "list_agents", nil)
		texts := contentTexts(res)
		if len(texts) != 1 {
			t.Errorf("call %d content = %q, want only the tool text", i+1, texts)
		}
		if hasHint(res) {
			t.Errorf("call %d carries a hint after Unimplemented: %q", i+1, texts)
		}
		if res.IsError {
			t.Errorf("call %d turned the tool result into an error: %q", i+1, texts)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("UpdateStatus called %d times, want 2 (each tool call retries after a failure)", got)
	}
}

// TestUpdateHintTimeoutIsSkipped checks that the source sees the product's query
// timeout and a timeout appends no hint.
func TestUpdateHintTimeoutIsSkipped(t *testing.T) {
	const queryTimeout = 20 * time.Millisecond
	const deadlineBound = time.Second
	var calls atomic.Int32
	deadlines := make(chan time.Duration, 1)
	source := func(ctx context.Context) (updateStatus, error) {
		calls.Add(1)
		deadline, ok := ctx.Deadline()
		if !ok {
			deadlines <- -1
			return updateStatus{}, errors.New("the update query context has no deadline")
		}
		remaining := time.Until(deadline)
		deadlines <- remaining
		if remaining > deadlineBound {
			return updateStatus{}, errors.New("the update query timeout was not applied")
		}
		<-ctx.Done()
		return updateStatus{}, ctx.Err()
	}
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{Self: "me@host"}, nil
		},
	}
	cs := connectHintClient(t, ts, source, queryTimeout)

	res := callTool(t, cs, "list_agents", nil)
	var got time.Duration
	select {
	case got = <-deadlines:
	default:
		t.Fatal("update status source was not called")
	}
	if got < 0 || got > deadlineBound {
		t.Fatalf("update query context deadline was %v away, want about the %s query timeout", got, queryTimeout)
	}
	texts := contentTexts(res)
	if len(texts) != 1 {
		t.Errorf("content = %q, want only the tool text", texts)
	}
	if hasHint(res) {
		t.Errorf("result carries a hint after a timeout: %q", texts)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("UpdateStatus called %d times, want 1", got)
	}
}

// TestUpdateHintOnToolError checks that a hint is appended to a tool error
// result.
func TestUpdateHintOnToolError(t *testing.T) {
	var calls atomic.Int32
	const toolErr = "list_agents failed: broker unreachable (code: internal)"
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{}, errors.New(toolErr)
		},
	}
	cs := connectHintClient(t, ts, countingSource(&calls, updateStatus{
		CurrentVersion:   "0.1.0",
		AvailableVersion: "0.1.1",
	}), updateStatusTimeout)

	res := callTool(t, cs, "list_agents", nil)
	if !res.IsError {
		t.Fatal("result is not an error")
	}
	texts := contentTexts(res)
	if len(texts) != 2 {
		t.Fatalf("error result content = %q, want the error text and the hint", texts)
	}
	if texts[0] != toolErr {
		t.Errorf("first content = %q, want the error text %q", texts[0], toolErr)
	}
	if got := texts[len(texts)-1]; got != wantHint {
		t.Errorf("last content = %q, want %q", got, wantHint)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("UpdateStatus called %d times, want 1", got)
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestUpdateHintRechecksAfterTheInterval checks that calls inside the interval
// do not query, a newer release is hinted after the interval, and the same
// version is never hinted twice.
func TestUpdateHintRechecksAfterTheInterval(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	var calls atomic.Int32
	var responseMu sync.Mutex
	response := updateStatus{CurrentVersion: "0.1.0", AvailableVersion: "0.1.1"}
	source := func(context.Context) (updateStatus, error) {
		calls.Add(1)
		responseMu.Lock()
		defer responseMu.Unlock()
		return response, nil
	}
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{Self: "me@host"}, nil
		},
		listTopics: func(context.Context, NoArgs) (ListTopicsOut, error) {
			return ListTopicsOut{Joined: []string{"t"}}, nil
		},
	}
	cs := connectHintClientAt(t, ts, source, updateStatusTimeout, clock.Now)

	first := callTool(t, cs, "list_agents", nil)
	if !hasHint(first) {
		t.Fatalf("first result lacks the 0.1.1 hint: %q", contentTexts(first))
	}

	if res := callTool(t, cs, "list_topics", nil); hasHint(res) {
		t.Fatalf("result carries a hint inside the interval: %q", contentTexts(res))
	}
	clock.Advance(updateRecheckInterval - time.Second)
	if res := callTool(t, cs, "list_agents", nil); hasHint(res) {
		t.Fatalf("result carries a hint before the interval elapsed: %q", contentTexts(res))
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("UpdateStatus called %d times before the interval, want 1", got)
	}

	responseMu.Lock()
	response = updateStatus{CurrentVersion: "0.1.0", AvailableVersion: "0.1.2"}
	responseMu.Unlock()
	clock.Advance(time.Second)
	res := callTool(t, cs, "list_topics", nil)
	want := strings.Replace(wantHint, "0.1.1", "0.1.2", 1)
	if texts := contentTexts(res); len(texts) < 2 || texts[len(texts)-1] != want {
		t.Fatalf("result content = %q, want last %q", texts, want)
	}

	clock.Advance(updateRecheckInterval)
	if res := callTool(t, cs, "list_agents", nil); hasHint(res) {
		t.Fatalf("same version hinted twice: %q", contentTexts(res))
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("UpdateStatus called %d times, want 3", got)
	}
}

// TestUpdateHintRetriesAfterConnectionNotReady checks that a failed first query
// leaves the process unchecked and the next call retries.
func TestUpdateHintRetriesAfterConnectionNotReady(t *testing.T) {
	var calls atomic.Int32
	source := func(context.Context) (updateStatus, error) {
		if calls.Add(1) == 1 {
			return updateStatus{}, errNoBrokerConnection
		}
		return updateStatus{CurrentVersion: "0.1.0", AvailableVersion: "0.1.1"}, nil
	}
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{Self: "me@host"}, nil
		},
	}
	cs := connectHintClient(t, ts, source, updateStatusTimeout)

	first := callTool(t, cs, "list_agents", nil)
	if hasHint(first) {
		t.Fatalf("first result carries a hint without an answer: %q", contentTexts(first))
	}
	second := callTool(t, cs, "list_agents", nil)
	if !hasHint(second) {
		t.Fatalf("second result lacks the hint after the connection became ready: %q", contentTexts(second))
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("UpdateStatus called %d times, want 2", got)
	}
}

// TestUpdateHintSkipsDegradedBroker checks that a degraded Broker's refusal
// appends no hint and the next call tries again.
func TestUpdateHintSkipsDegradedBroker(t *testing.T) {
	fail, err := status.New(codes.FailedPrecondition, "config parse failed").WithDetails(&localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED,
		Message: "config parse failed",
	})
	if err != nil {
		t.Fatalf("building the error detail: %v", err)
	}
	var calls atomic.Int32
	source := func(context.Context) (updateStatus, error) {
		calls.Add(1)
		return updateStatus{}, fail.Err()
	}
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{Self: "me@host"}, nil
		},
	}
	cs := connectHintClient(t, ts, source, updateStatusTimeout)

	for i := 0; i < 2; i++ {
		res := callTool(t, cs, "list_agents", nil)
		if hasHint(res) {
			t.Fatalf("call %d carries a hint from a degraded Broker: %q", i+1, contentTexts(res))
		}
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("UpdateStatus called %d times, want 2", got)
	}
}

// TestUpdateHintNextProcessHintsNewVersion checks that a new process hints a
// newer version again.
func TestUpdateHintNextProcessHintsNewVersion(t *testing.T) {
	ts := &stubToolSet{
		listAgents: func(context.Context, ListAgentsIn) (ListAgentsOut, error) {
			return ListAgentsOut{}, nil
		},
	}

	var firstCalls atomic.Int32
	first := connectHintClient(t, ts, countingSource(&firstCalls, updateStatus{
		CurrentVersion:   "0.1.0",
		AvailableVersion: "0.1.1",
	}), updateStatusTimeout)
	if res := callTool(t, first, "list_agents", nil); !hasHint(res) {
		t.Fatalf("first process did not hint 0.1.1: %q", contentTexts(res))
	}

	var nextCalls atomic.Int32
	next := connectHintClient(t, ts, countingSource(&nextCalls, updateStatus{
		CurrentVersion:   "0.1.0",
		AvailableVersion: "0.1.2",
	}), updateStatusTimeout)
	res := callTool(t, next, "list_agents", nil)
	want := strings.Replace(wantHint, "0.1.1", "0.1.2", 1)
	if got := contentTexts(res); len(got) < 2 || got[len(got)-1] != want {
		t.Fatalf("new process content = %q, want last %q", got, want)
	}
	if got := nextCalls.Load(); got != 1 {
		t.Errorf("UpdateStatus called %d times, want 1", got)
	}
}
