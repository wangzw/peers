package syscontainer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/wangzw/peers/test/e2e/harness"
)

type logRecorder struct {
	lines []string
}

func (r *logRecorder) logf(format string, args ...any) {
	r.lines = append(r.lines, fmt.Sprintf(format, args...))
}

// TestPollCallWithinDeadlinePassedMakesNoCall checks that a wait whose deadline
// has passed returns an error without making a tool call and logs the skipped
// call.
func TestPollCallWithinDeadlinePassedMakesNoCall(t *testing.T) {
	calls := 0
	var logs logRecorder
	text, isError, err := pollCallWithin("alice", "list_agents", time.Now().Add(-time.Second), harness.ToolCallPollBound,
		func(context.Context) (*mcp.CallToolResult, error) {
			calls++
			return &mcp.CallToolResult{}, nil
		}, logs.logf)

	if err == nil {
		t.Fatal("pollCallWithin returned no error for a deadline that has already passed")
	}
	if calls != 0 {
		t.Errorf("pollCallWithin made %d calls after the deadline passed, want 0", calls)
	}
	if text != "" || isError {
		t.Errorf("pollCallWithin returned (%q, %v), want (\"\", false)", text, isError)
	}
	if !strings.Contains(err.Error(), "no time left in the wait") {
		t.Errorf("error %q does not say the wait had no time left", err)
	}
	if len(logs.lines) != 1 {
		t.Fatalf("pollCallWithin logged %d lines, want one for the skipped call: %q", len(logs.lines), logs.lines)
	}
	for _, want := range []string{"alice", "list_agents", "no time left in the wait"} {
		if !strings.Contains(logs.lines[0], want) {
			t.Errorf("log line %q does not contain %q", logs.lines[0], want)
		}
	}
}

// TestPollCallWithinRunsItsFullBoundLateInTheWait checks that a call answering
// after the wait's remaining time but inside its poll bound completes and
// succeeds.
func TestPollCallWithinRunsItsFullBoundLateInTheWait(t *testing.T) {
	const (
		waitLeft  = 100 * time.Millisecond
		pollBound = 500 * time.Millisecond
		callTime  = 300 * time.Millisecond
	)
	answerAfter := func(ctx context.Context) (*mcp.CallToolResult, error) {
		select {
		case <-time.After(callTime):
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "answered"}},
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	var logs logRecorder
	start := time.Now()
	text, isError, err := pollCallWithin("alice", "list_agents", time.Now().Add(waitLeft), pollBound, answerAfter, logs.logf)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("a call that answers inside its poll bound failed after %s: %v", elapsed, err)
	}
	if text != "answered" || isError {
		t.Errorf("pollCallWithin returned (%q, %v), want (\"answered\", false)", text, isError)
	}
	if elapsed < callTime {
		t.Fatalf("the call returned after %s, want it to run its %s answer; the wait's remaining time cut it short", elapsed, callTime)
	}
	if len(logs.lines) != 0 {
		t.Errorf("pollCallWithin logged %q for a successful call", logs.lines)
	}
}

// TestPollCallWithinMarksItsOwnTimeoutAsAStall checks that a call cut by its
// poll bound returns a stall-marked error that wraps context.DeadlineExceeded
// and logs the failure.
func TestPollCallWithinMarksItsOwnTimeoutAsAStall(t *testing.T) {
	const (
		waitLeft  = 40 * time.Millisecond
		pollBound = 120 * time.Millisecond
	)
	waitForContext := func(ctx context.Context) (*mcp.CallToolResult, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var logs logRecorder
	start := time.Now()
	text, isError, err := pollCallWithin("alice", "list_agents", time.Now().Add(waitLeft), pollBound, waitForContext, logs.logf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("pollCallWithin returned no error for a call that timed out")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %v does not wrap context.DeadlineExceeded", err)
	}
	if !strings.HasPrefix(err.Error(), "[stalled] ") {
		t.Errorf("error %q does not carry the stall marker", err)
	}
	if !strings.Contains(err.Error(), "reached its 120ms poll bound") {
		t.Errorf("error %q does not name the poll bound the call reached", err)
	}
	if text != "" || isError {
		t.Errorf("pollCallWithin returned (%q, %v), want (\"\", false)", text, isError)
	}
	if elapsed < pollBound {
		t.Fatalf("the call returned after %s, want the full %s poll bound; the wait's remaining time cut it short", elapsed, pollBound)
	}
	if len(logs.lines) != 1 {
		t.Fatalf("pollCallWithin logged %d lines, want 1: %q", len(logs.lines), logs.lines)
	}
	line := logs.lines[0]
	for _, want := range []string{"alice", "list_agents", "failed after", "context deadline exceeded"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line %q does not contain %q", line, want)
		}
	}
}

// TestPollCallWithinReturnsCallShape checks that a successful call returns the
// same shape as Call: the text contents joined with a newline and the result's
// IsError flag, with no log.
func TestPollCallWithinReturnsCallShape(t *testing.T) {
	var logs logRecorder
	text, isError, err := pollCallWithin("alice", "list_agents", time.Now().Add(time.Minute), harness.ToolCallPollBound,
		func(ctx context.Context) (*mcp.CallToolResult, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("the call context has no deadline")
			}
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: "first"},
					&mcp.TextContent{Text: "second"},
				},
				IsError: true,
			}, nil
		}, logs.logf)

	if err != nil {
		t.Fatalf("pollCallWithin returned error %v for a successful call", err)
	}
	if text != "first\nsecond" {
		t.Errorf("text = %q, want %q", text, "first\nsecond")
	}
	if !isError {
		t.Error("isError = false, want the result's IsError flag")
	}
	if len(logs.lines) != 0 {
		t.Errorf("pollCallWithin logged %q for a successful call", logs.lines)
	}
}
