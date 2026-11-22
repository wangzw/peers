package syscontainer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/test/e2e/harness"
)

// pollCallWithin makes one tool call from inside a wait. The call is bounded by
// pollBound and runs that bound in full; it is not shortened when the wait has
// less time left, because a call cut short by the window would report a healthy
// Broker as unobservable on a loaded runner. A stalled call can therefore
// outlast its wait by up to one poll bound. A deadline that has already passed
// makes no call and returns an error instead.
//
// A call that reaches pollBound is marked as a stall, so a wait or consistency
// window can tell a Broker that stopped answering from a retryable failure.
// The call and the log are parameters so the bound logic is testable without a
// live MCP session: Session.PollCall passes the session's CallTool and t.Logf.
// A transport or protocol failure is returned as an error and logged with the
// session, the tool, the elapsed time and the error instead of failing the
// test, so the wait can poll again.
func pollCallWithin(session, tool string, deadline time.Time, pollBound time.Duration, call func(context.Context) (*mcp.CallToolResult, error), logf func(format string, args ...any)) (text string, isError bool, err error) {
	if !time.Now().Before(deadline) {
		err := fmt.Errorf("syscontainer: calling %s as %s: no time left in the wait", tool, session)
		logf("syscontainer: session %s: %s not called: no time left in the wait", session, tool)
		return "", false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), pollBound)
	defer cancel()
	start := time.Now()
	result, callErr := call(ctx)
	if callErr != nil {
		err = fmt.Errorf("syscontainer: calling %s as %s: %w", tool, session, callErr)
		if pollBound > 0 && harness.IsCallTimeout(callErr) {
			err = fmt.Errorf("%w (syscontainer: calling %s as %s)", harness.StallError(pollBound, callErr), tool, session)
		}
		logf("syscontainer: session %s: %s failed after %s: %v", session, tool, time.Since(start), err)
		return "", false, err
	}
	text, isError = toolResultText(result)
	return text, isError, nil
}

// toolResultText returns the text and IsError flag Call and PollCall report: the
// result's text contents joined with a newline.
func toolResultText(result *mcp.CallToolResult) (string, bool) {
	var texts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			texts = append(texts, text.Text)
		}
	}
	return strings.Join(texts, "\n"), result.IsError
}
