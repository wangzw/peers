package syscontainer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/test/e2e/harness"
)

// pollCallWithin makes one tool call from inside a wait, bounded by pollBound
// and run in full even when the wait has little time left, so a loaded runner
// cannot report a healthy Broker as unobservable. A deadline that has passed
// makes no call; a call that reaches pollBound carries the stall marker, and a
// transport failure is returned and logged so the wait can poll again. The call
// and log are parameters so the bound logic is testable without a live session.
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
