//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

// pollDecode calls tool inside a wait and reports a topic tool's RPC timeout
// as a stall state rather than retrying it.
func pollDecode[T any](t *testing.T, deadline time.Time, s *harness.Session, tool string, args map[string]any) (T, string) {
	t.Helper()
	var zero T
	r, err := s.PollCallWithin(t, deadline, harness.ToolPollBound(tool), tool, args)
	if err != nil {
		return zero, err.Error()
	}
	if r.IsError {
		if brokerTopicTool(tool) {
			if state := harness.TopicTimeoutState(r.Text); state != "" {
				return zero, state
			}
		}
		return zero, r.Text
	}
	out, err := harness.Decode[T](string(r.Structured))
	if err != nil {
		return zero, "decoding " + tool + ": " + err.Error()
	}
	return out, ""
}
