//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

// pollDecode calls tool from inside a wait and decodes its structured result
// into T. A transport or tool failure is returned as the state string the wait
// callback reports, so the wait can poll again. join_topic, leave_topic and
// list_topics are polled with harness.TopicCallPollBound because each makes a
// Broker RPC bounded by topicRPCTimeout before the update hint runs; every
// other tool uses the default harness.ToolCallPollBound.
//
// A topic tool that returns the product's topic RPC timeout error did not
// answer: its call stayed inside topicRPCTimeout and never reached the poll
// bound. The state is marked as a stall so ConsistentlyPoll ends the window on
// evidence instead of retrying a Broker that stopped answering.
func pollDecode[T any](t *testing.T, deadline time.Time, s *harness.Session, tool string, args map[string]any) (T, string) {
	t.Helper()
	var zero T
	pollBound := harness.ToolCallPollBound
	if brokerTopicTool(tool) {
		pollBound = harness.TopicCallPollBound
	}
	r, err := s.PollCallWithin(t, deadline, pollBound, tool, args)
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
