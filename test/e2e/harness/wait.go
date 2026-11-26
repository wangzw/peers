package harness

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// PollInterval is how often the wait helpers re-run their condition.
const PollInterval = 50 * time.Millisecond

// EventTimeout is the fallback bound for a wait that no product timer governs.
// A wait that a product timer does govern must derive its bound from that timer
// and add EventTimeout only as margin.
const EventTimeout = 30 * time.Second

// RegistrationReportBound is how long a test waits for the Broker's
// not_registered event when a session name is unusable. It is
// ListAgentsWaitBound, which leaves room to retry one stalled list_agents poll.
const RegistrationReportBound = ListAgentsWaitBound

// Eventually calls cond immediately and then every PollInterval until cond
// reports success or bound elapses; on expiry it fails the test with the last
// state cond reported.
func Eventually(t testing.TB, bound time.Duration, what string, cond func() (ok bool, state string)) {
	t.Helper()
	eventuallyUntil(t, time.Now().Add(bound), bound, what, func(time.Time) (bool, string) {
		return cond()
	})
}

// EventuallyPoll is Eventually for a condition that makes poll-bounded tool
// calls. bound is lifted to ToolCallWaitBound, and cond receives the wait's
// deadline to pass to Session.PollCall or PollCallWithin.
func EventuallyPoll(t testing.TB, bound time.Duration, what string, cond func(deadline time.Time) (ok bool, state string)) {
	t.Helper()
	bound = effectiveWaitBound(bound)
	eventuallyUntil(t, time.Now().Add(bound), bound, what, cond)
}

// eventuallyUntil is the loop Eventually and EventuallyPoll share. It keeps the
// call that follows the final poll interval, because for a plain wait that call
// is a real observation: a condition that becomes true a few milliseconds
// before the deadline must still be seen. That call can start after the
// deadline, where a poll call answers it at once with "no time left in the
// wait", and that state must not replace the last state observed by a call that
// still had time left. cond reads the deadline only to decide whether the window
// still has time to start a sample; the sample then runs its own poll bound in
// full.
func eventuallyUntil(t testing.TB, deadline time.Time, bound time.Duration, what string, cond func(deadline time.Time) (ok bool, state string)) {
	t.Helper()
	var state string
	haveState := false
	for {
		startedWithTime := time.Now().Before(deadline)
		ok, callState := cond(deadline)
		if ok {
			return
		}
		if startedWithTime {
			state = callState
			haveState = true
		}
		if !time.Now().Before(deadline) {
			if !haveState {
				state = callState
			}
			break
		}
		time.Sleep(PollInterval)
	}
	t.Fatalf("%s: not within %s (last state: %s)", what, bound, state)
}

func effectiveWaitBound(bound time.Duration) time.Duration {
	if bound < ToolCallWaitBound {
		return ToolCallWaitBound
	}
	return bound
}

func liftWaitBound(bound time.Duration, tool string) time.Duration {
	if want := ToolWaitBound(tool); bound < want {
		return want
	}
	return bound
}

// CallUntilOK invokes a tool with a side effect that may be retried until it
// succeeds. It retries only when the result reports an error; a transport
// error fails the test on the first attempt, because the server may already
// have applied the call.
func (s *Session) CallUntilOK(t testing.TB, bound time.Duration, tool string, args map[string]any) Result {
	t.Helper()
	var result Result
	Eventually(t, bound, "calling "+tool, func() (bool, string) {
		result = s.Call(t, tool, args) // eventually-call-allow: transport errors must fail, not retry
		if result.IsError {
			return false, result.Text
		}
		return true, ""
	})
	return result
}

// WaitRegistered waits until the session's MCP server has registered it with
// its Broker. It polls list_agents, which needs the Agent identity and stays
// not_registered until registration completes, so its success is the observable
// registration point. bound is lifted to ListAgentsWaitBound.
func WaitRegistered(t testing.TB, s *Session, bound time.Duration) {
	t.Helper()
	EventuallyPoll(t, liftWaitBound(bound, "list_agents"), "the session is registered", func(deadline time.Time) (bool, string) {
		r, err := s.PollCall(t, deadline, "list_agents", nil)
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

// WaitConnected waits until the session's MCP server has connected to its
// Broker. It polls list_brokers, which needs no Agent identity, so it also
// works for an operator session that never registers. bound is lifted to
// ToolWaitBound("list_brokers").
func WaitConnected(t testing.TB, s *Session, bound time.Duration) {
	t.Helper()
	EventuallyPoll(t, liftWaitBound(bound, "list_brokers"), "the session is connected to its Broker", func(deadline time.Time) (bool, string) {
		r, err := s.PollCall(t, deadline, "list_brokers", nil)
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

// Consistently calls cond immediately and then every PollInterval for the
// whole window; the first failure ends the test with the state cond reported.
func Consistently(t testing.TB, window time.Duration, what string, cond func() (ok bool, state string)) {
	t.Helper()
	deadline := time.Now().Add(window)
	for {
		ok, state := cond()
		if !ok {
			t.Fatalf("%s: failed within %s (state: %s)", what, window, state)
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(PollInterval)
	}
}

// Observation is one sample a consistency window takes.
type Observation struct {
	// Conclusive reports whether the sample received an answer.
	Conclusive bool
	// OK reports whether the invariant held; it is meaningful only when
	// Conclusive is set.
	OK bool
	// State explains the sample for a failure message; for an inconclusive
	// sample it is the error.
	State string
	// Stalled reports that the sample was cut off instead of answering, by its
	// own poll bound or by the product's topic RPC timeout.
	Stalled bool
}

const stallStatePrefix = "[stalled] "

// StallState returns a state that marks the sample as stalled, with reason as
// its explanation.
func StallState(reason string) string {
	return stallStatePrefix + reason
}

// StallError wraps err with the stall marker and the poll bound it reached, so
// a helper that reports it as a state reaches Unobservable as a stall rather
// than as a retryable failure.
func StallError(pollBound time.Duration, err error) error {
	return fmt.Errorf("%sthe call reached its %s poll bound: %w", stallStatePrefix, pollBound, err)
}

// topicTimeoutText is the product's generic text for a Broker call that ran
// out of time: rpcError's no-detail branch renders textGenericLost, which any
// tool can return, so a change to the product's wording breaks this match.
const topicTimeoutText = "the Broker did not answer"

// TopicTimeoutState returns the stalled state for a topic tool error that is
// the product's topic RPC timeout, and "" for any other text. Without it the
// wait would retry the error as a state and could pass on an earlier sample.
func TopicTimeoutState(text string) string {
	if !strings.Contains(text, topicTimeoutText) {
		return ""
	}
	return StallState(text)
}

// Observed returns a conclusive sample: ok is whether the invariant held and
// state explains a violation.
func Observed(ok bool, state string) Observation {
	return Observation{Conclusive: true, OK: ok, State: state}
}

// Unobservable returns an inconclusive sample whose state is the error. A
// state carrying the stall marker marks the sample as stalled rather than
// retryable.
func Unobservable(state string) Observation {
	return Observation{State: state, Stalled: strings.HasPrefix(state, stallStatePrefix)}
}

// ConsistentlyPoll is Consistently for a condition that makes poll-bounded
// calls, sampling it every PollInterval over window. Each sample receives the
// window's deadline and runs its own poll bound in full even when the window has
// less time left, so a stalled call can outlast the window by at most one poll
// bound. pollBound is the stall threshold on a sample's whole run: one that took
// longer counts as stalled even when its call returned, and a non-positive
// pollBound turns that backstop off. onStall may be nil; when it is not, it logs
// evidence such as the node's Broker stack and returns where it logged it, and
// that location goes into the failure message. An inconclusive sample is
// retried, a conclusive violation fails at once, and a window with no conclusive
// sample fails as "could not be observed"; a stalled sample ends the window at
// once. The condition stays the last argument so the wait guard treats the call
// like the other waits.
func ConsistentlyPoll(t testing.TB, window, pollBound time.Duration, what string, onStall func(testing.TB) string, cond func(deadline time.Time) Observation) {
	t.Helper()
	deadline := time.Now().Add(window)
	var (
		lastErr    string
		conclusive int
	)
	for {
		sampleStart := time.Now()
		obs := cond(deadline)
		elapsed := time.Since(sampleStart)
		// The duration backstop catches a sample that returned inside its call
		// bound but ran past pollBound.
		if obs.Stalled || (pollBound > 0 && elapsed > pollBound) {
			state := obs.State
			if state == "" {
				state = "the sample took " + elapsed.String() + " and was cut off"
			}
			t.Logf("=== harness STALL: %s: a sample was cut off after %s; logging the node's Broker stack ===",
				what, elapsed)
			location := "no stall hook was given"
			if onStall != nil {
				location = onStall(t)
			}
			t.Fatalf("%s: could not be observed within %s: 1 sample stalled after %s (first stall): %s; the Broker stack is at %s",
				what, window, elapsed, state, location)
			return
		}
		if obs.Conclusive {
			conclusive++
			if !obs.OK {
				t.Fatalf("%s: failed within %s (state: %s)", what, window, obs.State)
				return
			}
		} else if obs.State != "" {
			lastErr = obs.State
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(PollInterval)
	}
	if conclusive == 0 {
		t.Fatalf("%s: could not be observed within %s (last error: %s)", what, window, lastErr)
	}
}
