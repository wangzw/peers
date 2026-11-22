package harness

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// PollInterval is how often Eventually and Consistently re-run their
// condition. Callers bound the total wait against the timers under test
// instead; a test that measures the span an observation covers uses this value
// to allow for the polling granularity.
const PollInterval = 50 * time.Millisecond

// EventTimeout is the fallback bound for a wait that no product timer governs.
// The event such a wait observes is caused by the test itself (a disconnect
// applied, a configuration file written, a process started), so only the
// runner's scheduling and the process, socket and filesystem crossings between
// the observation and the change set its latency. It only decides how long a
// wait that will never complete takes to fail: the success path selects on the
// event itself and never spends this time, so a larger value costs nothing.
// Thirty seconds is far above the worst normal latency under load; it is not a
// claim that an event should take that long. A wait that a product timer does
// govern must derive its bound from that timer and add EventTimeout only as
// margin, never use it as the bound itself.
const EventTimeout = 30 * time.Second

// RegistrationReportBound is how long a test waits for the Broker's
// not_registered event when a session name is unusable. The Broker reports a
// registration that has not completed after registrationWait, which is 10
// seconds (internal/broker/register.go; design-broker.md:78); two extra seconds
// let the event reach the MCP server process before the test gives up. The
// wait polls list_agents, whose handler may spend listAgentsRPCTimeout before
// it answers, so the bound is ListAgentsWaitBound: two list_agents poll bounds,
// which also leaves room to retry one stalled poll call.
const RegistrationReportBound = ListAgentsWaitBound

// Eventually calls cond immediately and then every 50ms until cond reports
// success or bound elapses. The first success returns. If the bound expires
// the test fails with the last state cond reported, which the caller uses to
// explain what it observed while waiting.
func Eventually(t testing.TB, bound time.Duration, what string, cond func() (ok bool, state string)) {
	t.Helper()
	eventuallyUntil(t, time.Now().Add(bound), bound, what, func(time.Time) (bool, string) {
		return cond()
	})
}

// EventuallyPoll is Eventually for a condition that makes poll-bounded tool
// calls. bound is lifted to ToolCallWaitBound; cond receives the wait's
// deadline to pass to Session.PollCall (or PollCallWithin).
func EventuallyPoll(t testing.TB, bound time.Duration, what string, cond func(deadline time.Time) (ok bool, state string)) {
	t.Helper()
	bound = effectiveWaitBound(bound)
	eventuallyUntil(t, time.Now().Add(bound), bound, what, cond)
}

// eventuallyUntil is the loop Eventually and EventuallyPoll share. cond
// receives the deadline the wait will enforce. A poll call reads it only to
// decide whether the window still has time to start a sample; the sample then
// runs its own poll bound in full and is never clamped to the time the wait has
// left.
func eventuallyUntil(t testing.TB, deadline time.Time, bound time.Duration, what string, cond func(deadline time.Time) (ok bool, state string)) {
	t.Helper()
	// The loop keeps the call that follows the final poll interval, because for
	// a plain wait that call is a real observation: a condition that becomes
	// true a few milliseconds before the deadline must still be seen. That call
	// can start after the deadline with no time left, and a poll call answers it
	// at once with "no time left in the wait". Its state must not replace the
	// last state observed by a call that still had time left.
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

// effectiveWaitBound lifts a caller's bound to ToolCallWaitBound, the fallback
// for a call with no product handler bound. A wait whose callback polls a
// known tool passes ToolWaitBound(tool) instead, so a call that uses its
// handler's full time still leaves room for a second poll; see WaitRegistered
// and WaitConnected.
func effectiveWaitBound(bound time.Duration) time.Duration {
	if bound < ToolCallWaitBound {
		return ToolCallWaitBound
	}
	return bound
}

// liftWaitBound returns bound lifted to ToolWaitBound(tool), keeping a longer
// caller bound. It is the tool-aware form of effectiveWaitBound.
func liftWaitBound(bound time.Duration, tool string) time.Duration {
	if want := ToolWaitBound(tool); bound < want {
		return want
	}
	return bound
}

// CallUntilOK invokes a tool that must succeed and may be retried, for a tool
// that has a side effect. It retries only when the tool's own result reports an
// error, which is a definite refusal. A transport error fails the test on the
// first attempt through Session.Call: the server may already have applied the
// call (a pairing key created, a message delivered), so a retry could apply it
// twice.
func (s *Session) CallUntilOK(t testing.TB, bound time.Duration, tool string, args map[string]any) Result {
	t.Helper()
	var result Result
	Eventually(t, bound, "calling "+tool, func() (bool, string) {
		// Call's sixty-second bound keeps a slow but successful side effect
		// from being cut off, and Call fails the test on a transport error
		// instead of retrying a call the server may already have applied.
		// Only an IsError result is retried below.
		result = s.Call(t, tool, args) // eventually-call-allow: transport errors must fail, not retry
		if result.IsError {
			return false, result.Text
		}
		return true, ""
	})
	return result
}

// WaitRegistered waits until the session's MCP server has registered it with
// its Broker. list_agents needs the Agent identity and fails with
// not_registered until registration completes, so its success is the
// observable registration point. get_config and list_brokers do not need the
// identity and can succeed while the Broker is still registering, so they
// cannot serve as this wait.
//
// bound is lifted to ListAgentsWaitBound by liftWaitBound, and EventuallyPoll
// lifts the result to at least ToolCallWaitBound, so one list_agents call that
// uses its handler's full time still leaves a second poll.
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
// Broker. A tool call made right after the MCP server starts can run while the
// background connection loop is still in PhaseStarting; it then answers with
// textStarting from internal/mcpserver/errtext.go, "the connection to the
// Broker is still being set up; retry shortly" (code internal), and every tool
// fails with it. list_brokers does not need the Agent identity, so this wait
// works for an operator session that never registers; polling it until it
// succeeds clears that window, and also the brief PhaseUnavailable window while
// the loop retries.
//
// This differs from WaitRegistered: this wait proves the Broker connection, not
// that registration completed. list_agents needs the Agent identity and stays
// not_registered until registration finishes, so it cannot serve an operator
// session. Among the identity-free tools, list_brokers is the cheapest read.
//
// bound is lifted to ToolWaitBound("list_brokers") by liftWaitBound, and
// EventuallyPoll lifts the result to at least ToolCallWaitBound, for the same
// reason as WaitRegistered.
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

// Consistently calls cond immediately and then every 50ms for the whole
// window. Any failure ends the test at once with the state cond reported. The
// window is a deliberate observation of duration, so tests that use it record
// why the window is long enough.
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

// Observation is one sample a consistency window takes. Conclusive separates
// "the sample received an answer" from "the sample could not be taken": a
// transport error or a call cut by its bound says nothing about the invariant
// and is retried, and only a conclusive sample can report a violation.
type Observation struct {
	// Conclusive reports whether the sample received an answer.
	Conclusive bool
	// OK reports whether the invariant held. It is meaningful only when
	// Conclusive is set.
	OK bool
	// State explains the sample for a failure message. For an inconclusive
	// sample it is the error, which the final "could not be observed" report
	// carries when no sample was conclusive.
	State string
	// Stalled reports that the sample was cut off instead of answering: its
	// call reached its own poll bound, or the tool returned the topic RPC
	// timeout error the product reports when the Broker does not answer. A
	// stalled sample is not a retryable state; it is evidence that the Broker
	// stopped answering, and ConsistentlyPoll ends the window on it.
	Stalled bool
}

// stallStatePrefix marks a state string as a stalled sample. It is the one
// place the marker is written and read, so a state built by PollCallWithin or
// by StallState reaches Unobservable as a stall without a second type.
const stallStatePrefix = "[stalled] "

// StallState reports a sample that was cut off: the product returned the topic
// RPC timeout error, or the call reached its own poll bound. An e2e helper that
// sees that error returns this state, and Unobservable marks the sample as
// stalled so the window stops instead of retrying a Broker that is not
// answering.
func StallState(reason string) string {
	return stallStatePrefix + reason
}

// StallError reports a call that reached its own poll bound: it wraps err and
// carries the stall marker, so a helper that reports the error as a state, here
// or in another test package, reaches Unobservable as a stall rather than as a
// retryable failure.
func StallError(pollBound time.Duration, err error) error {
	return fmt.Errorf("%sthe call reached its %s poll bound: %w", stallStatePrefix, pollBound, err)
}

// topicTimeoutText is the product's text for a Broker call that ran out of
// time inside a topic handler: rpcError's no-detail branch renders
// textGenericLost in internal/mcpserver/errtext.go.
const topicTimeoutText = "the Broker did not answer"

// TopicTimeoutState reports the stalled state for a topic tool error that is
// the product's topic RPC timeout. A list_topics, join_topic or leave_topic
// handler bounds its Broker call at topicRPCTimeout, so a Broker that does not
// answer returns an internal tool error after about ten seconds. That time is
// below the tool's poll bound, so the wait would otherwise retry the error as
// a state and could pass on an earlier successful sample. It returns "" for
// any other text.
func TopicTimeoutState(text string) string {
	if !strings.Contains(text, topicTimeoutText) {
		return ""
	}
	return StallState(text)
}

// Observed reports a sample that received an answer. ok is whether the
// invariant held; state explains a violation.
func Observed(ok bool, state string) Observation {
	return Observation{Conclusive: true, OK: ok, State: state}
}

// Unobservable reports a sample that received no answer, such as a transport
// error or a call that failed for a reason other than its own bound. A state
// carrying the stall marker means the sample was cut off by its own call bound
// or by the product's topic RPC timeout, which is evidence that the Broker
// stopped answering rather than a retryable blip.
func Unobservable(state string) Observation {
	return Observation{State: state, Stalled: strings.HasPrefix(state, stallStatePrefix)}
}

// ConsistentlyPoll is Consistently for a condition that makes poll-bounded
// calls. Each sample receives the window's deadline, which it passes to
// Session.PollCall or PollCallWithin. The call runs its own poll bound in full
// even when the window has less time left, so a slow call near the end of the
// window is not cut short and reported as unobservable; a stalled call can
// outlast the window by at most one poll bound. Samples are taken every 50ms
// for the whole window.
//
// An inconclusive sample is retried. A conclusive sample that reports a
// violation ends the test at once. A window that ends without one conclusive
// sample fails as "could not be observed" and carries the last error, which is
// a different outcome from observing a violation: a wait that never saw an
// answer must not pass, and must not be reported as if the invariant broke.
//
// A stalled sample ends the window at once as "could not be observed". A
// sample is stalled when its state carries the stall marker, which means the
// call reached its own poll bound or the product returned the topic RPC
// timeout error, or when the whole sample took longer than pollBound, which
// catches a sample that answered inside its call bound but ran past it before
// returning. The failure names the stall count, the first sample's duration and
// where the stack was logged. A window must not continue after that: a later
// successful sample would let it pass while the Broker is already dead, and the
// steps after it would fail somewhere less informative.
//
// A caller passes a stall hook before the condition, or nil. The hook logs
// evidence such as the node's Broker stack and returns where it logged it, so
// the failure message can point the reader at the stack:
//
//	harness.ConsistentlyPoll(t, window, harness.TopicCallPollBound, what, s.LogBrokerStack, cond)
//
// The condition stays the last argument so the wait guard treats this exactly
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
		// The stall signal comes from the sample itself, so it does not depend
		// on the sample's call bound matching the window's pollBound argument.
		// The duration backstop stays for a sample that returned inside its
		// call bound but whose remaining work ran past pollBound.
		if obs.Stalled || (pollBound > 0 && elapsed > pollBound) {
			state := obs.State
			if state == "" {
				state = "the sample took " + elapsed.String() + " and was cut off"
			}
			// This line is the first marker a CI log search can find; it also
			// names the stack the hook is about to log.
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
