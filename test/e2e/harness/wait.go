package harness

import (
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
// bound is set by the product's own timer, not the three-second test bound, and
// it must also leave room to retry one stalled poll call, so it is at least
// ToolCallWaitBound (fourteen seconds).
const RegistrationReportBound = ToolCallWaitBound

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
// receives the deadline the wait will enforce, so a poll call made from it can
// be clamped to the time the wait has left.
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

// effectiveWaitBound lifts a caller's bound to ToolCallWaitBound. These waits
// make a poll-bounded call, and PollCall clamps that call to the time the wait
// has left. A caller that passes three seconds, as most tests do, would
// therefore cut the first call at three seconds, which is exactly the update
// hint's own timeout; a Broker answering the hint slowly would consume the
// whole wait and the wait would fail on a call that was never given its poll
// bound. The caller's bound still applies when it is longer.
func effectiveWaitBound(bound time.Duration) time.Duration {
	if bound < ToolCallWaitBound {
		return ToolCallWaitBound
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
// bound is lifted to ToolCallWaitBound by EventuallyPoll so one poll call gets
// its full poll bound instead of being clamped to a short caller's bound.
func WaitRegistered(t testing.TB, s *Session, bound time.Duration) {
	t.Helper()
	EventuallyPoll(t, bound, "the session is registered", func(deadline time.Time) (bool, string) {
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
// bound is lifted to ToolCallWaitBound by EventuallyPoll for the same reason as
// WaitRegistered.
func WaitConnected(t testing.TB, s *Session, bound time.Duration) {
	t.Helper()
	EventuallyPoll(t, bound, "the session is connected to its Broker", func(deadline time.Time) (bool, string) {
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
