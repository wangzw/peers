package harness

import (
	"strings"
	"testing"
	"time"
)

func TestConsistentlyPollSeparatesUnobservableFromViolation(t *testing.T) {
	t.Run("unobservable samples are retried", func(t *testing.T) {
		tb := &recordTB{}
		calls := 0
		ConsistentlyPoll(tb, 150*time.Millisecond, 50*time.Millisecond, "the invariant", nil, func(time.Time) Observation {
			calls++
			if calls < 3 {
				return Unobservable("no answer yet")
			}
			return Observed(true, "")
		})
		if calls < 3 {
			t.Fatalf("made %d samples, want at least 3", calls)
		}
	})

	t.Run("a window without an answer reports could not be observed", func(t *testing.T) {
		tb := &recordTB{}
		msg := captureFatal(t, tb, func() {
			ConsistentlyPoll(tb, 60*time.Millisecond, 10*time.Millisecond, "the invariant", nil, func(time.Time) Observation {
				return Unobservable("connection reset")
			})
		})
		if !strings.Contains(msg, "could not be observed") {
			t.Fatalf("failure %q does not report that the window could not be observed", msg)
		}
		if !strings.Contains(msg, "connection reset") {
			t.Fatalf("failure %q does not carry the last error", msg)
		}
	})

	t.Run("a conclusive violation fails at once", func(t *testing.T) {
		tb := &recordTB{}
		start := time.Now()
		msg := captureFatal(t, tb, func() {
			ConsistentlyPoll(tb, time.Second, 10*time.Millisecond, "the invariant", nil, func(time.Time) Observation {
				return Observed(false, "it broke")
			})
		})
		if !strings.Contains(msg, "it broke") {
			t.Fatalf("failure %q does not carry the violation", msg)
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("a violation took %s to fail, want it at once", elapsed)
		}
	})

	t.Run("an overrun fails at once with the stall evidence", func(t *testing.T) {
		tb := &recordTB{}
		stalls := 0
		msg := captureFatal(t, tb, func() {
			ConsistentlyPoll(tb, 160*time.Millisecond, 5*time.Millisecond, "the invariant", func(testing.TB) string {
				stalls++
				return "the fixture stack"
			}, func(time.Time) Observation {
				time.Sleep(10 * time.Millisecond)
				return Observed(true, "")
			})
		})
		if stalls != 1 {
			t.Fatalf("the stall hook ran %d times, want once", stalls)
		}
		for _, want := range []string{"could not be observed", "1 sample stalled", "the fixture stack"} {
			if !strings.Contains(msg, want) {
				t.Errorf("failure %q does not contain %q", msg, want)
			}
		}
		joined := strings.Join(tb.logs, "\n")
		if !strings.Contains(joined, "harness STALL") {
			t.Fatalf("the window did not log a STALL marker:\n%s", joined)
		}
	})
}

// TestConsistentlyPollStallsWhenTheWindowEndsBeforeTheCallBound reproduces the
// short-window defect: a sample starts with less time left than its call bound
// and its call stalls. The old code shortened the call to the time the window
// had left and did not mark the timeout as a stall, so the window ended without
// stall evidence. The call must run its full bound, be reported as a stall, and
// end the window at once.
func TestConsistentlyPollStallsWhenTheWindowEndsBeforeTheCallBound(t *testing.T) {
	backend := &blockingBackend{}
	session := &Session{name: "short-window", impl: backend}
	tb := &recordTB{}
	const (
		window    = 100 * time.Millisecond
		callBound = 400 * time.Millisecond
	)

	start := time.Now()
	msg := captureFatal(t, tb, func() {
		ConsistentlyPoll(tb, window, callBound, "the short window", nil, func(deadline time.Time) Observation {
			_, err := session.PollCallWithin(tb, deadline, callBound, "list_topics", nil)
			return Unobservable(err.Error())
		})
	})
	elapsed := time.Since(start)

	if msg == "" {
		t.Fatal("ConsistentlyPoll returned without reporting the stalled sample")
	}
	for _, want := range []string{"could not be observed", "1 sample stalled", "reached its 400ms poll bound"} {
		if !strings.Contains(msg, want) {
			t.Errorf("failure %q does not contain %q", msg, want)
		}
	}
	if elapsed < callBound {
		t.Fatalf("the window returned after %s, want at least the %s call bound: the call was cut short by the window",
			elapsed, callBound)
	}
	if logged := strings.Join(tb.logs, "\n"); !strings.Contains(logged, "=== harness STALL: the short window") {
		t.Errorf("the STALL marker was not logged:\n%s", logged)
	}
}
