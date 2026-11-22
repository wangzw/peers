package broker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

// newWriteInboxDelivery builds a delivery for the writeInbox tests and returns
// the buffer its log records go to.
func newWriteInboxDelivery(t *testing.T, settings sendSettings) (*delivery, *bytes.Buffer) {
	t.Helper()
	var level slog.LevelVar
	buf := &bytes.Buffer{}
	d := newDelivery(deliveryDeps{
		Settings: newFakeSettings(settings, "hostA"),
		Log:      NewLogger(buf, &level),
	})
	return d, buf
}

// writeInboxTarget builds a target Agent whose inbox writer is w.
func writeInboxTarget(name string, w inboxWriter) agentView {
	return agentView{Name: name, Session: inbox.Session{}, Inbox: w}
}

// TestWriteInboxSuccess checks that a first successful write returns written
// after exactly one call and passes the message id through unchanged.
func TestWriteInboxSuccess(t *testing.T) {
	const msgID = "3f2a1c4e-5b6d-4e8f-9a0b-1c2d3e4f5a6b"
	rec := newRecordingInbox()
	d, _ := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: 2,
		InboxBackoff: time.Millisecond,
		InboxTimeout: time.Second,
	})
	target := writeInboxTarget("api-worker", rec)

	got := d.writeInbox(context.Background(), time.Now().Add(time.Second), target, "api-worker@hostA", "hello", msgID)
	if got != written {
		t.Fatalf("outcome = %v, want written", got)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if calls[0].MsgID != msgID {
		t.Fatalf("MsgID = %q, want %q", calls[0].MsgID, msgID)
	}
}

// TestWriteInboxRetriesNotWritten checks that a certainly-not-written failure
// is retried exactly InboxRetries more times, with the backoff between calls
// (REQ-agent-messaging-018).
func TestWriteInboxRetriesNotWritten(t *testing.T) {
	const (
		retries = 2
		backoff = 5 * time.Millisecond
	)
	rec := newRecordingInbox(inbox.ErrNotWritten, inbox.ErrNotWritten, inbox.ErrNotWritten)
	d, _ := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: retries,
		InboxBackoff: backoff,
		InboxTimeout: time.Second,
	})
	target := writeInboxTarget("api-worker", rec)

	start := time.Now()
	got := d.writeInbox(context.Background(), start.Add(2*time.Second), target, "sig", "text", "msg")
	if got != notWritten {
		t.Fatalf("outcome = %v, want notWritten", got)
	}
	calls := rec.Calls()
	if len(calls) != retries+1 {
		t.Fatalf("calls = %d, want %d", len(calls), retries+1)
	}
	for i := 1; i < len(calls); i++ {
		if gap := calls[i].At.Sub(calls[i-1].At); gap < backoff {
			t.Fatalf("gap between calls %d and %d = %v, want at least %v", i-1, i, gap, backoff)
		}
	}
}

// TestWriteInboxRecoversOnRetry checks that a retry after ErrNotWritten can
// still succeed instead of giving up on the first failure.
func TestWriteInboxRecoversOnRetry(t *testing.T) {
	rec := newRecordingInbox(inbox.ErrNotWritten, nil)
	d, _ := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: 2,
		InboxBackoff: time.Millisecond,
		InboxTimeout: time.Second,
	})
	target := writeInboxTarget("api-worker", rec)

	got := d.writeInbox(context.Background(), time.Now().Add(time.Second), target, "sig", "text", "msg")
	if got != written {
		t.Fatalf("outcome = %v, want written", got)
	}
	if n := rec.Count(); n != 2 {
		t.Fatalf("calls = %d, want 2", n)
	}
}

// TestWriteInboxUncertainNoRetry checks that a possibly-written frame is never
// retried, so the target does not receive two copies
// (REQ-agent-messaging-020, REQ-agent-messaging-027).
func TestWriteInboxUncertainNoRetry(t *testing.T) {
	rec := newRecordingInbox(inbox.ErrUncertain)
	d, _ := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: 5,
		InboxBackoff: time.Millisecond,
		InboxTimeout: time.Second,
	})
	target := writeInboxTarget("api-worker", rec)

	got := d.writeInbox(context.Background(), time.Now().Add(time.Second), target, "sig", "text", "msg")
	if got != uncertain {
		t.Fatalf("outcome = %v, want uncertain", got)
	}
	if n := rec.Count(); n != 1 {
		t.Fatalf("calls = %d, want 1", n)
	}
}

// TestWriteInboxStopsAtDeadline checks that no write starts at or after the
// deadline, so a message reported as not sent cannot arrive later
// (REQ-agent-messaging-019).
func TestWriteInboxStopsAtDeadline(t *testing.T) {
	t.Run("deadline already passed", func(t *testing.T) {
		rec := newRecordingInbox()
		d, _ := newWriteInboxDelivery(t, sendSettings{
			InboxRetries: 10,
			InboxBackoff: time.Millisecond,
			InboxTimeout: time.Second,
		})
		target := writeInboxTarget("api-worker", rec)

		got := d.writeInbox(context.Background(), time.Now().Add(-time.Second), target, "sig", "text", "msg")
		if got != notWritten {
			t.Fatalf("outcome = %v, want notWritten", got)
		}
		if n := rec.Count(); n != 0 {
			t.Fatalf("calls = %d, want 0", n)
		}
	})

	t.Run("attempts stay before the deadline", func(t *testing.T) {
		rec := newRecordingInbox(inbox.ErrNotWritten)
		d, _ := newWriteInboxDelivery(t, sendSettings{
			InboxRetries: 10,
			InboxBackoff: 20 * time.Millisecond,
			InboxTimeout: time.Second,
		})
		target := writeInboxTarget("api-worker", rec)

		// The deadline here stands in for a caller's budget, but the wait for the
		// first attempt is only bounded by the package fallback: a loaded runner
		// must not make writeInbox observe an already-passed deadline and skip the
		// retriable attempts this subtest checks. A run whose deadline has already
		// passed before the first check, making zero attempts, is the case the
		// "deadline already passed" subtest covers.
		deadline := time.Now().Add(eventTimeout)
		got := d.writeInbox(context.Background(), deadline, target, "sig", "text", "msg")
		if got != notWritten {
			t.Fatalf("outcome = %v, want notWritten", got)
		}
		calls := rec.Calls()
		if len(calls) == 0 {
			t.Fatal("no call was made before the deadline")
		}
		for i, call := range calls {
			if !call.At.Before(deadline) {
				t.Fatalf("call %d at %v, want before the deadline %v", i, call.At, deadline)
			}
		}
	})
}

// TestWriteInboxTimeoutCappedByRemaining checks that one attempt never gets
// more time than is left before the deadline.
func TestWriteInboxTimeoutCappedByRemaining(t *testing.T) {
	rec := newRecordingInbox()
	d, _ := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: 2,
		InboxBackoff: time.Millisecond,
		InboxTimeout: time.Hour,
	})
	target := writeInboxTarget("api-worker", rec)

	// The remaining time is the package fallback rather than a short test
	// window, so a loaded runner still reaches the first attempt. It is also far
	// below InboxTimeout, so a cap that used the configured timeout instead of
	// the time left before the deadline passes a value this test rejects.
	got := d.writeInbox(context.Background(), time.Now().Add(eventTimeout), target, "sig", "text", "msg")
	if got != written {
		t.Fatalf("outcome = %v, want written", got)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if timeout := calls[0].Timeout; timeout <= 0 || timeout > eventTimeout {
		t.Fatalf("timeout = %v, want in (0, %v]", timeout, eventTimeout)
	}
}

// TestWriteInboxBadSignature checks that a signature defect is reported as not
// written, logged, and not retried.
func TestWriteInboxBadSignature(t *testing.T) {
	rec := newRecordingInbox(inbox.ErrBadSignature)
	d, buf := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: 2,
		InboxBackoff: time.Millisecond,
		InboxTimeout: time.Second,
	})
	target := writeInboxTarget("api-worker", rec)

	got := d.writeInbox(context.Background(), time.Now().Add(time.Second), target, "sig", "text", "msg")
	if got != notWritten {
		t.Fatalf("outcome = %v, want notWritten", got)
	}
	if n := rec.Count(); n != 1 {
		t.Fatalf("calls = %d, want 1", n)
	}
	if log := buf.String(); !strings.Contains(log, "bad_signature") {
		t.Fatalf("log = %q, want it to contain %q", log, "bad_signature")
	}
}

// TestWriteInboxUsesTargetsBackend checks that the write goes through the
// target's own backend, not some shared one; under a system Broker that keeps
// the write in the target session's account (design-broker.md:232).
func TestWriteInboxUsesTargetsBackend(t *testing.T) {
	first := newRecordingInbox()
	second := newRecordingInbox()
	d, _ := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: 2,
		InboxBackoff: time.Millisecond,
		InboxTimeout: time.Second,
	})

	got := d.writeInbox(context.Background(), time.Now().Add(time.Second), writeInboxTarget("first", first), "sig", "text", "msg")
	if got != written {
		t.Fatalf("outcome = %v, want written", got)
	}
	if n := first.Count(); n != 1 {
		t.Fatalf("first backend calls = %d, want 1", n)
	}
	if n := second.Count(); n != 0 {
		t.Fatalf("second backend calls = %d, want 0", n)
	}

	got = d.writeInbox(context.Background(), time.Now().Add(time.Second), writeInboxTarget("second", second), "sig", "text", "msg")
	if got != written {
		t.Fatalf("second target outcome = %v, want written", got)
	}
	if n := first.Count(); n != 1 {
		t.Fatalf("first backend calls after the second target = %d, want 1", n)
	}
	if n := second.Count(); n != 1 {
		t.Fatalf("second backend calls = %d, want 1", n)
	}
}

// TestWriteInboxNilBackend checks that a snapshot without a backend is
// reported as not written and logged instead of crashing the Broker.
func TestWriteInboxNilBackend(t *testing.T) {
	d, buf := newWriteInboxDelivery(t, sendSettings{
		InboxRetries: 2,
		InboxBackoff: time.Millisecond,
		InboxTimeout: time.Second,
	})
	target := agentView{Name: "api-worker", Session: inbox.Session{}}

	got := d.writeInbox(context.Background(), time.Now().Add(time.Second), target, "sig", "text", "msg")
	if got != notWritten {
		t.Fatalf("outcome = %v, want notWritten", got)
	}
	if log := buf.String(); !strings.Contains(log, "no_inbox_writer") {
		t.Fatalf("log = %q, want it to contain %q", log, "no_inbox_writer")
	}
}
