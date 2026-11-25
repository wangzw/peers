package config

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestNotifierDeliversInOrder checks that the notifier calls callbacks in
// submission order.
func TestNotifierDeliversInOrder(t *testing.T) {
	n := newNotifier(testLogger())
	defer n.close()
	got := make(chan int, 3)
	for i := 0; i < 3; i++ {
		i := i
		n.enqueue(func() { got <- i })
	}
	for want := 0; want < 3; want++ {
		select {
		case v := <-got:
			if v != want {
				t.Fatalf("notification %d arrived as %d", want, v)
			}
		case <-time.After(eventTimeout):
			t.Fatalf("notification %d did not arrive", want)
		}
	}
}

// TestNotifierRecoversFromPanic checks that a panicking callback does not stop
// later callbacks.
func TestNotifierRecoversFromPanic(t *testing.T) {
	n := newNotifier(testLogger())
	defer n.close()
	done := make(chan struct{})
	n.enqueue(func() { panic("boom") })
	n.enqueue(func() { close(done) })
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("the notifier stopped after a panicking callback")
	}
}

// TestNotifierCloseRunsCallbacksEnqueuedBeforeClose checks that close runs the
// callbacks enqueued before it in order and drops those enqueued after.
func TestNotifierCloseRunsCallbacksEnqueuedBeforeClose(t *testing.T) {
	n := newNotifier(testLogger()).(*asyncNotifier)
	closeMarked := make(chan struct{})
	n.closeMarked = func() { close(closeMarked) }

	started := make(chan struct{})
	release := make(chan struct{})
	ran := make(chan int, 3)
	n.enqueue(func() {
		close(started)
		<-release
		ran <- 1
	})
	n.enqueue(func() { ran <- 2 })
	n.enqueue(func() { ran <- 3 })

	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("the first callback never started")
	}
	closed := make(chan struct{})
	go func() {
		n.close()
		close(closed)
	}()

	// Do not release the first callback until close has set its closed flag:
	// the callbacks behind it are then provably still in the queue.
	select {
	case <-closeMarked:
	case <-time.After(eventTimeout):
		t.Fatal("close did not mark the notifier closed")
	}

	// A callback enqueued after close has marked the notifier closed but
	// before close returns must be dropped.
	lateRan := make(chan struct{}, 1)
	n.enqueue(func() { lateRan <- struct{}{} })

	close(release)

	for want := 1; want <= 3; want++ {
		select {
		case got := <-ran:
			if got != want {
				t.Fatalf("callback %d ran as %d, want submission order", want, got)
			}
		case <-time.After(eventTimeout):
			t.Fatalf("callback %d enqueued before close never ran", want)
		}
	}
	select {
	case <-closed:
	case <-time.After(eventTimeout):
		t.Fatal("close did not return after the queued callbacks ran")
	}
	select {
	case <-lateRan:
		t.Fatal("a callback enqueued after close ran")
	default:
	}

	// Nothing may start after close returned either.
	afterRan := make(chan struct{}, 1)
	n.enqueue(func() { afterRan <- struct{}{} })
	select {
	case <-afterRan:
		t.Fatal("a callback enqueued after close returned ran")
	default:
	}
}

// TestNotifierCloseIsIdempotentAndDropsLateWork checks that close can be called
// twice and that work enqueued afterwards is dropped and logged.
func TestNotifierCloseIsIdempotentAndDropsLateWork(t *testing.T) {
	var logs bytes.Buffer
	n := newNotifier(slog.New(slog.NewTextHandler(&logs, nil)))
	n.enqueue(func() {})
	n.close()
	n.close()

	// enqueue sees the closed flag, so the drop is asserted from the log entry it
	// writes rather than by waiting for a callback that must never happen.
	ran := make(chan struct{}, 1)
	n.enqueue(func() { ran <- struct{}{} })
	select {
	case <-ran:
		t.Fatal("a callback enqueued after close ran")
	default:
	}
	if got := logs.String(); !strings.Contains(got, "dropping a notification after close") {
		t.Fatalf("late enqueue was not logged; log = %q", got)
	}
}

// TestPackageAPIWithoutLoad checks the behaviour of the exported functions
// before Load.
func TestPackageAPIWithoutLoad(t *testing.T) {
	Close()
	if _, err := Get("broker.alias"); err == nil {
		t.Fatal("Get before Load did not fail")
	}
	if All() != nil {
		t.Fatal("All before Load did not return nil")
	}
	if Secret("pairing.static_key") != "" {
		t.Fatal("Secret before Load did not return an empty string")
	}
	if Advertised() != nil {
		t.Fatal("Advertised before Load did not return nil")
	}
	if Paths() != (PathSet{}) {
		t.Fatal("Paths before Load did not return the zero value")
	}
	if _, err := ReadAudit(time.Time{}); err == nil {
		t.Fatal("ReadAudit before Load did not fail")
	}
	Audit(Event{Event: EventLinkUp})
	assertPanics(t, func() { Validate("broker.alias", func(Value, Value) error { return nil }) })
	assertPanics(t, func() { Subscribe("broker.alias", func(Value, Value) {}) })
	assertPanics(t, func() { SubscribeAdvertised(func([]AdvertisedAddr, []AdvertisedAddr) {}) })
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	fn()
}
