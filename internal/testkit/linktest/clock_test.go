package linktest

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestFakeClockFiresInDeadlineOrder(t *testing.T) {
	clock := NewFakeClock(time.Unix(0, 0))
	ch := make(chan string, 3)
	clock.AfterFunc(3*time.Second, func() { ch <- "3" })
	clock.AfterFunc(time.Second, func() { ch <- "1" })
	clock.AfterFunc(2*time.Second, func() { ch <- "2" })

	clock.Advance(3 * time.Second)

	want := []string{"1", "2", "3"}
	for _, expected := range want {
		select {
		case got := <-ch:
			if got != expected {
				t.Fatalf("timer fired out of order: got %q, want %q", got, expected)
			}
		case <-time.After(eventTimeout):
			t.Fatalf("timed out waiting for timer %q", expected)
		}
	}
}

func TestFakeClockStop(t *testing.T) {
	clock := NewFakeClock(time.Unix(0, 0))

	var fired atomic.Int32
	stop := clock.AfterFunc(time.Second, func() { fired.Add(1) })
	if !stop() {
		t.Fatal("first stop returned false")
	}
	if stop() {
		t.Fatal("second stop returned true")
	}

	barrier := make(chan struct{})
	clock.AfterFunc(time.Second, func() { close(barrier) })
	clock.Advance(2 * time.Second)
	select {
	case <-barrier:
	case <-time.After(eventTimeout):
		t.Fatal("barrier timer did not fire")
	}
	if n := fired.Load(); n != 0 {
		t.Fatalf("stopped timer fired %d times", n)
	}

	done := make(chan struct{})
	firedStop := clock.AfterFunc(time.Second, func() { close(done) })
	clock.Advance(time.Second)
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("timer did not fire")
	}
	if firedStop() {
		t.Fatal("stop after firing returned true")
	}
}

func TestFakeClockEarliest(t *testing.T) {
	start := time.Unix(0, 0)
	clock := NewFakeClock(start)
	if _, ok := clock.Earliest(); ok {
		t.Fatal("Earliest reported a timer on an empty clock")
	}

	clock.AfterFunc(3*time.Second, func() {})
	stopped := clock.AfterFunc(time.Second, func() {})
	if earliest, ok := clock.Earliest(); !ok || !earliest.Equal(start.Add(time.Second)) {
		t.Fatalf("Earliest = %v, %v; want %v, true", earliest, ok, start.Add(time.Second))
	}

	if !stopped() {
		t.Fatal("stop of the earliest timer returned false")
	}
	if earliest, ok := clock.Earliest(); !ok || !earliest.Equal(start.Add(3*time.Second)) {
		t.Fatalf("Earliest after stop = %v, %v; want %v, true", earliest, ok, start.Add(3*time.Second))
	}

	clock.Advance(3 * time.Second)
	if earliest, ok := clock.Earliest(); ok {
		t.Fatalf("Earliest after firing = %v, true; want the zero time, false", earliest)
	}
}

func TestFakeClockWaitPending(t *testing.T) {
	clock := NewFakeClock(time.Unix(0, 0))
	registered := make(chan struct{})
	go func() {
		clock.AfterFunc(time.Hour, func() {})
		close(registered)
	}()

	clock.WaitPending(t, 1)
	<-registered
	if got := clock.Pending(); got != 1 {
		t.Fatalf("Pending() = %d, want 1", got)
	}
}
