package broker

import (
	"testing"
	"time"
)

// TestCheckTicksReturnsStoppableTicker checks that an injected channel comes
// back with a nil ticker while a real ticker comes back non-nil and stoppable.
func TestCheckTicksReturnsStoppableTicker(t *testing.T) {
	injected := make(chan time.Time)
	tick, ticker := checkTicks(injected)
	if tick != injected {
		t.Fatalf("checkTicks(injected) channel = %v, want the injected channel", tick)
	}
	if ticker != nil {
		t.Fatalf("checkTicks(injected) ticker = %v, want nil", ticker)
	}

	tick, ticker = checkTicks(nil)
	if ticker == nil {
		t.Fatal("checkTicks(nil) ticker = nil; Run cannot stop the real ticker, so it leaks")
	}
	if tick != ticker.C {
		t.Fatalf("checkTicks(nil) channel = %v, want the ticker's own channel %v", tick, ticker.C)
	}
	// Stop is what Run defers; it must be callable on the returned ticker.
	ticker.Stop()
}
