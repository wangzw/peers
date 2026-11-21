//go:build peers_testenv

package broker

import (
	"testing"
	"time"
)

// TestInboundLimitIntervalUsesTestOverride proves that a peers_testenv build
// reads PEERS_TEST_INBOUND_LIMIT_INTERVAL as a Go duration.
func TestInboundLimitIntervalUsesTestOverride(t *testing.T) {
	t.Setenv(testInboundLimitIntervalEnv, "1h")
	if got, want := inboundLimitInterval(), time.Hour; got != want {
		t.Fatalf("inboundLimitInterval() = %v with the override, want %v", got, want)
	}
}

// TestInboundLimitIntervalFallsBackWhenInvalid proves that an unset, unparsable
// or non-positive PEERS_TEST_INBOUND_LIMIT_INTERVAL keeps the release default.
func TestInboundLimitIntervalFallsBackWhenInvalid(t *testing.T) {
	for _, value := range []string{"", "not-a-duration", "5", "0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(testInboundLimitIntervalEnv, value)
			if got, want := inboundLimitInterval(), defaultInboundLimitInterval; got != want {
				t.Fatalf("inboundLimitInterval() = %v for %q, want the default %v", got, value, want)
			}
		})
	}
}
