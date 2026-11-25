//go:build !peers_testenv

package broker

import "testing"

// TestInboundLimitIntervalIgnoresTestOverrideInReleaseBuild checks that a release build ignores PEERS_TEST_INBOUND_LIMIT_INTERVAL.
func TestInboundLimitIntervalIgnoresTestOverrideInReleaseBuild(t *testing.T) {
	t.Setenv("PEERS_TEST_INBOUND_LIMIT_INTERVAL", "1h")
	if got, want := inboundLimitInterval(), defaultInboundLimitInterval; got != want {
		t.Fatalf("inboundLimitInterval() = %v, want the release default %v", got, want)
	}
}
