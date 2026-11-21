//go:build !peers_testenv

package broker

import "testing"

// TestInboundLimitIntervalIgnoresTestOverrideInReleaseBuild proves that a build
// without the peers_testenv tag never reads PEERS_TEST_INBOUND_LIMIT_INTERVAL,
// even when the variable is set. The goreleaser hook also checks that the
// literal variable name is absent from the release binary.
func TestInboundLimitIntervalIgnoresTestOverrideInReleaseBuild(t *testing.T) {
	t.Setenv("PEERS_TEST_INBOUND_LIMIT_INTERVAL", "1h")
	if got, want := inboundLimitInterval(), defaultInboundLimitInterval; got != want {
		t.Fatalf("inboundLimitInterval() = %v, want the release default %v", got, want)
	}
}
