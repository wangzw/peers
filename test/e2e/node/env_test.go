package node

import (
	"strings"
	"testing"
)

// TestBaseEnvSetsInboundLimitInterval proves every Broker the end-to-end suite
// starts gets the long inbound quota refill interval, so a burst sent back to
// back cannot see a refilled token.
func TestBaseEnvSetsInboundLimitInterval(t *testing.T) {
	env := BaseEnv{Home: "/home/peer", Path: "/usr/bin"}.Build(nil)
	want := testInboundLimitIntervalEnv + "=" + testInboundLimitInterval
	for _, entry := range env {
		if strings.HasPrefix(entry, testInboundLimitIntervalEnv+"=") {
			if entry != want {
				t.Fatalf("environment holds %q, want %q", entry, want)
			}
			return
		}
	}
	t.Fatalf("environment does not hold %q", want)
}
