package node

import (
	"strings"
	"testing"
)

// TestBaseEnvSetsInboundLimitInterval checks that the environment sets the long
// inbound quota refill interval.
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
