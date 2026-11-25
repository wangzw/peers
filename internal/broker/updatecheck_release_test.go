//go:build !peers_testenv

package broker

import (
	"testing"
	"time"
)

// TestUpdateCheckInitialDelayIgnoresTestOverrideInReleaseBuild checks that a
// release build ignores the test override even when the variable is set.
func TestUpdateCheckInitialDelayIgnoresTestOverrideInReleaseBuild(t *testing.T) {
	t.Setenv("PEERS_TEST_UPDATE_INITIAL_DELAY", "1ms")
	if got, want := updateCheckInitialDelay(), time.Minute; got != want {
		t.Fatalf("updateCheckInitialDelay() = %v, want the release default %v", got, want)
	}
}
