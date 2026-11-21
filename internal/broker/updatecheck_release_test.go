//go:build !peers_testenv

package broker

import (
	"testing"
	"time"
)

// TestUpdateCheckInitialDelayIgnoresTestOverrideInReleaseBuild proves that a
// build without the peers_testenv tag never reads
// PEERS_TEST_UPDATE_INITIAL_DELAY, even when the variable is set. The
// goreleaser hook also checks that the literal variable name is absent from
// the release binary.
func TestUpdateCheckInitialDelayIgnoresTestOverrideInReleaseBuild(t *testing.T) {
	t.Setenv("PEERS_TEST_UPDATE_INITIAL_DELAY", "1ms")
	if got, want := updateCheckInitialDelay(), time.Minute; got != want {
		t.Fatalf("updateCheckInitialDelay() = %v, want the release default %v", got, want)
	}
}
