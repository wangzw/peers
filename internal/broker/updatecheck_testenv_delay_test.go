//go:build peers_testenv

package broker

import (
	"testing"
	"time"
)

// TestUpdateCheckInitialDelayUsesTestOverride checks that a peers_testenv build
// reads the override as a Go duration.
func TestUpdateCheckInitialDelayUsesTestOverride(t *testing.T) {
	t.Setenv(testUpdateInitialDelayEnv, "3s")
	if got, want := updateCheckInitialDelay(), 3*time.Second; got != want {
		t.Fatalf("updateCheckInitialDelay() = %v with the override, want %v", got, want)
	}
}

// TestUpdateCheckInitialDelayFallsBackWhenInvalid checks that an unset,
// unparsable or non-positive override keeps the one-minute default.
func TestUpdateCheckInitialDelayFallsBackWhenInvalid(t *testing.T) {
	for _, value := range []string{"", "not-a-duration", "5", "0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(testUpdateInitialDelayEnv, value)
			if got, want := updateCheckInitialDelay(), time.Minute; got != want {
				t.Fatalf("updateCheckInitialDelay() = %v for %q, want the default %v", got, value, want)
			}
		})
	}
}
