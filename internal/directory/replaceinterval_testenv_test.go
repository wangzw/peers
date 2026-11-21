//go:build peers_testenv

package directory

import (
	"testing"
	"time"
)

// TestDirectoryReplaceIntervalUsesTestOverride proves that a peers_testenv
// build reads PEERS_TEST_DIRECTORY_REPLACE_INTERVAL as a Go duration.
func TestDirectoryReplaceIntervalUsesTestOverride(t *testing.T) {
	t.Setenv(testDirectoryReplaceIntervalEnv, "1h")
	if got, want := directoryReplaceInterval(), time.Hour; got != want {
		t.Fatalf("directoryReplaceInterval() = %v with the override, want %v", got, want)
	}
}

// TestDirectoryReplaceIntervalAcceptsShortInterval proves the value the
// end-to-end suite injects, one second, is read as written.
func TestDirectoryReplaceIntervalAcceptsShortInterval(t *testing.T) {
	t.Setenv(testDirectoryReplaceIntervalEnv, "1s")
	if got, want := directoryReplaceInterval(), time.Second; got != want {
		t.Fatalf("directoryReplaceInterval() = %v with the override, want %v", got, want)
	}
}

// TestDirectoryReplaceIntervalFallsBackWhenInvalid proves that an unset,
// unparsable or non-positive PEERS_TEST_DIRECTORY_REPLACE_INTERVAL keeps the
// release default.
func TestDirectoryReplaceIntervalFallsBackWhenInvalid(t *testing.T) {
	for _, value := range []string{"", "not-a-duration", "5", "0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(testDirectoryReplaceIntervalEnv, value)
			if got, want := directoryReplaceInterval(), defaultReplaceInterval; got != want {
				t.Fatalf("directoryReplaceInterval() = %v for %q, want the default %v", got, value, want)
			}
		})
	}
}
