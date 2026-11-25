//go:build !peers_testenv

package directory

import "testing"

// TestDirectoryReplaceIntervalIgnoresTestOverrideInReleaseBuild checks that a release build ignores the environment override.
func TestDirectoryReplaceIntervalIgnoresTestOverrideInReleaseBuild(t *testing.T) {
	t.Setenv("PEERS_TEST_DIRECTORY_REPLACE_INTERVAL", "1h")
	if got, want := directoryReplaceInterval(), defaultReplaceInterval; got != want {
		t.Fatalf("directoryReplaceInterval() = %v, want the release default %v", got, want)
	}
}
