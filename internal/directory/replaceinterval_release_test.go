//go:build !peers_testenv

package directory

import "testing"

// TestDirectoryReplaceIntervalIgnoresTestOverrideInReleaseBuild proves that a
// build without the peers_testenv tag never reads
// PEERS_TEST_DIRECTORY_REPLACE_INTERVAL, even when the variable is set. The
// release build hook also checks that the literal variable name is absent from
// the binary.
func TestDirectoryReplaceIntervalIgnoresTestOverrideInReleaseBuild(t *testing.T) {
	t.Setenv("PEERS_TEST_DIRECTORY_REPLACE_INTERVAL", "1h")
	if got, want := directoryReplaceInterval(), defaultReplaceInterval; got != want {
		t.Fatalf("directoryReplaceInterval() = %v, want the release default %v", got, want)
	}
}
