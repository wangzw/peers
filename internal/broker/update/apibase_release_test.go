//go:build !peers_testenv

package update

import "testing"

// TestAPIBaseIgnoresTestOverrideInReleaseBuild checks that a release build
// ignores PEERS_TEST_UPDATE_API even when it is set.
func TestAPIBaseIgnoresTestOverrideInReleaseBuild(t *testing.T) {
	t.Setenv("PEERS_TEST_UPDATE_API", "http://127.0.0.1:1")
	if got := APIBase(); got != DefaultAPI {
		t.Fatalf("APIBase() = %q, a release build must ignore the test override", got)
	}
}
