//go:build !peers_testenv

package update

import "testing"

// TestReleaseURLDefaultsToThePublicAPI checks that an empty base resolves to
// the GitHub API release endpoint.
func TestReleaseURLDefaultsToThePublicAPI(t *testing.T) {
	want := DefaultAPI + "/repos/wangzw/peers/releases/latest"
	if got := releaseURL("", "latest"); got != want {
		t.Errorf("releaseURL(%q, %q) = %q, want %q", "", "latest", got, want)
	}
}
