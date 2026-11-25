//go:build peers_testenv

package update

import "testing"

// TestAPIBaseUsesTestOverride checks that a peers_testenv build reads the
// override and falls back to DefaultAPI without one.
func TestAPIBaseUsesTestOverride(t *testing.T) {
	t.Setenv(testAPIEnv, "http://127.0.0.1:18123")
	if got, want := APIBase(), "http://127.0.0.1:18123"; got != want {
		t.Fatalf("APIBase() = %q, want the override %q", got, want)
	}
	t.Setenv(testAPIEnv, "")
	if got := APIBase(); got != DefaultAPI {
		t.Fatalf("APIBase() with no override = %q, want %q", got, DefaultAPI)
	}
}

// TestReleaseURLUsesTestOverrideWhenAPIEmpty checks that an empty API base
// reaches the test API in a test build.
func TestReleaseURLUsesTestOverrideWhenAPIEmpty(t *testing.T) {
	t.Setenv(testAPIEnv, "http://127.0.0.1:18123")
	want := "http://127.0.0.1:18123/repos/wangzw/peers/releases/latest"
	if got := releaseURL("", "latest"); got != want {
		t.Fatalf("releaseURL with an empty base = %q, want the test base %q", got, want)
	}
}
