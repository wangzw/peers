//go:build !peers_testenv

package inbox

import (
	"os"
	"testing"
)

// TestAncestorSkipSwitchIgnoredInReleaseBuild checks that a build without the peers_testenv tag still runs the ancestor walk when the skip variable is set.
func TestAncestorSkipSwitchIgnoredInReleaseBuild(t *testing.T) {
	prepareInSession(t)
	t.Setenv("PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK", "1")
	start := os.Getppid()
	stubTree(t, fakeTree{
		parents: map[int]int{start: start + 1, start + 1: 1},
		envs:    map[int][]string{start + 1: {sessionEnvVar + "=/tmp/session/inbox.sock"}},
	})

	if !InSession() {
		t.Fatal("InSession() = false in a release build with PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK set, want the ancestor walk to run")
	}
}
