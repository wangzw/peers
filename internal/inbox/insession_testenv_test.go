//go:build peers_testenv

package inbox

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAncestorSkipSwitchSkipsAncestorEnvironment checks that a peers_testenv build skips the ancestor walk when the sandbox switch is set.
func TestAncestorSkipSwitchSkipsAncestorEnvironment(t *testing.T) {
	prepareInSession(t)
	t.Setenv(skipAncestorSessionCheckEnv, "1")
	start := os.Getppid()
	stubTree(t, fakeTree{
		parents: map[int]int{start: start + 1, start + 1: 1},
		envs:    map[int][]string{start + 1: {sessionEnvVar + "=/tmp/session/inbox.sock"}},
	})

	if InSession() {
		t.Fatal("InSession() = true although the sandbox switch should skip the ancestor walk")
	}
}

// TestAncestorSkipSwitchSkipsAncestorRecord checks that the switch also ignores a parent's registry record.
func TestAncestorSkipSwitchSkipsAncestorRecord(t *testing.T) {
	home := prepareInSession(t)
	t.Setenv(skipAncestorSessionCheckEnv, "1")
	start := os.Getppid()
	writeSessionRecord(t, filepath.Join(home, ".claude", "sessions"), start, `{"name":"worker"}`)
	stubTree(t, fakeTree{parents: map[int]int{start: 1}})

	if InSession() {
		t.Fatal("InSession() = true although the sandbox switch should skip the ancestor walk")
	}
}

// TestAncestorSkipSwitchKeepsOwnEnvironment checks that the switch leaves the process's own-environment check in force.
func TestAncestorSkipSwitchKeepsOwnEnvironment(t *testing.T) {
	prepareInSession(t)
	t.Setenv(skipAncestorSessionCheckEnv, "1")
	t.Setenv(sessionEnvVar, "/tmp/session/inbox.sock")
	stubReaders(t, missingParent, emptyEnviron)

	if !InSession() {
		t.Fatal("InSession() = false with its own session variable set; the switch must not disable the own-environment check")
	}
}

// TestAncestorSkipSwitchEmptyKeepsAncestorWalk checks that only a non-empty switch value turns the walk off.
func TestAncestorSkipSwitchEmptyKeepsAncestorWalk(t *testing.T) {
	prepareInSession(t)
	t.Setenv(skipAncestorSessionCheckEnv, "")
	start := os.Getppid()
	stubTree(t, fakeTree{
		parents: map[int]int{start: start + 1, start + 1: 1},
		envs:    map[int][]string{start + 1: {sessionEnvVar + "=/tmp/session/inbox.sock"}},
	})

	if !InSession() {
		t.Fatal("InSession() = false for an empty switch value; the ancestor walk must still run")
	}
}
