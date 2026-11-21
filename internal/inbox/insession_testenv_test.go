//go:build peers_testenv

package inbox

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAncestorSkipSwitchSkipsAncestorEnvironment proves that a peers_testenv
// build honors the sandbox switch by not walking the parent chain. The test
// runner itself lives inside a session's process tree, so a Broker started by
// a test sandbox would otherwise be refused as if a session had started it.
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

// TestAncestorSkipSwitchSkipsAncestorRecord proves that the switch turns off
// the whole ancestor walk, not only the ancestor environment read: a registry
// record for the parent must not mark the process as in-session either.
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

// TestAncestorSkipSwitchKeepsOwnEnvironment proves that the switch turns off
// only the parent-chain walk. A process that carries the session variable in
// its own environment is still refused, so a test that exercises the refusal
// through the sandbox behaves like production.
func TestAncestorSkipSwitchKeepsOwnEnvironment(t *testing.T) {
	prepareInSession(t)
	t.Setenv(skipAncestorSessionCheckEnv, "1")
	t.Setenv(sessionEnvVar, "/tmp/session/inbox.sock")
	stubReaders(t, missingParent, emptyEnviron)

	if !InSession() {
		t.Fatal("InSession() = false with its own session variable set; the switch must not disable the own-environment check")
	}
}

// TestAncestorSkipSwitchEmptyKeepsAncestorWalk proves that only a non-empty
// value turns the walk off, so an empty entry left in a sandbox environment is
// the same as an absent one.
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
