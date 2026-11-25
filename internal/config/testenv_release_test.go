//go:build !peers_testenv

package config

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

const (
	releaseChildEnv = "PEERS_TESTENV_RELEASE_CHILD"
	releaseRoot     = "/r"
	releaseHome     = "/h"
)

// TestTestEnvIgnoredInReleaseBuild checks that a build without the peers_testenv
// tag ignores the test environment hooks.
func TestTestEnvIgnoredInReleaseBuild(t *testing.T) {
	if os.Getenv(releaseChildEnv) == "1" {
		if got, want := runtimeDirPath(1000), "/tmp/peers-1000"; got != want {
			t.Errorf("runtimeDirPath(1000) = %q, want %q", got, want)
		}
		got, err := homeDirSource()
		if err != nil {
			t.Fatalf("homeDirSource() error = %v", err)
		}
		if got == releaseHome {
			t.Errorf("homeDirSource() = %q, release build must ignore PEERS_TEST_HOME", got)
		}
		return
	}
	runReleaseChild(t, "TestTestEnvIgnoredInReleaseBuild")
}

// runReleaseChild runs name in a fresh process and fails when it fails; the
// explicit PASS line catches a -test.run pattern that matches nothing, which
// would otherwise exit successfully without checking anything.
func runReleaseChild(t *testing.T, name string) {
	t.Helper()
	// The hooks are installed by package init, so the selectors must be in the
	// child's environment before it starts; t.Setenv runs after init and would
	// pass whether or not the hooks exist.
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.v")
	cmd.Env = releaseChildEnvVars(
		releaseChildEnv+"=1",
		"PEERS_TEST_RUNTIME_ROOT="+releaseRoot,
		"PEERS_TEST_HOME="+releaseHome,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("release-build child failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "--- PASS: "+name) {
		t.Fatalf("child did not run %s:\n%s", name, out)
	}
}

func releaseChildEnvVars(overrides ...string) []string {
	keys := make(map[string]bool, len(overrides))
	for _, kv := range overrides {
		keys[strings.SplitN(kv, "=", 2)[0]] = true
	}
	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, kv := range os.Environ() {
		if !keys[strings.SplitN(kv, "=", 2)[0]] {
			env = append(env, kv)
		}
	}
	return append(env, overrides...)
}
