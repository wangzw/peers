//go:build peers_testenv

package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestTestRuntimeRoot checks that the test runtime root places root under
// "root" and other accounts under their euid.
func TestTestRuntimeRoot(t *testing.T) {
	if got, want := testRuntimeRoot("/r", 1000), "/r/1000"; got != want {
		t.Errorf("testRuntimeRoot(%q, 1000) = %q, want %q", "/r", got, want)
	}
	if got, want := testRuntimeRoot("/r", 0), "/r/root"; got != want {
		t.Errorf("testRuntimeRoot(%q, 0) = %q, want %q", "/r", got, want)
	}
}

// TestTestEnvHooksInstalled checks that the environment hooks redirect the
// runtime directory and home directory when the selectors are set.
func TestTestEnvHooksInstalled(t *testing.T) {
	root := os.Getenv("PEERS_TEST_RUNTIME_ROOT")
	home := os.Getenv("PEERS_TEST_HOME")
	if root == "" || home == "" {
		t.Skip("PEERS_TEST_RUNTIME_ROOT and PEERS_TEST_HOME are not set")
	}
	if got, want := runtimeDirPath(1000), filepath.Join(root, "1000"); got != want {
		t.Errorf("runtimeDirPath(1000) = %q, want %q", got, want)
	}
	got, err := homeDirSource()
	if err != nil {
		t.Fatalf("homeDirSource() error = %v", err)
	}
	if got != home {
		t.Errorf("homeDirSource() = %q, want %q", got, home)
	}
}
