package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadReportsPathResolutionFailure checks that a home directory that cannot
// be determined is reported and no paths are published.
func TestLoadReportsPathResolutionFailure(t *testing.T) {
	base := t.TempDir()
	old := runtimeRoot
	runtimeRoot = func(int) string { return filepath.Join(base, "run") }
	t.Cleanup(func() { runtimeRoot = old })

	c := newLoadTestCore(t, t.TempDir(), oneRoute(t, "192.0.2.20"))
	c.homeDir = func() (string, error) { return "", errors.New("no home directory") }

	err := c.load(ModeUser, "")
	if err == nil {
		t.Fatal("load succeeded without a home directory")
	}
	if !strings.Contains(err.Error(), "cannot determine the home directory") {
		t.Fatalf("load error = %v, want it to name the home directory", err)
	}
	if c.paths.ConfigFile != "" {
		t.Fatalf("paths were published after path resolution failed: %+v", c.paths)
	}
}

// TestLoadReportsRuntimeCheckFailure checks that a failed runtime directory
// check is returned from load.
func TestLoadReportsRuntimeCheckFailure(t *testing.T) {
	c := newLoadTestCore(t, t.TempDir(), oneRoute(t, "192.0.2.20"))
	sentinel := errors.New("runtime directory unavailable")
	c.checkRuntime = func() (string, error) { return "", sentinel }

	if err := c.load(ModeUser, ""); !errors.Is(err, sentinel) {
		t.Fatalf("load error = %v, want the runtime check error", err)
	}
}

// TestCheckRuntimeForLoadCreatesThePreferredDirectory checks that the default
// runtime check creates the preferred runtime directory with its mode.
func TestCheckRuntimeForLoadCreatesThePreferredDirectory(t *testing.T) {
	base := t.TempDir()
	old := runtimeRoot
	runtimeRoot = func(int) string { return filepath.Join(base, "run") }
	t.Cleanup(func() { runtimeRoot = old })

	c := newCore()
	if err := c.checkRuntimeForLoad(); err != nil {
		t.Fatalf("checkRuntimeForLoad: %v", err)
	}
	info, err := os.Stat(filepath.Join(base, "run"))
	if err != nil {
		t.Fatalf("the runtime directory was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("the runtime path is a %v, want a directory", info.Mode())
	}
	want := os.FileMode(0o700)
	if os.Geteuid() == 0 {
		want = 0o755
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("runtime directory mode = %04o, want %04o", got, want)
	}
}

// TestLoadRejectsInvalidListenValues checks that a listen value that is not
// host:port is refused from both the command-line override and the file.
func TestLoadRejectsInvalidListenValues(t *testing.T) {
	t.Run("override", func(t *testing.T) {
		c := newLoadTestCore(t, t.TempDir(), oneRoute(t, "192.0.2.20"))
		err := c.load(ModeUser, "not-a-host-port")
		if !errors.Is(err, ErrInvalidValue) || !strings.Contains(err.Error(), "broker.listen") {
			t.Fatalf("load error = %v, want an invalid broker.listen error", err)
		}
	})

	t.Run("configured", func(t *testing.T) {
		home := t.TempDir()
		writeLoadTestConfig(t, home, "broker:\n  listen: not-a-host-port\n")
		c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
		err := c.load(ModeUser, "")
		if !errors.Is(err, ErrInvalidValue) || !strings.Contains(err.Error(), "broker.listen") {
			t.Fatalf("load error = %v, want an invalid broker.listen error", err)
		}
	})
}
