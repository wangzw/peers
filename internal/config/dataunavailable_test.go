package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dataLoadCore builds a user-mode core that really prepares the data directory
// under home.
func dataLoadCore(t *testing.T, home string) *core {
	t.Helper()
	c := newLoadTestCore(t, home, oneRoute(t, "192.0.2.20"))
	c.skipDataCheck = false
	return c
}

// TestLoadClassifiesDataDirectoryFailure checks that an unusable data directory
// makes Load report a DataUnavailableError matching both the sentinel and the
// concrete cause.
func TestLoadClassifiesDataDirectoryFailure(t *testing.T) {
	home := t.TempDir()
	c := dataLoadCore(t, home)
	if err := os.Chmod(home, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(home, 0o700) })

	err := c.load(ModeUser, "")
	if !errors.Is(err, ErrDataUnavailable) {
		t.Fatalf("load error = %v, want ErrDataUnavailable", err)
	}
	var de *DataUnavailableError
	if !errors.As(err, &de) {
		t.Fatalf("load error %T is not a *DataUnavailableError", err)
	}
	if de.Paths.DataDir != filepath.Join(home, ".peers") {
		t.Errorf("Paths.DataDir = %q", de.Paths.DataDir)
	}
	if de.Err == nil {
		t.Fatal("DataUnavailableError carries no cause")
	}
}

// errorTextWithoutPaths removes the resolved paths from a DataUnavailableError's
// text, so a check that no configuration value leaks does not mistake a path for
// one; longer paths go first because the others live under them.
func errorTextWithoutPaths(err error, paths PathSet) string {
	text := err.Error()
	for _, p := range []string{paths.AuditLog, paths.ConfigFile, paths.DataDir, paths.RuntimeDir} {
		if p != "" {
			text = strings.ReplaceAll(text, p, "")
		}
	}
	return text
}

// TestLoadCarriesSocketSettingsOnAuditFailure checks that an unopenable audit
// log still carries the loaded socket settings and keeps them out of the error
// text.
func TestLoadCarriesSocketSettingsOnAuditFailure(t *testing.T) {
	// A fixed "0660" in the home directory makes a check that mistakes a path
	// for a configuration value fail deterministically rather than only when a
	// random temporary name happens to contain the digits.
	home := filepath.Join(t.TempDir(), "home-0660")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(filepath.Join(dir, "audit.log"), 0o700); err != nil {
		t.Fatal(err)
	}
	body := "broker:\n  local_socket_group: agents\n  local_socket_mode: \"0660\"\n"
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c := dataLoadCore(t, home)

	err := c.load(ModeUser, "")
	if !errors.Is(err, ErrDataUnavailable) {
		t.Fatalf("load error = %v, want ErrDataUnavailable", err)
	}
	var de *DataUnavailableError
	if !errors.As(err, &de) {
		t.Fatalf("load error %T is not a *DataUnavailableError", err)
	}
	if !strings.Contains(err.Error(), de.Paths.AuditLog) {
		t.Errorf("error text %q does not name the audit log %s", err, de.Paths.AuditLog)
	}
	if de.LocalSocketGroup != "agents" || de.LocalSocketMode != "0660" {
		t.Errorf("socket settings = %q/%q, want agents/0660", de.LocalSocketGroup, de.LocalSocketMode)
	}
	if text := errorTextWithoutPaths(err, de.Paths); strings.Contains(text, "agents") || strings.Contains(text, "0660") {
		t.Errorf("error text %q names a configuration value", err)
	}
}

// TestLoadReportsParseErrorBeforeDataUnavailable checks that an unparsable
// configuration is reported as a configuration error even when the data
// directory is also unusable.
func TestLoadReportsParseErrorBeforeDataUnavailable(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("broker: [unclosed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(home, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(home, 0o700) })
	c := dataLoadCore(t, home)

	err := c.load(ModeUser, "")
	if err == nil {
		t.Fatal("load succeeded on an unparsable configuration")
	}
	if errors.Is(err, ErrDataUnavailable) {
		t.Fatalf("load error = %v, want a configuration error", err)
	}
}

// TestLoadCanBeRetriedAfterDataUnavailable checks that the same core loads
// successfully once the data directory becomes available.
func TestLoadCanBeRetriedAfterDataUnavailable(t *testing.T) {
	home := t.TempDir()
	c := dataLoadCore(t, home)
	if err := os.Chmod(home, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := c.load(ModeUser, ""); !errors.Is(err, ErrDataUnavailable) {
		t.Fatalf("first load error = %v, want ErrDataUnavailable", err)
	}
	if err := os.Chmod(home, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := c.load(ModeUser, ""); err != nil {
		t.Fatalf("second load: %v", err)
	}
	if c.audit == nil {
		t.Error("the retried load did not open the audit log")
	}
}
