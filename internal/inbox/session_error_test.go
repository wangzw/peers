package inbox

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLookup_UnresolvableHome checks that an empty configuration directory is resolved against the account home and a missing home is reported.
func TestLookup_UnresolvableHome(t *testing.T) {
	t.Setenv("HOME", "")
	if _, err := Lookup("", "/tmp/inbox.sock"); err == nil ||
		!strings.Contains(err.Error(), "cannot resolve the home directory") {
		t.Fatalf("Lookup with an unresolvable home error = %v, want the home directory error", err)
	}

	t.Setenv("HOME", t.TempDir())
	if _, err := Lookup("", "/tmp/inbox.sock"); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("Lookup with a resolvable home error = %v, want ErrNoRecord", err)
	}
}

// TestReread_UnresolvableHome checks that Reread reports a home directory error when HOME cannot be resolved and ErrNoRecord once it can.
func TestReread_UnresolvableHome(t *testing.T) {
	t.Setenv("HOME", "")
	if _, err := Reread(Session{PID: 4242}); err == nil ||
		!strings.Contains(err.Error(), "cannot resolve the home directory") {
		t.Fatalf("Reread with an unresolvable home error = %v, want the home directory error", err)
	}

	t.Setenv("HOME", t.TempDir())
	if _, err := Reread(Session{PID: 4242}); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("Reread with a resolvable home error = %v, want ErrNoRecord", err)
	}
}

func aSkipWithoutFilePermissions(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permissions, so this denial cannot be produced")
	}
}

// TestLookup_RegistryPermissionDenied checks that a registry directory the account may not read is reported with a permission error.
func TestLookup_RegistryPermissionDenied(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 6101, map[string]any{
		"name":                "agent",
		"messagingSocketPath": socket,
	})
	sessions := aSessionsDir(dir)
	if err := os.Chmod(sessions, 0o000); err != nil {
		t.Fatalf("removing permissions from the registry directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(sessions, 0o700) })

	_, err := Lookup(dir, socket)
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("Lookup of an unreadable registry directory error = %v, want a permission error", err)
	}
	if !strings.Contains(err.Error(), "cannot read the session registry") ||
		!strings.Contains(err.Error(), sessions) {
		t.Fatalf("Lookup error %q does not name the registry directory %q", err, sessions)
	}

	if err := os.Chmod(sessions, 0o700); err != nil {
		t.Fatalf("restoring registry directory permissions: %v", err)
	}
	s, err := Lookup(dir, socket)
	if err != nil || s.PID != 6101 {
		t.Fatalf("Lookup after restoring permissions = (%+v, %v), want record 6101", s, err)
	}
}

// TestLookup_UnreadableRecordIsReported checks that a record the account may not open is reported with a permission error.
func TestLookup_UnreadableRecordIsReported(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 6201, map[string]any{
		"name":                "good",
		"messagingSocketPath": socket,
	})
	unreadable := aWriteRecord(t, dir, 6202, map[string]any{
		"name":                "other",
		"messagingSocketPath": filepath.Join(dir, "other.sock"),
	})
	if err := os.Chmod(unreadable, 0o000); err != nil {
		t.Fatalf("removing read permission from the record: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o600) })

	_, err := Lookup(dir, socket)
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("Lookup alongside an unreadable record error = %v, want a permission error", err)
	}
	if !strings.Contains(err.Error(), "cannot open session record") ||
		!strings.Contains(err.Error(), unreadable) {
		t.Fatalf("Lookup error %q does not name the unreadable record %q", err, unreadable)
	}

	if err := os.Chmod(unreadable, 0o600); err != nil {
		t.Fatalf("restoring record permissions: %v", err)
	}
	s, err := Lookup(dir, socket)
	if err != nil || s.PID != 6201 {
		t.Fatalf("Lookup after restoring permissions = (%+v, %v), want record 6201", s, err)
	}
}

// TestReadRecordData_ReportsInspectingPermissionDenied checks that a record under a directory that may not be searched is reported with a permission error.
func TestReadRecordData_ReportsInspectingPermissionDenied(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	dir := t.TempDir()
	sessions := aSessionsDir(dir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatalf("creating the registry directory: %v", err)
	}
	path := filepath.Join(sessions, "6301.json")
	aWriteFile(t, path, []byte(`{"name":"agent"}`))
	if err := os.Chmod(sessions, 0o000); err != nil {
		t.Fatalf("removing permissions from the registry directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(sessions, 0o700) })

	_, err := readRecordData(path)
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("readRecordData of a record under an unsearchable directory error = %v, want a permission error", err)
	}
	if !strings.Contains(err.Error(), "cannot inspect session record") ||
		!strings.Contains(err.Error(), path) {
		t.Fatalf("readRecordData error %q does not name the record %q", err, path)
	}

	if err := os.Chmod(sessions, 0o700); err != nil {
		t.Fatalf("restoring registry directory permissions: %v", err)
	}
	data, err := readRecordData(path)
	if err != nil {
		t.Fatalf("readRecordData after restoring permissions: %v", err)
	}
	if string(data) != `{"name":"agent"}` {
		t.Fatalf("readRecordData = %q, want the record bytes", data)
	}
}

// TestReadRecordData_ReportsOpenPermissionDenied checks that an unreadable record is reported with its path and a permission error.
func TestReadRecordData_ReportsOpenPermissionDenied(t *testing.T) {
	aSkipWithoutFilePermissions(t)
	dir := t.TempDir()
	path := aWriteRecord(t, dir, 6401, map[string]any{"name": "agent"})
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatalf("removing read permission from the record: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

	_, err := readRecordData(path)
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("readRecordData of an unreadable record error = %v, want a permission error", err)
	}
	if !strings.Contains(err.Error(), "cannot open session record") ||
		!strings.Contains(err.Error(), path) {
		t.Fatalf("readRecordData error %q does not name the record %q", err, path)
	}

	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("restoring record permissions: %v", err)
	}
	data, err := readRecordData(path)
	if err != nil {
		t.Fatalf("readRecordData after restoring permissions: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("readRecordData returned no bytes for a readable record")
	}
}
