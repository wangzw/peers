package inbox

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestLookup_DefaultConfigDir checks that an empty configuration directory resolves to the account's default and that Reread does the same.
func TestLookup_DefaultConfigDir(t *testing.T) {
	home := t.TempDir()
	other := t.TempDir()
	t.Setenv("HOME", home)
	// CLAUDE_CONFIG_DIR must not influence the default.
	t.Setenv("CLAUDE_CONFIG_DIR", other)

	defaultDir := filepath.Join(home, ".claude")
	aWriteRecord(t, defaultDir, 4201, map[string]any{
		"name":                "home-agent",
		"messagingSocketPath": "/tmp/home-inbox.sock",
	})
	aWriteRecord(t, other, 4202, map[string]any{
		"name":                "other-agent",
		"messagingSocketPath": "/tmp/home-inbox.sock",
	})

	s, err := Lookup("", "/tmp/home-inbox.sock")
	if err != nil {
		t.Fatalf("Lookup with the default configuration directory: %v", err)
	}
	if s.Name != "home-agent" || s.PID != 4201 {
		t.Fatalf("Lookup used the wrong configuration directory: %+v", s)
	}
	if s.ConfigDir != defaultDir {
		t.Fatalf("Session.ConfigDir = %q, want %q", s.ConfigDir, defaultDir)
	}

	reread, err := Reread(Session{PID: 4201})
	if err != nil {
		t.Fatalf("Reread with the default configuration directory: %v", err)
	}
	if reread.Name != "home-agent" || reread.ConfigDir != defaultDir {
		t.Fatalf("Reread resolved the wrong configuration directory: %+v", reread)
	}
}

// TestLookup_ConfigDirIsCleaned checks that a configuration directory is cleaned lexically and is not made absolute.
func TestLookup_ConfigDirIsCleaned(t *testing.T) {
	cwd, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolving working directory: %v", err)
	}
	dir := t.TempDir()
	aWriteRecord(t, dir, 4203, map[string]any{
		"name":                "relative-agent",
		"messagingSocketPath": "/tmp/relative-inbox.sock",
	})

	relative, err := filepath.Rel(cwd, dir)
	if err != nil {
		t.Fatalf("making the configuration directory relative: %v", err)
	}
	s, err := Lookup(relative, "/tmp/relative-inbox.sock")
	if err != nil {
		t.Fatalf("Lookup with a relative configuration directory: %v", err)
	}
	if want := filepath.Clean(relative); s.ConfigDir != want {
		t.Fatalf("Session.ConfigDir = %q, want the cleaned %q", s.ConfigDir, want)
	}
	if s.Name != "relative-agent" {
		t.Fatalf("Lookup returned the wrong record: %+v", s)
	}

	messy := dir + string(filepath.Separator) + "." + string(filepath.Separator)
	s, err = Lookup(messy, "/tmp/relative-inbox.sock")
	if err != nil {
		t.Fatalf("Lookup with an uncleaned configuration directory: %v", err)
	}
	if s.ConfigDir != dir {
		t.Fatalf("Session.ConfigDir = %q, want the cleaned %q", s.ConfigDir, dir)
	}
}

// TestLookup_NoRecord checks that Lookup reports ErrNoRecord when no usable record matches.
func TestLookup_NoRecord(t *testing.T) {
	t.Run("missing config directory", func(t *testing.T) {
		_, err := Lookup(filepath.Join(t.TempDir(), "absent"), "/tmp/none.sock")
		if !errors.Is(err, ErrNoRecord) {
			t.Fatalf("Lookup error = %v, want ErrNoRecord", err)
		}
	})

	t.Run("sessions is a regular file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "sessions"), []byte("not a directory"), 0o600); err != nil {
			t.Fatalf("creating sessions file: %v", err)
		}
		_, err := Lookup(dir, "/tmp/none.sock")
		if !errors.Is(err, ErrNoRecord) {
			t.Fatalf("Lookup error = %v, want ErrNoRecord", err)
		}
	})

	t.Run("no matching socket", func(t *testing.T) {
		dir := t.TempDir()
		aWriteRecord(t, dir, 4204, map[string]any{"messagingSocketPath": "/tmp/some.sock"})
		_, err := Lookup(dir, "/tmp/another.sock")
		if !errors.Is(err, ErrNoRecord) {
			t.Fatalf("Lookup error = %v, want ErrNoRecord", err)
		}
	})

	t.Run("empty socket path", func(t *testing.T) {
		dir := t.TempDir()
		aWriteRecord(t, dir, 4205, map[string]any{"messagingSocketPath": "/tmp/some.sock"})
		_, err := Lookup(dir, "")
		if !errors.Is(err, ErrNoRecord) {
			t.Fatalf("Lookup error = %v, want ErrNoRecord", err)
		}
	})
}
