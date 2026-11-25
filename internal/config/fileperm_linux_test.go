//go:build linux

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func makeNonRootOwner(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() == 0 {
		if err := os.Chown(path, 12345, 12345); err != nil {
			t.Fatalf("chown %s: %v", path, err)
		}
	}
}

// TestConfigFileRootOnlyWritable checks the missing-file, symlink and
// other-owner cases of the root-only rule.
func TestConfigFileRootOnlyWritable(t *testing.T) {
	t.Run("a missing file is safe", func(t *testing.T) {
		ok, err := configFileRootOnlyWritable(filepath.Join(t.TempDir(), "absent.yaml"))
		if err != nil {
			t.Fatalf("configFileRootOnlyWritable: %v", err)
		}
		if !ok {
			t.Error("a missing file is unsafe, want safe")
		}
	})

	t.Run("a symlink is unsafe", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target.yaml")
		if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(dir, "config.yaml")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}

		ok, err := configFileRootOnlyWritable(link)
		if err != nil {
			t.Fatalf("configFileRootOnlyWritable: %v", err)
		}
		if ok {
			t.Error("a symlink is safe, want unsafe")
		}
	})

	t.Run("a file another account owns is unsafe", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		makeNonRootOwner(t, path, 0o600)

		ok, err := configFileRootOnlyWritable(path)
		if err != nil {
			t.Fatalf("configFileRootOnlyWritable: %v", err)
		}
		if ok {
			t.Error("a file owned by another account is safe, want unsafe")
		}
	})
}
