package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestWriteConfigFileDoesNotFollowSymlink checks that writing through a
// symbolic link fails and leaves the link's target unchanged.
func TestWriteConfigFileDoesNotFollowSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.yaml")
	original := []byte("broker:\n  alias: original\n")
	if err := os.WriteFile(target, original, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "config.yaml")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	err := writeConfigFile(link, []byte("broker:\n  alias: changed\n"), ModeUser)
	if err == nil {
		t.Fatal("writeConfigFile wrote through a symbolic link")
	}
	if !errors.Is(err, ErrFileWrite) {
		t.Fatalf("writeConfigFile error = %v, want ErrFileWrite", err)
	}

	got, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("read the link target: %v", readErr)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("the link target changed:\ngot:\n%s\nwant:\n%s", got, original)
	}
}
