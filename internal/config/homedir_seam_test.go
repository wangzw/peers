package config

import "testing"

// TestNewCoreUsesHomeDirSource checks that newCore reads the home directory
// through the homeDirSource seam.
func TestNewCoreUsesHomeDirSource(t *testing.T) {
	old := homeDirSource
	t.Cleanup(func() { homeDirSource = old })

	homeDirSource = func() (string, error) { return "/seam/home", nil }

	got, err := newCore().homeDir()
	if err != nil {
		t.Fatalf("homeDir() error = %v, want nil", err)
	}
	if got != "/seam/home" {
		t.Fatalf("homeDir() = %q, want %q", got, "/seam/home")
	}
}

// TestHomeDirSourceDefaultsToAccountHomeDir checks that the homeDirSource seam
// defaults to accountHomeDir.
func TestHomeDirSourceDefaultsToAccountHomeDir(t *testing.T) {
	gotSource, errSource := homeDirSource()
	gotAccount, errAccount := accountHomeDir()
	if gotSource != gotAccount {
		t.Fatalf("homeDirSource() = %q, accountHomeDir() = %q", gotSource, gotAccount)
	}
	if (errSource == nil) != (errAccount == nil) {
		t.Fatalf("homeDirSource() error = %v, accountHomeDir() error = %v", errSource, errAccount)
	}
}
