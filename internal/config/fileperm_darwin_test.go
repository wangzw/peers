//go:build darwin

package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestConfigFileRootOnlyWritableDarwin checks the root-only rule against real
// root-owned files, re-running itself through sudo to read the security ACL.
func TestConfigFileRootOnlyWritableDarwin(t *testing.T) {
	if os.Geteuid() != 0 {
		out, err := exec.Command("sudo", "-n", os.Args[0],
			"-test.run", "^TestConfigFileRootOnlyWritableDarwin$", "-test.v").CombinedOutput()
		if err != nil {
			// The macOS CI runner grants passwordless sudo: fail there rather
			// than skip, so a runner that loses that grant cannot silently drop
			// the ACL coverage.
			if os.Getenv("GITHUB_ACTIONS") == "true" {
				t.Fatalf("the macOS CI runner must allow passwordless sudo: %v\n%s", err, out)
			}
			t.Skipf("requires root to read the security ACL: %v\n%s", err, out)
		}
		return
	}

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
		path := writePermConfigFile(t, 0o600)
		if err := os.Chown(path, 12345, 12345); err != nil {
			t.Fatalf("chown %s: %v", path, err)
		}

		ok, err := configFileRootOnlyWritable(path)
		if err != nil {
			t.Fatalf("configFileRootOnlyWritable: %v", err)
		}
		if ok {
			t.Error("a file owned by another account is safe, want unsafe")
		}
	})

	t.Run("a root-owned file without an ACL is safe", func(t *testing.T) {
		path := writePermConfigFile(t, 0o600)

		ok, err := configFileRootOnlyWritable(path)
		if err != nil {
			t.Fatalf("configFileRootOnlyWritable: %v", err)
		}
		if !ok {
			t.Error("a root-only file is unsafe, want safe")
		}
	})

	t.Run("a root-owned 0640 file is safe", func(t *testing.T) {
		path := writePermConfigFile(t, 0o640)

		ok, err := configFileRootOnlyWritable(path)
		if err != nil {
			t.Fatalf("configFileRootOnlyWritable: %v", err)
		}
		if !ok {
			t.Error("a root-owned 0640 file is unsafe, want safe")
		}
	})

	t.Run("a root-owned file with an ACL is unsafe", func(t *testing.T) {
		path := writePermConfigFile(t, 0o600)
		if out, err := exec.Command("chmod", "+a", "everyone allow write", path).CombinedOutput(); err != nil {
			t.Fatalf("chmod +a %s: %v: %s", path, err, out)
		}

		ok, err := configFileRootOnlyWritable(path)
		if err != nil {
			t.Fatalf("configFileRootOnlyWritable: %v", err)
		}
		if ok {
			t.Error("a root-owned file with a write ACL is safe, want unsafe")
		}
	})
}

func writePermConfigFile(t *testing.T, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("x"), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
