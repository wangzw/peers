package mcpserver

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// runtimeFallbackCases is the shared scenario table. internal/config carries
// the same cases, because the two packages implement the rule separately and
// must agree on the location. configErr there means config refuses startup;
// here the same situation leaves the preferred path, which is where the MCP
// client then finds no socket.
var runtimeFallbackCases = []struct {
	name         string
	setup        func(t *testing.T, preferred, fallback string)
	wantFallback bool
}{
	{
		name:  "preferred directory is absent",
		setup: func(t *testing.T, preferred, fallback string) {},
	},
	{
		name: "preferred directory is a symbolic link",
		setup: func(t *testing.T, preferred, fallback string) {
			target := filepath.Join(filepath.Dir(preferred), "target")
			mkdir(t, target, 0o700)
			symlink(t, target, preferred)
			mkdir(t, fallback, 0o700)
		},
		wantFallback: true,
	},
	{
		name: "preferred path is a regular file",
		setup: func(t *testing.T, preferred, fallback string) {
			writeFile(t, preferred)
			mkdir(t, fallback, 0o700)
		},
		wantFallback: true,
	},
	{
		name: "preferred is occupied and the fallback base is missing",
		setup: func(t *testing.T, preferred, fallback string) {
			target := filepath.Join(filepath.Dir(preferred), "target")
			mkdir(t, target, 0o700)
			symlink(t, target, preferred)
		},
	},
	{
		name: "preferred is occupied and the fallback base is accessible by group",
		setup: func(t *testing.T, preferred, fallback string) {
			target := filepath.Join(filepath.Dir(preferred), "target")
			mkdir(t, target, 0o700)
			symlink(t, target, preferred)
			mkdir(t, fallback, 0o700)
			if err := os.Chmod(fallback, 0o750); err != nil {
				t.Fatal(err)
			}
		},
	},
	{
		name: "preferred is occupied and the fallback base is usable",
		setup: func(t *testing.T, preferred, fallback string) {
			target := filepath.Join(filepath.Dir(preferred), "target")
			mkdir(t, target, 0o700)
			symlink(t, target, preferred)
			mkdir(t, fallback, 0o700)
		},
		wantFallback: true,
	},
}

func withRuntimeDirs(t *testing.T, preferred, fallback string) {
	t.Helper()
	oldPreferred := preferredRuntimeDir
	oldFallback := fallbackRuntimeBase
	preferredRuntimeDir = func(int) (string, error) { return preferred, nil }
	fallbackRuntimeBase = func(int) (string, error) { return fallback, nil }
	t.Cleanup(func() {
		preferredRuntimeDir = oldPreferred
		fallbackRuntimeBase = oldFallback
	})
}

func mkdir(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(path, mode); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func symlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink %s: %v", link, err)
	}
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("occupied"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestRuntimeDirTreatsAnotherAccountsDirectoryAsOccupied covers the owner
// comparison: a plain directory this account does not own counts as occupied,
// so the MCP server looks in the fallback location instead of the directory
// another account planted. The root directory stands in for the other
// account's, so the test needs no root and no chown.
func TestRuntimeDirTreatsAnotherAccountsDirectoryAsOccupied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	info, err := os.Lstat("/")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		t.Skipf("/ is not a plain directory on this platform")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) == os.Geteuid() {
		t.Skip("/ is not owned by another account on this platform")
	}
	if !runtimeDirOccupied("/", os.Geteuid()) {
		t.Error("runtimeDirOccupied treated another account's directory as free")
	}

	base := t.TempDir()
	fallback := filepath.Join(base, "fallback")
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, "/", fallback)
	if want, got := filepath.Join(fallback, "peers"), runtimeDir(os.Geteuid()); got != want {
		t.Errorf("runtimeDir() = %q, want the fallback %q", got, want)
	}
}

// TestRuntimeDirFallbackTable pins where the MCP server looks for the Broker's
// local socket, per the shared scenarios.
func TestRuntimeDirFallbackTable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	for _, tc := range runtimeFallbackCases {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			preferred := filepath.Join(base, "preferred")
			fallback := filepath.Join(base, "fallback")
			tc.setup(t, preferred, fallback)
			withRuntimeDirs(t, preferred, fallback)

			want := preferred
			if tc.wantFallback {
				want = filepath.Join(fallback, "peers")
			}
			if got := runtimeDir(os.Geteuid()); got != want {
				t.Errorf("runtimeDir() = %q, want %q", got, want)
			}
		})
	}
}
