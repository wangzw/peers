package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runtimeFallbackCases is the shared scenario table. The mcpserver package
// carries the same cases, because the two packages implement the rule
// separately and must agree on the location.
//
// setup builds the preferred and fallback trees; wantFallback names whether a
// usable fallback exists; configErr names whether config must refuse startup.
var runtimeFallbackCases = []struct {
	name         string
	setup        func(t *testing.T, preferred, fallback string)
	wantFallback bool
	configErr    bool
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
		configErr: true,
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
		configErr: true,
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
	oldRoot := runtimeRoot
	oldFallback := runtimeFallbackBase
	runtimeRoot = func(int) string { return preferred }
	runtimeFallbackBase = func(int) (string, error) { return fallback, nil }
	t.Cleanup(func() {
		runtimeRoot = oldRoot
		runtimeFallbackBase = oldFallback
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

// TestRuntimeDirFallbackTable pins where the runtime directory is, per the
// shared scenarios.
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

			if tc.configErr {
				_, err := ensureRuntimeDir(os.Geteuid())
				if err == nil {
					t.Fatal("ensureRuntimeDir accepted an occupied preferred directory with no fallback")
				}
				if !strings.Contains(err.Error(), "no other runtime directory is available") {
					t.Errorf("ensureRuntimeDir error = %v, want it to say no other runtime directory is available", err)
				}
				return
			}

			want := preferred
			if tc.wantFallback {
				want = filepath.Join(fallback, "peers")
			}
			if got := runtimeDirPath(os.Geteuid()); got != want {
				t.Errorf("runtimeDirPath() = %q, want %q", got, want)
			}
		})
	}
}

// TestEnsureRuntimeDirUsesTheFallback covers the creation path: an occupied
// preferred directory yields the fallback directory, created and checked like a
// user runtime directory.
func TestEnsureRuntimeDirUsesTheFallback(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	fallback := filepath.Join(base, "fallback")
	target := filepath.Join(base, "target")
	mkdir(t, target, 0o700)
	symlink(t, target, preferred)
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, preferred, fallback)

	got, err := ensureRuntimeDir(os.Geteuid())
	if err != nil {
		t.Fatalf("ensureRuntimeDir: %v", err)
	}
	want := filepath.Join(fallback, "peers")
	if got != want {
		t.Fatalf("ensureRuntimeDir() = %q, want the fallback %q", got, want)
	}
	info, err := os.Lstat(got)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		t.Errorf("fallback directory mode = %v, want a private directory", info.Mode())
	}
}

// TestEnsureRuntimeDirRefusesOwnedPreferredWithLoosePermissions covers the one
// case that does not use the fallback: a preferred directory this account owns
// with permissions that are too wide. The account can fix it, so the ordinary
// rule still refuses startup.
func TestEnsureRuntimeDirRefusesOwnedPreferredWithLoosePermissions(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	fallback := filepath.Join(base, "fallback")
	mkdir(t, preferred, 0o700)
	if err := os.Chmod(preferred, 0o750); err != nil {
		t.Fatal(err)
	}
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, preferred, fallback)

	if _, err := ensureRuntimeDir(os.Geteuid()); err == nil {
		t.Fatal("ensureRuntimeDir accepted a preferred directory accessible by group")
	}
	if _, err := os.Lstat(filepath.Join(fallback, "peers")); err == nil {
		t.Error("the fallback was created for a preferred directory this account owns")
	}
}

// TestRuntimeDirTreatsAnotherAccountsDirectoryAsOccupied covers the owner
// comparison itself: a directory this account does not own counts as taken even
// though it is a plain directory. The root directory stands in for the other
// account's directory, so the test needs no root and no chown.
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
	if owner, ok := dirOwnerUID(info); !ok || owner == os.Geteuid() {
		t.Skip("/ is not owned by another account on this platform")
	}

	base := t.TempDir()
	fallback := filepath.Join(base, "fallback")
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, "/", fallback)

	want := filepath.Join(fallback, "peers")
	if got := runtimeDirPath(os.Geteuid()); got != want {
		t.Errorf("runtimeDirPath() = %q, want the fallback %q", got, want)
	}
}

// TestEnsureRuntimeDirReResolvesWhenThePreferredPathIsTakenInBetween covers the
// race the creation path handles: the preferred directory looks free when it is
// resolved and belongs to another account by the time it is created. The owner
// lookup is replaced so the same directory plays both states, which lets the
// test run without root.
func TestEnsureRuntimeDirReResolvesWhenThePreferredPathIsTakenInBetween(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	fallback := filepath.Join(base, "fallback")
	mkdir(t, preferred, 0o700)
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, preferred, fallback)

	euid := os.Geteuid()
	oldOwner := dirOwnerUID
	lookups := 0
	dirOwnerUID = func(info os.FileInfo) (int, bool) {
		if info.Name() != filepath.Base(preferred) {
			return oldOwner(info)
		}
		// The first lookup keeps the preferred path; every later one sees it
		// as another account's directory.
		lookups++
		if lookups == 1 {
			return euid, true
		}
		return euid + 1, true
	}
	t.Cleanup(func() { dirOwnerUID = oldOwner })

	got, err := ensureRuntimeDir(euid)
	if err != nil {
		t.Fatalf("ensureRuntimeDir: %v", err)
	}
	if want := filepath.Join(fallback, "peers"); got != want {
		t.Fatalf("ensureRuntimeDir() = %q, want the fallback %q after re-resolving", got, want)
	}
	if lookups < 2 {
		t.Fatalf("the owner was read %d times, want the directory re-resolved after it was taken", lookups)
	}
}

// TestResolvePathsRecordsTheFallback covers the PathSet the Broker logs from.
func TestResolvePathsRecordsTheFallback(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the fallback rule applies to non-root accounts")
	}
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	fallback := filepath.Join(base, "fallback")
	target := filepath.Join(base, "target")
	mkdir(t, target, 0o700)
	symlink(t, target, preferred)
	mkdir(t, fallback, 0o700)
	withRuntimeDirs(t, preferred, fallback)

	c := &core{
		mode:    ModeUser,
		euid:    os.Geteuid,
		homeDir: func() (string, error) { return base, nil },
	}
	paths, err := c.resolvePaths()
	if err != nil {
		t.Fatalf("resolvePaths: %v", err)
	}
	if want := filepath.Join(fallback, "peers"); paths.RuntimeDir != want {
		t.Errorf("RuntimeDir = %q, want %q", paths.RuntimeDir, want)
	}
	if paths.RuntimeDirFallback == nil {
		t.Fatal("RuntimeDirFallback is nil for a fallback runtime directory")
	}
	if paths.RuntimeDirFallback.TakenPath != preferred {
		t.Errorf("TakenPath = %q, want %q", paths.RuntimeDirFallback.TakenPath, preferred)
	}
}
