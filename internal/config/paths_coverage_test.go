package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
)

// withZeroUmask clears the process umask for the test and restores it when the
// test ends. Tests must not run in parallel while the umask is cleared.
func withZeroUmask(t *testing.T) {
	t.Helper()
	old := syscall.Umask(0)
	t.Cleanup(func() { syscall.Umask(old) })
}

// withDirOwner makes the directory-owner lookup report owner for every entry.
func withDirOwner(t *testing.T, owner int) {
	t.Helper()
	old := dirOwnerUID
	dirOwnerUID = func(os.FileInfo) (int, bool) { return owner, true }
	t.Cleanup(func() { dirOwnerUID = old })
}

// assertDirMode checks that path is a directory whose permission bits are want.
func assertDirMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat %s: %v", path, err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", path)
	}
	if perm := info.Mode().Perm(); perm != want {
		t.Fatalf("%s mode = %#o, want %#o", path, perm, want)
	}
}

// TestResolvePathsOccupiedRuntimeDirWithoutFallback checks that a runtime
// directory held by another account with no usable fallback is an error.
func TestResolvePathsOccupiedRuntimeDirWithoutFallback(t *testing.T) {
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	target := filepath.Join(base, "target")
	mkdir(t, target, 0o700)
	symlink(t, target, preferred)
	missingBase := filepath.Join(base, "missing")

	oldRoot, oldFallback := runtimeRoot, runtimeFallbackBase
	runtimeRoot = func(int) string { return preferred }
	runtimeFallbackBase = func(int) (string, error) { return missingBase, nil }
	t.Cleanup(func() {
		runtimeRoot = oldRoot
		runtimeFallbackBase = oldFallback
	})

	c := &core{
		mode:    ModeUser,
		euid:    func() int { return 1000 },
		homeDir: func() (string, error) { return base, nil },
	}
	paths, err := c.resolvePaths()
	if err == nil {
		t.Fatalf("resolvePaths() = %+v, want an error", paths)
	}
	if !strings.Contains(err.Error(), "no other runtime directory is available") {
		t.Fatalf("error = %v, want it to say no other runtime directory is available", err)
	}
}

// TestDarwinFallbackBaseRejectsEmptyOutput checks that an empty getconf answer
// is an error and yields no fallback base.
func TestDarwinFallbackBaseRejectsEmptyOutput(t *testing.T) {
	resetFallbackBaseCache()
	old := getconfOutput
	getconfOutput = func(context.Context) ([]byte, error) { return []byte("  \n"), nil }
	t.Cleanup(func() {
		getconfOutput = old
		resetFallbackBaseCache()
	})

	dir, err := darwinFallbackBase(7100)
	if err == nil {
		t.Fatalf("an empty getconf answer was accepted as %q", dir)
	}
	if dir != "" {
		t.Fatalf("darwinFallbackBase returned %q with an error, want an empty string", dir)
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("error = %v, want it to name the empty answer", err)
	}
}

// TestUsableFallbackBaseRejectsUnsafeBases checks that a symbolic link, a
// regular file and a directory owned by another account are refused as a
// fallback base while a private directory is accepted.
func TestUsableFallbackBaseRejectsUnsafeBases(t *testing.T) {
	euid := os.Geteuid()
	base := t.TempDir()

	target := filepath.Join(base, "target")
	mkdir(t, target, 0o700)
	link := filepath.Join(base, "link")
	symlink(t, target, link)
	if usableFallbackBase(link, euid) {
		t.Error("a symbolic link was accepted as a fallback base")
	}

	file := filepath.Join(base, "file")
	writeFile(t, file)
	if usableFallbackBase(file, euid) {
		t.Error("a regular file was accepted as a fallback base")
	}

	other := filepath.Join(base, "other")
	mkdir(t, other, 0o700)
	if usableFallbackBase(other, euid+1) {
		t.Errorf("a directory owned by uid %d was accepted for uid %d", euid, euid+1)
	}

	good := filepath.Join(base, "good")
	mkdir(t, good, 0o700)
	if !usableFallbackBase(good, euid) {
		t.Error("a private directory owned by this account was refused")
	}
}

// withCandidatesSeams replaces the two runtime-directory seams for one test.
func withCandidatesSeams(t *testing.T, preferred, fallbackBase string, fallbackErr error) {
	t.Helper()
	oldRoot, oldFallback := runtimeRoot, runtimeFallbackBase
	runtimeRoot = func(int) string { return preferred }
	runtimeFallbackBase = func(int) (string, error) {
		if fallbackErr != nil {
			return "", fallbackErr
		}
		return fallbackBase, nil
	}
	t.Cleanup(func() {
		runtimeRoot = oldRoot
		runtimeFallbackBase = oldFallback
	})
}

// TestRuntimeDirCandidates covers the list of runtime directories a Broker may
// run in: root has one, a user has the preferred one plus the fallback, an
// unavailable fallback leaves only the preferred one, and a fallback equal to
// the preferred one is not repeated.
func TestRuntimeDirCandidates(t *testing.T) {
	const userEUID = 1001
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	fallbackBase := filepath.Join(base, "fallback")

	t.Run("root has only the preferred directory", func(t *testing.T) {
		withCandidatesSeams(t, preferred, fallbackBase, nil)
		if got := runtimeDirCandidates(0); !slices.Equal(got, []string{preferred}) {
			t.Fatalf("runtimeDirCandidates(0) = %v, want [%s]", got, preferred)
		}
	})

	t.Run("a user has the preferred and the fallback directory", func(t *testing.T) {
		withCandidatesSeams(t, preferred, fallbackBase, nil)
		want := []string{preferred, filepath.Join(fallbackBase, "peers")}
		if got := runtimeDirCandidates(userEUID); !slices.Equal(got, want) {
			t.Fatalf("runtimeDirCandidates(%d) = %v, want %v", userEUID, got, want)
		}
	})

	t.Run("an unavailable fallback leaves only the preferred directory", func(t *testing.T) {
		withCandidatesSeams(t, preferred, "", errors.New("no base"))
		if got := runtimeDirCandidates(userEUID); !slices.Equal(got, []string{preferred}) {
			t.Fatalf("runtimeDirCandidates(%d) = %v, want [%s]", userEUID, got, preferred)
		}
	})

	t.Run("a fallback equal to the preferred is not repeated", func(t *testing.T) {
		same := filepath.Join(fallbackBase, "peers")
		withCandidatesSeams(t, same, fallbackBase, nil)
		if got := runtimeDirCandidates(userEUID); !slices.Equal(got, []string{same}) {
			t.Fatalf("runtimeDirCandidates(%d) = %v, want [%s]", userEUID, got, same)
		}
	})
}

// TestAccountLockCandidatesExportedEntry checks the exported list: one
// broker.lock path per runtime directory the account's Broker may use.
func TestAccountLockCandidatesExportedEntry(t *testing.T) {
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	fallbackBase := filepath.Join(base, "fallback")
	withCandidatesSeams(t, preferred, fallbackBase, nil)

	want := []string{filepath.Join(preferred, "broker.lock")}
	if os.Geteuid() != 0 {
		want = append(want, filepath.Join(fallbackBase, "peers", "broker.lock"))
	}
	if got := AccountLockCandidates(); !slices.Equal(got, want) {
		t.Fatalf("AccountLockCandidates() = %v, want %v", got, want)
	}
}

// TestRuntimeDirExportedEntry checks the exported runtime-directory lookup for
// both a user account and root.
func TestRuntimeDirExportedEntry(t *testing.T) {
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	withCandidatesSeams(t, preferred, filepath.Join(base, "fallback"), nil)

	if got := RuntimeDir(1001); got != preferred {
		t.Fatalf("RuntimeDir(1001) = %q, want %q", got, preferred)
	}
	if got := RuntimeDir(0); got != preferred {
		t.Fatalf("RuntimeDir(0) = %q, want %q", got, preferred)
	}
}

// TestValidateDirReportsInspectFailure covers the first check: a path that
// cannot be inspected is an error naming the path.
func TestValidateDirReportsInspectFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	err := validateDir(missing, os.Geteuid(), permRuntime)
	if err == nil {
		t.Fatal("validateDir accepted a path that does not exist")
	}
	if !strings.Contains(err.Error(), "cannot inspect") {
		t.Fatalf("error = %v, want it to name the failed inspection", err)
	}
}

// TestValidateDirReportsOpenFailure checks that a directory this account cannot
// open is an error.
func TestValidateDirReportsOpenFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can open a mode-0000 directory")
	}
	dir := filepath.Join(t.TempDir(), "closed")
	if err := os.Mkdir(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := validateDir(dir, os.Geteuid(), permRuntime)
	if err == nil {
		t.Fatal("validateDir accepted a directory it cannot open")
	}
	if !strings.Contains(err.Error(), "cannot open") {
		t.Fatalf("error = %v, want it to name the failed open", err)
	}
}

// TestEnsureRuntimeDirRootMode checks that root's runtime directory is created
// with mode 0755.
func TestEnsureRuntimeDirRootMode(t *testing.T) {
	withZeroUmask(t)
	withDirOwner(t, 0)
	base := t.TempDir()
	preferred := filepath.Join(base, "runtime")

	oldRoot := runtimeRoot
	runtimeRoot = func(int) string { return preferred }
	t.Cleanup(func() { runtimeRoot = oldRoot })

	got, err := ensureRuntimeDir(0)
	if err != nil {
		t.Fatalf("ensureRuntimeDir(0): %v", err)
	}
	if got != preferred {
		t.Fatalf("ensureRuntimeDir(0) = %q, want %q", got, preferred)
	}
	assertDirMode(t, preferred, 0o755)
}

// TestPrepareDataDirsSystemPaths covers the system-mode directory rules: the
// configuration and audit directories are readable by group and other but not
// writable, the data directory is private, and a failure at any step stops the
// ones after it.
func TestPrepareDataDirsSystemPaths(t *testing.T) {
	withZeroUmask(t)
	withDirOwner(t, 0)
	euid := func() int { return 0 }

	systemPaths := func(base string) PathSet {
		return PathSet{
			ConfigFile: filepath.Join(base, "etc", "peers", "config.yaml"),
			DataDir:    filepath.Join(base, "var", "lib", "peers"),
			AuditLog:   filepath.Join(base, "var", "log", "peers", "audit.log"),
		}
	}

	t.Run("creates every system directory with its rule's mode", func(t *testing.T) {
		base := t.TempDir()
		paths := systemPaths(base)
		c := &core{mode: ModeSystem, euid: euid}

		if err := c.prepareDataDirs(paths); err != nil {
			t.Fatalf("prepareDataDirs: %v", err)
		}
		assertDirMode(t, filepath.Dir(paths.ConfigFile), 0o755)
		assertDirMode(t, paths.DataDir, 0o700)
		assertDirMode(t, filepath.Dir(paths.AuditLog), 0o755)
	})

	t.Run("stops when the configuration directory cannot be created", func(t *testing.T) {
		base := t.TempDir()
		paths := systemPaths(base)
		configDir := filepath.Dir(paths.ConfigFile)
		mkdir(t, filepath.Dir(configDir), 0o755)
		writeFile(t, configDir) // a regular file blocks the directory creation
		c := &core{mode: ModeSystem, euid: euid}

		err := c.prepareDataDirs(paths)
		if err == nil {
			t.Fatal("prepareDataDirs accepted a configuration directory that is a regular file")
		}
		if !strings.Contains(err.Error(), "cannot create") {
			t.Fatalf("error = %v, want it to name the failed creation", err)
		}
		if _, statErr := os.Lstat(paths.DataDir); statErr == nil {
			t.Error("the data directory was created after the configuration directory failed")
		}
	})

	t.Run("stops when the data directory is accessible by group", func(t *testing.T) {
		base := t.TempDir()
		paths := systemPaths(base)
		mkdir(t, paths.DataDir, 0o755)
		c := &core{mode: ModeSystem, euid: euid}

		err := c.prepareDataDirs(paths)
		if err == nil {
			t.Fatal("prepareDataDirs accepted a data directory accessible by group")
		}
		if !strings.Contains(err.Error(), "group or other") {
			t.Fatalf("error = %v, want it to name the group or other access", err)
		}
		assertDirMode(t, filepath.Dir(paths.ConfigFile), 0o755)
		if _, statErr := os.Lstat(filepath.Dir(paths.AuditLog)); statErr == nil {
			t.Error("the audit directory was created after the data directory failed")
		}
	})

	t.Run("stops when the audit directory is writable by group", func(t *testing.T) {
		base := t.TempDir()
		paths := systemPaths(base)
		auditDir := filepath.Dir(paths.AuditLog)
		mkdir(t, auditDir, 0o777)
		c := &core{mode: ModeSystem, euid: euid}

		err := c.prepareDataDirs(paths)
		if err == nil {
			t.Fatal("prepareDataDirs accepted an audit directory writable by group")
		}
		if !strings.Contains(err.Error(), "group or world writable") {
			t.Fatalf("error = %v, want it to name the group or world write access", err)
		}
		assertDirMode(t, paths.DataDir, 0o700)
	})
}
