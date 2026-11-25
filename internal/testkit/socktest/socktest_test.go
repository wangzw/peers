package socktest

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fatalCaseEnv carries the name the child process of TestPathRejectsBadNames
// passes to Path.
const fatalCaseEnv = "SOCKTEST_PATH_FATAL_CASE"

// TestDirCreatesDirectoryUnderTempDir checks that Dir builds a 0700 directory
// directly under TMPDIR and removes it at cleanup.
func TestDirCreatesDirectoryUnderTempDir(t *testing.T) {
	base := shortTempRoot(t)
	t.Setenv("TMPDIR", base)

	var dir string
	t.Run("create", func(t *testing.T) {
		dir = Dir(t)
		if got := filepath.Dir(dir); got != base {
			t.Fatalf("Dir() = %q, want a directory directly under TMPDIR %q", dir, base)
		}
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("%s mode = %#o, want 0700", dir, got)
		}
		if got := len(filepath.Join(dir, strings.Repeat("x", MaxName))); got > MaxPath {
			t.Fatalf("Dir() = %q leaves a %d-byte socket path, over %d", dir, got, MaxPath)
		}
	})
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s after cleanup: err = %v, want the directory removed", dir, err)
	}
}

// TestDirFallsBackToTmpForLongTempDir checks that a TMPDIR too long for a
// socket path makes Dir use /tmp instead.
func TestDirFallsBackToTmpForLongTempDir(t *testing.T) {
	base := longTempRoot(t, "x")
	t.Setenv("TMPDIR", base)

	dir := Dir(t)
	if strings.HasPrefix(dir, base+string(filepath.Separator)) {
		t.Fatalf("Dir() = %q, want a directory outside the long TMPDIR %q", dir, base)
	}
	if !strings.HasPrefix(dir, "/tmp"+string(filepath.Separator)) {
		t.Fatalf("Dir() = %q, want a directory under /tmp", dir)
	}
	if len(dir) > maxDirLen {
		t.Fatalf("fallback directory %q is %d bytes, over the %d-byte limit", dir, len(dir), maxDirLen)
	}
	if got := len(filepath.Join(dir, strings.Repeat("x", MaxName))); got > MaxPath {
		t.Fatalf("fallback Dir() = %q leaves a %d-byte socket path, over %d", dir, got, MaxPath)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat %s: %v", dir, err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("%s mode = %#o, want 0700", dir, got)
	}
}

// TestPathBindsSocketWithMaxNameUnderLongTempDir checks that a MaxName-long
// socket name under a long TMPDIR still yields a path that binds.
func TestPathBindsSocketWithMaxNameUnderLongTempDir(t *testing.T) {
	base := longTempRoot(t, "t")
	t.Setenv("TMPDIR", base)

	name := strings.Repeat("x", MaxName)
	path := Path(t, name)
	if len(path) > MaxPath {
		t.Fatalf("Path() = %q is %d bytes, over %d", path, len(path), MaxPath)
	}
	if got := filepath.Base(path); got != name {
		t.Fatalf("Path() base = %q, want %q", got, name)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listening on %q (%d bytes): %v", path, len(path), err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("closing the listener: %v", err)
	}
}

// TestPathRejectsBadNames checks that Path fatals on a name that names no file,
// is too long, or contains a path separator.
func TestPathRejectsBadNames(t *testing.T) {
	if name, ok := os.LookupEnv(fatalCaseEnv); ok {
		Path(t, name)
		return
	}
	t.Run("empty", func(t *testing.T) {
		out := runPathFatalChild(t, "")
		if want := `socket name "" names no file`; !strings.Contains(out, want) {
			t.Fatalf("child output does not contain %q:\n%s", want, out)
		}
	})
	t.Run("dot", func(t *testing.T) {
		out := runPathFatalChild(t, ".")
		if want := `socket name "." names no file`; !strings.Contains(out, want) {
			t.Fatalf("child output does not contain %q:\n%s", want, out)
		}
	})
	t.Run("dotdot", func(t *testing.T) {
		out := runPathFatalChild(t, "..")
		if want := `socket name ".." names no file`; !strings.Contains(out, want) {
			t.Fatalf("child output does not contain %q:\n%s", want, out)
		}
	})
	t.Run("long", func(t *testing.T) {
		name := strings.Repeat("x", MaxName+1)
		out := runPathFatalChild(t, name)
		want := "socket name " + strconv.Quote(name) + " is 25 bytes, over the 24-byte limit"
		if !strings.Contains(out, want) {
			t.Fatalf("child output does not contain %q:\n%s", want, out)
		}
	})
	t.Run("separator", func(t *testing.T) {
		out := runPathFatalChild(t, "a/b")
		if want := `socket name "a/b" contains a path separator`; !strings.Contains(out, want) {
			t.Fatalf("child output does not contain %q:\n%s", want, out)
		}
	})
}

// TestFitsBoundary checks that exactly maxDirLen bytes fit and one more does
// not, so moving the bound by one fails here.
func TestFitsBoundary(t *testing.T) {
	for _, tc := range []struct {
		size int
		want bool
	}{
		{78, true},
		{79, false},
	} {
		dir := strings.Repeat("x", tc.size)
		if got := fits(dir); got != tc.want {
			t.Fatalf("fits(%d-byte directory) = %v, want %v", tc.size, got, tc.want)
		}
	}
}

// TestDirSupportsConcurrentCalls checks that concurrent Dir calls each return a
// distinct directory.
func TestDirSupportsConcurrentCalls(t *testing.T) {
	base := shortTempRoot(t)
	t.Setenv("TMPDIR", base)

	const calls = 4
	dirs := make([]string, calls)
	var wg sync.WaitGroup
	wg.Add(calls)
	for i := range dirs {
		go func() {
			defer wg.Done()
			dirs[i] = Dir(t)
		}()
	}
	wg.Wait()

	seen := make(map[string]struct{}, calls)
	for i, dir := range dirs {
		if dir == "" {
			t.Fatalf("concurrent call %d returned no path", i)
		}
		if _, ok := seen[dir]; ok {
			t.Fatalf("concurrent call %d reused %q", i, dir)
		}
		seen[dir] = struct{}{}
	}
}

// runPathFatalChild runs TestPathRejectsBadNames in a fresh process of this
// test binary with name as the case, and returns its combined output. A fatal
// call cannot be observed in the calling process, so the child must fail.
func runPathFatalChild(t *testing.T, name string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPathRejectsBadNames$", "-test.count=1")
	cmd.Env = append(os.Environ(), fatalCaseEnv+"="+name)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child calling Path(%q) succeeded, want a fatal failure:\n%s", name, out)
	}
	return string(out)
}

// shortTempRoot returns a directory under /tmp removed at cleanup. Tests use it
// as TMPDIR so they do not depend on the host's TMPDIR, which CI may set to a
// long path.
func shortTempRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "st-")
	if err != nil {
		t.Fatalf("creating a short temporary directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// longTempRoot returns an existing directory whose absolute path alone exceeds
// maxDirLen, inside a unique temporary root under /tmp, so a directory created
// within it cannot fit a socket path.
func longTempRoot(t *testing.T, fill string) string {
	t.Helper()
	parent, err := os.MkdirTemp("/tmp", "lt-")
	if err != nil {
		t.Fatalf("creating the long temporary root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	dir := filepath.Join(parent, strings.Repeat(fill, maxDirLen))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating a long temporary directory: %v", err)
	}
	if len(dir) <= maxDirLen {
		t.Fatalf("long temporary directory %q is %d bytes, want more than %d", dir, len(dir), maxDirLen)
	}
	return dir
}
