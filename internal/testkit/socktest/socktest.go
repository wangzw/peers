// Package socktest provides temporary directories whose Unix socket paths fit
// the sun_path limit on every supported platform.
package socktest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// MaxPath is the longest Unix socket path, in bytes, that binds on both linux
// and darwin.
const MaxPath = 103

// MaxName is the longest file name a caller may join to the directory Dir
// returns.
const MaxName = 24

// maxDirLen is the longest directory path that leaves room for a separator and
// a MaxName-long file name within MaxPath.
const maxDirLen = MaxPath - 1 - MaxName

// fits reports whether dir leaves room for a separator and a MaxName-long file
// name within MaxPath.
func fits(dir string) bool {
	return len(dir) <= maxDirLen
}

// Dir returns the absolute path of a new private directory (mode 0700) that is
// removed in tb's cleanup. A MaxName-long file name joined to the result fits
// MaxPath. The directory is created under os.TempDir(), or under /tmp when that
// path is too long or cannot be created; Dir fatals when neither fits. It is
// safe to call from parallel tests.
func Dir(tb testing.TB) string {
	tb.Helper()
	tmp := os.TempDir()
	dir, err := socketDir(tmp)
	if err == nil {
		tb.Cleanup(func() { _ = os.RemoveAll(dir) })
		return dir
	}
	first := err
	dir, err = socketDir("/tmp")
	if err != nil {
		tb.Fatalf("socktest: no temporary directory fits %d bytes: TMPDIR %q is %d bytes (%v); /tmp: %v",
			maxDirLen, tmp, len(tmp), first, err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// socketDir creates a directory under base that fits. A directory that does not
// fit is removed and its path reported in the error.
func socketDir(base string) (string, error) {
	dir, err := os.MkdirTemp(base, "socktest-")
	if err != nil {
		return "", fmt.Errorf("creating a temporary directory under %s: %w", base, err)
	}
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	if !fits(dir) {
		_ = os.RemoveAll(dir)
		return dir, fmt.Errorf("temporary directory %q is %d bytes, over the %d-byte limit", dir, len(dir), maxDirLen)
	}
	return dir, nil
}

// Path returns filepath.Join(Dir(tb), name), a socket path within MaxPath. It
// fatals when name is empty, is "." or "..", is longer than MaxName, or
// contains a path separator.
func Path(tb testing.TB, name string) string {
	tb.Helper()
	if name == "" || name == "." || name == ".." {
		tb.Fatalf("socktest: socket name %q names no file", name)
	}
	if len(name) > MaxName {
		tb.Fatalf("socktest: socket name %q is %d bytes, over the %d-byte limit", name, len(name), MaxName)
	}
	if strings.ContainsRune(name, filepath.Separator) {
		tb.Fatalf("socktest: socket name %q contains a path separator", name)
	}
	return filepath.Join(Dir(tb), name)
}
