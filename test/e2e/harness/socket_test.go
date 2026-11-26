package harness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// maxSocketPathBytes is the largest path a Unix socket can be bound to. The
// kernel's sun_path field is 104 bytes on macOS and 108 on Linux, both
// including the terminating NUL, so the macOS limit is the portable one.
const maxSocketPathBytes = 103

// socketPathFits reports whether path leaves room for sun_path's terminating
// NUL.
func socketPathFits(path string) bool {
	return len(path) <= maxSocketPathBytes
}

// chooseSocketDir creates a temporary directory holding socketName whose full
// path fits in sun_path, trying bases in order; an empty base means the process
// temporary directory, and callers pass "/tmp" because macOS puts temporary
// directories under /var/folders, often too long for a socket.
func chooseSocketDir(socketName string, bases ...string) (string, error) {
	var errs []error
	for _, base := range bases {
		dir, err := os.MkdirTemp(base, "ape2e-")
		if err != nil {
			errs = append(errs, fmt.Errorf("creating a directory under %q: %w", base, err))
			continue
		}
		if socketPathFits(filepath.Join(dir, socketName)) {
			return dir, nil
		}
		_ = os.RemoveAll(dir)
	}
	if len(errs) > 0 {
		return "", errors.Join(errs...)
	}
	return "", fmt.Errorf("no temporary directory short enough for the %s socket", socketName)
}

func newSocketTempDir(t *testing.T, socketName string) string {
	t.Helper()
	dir, err := chooseSocketDir(socketName, "", "/tmp")
	if err != nil {
		t.Fatalf("creating a socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// TestSocketPathFits checks socketPathFits against the limit, one byte over it, and a macOS temporary-directory path.
func TestSocketPathFits(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "short", path: "/tmp/broker.sock", want: true},
		{name: "at the limit", path: "/" + strings.Repeat("a", maxSocketPathBytes-1), want: true},
		{name: "one byte over", path: "/" + strings.Repeat("a", maxSocketPathBytes), want: false},
		{
			name: "macOS temporary directory",
			path: "/var/folders/xy/" + strings.Repeat("z", 80) + "/T/broker.sock",
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := socketPathFits(tc.path); got != tc.want {
				t.Errorf("socketPathFits(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

// TestChooseSocketDirUsesFirstFittingBase checks that chooseSocketDir uses the first base whose temporary directory is short enough.
func TestChooseSocketDirUsesFirstFittingBase(t *testing.T) {
	base, err := os.MkdirTemp("/tmp", "ape2e-base-")
	if err != nil {
		t.Fatalf("creating the base directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })

	dir, err := chooseSocketDir("broker.sock", base)
	if err != nil {
		t.Fatalf("chooseSocketDir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if !strings.HasPrefix(dir, base+string(os.PathSeparator)) {
		t.Errorf("directory = %q, want it under the first base %q", dir, base)
	}
	if socket := filepath.Join(dir, "broker.sock"); !socketPathFits(socket) {
		t.Errorf("socket path = %q, want it to fit in sun_path", socket)
	}
}

// TestChooseSocketDirFallsBackToShortBase checks that chooseSocketDir falls back to a short base when the first one is too long.
func TestChooseSocketDirFallsBackToShortBase(t *testing.T) {
	longBase := t.TempDir()
	for socketPathFits(filepath.Join(longBase, "broker.sock")) {
		longBase = filepath.Join(longBase, strings.Repeat("d", 40))
		if err := os.Mkdir(longBase, 0o755); err != nil {
			t.Fatalf("creating the long base: %v", err)
		}
	}

	dir, err := chooseSocketDir("broker.sock", longBase, "/tmp")
	if err != nil {
		t.Fatalf("chooseSocketDir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if strings.HasPrefix(dir, longBase+string(os.PathSeparator)) {
		t.Errorf("directory = %q, want a fallback outside the long base %q", dir, longBase)
	}
	if socket := filepath.Join(dir, "broker.sock"); !socketPathFits(socket) {
		t.Errorf("socket path = %q, want it to fit in sun_path", socket)
	}
}
