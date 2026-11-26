package mcpserver

import (
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// TestUserSocketPath checks the user socket path for an account and that no
// environment variable changes it.
func TestUserSocketPath(t *testing.T) {
	const want = "/tmp/peers-1001/broker.sock"
	if got := userSocketPath(1001); got != want {
		t.Fatalf("userSocketPath(1001) = %q, want %q", got, want)
	}

	t.Setenv("TMPDIR", "/elsewhere/tmp")
	t.Setenv("XDG_RUNTIME_DIR", "/elsewhere/xdg")
	t.Setenv("PEERS_TEST_RUNTIME_ROOT", "/elsewhere/test")
	if got := userSocketPath(1001); got != want {
		t.Fatalf("userSocketPath(1001) with environment overrides = %q, want %q", got, want)
	}
}

// TestSystemRuntimeDir checks the system runtime directory per platform.
func TestSystemRuntimeDir(t *testing.T) {
	cases := []struct{ goos, want string }{
		{"linux", "/run/peers"},
		{"darwin", "/var/run/peers"},
		{"freebsd", "/run/peers"},
	}
	for _, c := range cases {
		if got := systemRuntimeDir(c.goos); got != c.want {
			t.Errorf("systemRuntimeDir(%q) = %q, want %q", c.goos, got, c.want)
		}
	}
}

// TestSystemSocketPath checks that the system socket path follows the user path
// of root and the runtime directory seam.
func TestSystemSocketPath(t *testing.T) {
	if got, want := systemSocketPath(), userSocketPath(0); got != want {
		t.Fatalf("systemSocketPath() = %q, want userSocketPath(0) = %q", got, want)
	}
	want := filepath.Join(systemRuntimeDir(runtime.GOOS), "broker.sock")
	if got := systemSocketPath(); got != want {
		t.Fatalf("systemSocketPath() = %q, want %q", got, want)
	}

	t.Setenv("TMPDIR", "/elsewhere/tmp")
	t.Setenv("XDG_RUNTIME_DIR", "/elsewhere/xdg")
	t.Setenv("PEERS_TEST_RUNTIME_ROOT", "/elsewhere/test")
	if got, want := systemSocketPath(), userSocketPath(0); got != want {
		t.Fatalf("systemSocketPath() with environment overrides = %q, want %q", got, want)
	}

	orig := runtimeDir
	t.Cleanup(func() { runtimeDir = orig })
	runtimeDir = func(euid int) string { return "/r/" + strconv.Itoa(euid) }
	if got, want := userSocketPath(5), "/r/5/broker.sock"; got != want {
		t.Fatalf("userSocketPath(5) with replaced runtimeDir = %q, want %q", got, want)
	}
	if got, want := systemSocketPath(), "/r/0/broker.sock"; got != want {
		t.Fatalf("systemSocketPath() with replaced runtimeDir = %q, want %q", got, want)
	}
}
