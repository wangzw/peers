package service

import (
	"strconv"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

// TestLocalSocketPathFollowsTheResolvedRuntimeDir checks that the user socket
// follows the resolved runtime directory, including the fallback location, and
// that the system sockets use the fixed system paths.
func TestLocalSocketPathFollowsTheResolvedRuntimeDir(t *testing.T) {
	old := runtimeDirFor
	runtimeDirFor = func(euid int) string {
		if euid == 4321 {
			return "/run/user/4321/peers"
		}
		return "/tmp/peers-" + strconv.Itoa(euid)
	}
	t.Cleanup(func() { runtimeDirFor = old })

	fallback := baseOptions(t)
	fallback.EUID = 4321
	if got, want := localSocketPath(fallback), "/run/user/4321/peers/broker.sock"; got != want {
		t.Errorf("localSocketPath(fallback) = %q, want %q", got, want)
	}

	preferred := baseOptions(t)
	preferred.EUID = 1000
	if got, want := localSocketPath(preferred), "/tmp/peers-1000/broker.sock"; got != want {
		t.Errorf("localSocketPath(preferred) = %q, want %q", got, want)
	}

	system := baseOptions(t)
	system.System = true
	system.EUID = 0
	if got, want := localSocketPath(system), "/run/peers/broker.sock"; got != want {
		t.Errorf("localSocketPath(system linux) = %q, want %q", got, want)
	}
	system.GOOS = "darwin"
	if got, want := localSocketPath(system), "/var/run/peers/broker.sock"; got != want {
		t.Errorf("localSocketPath(system darwin) = %q, want %q", got, want)
	}
}

// TestRuntimeDirForFollowsConfig checks that the runtime directory resolves
// through config.RuntimeDir.
func TestRuntimeDirForFollowsConfig(t *testing.T) {
	for _, euid := range []int{0, 1000, 4321} {
		if got, want := runtimeDirFor(euid), config.RuntimeDir(euid); got != want {
			t.Errorf("runtimeDirFor(%d) = %q, want config.RuntimeDir %q", euid, got, want)
		}
	}
}
