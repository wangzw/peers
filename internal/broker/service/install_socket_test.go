package service

import (
	"strconv"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

// TestLocalSocketPathFollowsTheResolvedRuntimeDir pins the user deployment's
// socket to the runtime directory the location rule resolves, including the
// fallback location. A fixed /tmp/peers-<uid> would not find a Broker running
// from the fallback directory, so the service install and removal would check
// and clean the wrong place.
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

// TestRuntimeDirForFollowsConfig pins the production default: the service
// resolves the runtime directory through the config rule, not a path of its
// own.
func TestRuntimeDirForFollowsConfig(t *testing.T) {
	for _, euid := range []int{0, 1000, 4321} {
		if got, want := runtimeDirFor(euid), config.RuntimeDir(euid); got != want {
			t.Errorf("runtimeDirFor(%d) = %q, want config.RuntimeDir %q", euid, got, want)
		}
	}
}
