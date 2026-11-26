//go:build !peers_testenv

package mcpserver

import (
	"context"
	"testing"

	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestReleaseBuildIgnoresSocketEnvironment checks that a release build reads no
// socket environment variable and selects the account's user Broker.
func TestReleaseBuildIgnoresSocketEnvironment(t *testing.T) {
	envPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, envPath, fakelocal.Handlers{})
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})
	systemPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, systemPath, fakelocal.Handlers{})

	t.Setenv("AGENT_PEER_SOCKET", envPath)
	t.Setenv("PEERS_TEST_SOCKET", envPath)

	var asked []string
	s := &selector{
		getenv: func(name string) string {
			asked = append(asked, name)
			return ""
		},
		userSocket:   userPath,
		systemSocket: systemPath,
		probe:        inspectSocket,
	}

	got, err := s.Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Kind != kindUser || got.Path != userPath {
		t.Fatalf("Find = %+v, want {%s kindUser}", got, userPath)
	}
	if len(asked) != 0 {
		t.Fatalf("the release selector looked up %q; it must read no socket environment variable", asked)
	}
}
