//go:build !peers_testenv

package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// releaseOverrideChildEnv marks the re-executed test binary of
// TestProtocolOverridesIgnoredInReleaseBuild.
const releaseOverrideChildEnv = "PEERS_TEST_PROTOCOL_OVERRIDE_CHILD"

// TestProtocolOverridesIgnoredInReleaseBuild checks that a build without
// peers_testenv ignores the protocol override variables.
func TestProtocolOverridesIgnoredInReleaseBuild(t *testing.T) {
	if os.Getenv(releaseOverrideChildEnv) == "1" {
		local, link := protocolOverrides()
		if local != 0 || link != 0 {
			t.Fatalf("protocolOverrides() = (%d, %d), release build must return (0, 0)", local, link)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestProtocolOverridesIgnoredInReleaseBuild$")
	cmd.Env = releaseChildEnv()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the release build read a protocol override from the environment: %v\n%s", err, out)
	}
}

func releaseChildEnv() []string {
	dropped := []string{
		releaseOverrideChildEnv + "=",
		"PEERS_TEST_LOCAL_PROTOCOL_VERSION=",
		"PEERS_TEST_LINK_PROTOCOL_VERSION=",
	}
	env := make([]string, 0, len(os.Environ())+3)
	for _, entry := range os.Environ() {
		skip := false
		for _, prefix := range dropped {
			if strings.HasPrefix(entry, prefix) {
				skip = true
				break
			}
		}
		if !skip {
			env = append(env, entry)
		}
	}
	return append(env,
		releaseOverrideChildEnv+"=1",
		"PEERS_TEST_LOCAL_PROTOCOL_VERSION=7",
		"PEERS_TEST_LINK_PROTOCOL_VERSION=7",
	)
}
