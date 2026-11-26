package node

import (
	"strings"
	"testing"
)

// TestBaseEnvSetsDirectoryReplaceInterval checks that the environment sets the
// one-second directory replacement interval.
func TestBaseEnvSetsDirectoryReplaceInterval(t *testing.T) {
	env := BaseEnv{Home: "/home/peer", Path: "/usr/bin"}.Build(nil)
	want := testDirectoryReplaceIntervalEnv + "=" + testDirectoryReplaceInterval
	for _, entry := range env {
		if strings.HasPrefix(entry, testDirectoryReplaceIntervalEnv+"=") {
			if entry != want {
				t.Fatalf("environment holds %q, want %q", entry, want)
			}
			return
		}
	}
	t.Fatalf("environment does not hold %q", want)
}
