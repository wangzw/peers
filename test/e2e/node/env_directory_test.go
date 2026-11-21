package node

import (
	"strings"
	"testing"
)

// TestBaseEnvSetsDirectoryReplaceInterval proves every child process the
// end-to-end suite starts gets the one-second replacement interval, so the
// discovery cases do not wait out the release window of 60 seconds.
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
