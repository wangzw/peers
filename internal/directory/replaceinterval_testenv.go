//go:build peers_testenv

package directory

import (
	"os"
	"time"
)

const testDirectoryReplaceIntervalEnv = "PEERS_TEST_DIRECTORY_REPLACE_INTERVAL"

// directoryReplaceInterval reads testDirectoryReplaceIntervalEnv and falls back
// to defaultReplaceInterval when it is unset, unparsable or non-positive.
func directoryReplaceInterval() time.Duration {
	d, err := time.ParseDuration(os.Getenv(testDirectoryReplaceIntervalEnv))
	if err != nil || d <= 0 {
		return defaultReplaceInterval
	}
	return d
}
