//go:build peers_testenv

package directory

import (
	"os"
	"time"
)

// testDirectoryReplaceIntervalEnv names the variable that shortens the
// replacement interval in a test build.
const testDirectoryReplaceIntervalEnv = "PEERS_TEST_DIRECTORY_REPLACE_INTERVAL"

// directoryReplaceInterval returns the time a record accepted from a link is
// held before a newer version may replace it, overridden by
// testDirectoryReplaceIntervalEnv. The override exists only in builds carrying
// the peers_testenv tag; the end-to-end suite sets it so a card or revocation
// propagates through a replacement window in seconds rather than a minute. An
// unset or unparsable value keeps the release default, and so does a
// non-positive one: a zero or negative wait is not a shorter interval.
func directoryReplaceInterval() time.Duration {
	d, err := time.ParseDuration(os.Getenv(testDirectoryReplaceIntervalEnv))
	if err != nil || d <= 0 {
		return defaultReplaceInterval
	}
	return d
}
