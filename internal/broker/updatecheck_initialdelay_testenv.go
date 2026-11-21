//go:build peers_testenv

package broker

import (
	"os"
	"time"
)

// testUpdateInitialDelayEnv names the variable that shortens the first update
// check in a test build.
const testUpdateInitialDelayEnv = "PEERS_TEST_UPDATE_INITIAL_DELAY"

// updateCheckInitialDelay returns how long a Broker waits after startup before
// its first periodic update check, overridden by testUpdateInitialDelayEnv.
// The override exists only in builds carrying the peers_testenv tag. An unset
// or unparsable value keeps the one-minute default, and so does a zero or
// negative one: the override exists to shorten the wait, not to disable the
// check, which a non-positive delay could not express sensibly.
func updateCheckInitialDelay() time.Duration {
	d, err := time.ParseDuration(os.Getenv(testUpdateInitialDelayEnv))
	if err != nil || d <= 0 {
		return time.Minute
	}
	return d
}
