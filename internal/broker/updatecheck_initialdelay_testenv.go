//go:build peers_testenv

package broker

import (
	"os"
	"time"
)

const testUpdateInitialDelayEnv = "PEERS_TEST_UPDATE_INITIAL_DELAY"

// updateCheckInitialDelay returns the wait before the first periodic update
// check, overridden by testUpdateInitialDelayEnv in a peers_testenv build. An
// unset, unparsable or non-positive value keeps the one-minute default.
func updateCheckInitialDelay() time.Duration {
	d, err := time.ParseDuration(os.Getenv(testUpdateInitialDelayEnv))
	if err != nil || d <= 0 {
		return time.Minute
	}
	return d
}
