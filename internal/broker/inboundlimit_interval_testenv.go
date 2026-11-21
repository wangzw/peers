//go:build peers_testenv

package broker

import (
	"os"
	"time"
)

// testInboundLimitIntervalEnv names the variable that lengthens the inbound
// quota refill interval in a test build.
const testInboundLimitIntervalEnv = "PEERS_TEST_INBOUND_LIMIT_INTERVAL"

// inboundLimitInterval returns the time between refills of one inbound quota
// token, overridden by testInboundLimitIntervalEnv. The override exists only in
// builds carrying the peers_testenv tag; the end-to-end suite sets it so a
// back-to-back burst never sees a refilled token. An unset or unparsable value
// keeps the release default, and so does a non-positive one: the override
// exists to lengthen the wait, which a non-positive delay cannot express.
func inboundLimitInterval() time.Duration {
	d, err := time.ParseDuration(os.Getenv(testInboundLimitIntervalEnv))
	if err != nil || d <= 0 {
		return defaultInboundLimitInterval
	}
	return d
}
