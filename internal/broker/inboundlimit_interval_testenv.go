//go:build peers_testenv

package broker

import (
	"os"
	"time"
)

const testInboundLimitIntervalEnv = "PEERS_TEST_INBOUND_LIMIT_INTERVAL"

// inboundLimitInterval reads testInboundLimitIntervalEnv; an unset, unparsable
// or non-positive value keeps defaultInboundLimitInterval.
func inboundLimitInterval() time.Duration {
	d, err := time.ParseDuration(os.Getenv(testInboundLimitIntervalEnv))
	if err != nil || d <= 0 {
		return defaultInboundLimitInterval
	}
	return d
}
