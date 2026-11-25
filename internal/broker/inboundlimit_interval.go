//go:build !peers_testenv

package broker

import "time"

// inboundLimitInterval returns the release refill interval; only a peers_testenv
// build can change it, through PEERS_TEST_INBOUND_LIMIT_INTERVAL.
func inboundLimitInterval() time.Duration {
	return defaultInboundLimitInterval
}
