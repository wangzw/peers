//go:build !peers_testenv

package broker

import "time"

// inboundLimitInterval returns the time between refills of one inbound quota
// token. Release builds always use defaultInboundLimitInterval; builds carrying
// the peers_testenv tag may lengthen it.
func inboundLimitInterval() time.Duration {
	return defaultInboundLimitInterval
}
