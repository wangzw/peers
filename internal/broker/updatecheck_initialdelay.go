//go:build !peers_testenv

package broker

import "time"

// updateCheckInitialDelay returns the wait before the first periodic update
// check. Release builds wait one minute; peers_testenv builds may shorten it.
func updateCheckInitialDelay() time.Duration {
	return time.Minute
}
