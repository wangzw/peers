//go:build !peers_testenv

package broker

import "time"

// updateCheckInitialDelay returns how long a Broker waits after startup before
// its first periodic update check. Release builds always wait one minute;
// builds carrying the peers_testenv tag may shorten it.
func updateCheckInitialDelay() time.Duration {
	return time.Minute
}
