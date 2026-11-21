//go:build !peers_testenv

package directory

import "time"

// directoryReplaceInterval returns the time a record accepted from a link is
// held before a newer version may replace it. Release builds always use
// defaultReplaceInterval; builds carrying the peers_testenv tag may shorten it.
func directoryReplaceInterval() time.Duration {
	return defaultReplaceInterval
}
