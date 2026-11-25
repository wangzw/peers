//go:build !peers_testenv

package directory

import "time"

// directoryReplaceInterval returns the replacement interval; a release build
// uses defaultReplaceInterval and a peers_testenv build may override it.
func directoryReplaceInterval() time.Duration {
	return defaultReplaceInterval
}
