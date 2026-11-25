//go:build peers_testenv

package update

import "os"

const testAPIEnv = "PEERS_TEST_UPDATE_API"

// APIBase returns the GitHub API base URL, overridden by testAPIEnv in a
// peers_testenv build.
func APIBase() string {
	if api := os.Getenv(testAPIEnv); api != "" {
		return api
	}
	return DefaultAPI
}
