//go:build peers_testenv

package inbox

import "os"

// skipAncestorSessionCheckEnv is the switch a command-line test sandbox sets to
// turn off the parent-chain walk.
const skipAncestorSessionCheckEnv = "PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK"

// skipAncestorSessionCheck reports whether the parent-chain walk is disabled.
// Only a non-empty value disables it, and the process's own environment is
// checked first, so the switch cannot hide a process a session started
// directly.
func skipAncestorSessionCheck() bool {
	return os.Getenv(skipAncestorSessionCheckEnv) != ""
}
