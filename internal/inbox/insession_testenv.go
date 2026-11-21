//go:build peers_testenv

package inbox

import "os"

// skipAncestorSessionCheckEnv names the variable with which a command-line test
// sandbox turns off the parent-chain walk. The test runner that starts the
// sandbox already lives inside a session's process tree, so without the switch
// every Broker a sandbox starts would be refused. The variable exists only in
// builds carrying the peers_testenv tag.
const skipAncestorSessionCheckEnv = "PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK"

// skipAncestorSessionCheck reports whether the parent-chain walk is disabled.
// Only a non-empty value disables it, so an empty entry is the same as an
// absent one. The process's own environment variable is checked before this
// switch, so the switch cannot hide a process that a session started directly.
func skipAncestorSessionCheck() bool {
	return os.Getenv(skipAncestorSessionCheckEnv) != ""
}
