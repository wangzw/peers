//go:build !peers_testenv

package inbox

// skipAncestorSessionCheck reports whether the parent-chain walk is disabled.
// A release build never disables it. Only a build carrying the peers_testenv
// tag reads the test switch, so a process inside a session cannot turn the
// check off by setting an environment variable.
func skipAncestorSessionCheck() bool {
	return false
}
