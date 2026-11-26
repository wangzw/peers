//go:build !peers_testenv

package inbox

// skipAncestorSessionCheck reports whether the parent-chain walk is disabled. A
// release build never disables it: only a peers_testenv build reads the switch,
// so a session's process cannot turn the check off through the environment.
func skipAncestorSessionCheck() bool {
	return false
}
