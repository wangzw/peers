//go:build !linux && !darwin

package main

// envLockHolders returns nil on this platform, so the lock check reports an
// unknown holder rather than guessing.
func envLockHolders(string, int) []int {
	return nil
}

// envProcessExe returns "" on this platform.
func envProcessExe(int) string {
	return ""
}
