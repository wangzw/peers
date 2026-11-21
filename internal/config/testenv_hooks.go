//go:build peers_testenv

package config

import (
	"os"
	"path/filepath"
	"strconv"
)

// testRuntimeRoot returns the runtime directory of one account under the test
// root: root/root for the superuser and root/<euid> for everyone else.
func testRuntimeRoot(root string, euid int) string {
	if euid == 0 {
		return filepath.Join(root, "root")
	}
	return filepath.Join(root, strconv.Itoa(euid))
}

// init installs the test hooks when the environment selects them. The hooks
// exist only in builds with the peers_testenv tag, so a release build can
// never be redirected away from the real runtime and home directories.
func init() {
	if root := os.Getenv("PEERS_TEST_RUNTIME_ROOT"); root != "" {
		if !filepath.IsAbs(root) {
			panic("PEERS_TEST_RUNTIME_ROOT must be absolute")
		}
		runtimeRoot = func(euid int) string { return testRuntimeRoot(root, euid) }
		runtimeFallbackBase = func(euid int) (string, error) {
			return filepath.Join(root, "fallback", strconv.Itoa(euid)), nil
		}
	}
	if home := os.Getenv("PEERS_TEST_HOME"); home != "" {
		if !filepath.IsAbs(home) {
			panic("PEERS_TEST_HOME must be absolute")
		}
		homeDirSource = func() (string, error) { return home, nil }
	}
}
