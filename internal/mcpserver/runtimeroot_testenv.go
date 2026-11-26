//go:build peers_testenv

package mcpserver

import (
	"os"
	"path/filepath"
	"strconv"
)

// init installs the runtime directory hook from PEERS_TEST_RUNTIME_ROOT; it
// exists only under the peers_testenv tag, so a release build is never
// redirected from the real runtime directories.
func init() {
	root := os.Getenv("PEERS_TEST_RUNTIME_ROOT")
	if root == "" {
		return
	}
	if !filepath.IsAbs(root) {
		panic("PEERS_TEST_RUNTIME_ROOT must be absolute")
	}
	preferredRuntimeDir = func(euid int) (string, error) {
		if euid == 0 {
			return filepath.Join(root, "root"), nil
		}
		return filepath.Join(root, strconv.Itoa(euid)), nil
	}
	fallbackRuntimeBase = func(euid int) (string, error) {
		return filepath.Join(root, "fallback", strconv.Itoa(euid)), nil
	}
}
