//go:build peers_testenv

package mcpserver

import (
	"os"
	"path/filepath"
	"strconv"
)

// init installs the runtime directory hook when the environment selects it.
// The hook exists only in builds with the peers_testenv tag, so a release
// build can never be redirected away from the real runtime directories that
// carry the single-instance guarantee.
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
