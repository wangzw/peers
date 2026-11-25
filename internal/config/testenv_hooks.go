//go:build peers_testenv

package config

import (
	"os"
	"path/filepath"
	"strconv"
)

func testRuntimeRoot(root string, euid int) string {
	if euid == 0 {
		return filepath.Join(root, "root")
	}
	return filepath.Join(root, strconv.Itoa(euid))
}

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
