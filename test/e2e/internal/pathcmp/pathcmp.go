// Package pathcmp compares filesystem paths as the kernel reports them.
package pathcmp

import (
	"fmt"
	"path/filepath"
)

// Resolve returns path with every symbolic link resolved to its target.
func Resolve(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolving %s: %w", path, err)
	}
	return resolved, nil
}

// SamePath reports whether a and b name the same location once symbolic links
// in both are resolved.
func SamePath(a, b string) (bool, error) {
	resolvedA, err := Resolve(a)
	if err != nil {
		return false, err
	}
	resolvedB, err := Resolve(b)
	if err != nil {
		return false, err
	}
	return resolvedA == resolvedB, nil
}
