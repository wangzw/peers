//go:build !linux

package helper

import "golang.org/x/sys/unix"

// faccessat2 asks the kernel to judge the effective ids; Darwin has no
// faccessat2, and faccessat with AT_EACCESS has no permission-bit fallback.
func faccessat2(path string, mode uint32) error {
	return unix.Faccessat(unix.AT_FDCWD, path, mode, unix.AT_EACCESS)
}
