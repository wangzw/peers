//go:build linux

package helper

import "golang.org/x/sys/unix"

// faccessat2 asks the kernel to judge the effective ids and must not fall back
// to the permission bits; x/sys does, so this calls faccessat2 directly.
func faccessat2(path string, mode uint32) error {
	return unix.Faccessat2(unix.AT_FDCWD, path, mode, unix.AT_EACCESS)
}
