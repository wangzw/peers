//go:build darwin

package config

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// aclXattr is where macOS stores a file's access control list. Unlike Linux's
// POSIX ACL, a Darwin ACL entry can grant write access that the permission bits
// do not show, so a mode-only check is not enough.
const aclXattr = "com.apple.system.Security"

// configFileRootOnlyWritable reports whether the file at path is writable only
// by root. A missing file counts as safe: the write that follows creates it as
// root with mode 0600.
//
// Any security ACL makes the file ineligible. An ACL that grants no write is
// also refused: without parsing the Darwin ACL encoding, the presence of one is
// the only signal available here, and refusing is the safe direction.
func configFileRootOnlyWritable(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, nil
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, errors.New("the file has no owner information")
	}
	if !rootOnlyWritableFor(st.Uid, info.Mode()) {
		return false, nil
	}
	size, err := unix.Getxattr(path, aclXattr, nil)
	switch {
	case err == nil && size > 0:
		return false, nil
	case err == nil, errors.Is(err, unix.ENOATTR), errors.Is(err, unix.ENOTSUP):
		return true, nil
	default:
		return false, err
	}
}
