//go:build darwin

package config

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// aclXattr is where macOS stores a file's access control list; a Darwin ACL can
// grant write access the permission bits do not show.
const aclXattr = "com.apple.system.Security"

// Any security ACL makes the file ineligible, even one that grants no write:
// without parsing the Darwin ACL encoding, its presence is the only signal
// available here, and refusing is the safe direction.
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
