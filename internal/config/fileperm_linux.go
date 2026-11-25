//go:build linux

package config

import (
	"errors"
	"os"
	"syscall"
)

// A POSIX ACL cannot widen write access past the file mode's group bits, which
// record the ACL mask, so checking the owner and those bits is enough on Linux.
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
	return rootOnlyWritableFor(st.Uid, info.Mode()), nil
}
