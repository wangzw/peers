//go:build linux

package config

import (
	"errors"
	"os"
	"syscall"
)

// configFileRootOnlyWritable reports whether the file at path is writable only
// by root. A missing file counts as safe: the write that follows creates it as
// root with mode 0600.
//
// A POSIX ACL cannot widen write access past the file mode's group bits: those
// bits record the ACL mask, so a zero group write bit masks every named user
// and group entry down to no write. Checking the owner and those bits is
// therefore enough on Linux.
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
