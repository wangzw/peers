//go:build unix

package update

import (
	"os"
	"syscall"
)

// chownFile keeps the owner of the binary being replaced; a non-root process
// updating its own binary may set the same values it already has.
func chownFile(f *os.File, info os.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	return f.Chown(int(st.Uid), int(st.Gid))
}
