// Package procsig sends signals to processes while refusing a pid that does
// not name exactly one process. A held pid can stop naming a process:
// Process.Release sets it to -1, a service manager may report 0, and a parent
// id becomes 1 after adoption by init.
package procsig

import (
	"errors"
	"fmt"
	"syscall"
)

// SendSignal sends sig to pid after checking that pid names one process. It
// refuses 1, 0 and negative values: pid 1 names init, pid 0 the caller's
// process group, pid -1 every process the caller may signal, and any other
// negative pid the process group with that id.
func SendSignal(pid int, sig syscall.Signal) error {
	if pid <= 1 {
		return fmt.Errorf("refusing to signal pid %d: a signal must name one process above pid 1", pid)
	}
	return syscall.Kill(pid, sig)
}

// Alive reports whether pid names a process the caller may probe. It refuses
// 1, 0 and negative values: pid 1 names init, pid 0 the caller's process
// group, pid -1 every process the caller may signal, and any other negative
// pid the process group with that id. A permission error still means the
// process exists.
func Alive(pid int) bool {
	if pid <= 1 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
