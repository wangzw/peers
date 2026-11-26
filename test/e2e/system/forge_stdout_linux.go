//go:build e2e && linux

// Package e2e holds the end-to-end tests that install the system and user
// Brokers in containers and drive them through terminal commands and MCP
// sessions.
package e2e

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// openProcessStdout tries /proc/<pid>/fd/1 first, then pidfd_getfd: the pipe's
// mode refuses an unprivileged caller, while pidfd_getfd is gated by ptrace
// access.
func openProcessStdout(pid int) (*os.File, error) {
	path := fmt.Sprintf("/proc/%d/fd/1", pid)
	if f, err := os.OpenFile(path, os.O_WRONLY, 0); err == nil {
		return f, nil
	} else {
		targetErr := err

		pidfd, pidfdErr := unix.PidfdOpen(pid, 0)
		if pidfdErr != nil {
			return nil, fmt.Errorf("%v; pidfd_open: %w", targetErr, pidfdErr)
		}
		defer func() { _ = unix.Close(pidfd) }()

		fd, fdErr := unix.PidfdGetfd(pidfd, 1, 0)
		if fdErr != nil {
			return nil, fmt.Errorf("%v; pidfd_getfd: %w", targetErr, fdErr)
		}
		return os.NewFile(uintptr(fd), path), nil
	}
}
