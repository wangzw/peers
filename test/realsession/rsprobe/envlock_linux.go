//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// envLockHolders finds processes with lockPath open, excluding self, which
// already holds a descriptor for the lock test.
func envLockHolders(lockPath string, self int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == self {
			continue
		}
		fdDir := fmt.Sprintf("/proc/%d/fd", pid)
		fds, err := os.ReadDir(fdDir)
		if err != nil {
			continue
		}
		for _, fd := range fds {
			target, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
			if err != nil {
				continue
			}
			if target == lockPath || target == lockPath+" (deleted)" {
				pids = append(pids, pid)
				break
			}
		}
	}
	return pids
}

// envProcessExe returns the executable path of pid, or "" when it cannot be
// read.
func envProcessExe(pid int) string {
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return ""
	}
	return exe
}
