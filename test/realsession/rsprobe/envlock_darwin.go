//go:build darwin

package main

import (
	"errors"
	"os/exec"
	"strconv"
	"strings"
)

// envLockHolders finds processes with lockPath open, excluding self, which
// already holds a descriptor for the lock test; on macOS it asks lsof, whose
// exit status 1 means no match.
func envLockHolders(lockPath string, self int) []int {
	out, err := exec.Command("lsof", "-w", "-Fpn", lockPath).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			return nil
		}
	}
	return envLsofPIDs(string(out), self)
}

// envProcessExe returns the executable path of pid, or "" when it cannot be
// read; on macOS lsof reports it as an n field.
func envProcessExe(pid int) string {
	out, err := exec.Command("lsof", "-w", "-a", "-p", strconv.Itoa(pid), "-d", "txt", "-Fn").Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "n") {
			return line[1:]
		}
	}
	return ""
}
