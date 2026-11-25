package service

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

// MainPID returns the pid the service manager reports for the installed
// peers service at this deployment level, and whether it reports a running
// one. A missing unit, a stopped service, a failed query and an unsupported
// platform all report false; callers must compare the pid with their own before
// treating the process as service-managed.
func MainPID(system bool) (int, bool) {
	return serviceMainPID(Options{
		System: system,
		GOOS:   runtime.GOOS,
		UID:    os.Getuid(),
		Run: func(name string, args ...string) ([]byte, error) {
			return exec.Command(name, args...).CombinedOutput()
		},
	}, serviceDomain(system, os.Getuid()))
}

func serviceDomain(system bool, uid int) string {
	if system {
		return "system"
	}
	return fmt.Sprintf("gui/%d", uid)
}
