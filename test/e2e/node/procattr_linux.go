//go:build linux

package node

import "syscall"

// setDeathSignal makes the kernel kill the child when the process that started
// it dies, since a test binary killed by go test's timeout never runs its
// cleanup.
func setDeathSignal(attr *syscall.SysProcAttr) {
	attr.Pdeathsig = syscall.SIGKILL
}
