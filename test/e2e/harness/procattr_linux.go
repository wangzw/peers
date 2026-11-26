//go:build linux

package harness

import "syscall"

func setParentDeath(attr *syscall.SysProcAttr) {
	attr.Pdeathsig = syscall.SIGKILL
}
