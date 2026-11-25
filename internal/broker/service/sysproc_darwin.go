//go:build darwin

package service

import "syscall"

// detachedSysProcAttr starts the helper in a new session, so launchd's bootout
// of the job's process group cannot reach it.
func detachedSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
