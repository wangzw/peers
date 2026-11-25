//go:build !darwin

package service

import "syscall"

// detachedSysProcAttr is used only by the macOS post-update helper, but still
// sets Setsid so the tests check the property the macOS build relies on.
func detachedSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
