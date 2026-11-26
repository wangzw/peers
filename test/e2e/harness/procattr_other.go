//go:build !linux

package harness

import "syscall"

func setParentDeath(*syscall.SysProcAttr) {}
