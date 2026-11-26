//go:build !linux

package node

import "syscall"

func setDeathSignal(*syscall.SysProcAttr) {}
