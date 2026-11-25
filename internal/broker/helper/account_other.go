//go:build !linux && !darwin

package helper

import (
	"os/exec"
	"syscall"
)

// accountChildIdentity hands the identity, group table included, to the kernel
// before exec.
func accountChildIdentity(cmd *exec.Cmd, acct Account) {
	cmd.SysProcAttr.Credential = &syscall.Credential{
		Uid:    acct.UID,
		Gid:    acct.GID,
		Groups: acct.Groups,
	}
}

func enterHelperAccount() error { return nil }

func setParentDeathSignal(*syscall.SysProcAttr) {}
