package helper

import (
	"os/exec"
	"syscall"
)

// accountChildIdentity hands the full group table to the kernel before exec;
// Linux's setgroups has no count limit.
func accountChildIdentity(cmd *exec.Cmd, acct Account) {
	cmd.SysProcAttr.Credential = &syscall.Credential{
		Uid:    acct.UID,
		Gid:    acct.GID,
		Groups: acct.Groups,
	}
}

func enterHelperAccount() error { return nil }

// setParentDeathSignal makes the kernel kill the child when the Broker dies, so
// a helper cannot outlive it while holding the account's inbox socket.
func setParentDeathSignal(a *syscall.SysProcAttr) {
	a.Pdeathsig = syscall.SIGKILL
}
