//go:build darwin

package helper

import (
	"os"
	"os/exec"
	"syscall"
)

// accountChildIdentity hands the identity to the child through the environment,
// because Darwin's setgroups is capped at NGROUPS and the credential cannot
// carry the full group table.
func accountChildIdentity(cmd *exec.Cmd, acct Account) {
	cmd.Env = append(cmd.Env, helperIdentityEnv(acct)...)
}

// enterHelperAccount switches the root-started account-helper to its account,
// using the initgroups system call so the kernel resolves the groups through
// memberd.
func enterHelperAccount() error {
	return enterHelperAccountFromEnv(helperIdentityOps{
		geteuid:    os.Geteuid,
		setgid:     syscall.Setgid,
		initgroups: callInitgroups,
		setuid:     syscall.Setuid,
		getuid:     os.Getuid,
		getgid:     os.Getgid,
		getegid:    os.Getegid,
		getgroups:  syscall.Getgroups,
	})
}

func setParentDeathSignal(*syscall.SysProcAttr) {}
