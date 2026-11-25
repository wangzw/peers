//go:build !darwin

package helper

import (
	"fmt"
	"syscall"
)

// switchSetgroups, switchSetegid and switchSeteuid replace the credential
// syscalls switchAccount calls, so a unit test can check the call order and
// inject a failure.
var (
	switchSetgroups = syscall.Setgroups
	switchSetegid   = syscall.Setegid
	switchSeteuid   = syscall.Seteuid
)

// switchAccount replaces the process's supplementary groups, effective gid and
// effective uid, in that order. It is the Linux and BSD path; Darwin uses the
// initgroups system call instead, because its setgroups is capped at NGROUPS.
func switchAccount(uid, gid uint32, groups []uint32) error {
	intGroups := make([]int, len(groups))
	for i, group := range groups {
		intGroups[i] = int(group)
	}
	if err := switchSetgroups(intGroups); err != nil {
		return fmt.Errorf("setting groups %v: %w", groups, err)
	}
	if err := switchSetegid(int(gid)); err != nil {
		return fmt.Errorf("setting effective gid %d: %w", gid, err)
	}
	if err := switchSeteuid(int(uid)); err != nil {
		return fmt.Errorf("setting effective uid %d: %w", uid, err)
	}
	return nil
}
