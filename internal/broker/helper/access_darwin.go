//go:build darwin

package helper

import (
	"fmt"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// sysInitgroups is Darwin's initgroups system call, number 243 in
// bsd/kern/syscalls.master; x/sys/unix issues the trap directly, with no libc
// stub. It is not libc's initgroups(name, basegid), which expands the group
// list itself and is bounded by NGROUPS.
const sysInitgroups = 243

// darwinNGroups is NGROUPS from Darwin's sys/param.h; the static gidset passed
// to initgroups may hold at most this many entries.
const darwinNGroups = 16

// The syscall seams are variables so the Darwin unit test can check the call
// sequence without being root.
var (
	switchSetegid    = syscall.Setegid
	switchSeteuid    = syscall.Seteuid
	switchInitgroups = callInitgroups
)

// switchAccount passes only the primary group and the account uid as gmuid, so
// the kernel resolves the rest through memberd and an account in more than
// NGROUPS groups still switches. The order matters: setegid and initgroups
// need root, so the effective uid is dropped last.
func switchAccount(uid, gid uint32, _ []uint32) error {
	if err := switchSetegid(int(gid)); err != nil {
		return fmt.Errorf("setting effective gid %d: %w", gid, err)
	}
	if err := switchInitgroups([]uint32{gid}, uid); err != nil {
		return fmt.Errorf("setting groups for uid %d: %w", uid, err)
	}
	if err := switchSeteuid(int(uid)); err != nil {
		return fmt.Errorf("setting effective uid %d: %w", uid, err)
	}
	return nil
}

func callInitgroups(gids []uint32, gmuid uint32) error {
	if len(gids) == 0 {
		return fmt.Errorf("initgroups needs at least one group")
	}
	if len(gids) > darwinNGroups {
		return fmt.Errorf("initgroups list has %d groups, Darwin allows %d", len(gids), darwinNGroups)
	}
	_, _, errno := unix.Syscall(sysInitgroups, uintptr(len(gids)), uintptr(unsafe.Pointer(&gids[0])), uintptr(gmuid))
	runtime.KeepAlive(gids)
	if errno != 0 {
		return errno
	}
	return nil
}
