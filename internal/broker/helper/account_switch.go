package helper

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

// The account-helper receives its identity in these variables, separate from
// the access check's, so a child never reads an identity meant for the other
// subcommand.
const (
	accountHelperUIDEnv = "PEERS_ACCOUNT_HELPER_UID"
	accountHelperGIDEnv = "PEERS_ACCOUNT_HELPER_GID"
)

func helperIdentityEnv(acct Account) []string {
	return []string{
		accountHelperUIDEnv + "=" + strconv.FormatUint(uint64(acct.UID), 10),
		accountHelperGIDEnv + "=" + strconv.FormatUint(uint64(acct.GID), 10),
	}
}

type helperIdentityOps struct {
	geteuid    func() int
	setgid     func(gid int) error
	initgroups func(gids []uint32, gmuid uint32) error
	setuid     func(uid int) error
	getuid     func() int
	getgid     func() int
	getegid    func() int
	getgroups  func() ([]int, error)
}

// enterHelperAccountFromEnv switches to the account named in the environment,
// and refuses when it did not start as root or the target uid is 0. The switch
// runs gid, groups, uid in that order; afterwards the process must prove it
// dropped root: all four ids match, setuid(0) fails, and no gid 0 remains
// unless that is the primary group. The identity variables are cleared once
// consumed.
func enterHelperAccountFromEnv(ops helperIdentityOps) error {
	uid, gid, present, err := lookupIdentityEnv(accountHelperUIDEnv, accountHelperGIDEnv)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	if euid := ops.geteuid(); euid != 0 {
		return fmt.Errorf("must start as root to switch to uid %d, running as uid %d", uid, euid)
	}
	if uid == 0 {
		return errors.New("refusing to switch to uid 0: the account-helper must not keep root")
	}

	if err := ops.setgid(int(gid)); err != nil {
		return fmt.Errorf("setting gid %d: %w", gid, err)
	}
	if err := ops.initgroups([]uint32{gid}, uid); err != nil {
		return fmt.Errorf("setting groups for uid %d: %w", uid, err)
	}
	if err := ops.setuid(int(uid)); err != nil {
		return fmt.Errorf("setting uid %d: %w", uid, err)
	}

	if got := ops.getuid(); got != int(uid) {
		return fmt.Errorf("after the switch the real uid is %d, want %d", got, uid)
	}
	if got := ops.geteuid(); got != int(uid) {
		return fmt.Errorf("after the switch the effective uid is %d, want %d", got, uid)
	}
	if got := ops.getgid(); got != int(gid) {
		return fmt.Errorf("after the switch the real gid is %d, want %d", got, gid)
	}
	if got := ops.getegid(); got != int(gid) {
		return fmt.Errorf("after the switch the effective gid is %d, want %d", got, gid)
	}

	// Four matching ids can hide root kept in a saved uid or a capability; a
	// successful setuid(0) catches that.
	if err := ops.setuid(0); err == nil {
		return errors.New("after the switch setuid(0) succeeded: the account-helper still holds root")
	}

	// A primary group of 0 is legitimate; any other gid 0 in the table keeps
	// root access the account must not have.
	if gid != 0 {
		groups, err := ops.getgroups()
		if err != nil {
			return fmt.Errorf("reading the supplementary groups after the switch: %w", err)
		}
		for _, g := range groups {
			if g == 0 {
				return errors.New("after the switch the supplementary groups contain gid 0")
			}
		}
	}

	if err := os.Unsetenv(accountHelperUIDEnv); err != nil {
		return fmt.Errorf("clearing %s: %w", accountHelperUIDEnv, err)
	}
	if err := os.Unsetenv(accountHelperGIDEnv); err != nil {
		return fmt.Errorf("clearing %s: %w", accountHelperGIDEnv, err)
	}
	return nil
}
