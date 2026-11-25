package helper

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/wangzw/peers/internal/accountdb"
)

// Account is one entry of the account database.
type Account struct {
	// Name is the account name.
	Name string
	// UID is the account's uid.
	UID uint32
	// GID is the account's primary group.
	GID uint32
	// Groups is the supplementary group table, including GID, as the account
	// database lists it.
	Groups []uint32
	// Home is the account's home directory.
	Home string
}

// ErrNoAccount reports that the account database has no entry for the uid.
var ErrNoAccount = errors.New("no such account")

// The account database lookups are variables so tests can inject a failure
// that must not be mistaken for a missing account.
var (
	lookupUIDFromDB = accountdb.LookupUID
	groupIDsFromDB  = accountdb.GroupIDs
)

// LookupAccount reads the account database afresh and returns its entry for
// uid. It returns an error wrapping ErrNoAccount when the uid has no entry,
// and another error when the lookup itself failed.
func LookupAccount(uid uint32) (Account, error) {
	u, err := lookupUIDFromDB(uid)
	if err != nil {
		if errors.Is(err, accountdb.ErrNotFound) {
			return Account{}, fmt.Errorf("%w: uid %d", ErrNoAccount, uid)
		}
		return Account{}, fmt.Errorf("cannot look up uid %d: %w", uid, err)
	}

	groups, err := groupIDsFromDB(u)
	if err != nil {
		if errors.Is(err, accountdb.ErrNotFound) {
			return Account{}, fmt.Errorf("%w: uid %d", ErrNoAccount, uid)
		}
		return Account{}, fmt.Errorf("cannot read account %q: supplementary groups: %w", u.Name, err)
	}

	return Account{
		Name:   u.Name,
		UID:    u.UID,
		GID:    u.GID,
		Groups: groups,
		Home:   u.Home,
	}, nil
}

func parseID(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

// lookupIdentityEnv reads the uid and gid from the named variables. An absent
// uid variable means the child already runs as the account; each hidden
// subcommand uses its own pair, so one cannot accept an identity meant for the
// other.
func lookupIdentityEnv(uidEnv, gidEnv string) (uid, gid uint32, present bool, err error) {
	uidValue, ok := os.LookupEnv(uidEnv)
	if !ok {
		return 0, 0, false, nil
	}
	uid, err = parseID(uidValue)
	if err != nil {
		return 0, 0, true, fmt.Errorf("account uid %q: %w", uidValue, err)
	}

	gidValue, ok := os.LookupEnv(gidEnv)
	if !ok {
		return 0, 0, true, errors.New("account gid is missing")
	}
	gid, err = parseID(gidValue)
	if err != nil {
		return 0, 0, true, fmt.Errorf("account gid %q: %w", gidValue, err)
	}
	return uid, gid, true, nil
}

func accountCommand(exe string, args []string, acct Account) *exec.Cmd {
	cmd := commandFor(exe, args, acct)
	cmd.Stderr = os.Stderr
	if acct.UID != uint32(os.Geteuid()) {
		accountChildIdentity(cmd, acct)
	}
	return cmd
}

// accessCheckCommand runs the access-check subcommand as root with the account
// identity in the environment; its stderr stays nil so the child cannot write
// into the Broker's log.
func accessCheckCommand(exe string, acct Account) *exec.Cmd {
	cmd := commandFor(exe, []string{"internal", "access-check"}, acct)
	cmd.Env = append(cmd.Env,
		accessCheckUIDEnv+"="+strconv.FormatUint(uint64(acct.UID), 10),
		accessCheckGIDEnv+"="+strconv.FormatUint(uint64(acct.GID), 10),
		accessCheckGroupsEnv+"="+joinIDs(acct.Groups),
	)
	return cmd
}

func commandFor(exe string, args []string, acct Account) *exec.Cmd {
	cmd := &exec.Cmd{
		Path: exe,
		Args: append([]string{exe}, args...),
		Env: []string{
			"HOME=" + acct.Home,
			"USER=" + acct.Name,
			"LOGNAME=" + acct.Name,
			"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		},
		Dir:         "/",
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true},
	}
	setParentDeathSignal(cmd.SysProcAttr)
	return cmd
}

func joinIDs(ids []uint32) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(uint64(id), 10))
	}
	return b.String()
}
