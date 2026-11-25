package broker

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// ErrAlreadyRunning means another Broker instance holds the account lock. The
// process already running must serve this account; the new process exits.
var ErrAlreadyRunning = errors.New("another Broker instance of this account is already running")

// AccountLock is an exclusive advisory lock on the runtime directory's lock
// file. It is released when the process exits or the descriptor is closed. The
// lock file is never removed by the Broker: removing it would let a second
// instance lock a fresh file at the same path while the first still runs.
type AccountLock struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	dev      uint64
	ino      uint64
	released bool
}

// AcquireAccountLock takes the exclusive account lock at path, creating the
// file when it does not exist. It never blocks: a lock held by another process
// yields ErrAlreadyRunning.
func AcquireAccountLock(path string) (*AccountLock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cannot open account lock %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: account lock %s is held by another process", ErrAlreadyRunning, path)
		}
		return nil, fmt.Errorf("lock account lock %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cannot stat account lock %s: %w", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		_ = f.Close()
		return nil, fmt.Errorf("stat account lock %s: missing file identity", path)
	}
	return &AccountLock{
		file: f,
		path: path,
		dev:  uint64(st.Dev),
		ino:  uint64(st.Ino),
	}, nil
}

func checkAccountLockAvailable(paths []string) error {
	for _, path := range paths {
		if accountLockHeldByOther(path) {
			return fmt.Errorf("%w: account lock %s is held by another process", ErrAlreadyRunning, path)
		}
	}
	return nil
}

var lockOwnerUID = func(info os.FileInfo) (int, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}

// A lock file owned by another account is not this account's lock, so an
// occupant of the preferred directory cannot block the Broker by planting one.
func accountLockHeldByOther(path string) bool {
	f, err := os.OpenFile(path, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	owner, ok := lockOwnerUID(fi)
	if !ok || owner != os.Geteuid() {
		return false
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.Is(err, unix.EWOULDBLOCK)
	}
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return false
}

// StillHeld reports whether the path still names the locked file. A missing
// path, a non-regular file, or a different file at the path means the lock is
// no longer trustworthy.
func (l *AccountLock) StillHeld() bool {
	l.mu.Lock()
	released := l.released
	path, dev, ino := l.path, l.dev, l.ino
	l.mu.Unlock()
	if released {
		return false
	}
	fi, err := os.Lstat(path)
	if err != nil {
		return false
	}
	if !fi.Mode().IsRegular() {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return uint64(st.Dev) == dev && uint64(st.Ino) == ino
}

// Touch sets the access and modification times of the lock file to now through
// the already open descriptor, so the path is neither followed nor reopened. It
// fails once the lock is released.
func (l *AccountLock) Touch(now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.file == nil {
		return fmt.Errorf("cannot update the timestamps of account lock %s: the lock is released", l.path)
	}
	tv := unix.NsecToTimeval(now.UnixNano())
	if err := unix.Futimes(int(l.file.Fd()), []unix.Timeval{tv, tv}); err != nil {
		return fmt.Errorf("cannot update the timestamps of account lock %s: %w", l.path, err)
	}
	return nil
}

// Release closes the lock's file descriptor, which releases the flock. It does
// not remove the lock file and is safe to call more than once.
func (l *AccountLock) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
}
