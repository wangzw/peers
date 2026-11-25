package broker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAccountLockSecondAcquireFails checks that a second Acquire on a held
// account lock returns ErrAlreadyRunning and that Release allows it again.
func TestAccountLockSecondAcquireFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.lock")
	first, err := AcquireAccountLock(path)
	if err != nil {
		t.Fatalf("first AcquireAccountLock: %v", err)
	}
	defer first.Release()

	if _, err := AcquireAccountLock(path); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second AcquireAccountLock error = %v, want ErrAlreadyRunning", err)
	} else if !strings.Contains(err.Error(), path) {
		t.Fatalf("second AcquireAccountLock error = %q, want it to name %s", err, path)
	}

	first.Release()
	second, err := AcquireAccountLock(path)
	if err != nil {
		t.Fatalf("AcquireAccountLock after Release: %v", err)
	}
	second.Release()
}

// TestAccountLockHeldByOtherIgnoresForeignOwner checks that a lock file owned
// by another account does not count as held.
func TestAccountLockHeldByOtherIgnoresForeignOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.lock")
	held, err := AcquireAccountLock(path)
	if err != nil {
		t.Fatalf("AcquireAccountLock: %v", err)
	}
	defer held.Release()

	if !accountLockHeldByOther(path) {
		t.Fatal("accountLockHeldByOther = false for a lock this account holds")
	}

	oldOwner := lockOwnerUID
	lockOwnerUID = func(info os.FileInfo) (int, bool) {
		if _, ok := oldOwner(info); !ok {
			return 0, false
		}
		return os.Geteuid() + 1, true
	}
	t.Cleanup(func() { lockOwnerUID = oldOwner })

	if accountLockHeldByOther(path) {
		t.Fatal("accountLockHeldByOther = true for a lock file owned by another account")
	}
}

// TestAccountLockReleaseKeepsFile checks that Release is idempotent and leaves
// the lock file in place.
func TestAccountLockReleaseKeepsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.lock")
	lock, err := AcquireAccountLock(path)
	if err != nil {
		t.Fatalf("AcquireAccountLock: %v", err)
	}
	lock.Release()
	lock.Release()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file missing after Release: %v", err)
	}
}

// TestAccountLockStillHeld checks that StillHeld is false after the lock file is
// removed or replaced.
func TestAccountLockStillHeld(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.lock")
	lock, err := AcquireAccountLock(path)
	if err != nil {
		t.Fatalf("AcquireAccountLock: %v", err)
	}
	defer lock.Release()

	if !lock.StillHeld() {
		t.Fatal("StillHeld() = false while the lock is held")
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove lock file: %v", err)
	}
	if lock.StillHeld() {
		t.Fatal("StillHeld() = true after the lock file was removed")
	}

	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("WriteFile replacement: %v", err)
	}
	if lock.StillHeld() {
		t.Fatal("StillHeld() = true after a different file took the lock path")
	}
}
