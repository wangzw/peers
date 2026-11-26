package harness

import (
	"os"
	"syscall"
	"testing"
)

// TestProjectNameCarriesOwner checks that a project name parses back to this
// process.
func TestProjectNameCarriesOwner(t *testing.T) {
	current := currentOwner()
	if current.pid != os.Getpid() {
		t.Fatalf("currentOwner pid = %d, want %d", current.pid, os.Getpid())
	}
	name := projectName("TestProjectNameCarriesOwner")
	got, ok := parseOwner(name)
	if !ok || got != current {
		t.Fatalf("parseOwner(%q) = %+v, %v; want %+v", name, got, ok, current)
	}
	if ownerGone(name) {
		t.Fatalf("ownerGone(%q) = true; the owner is this live process", name)
	}
}

// TestOwnerGone checks that a live owner, a dead owner and a name without the
// marker are told apart.
func TestOwnerGone(t *testing.T) {
	current := currentOwner()
	live := "ape2e-" + current.String() + "-test-abcdef"
	if ownerGone(live) {
		t.Fatalf("ownerGone(%q) = true; want false for the live owner", live)
	}
	dead := owner{pid: 999999999, start: 1, ns: current.ns}
	if !ownerGone("ape2e-" + dead.String() + "-test-abcdef") {
		t.Errorf("ownerGone did not report the dead owner")
	}
	for _, name := range []string{
		"ape2e-legacy-abcdef",
		"ape2e12345",
		"not-a-project",
	} {
		if ownerGone(name) {
			t.Errorf("ownerGone(%q) = true; a name without a marker has an unknown owner", name)
		}
	}
}

// TestOwnerStartTimeMismatch checks that a reused PID with a different start
// time does not protect residue.
func TestOwnerStartTimeMismatch(t *testing.T) {
	current := currentOwner()
	if current.start == 0 {
		t.Skip("this platform does not report a process start time")
	}
	reused := owner{pid: current.pid, start: current.start + 1, ns: current.ns}
	if reused.alive() {
		t.Fatalf("owner %+v is alive; the start time does not match", reused)
	}
	if !ownerGone("ape2e-" + reused.String() + "-test-abcdef") {
		t.Fatalf("ownerGone did not reject the mismatched start time %+v", reused)
	}
}

// TestOwnerAliveInAnotherNamespace checks that an owner from a different PID
// namespace is kept even when this namespace has no process with its PID.
func TestOwnerAliveInAnotherNamespace(t *testing.T) {
	const selfNS = 4026531836
	other := owner{pid: 999999999, start: 7, ns: selfNS + 1}
	alive := other.aliveIn(selfNS,
		func(int) (uint64, error) { return 0, os.ErrNotExist },
		func(int) bool { return false },
	)
	if !alive {
		t.Fatalf("owner %+v in another namespace was reported dead", other)
	}
}

// TestOwnerAliveMissingPIDInSameNamespace checks that a confirmed absence in
// the owner's own namespace lets a cleanup sweep it.
func TestOwnerAliveMissingPIDInSameNamespace(t *testing.T) {
	const selfNS = 4026531836
	gone := owner{pid: 999999999, start: 7, ns: selfNS}
	alive := gone.aliveIn(selfNS,
		func(int) (uint64, error) { return 0, os.ErrNotExist },
		func(int) bool { return false },
	)
	if alive {
		t.Fatalf("owner %+v was reported alive; its PID does not exist", gone)
	}
}

// TestOwnerAliveUnreadableStat checks that a read error other than a missing
// process keeps the resource alive.
func TestOwnerAliveUnreadableStat(t *testing.T) {
	const selfNS = 4026531836
	unreadable := owner{pid: 4242, start: 7, ns: selfNS}
	alive := unreadable.aliveIn(selfNS,
		func(int) (uint64, error) { return 0, os.ErrPermission },
		func(int) bool { return false },
	)
	if !alive {
		t.Fatalf("owner %+v was reported dead after an unreadable stat", unreadable)
	}
}

// TestOwnerAliveConfirmedESRCH checks that the kernel's ESRCH marks the process
// gone.
func TestOwnerAliveConfirmedESRCH(t *testing.T) {
	const selfNS = 4026531836
	gone := owner{pid: 4242, start: 7, ns: selfNS}
	if gone.aliveIn(selfNS, func(int) (uint64, error) { return 0, syscall.ESRCH }, func(int) bool { return true }) {
		t.Fatalf("owner %+v was reported alive after ESRCH", gone)
	}
}

// TestParseOwnerValue checks that a standalone marker round-trips through
// parseOwnerValue and that garbage is rejected.
func TestParseOwnerValue(t *testing.T) {
	current := currentOwner()
	got, ok := parseOwnerValue(current.String())
	if !ok || got != current {
		t.Fatalf("parseOwnerValue(%q) = %+v, %v; want %+v", current.String(), got, ok, current)
	}
	if _, ok := parseOwnerValue("garbage"); ok {
		t.Fatalf("parseOwnerValue accepted garbage")
	}
}
