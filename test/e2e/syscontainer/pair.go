// This file carries no e2e build tag so that the role parsing and the
// shared-directory exchange compile on the host without one.

package syscontainer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	// PairRoleEnv carries the role into a container that is part of a pair.
	// RunPairTest sets it to "A" for hostA and "B" for hostB.
	PairRoleEnv = "PEERS_E2E_PAIR_ROLE"

	// pairSharedEnv carries the shared directory path into a pair container.
	// The value is the container path, not the host path.
	pairSharedEnv = "PEERS_E2E_PAIR_SHARED"

	// PairSharedMount is the container path where the compose file mounts the
	// pair volume. The compose file fixes the mount, so PairOptions.SharedDir
	// must equal it, or the exchange fails only when a wait expires.
	PairSharedMount = "/shared"
)

// Role is the host a pair test process runs on.
type Role string

const (
	// RoleA is host A, which runs the system Broker and accepts the pairing.
	RoleA Role = "A"
	// RoleB is host B, which runs its own Broker and dials host A.
	RoleB Role = "B"
)

// PairOptions configures a test that runs on two hosts. TestMain passes one to
// RunPair and the test passes the same value to RunPairTest.
type PairOptions struct {
	// SharedDir is the directory path mounted at the same location in both
	// containers. The two roles exchange pairing files and completion markers
	// there. It must equal PairSharedMount; the host does not create it, the
	// compose volume provides it.
	SharedDir string
	// ExtraEnv adds NAME=value entries to both container runs. A pair that
	// needs an address the host chose at run time puts it here.
	ExtraEnv []string
}

// PairRole returns the role this process runs as, or "" when it is not part of
// a pair. A pair test calls it to select its half of the scenario.
func PairRole() Role {
	role := Role(os.Getenv(PairRoleEnv))
	switch role {
	case RoleA, RoleB:
		return role
	}
	return ""
}

// InPair reports whether this process runs as one host of a pair.
func InPair() bool {
	return PairRole() != ""
}

// PairSharedDir returns the directory the two pair containers share, or "" when
// this process is not part of a pair.
func PairSharedDir() string {
	return os.Getenv(pairSharedEnv)
}

func peerRole(role Role) Role {
	if role == RoleA {
		return RoleB
	}
	return RoleA
}

func pairFailureName(role Role) string {
	return "failed-" + string(role)
}

// pairGuardsMu guards pairGuards, the set of tests that armed a PairGuard.
var (
	pairGuardsMu sync.Mutex
	pairGuards   = make(map[*testing.T]bool)
)

func markPairGuard(t *testing.T, armed bool) {
	pairGuardsMu.Lock()
	defer pairGuardsMu.Unlock()
	if armed {
		pairGuards[t] = true
		return
	}
	delete(pairGuards, t)
}

func pairGuardArmed(t *testing.T) bool {
	pairGuardsMu.Lock()
	defer pairGuardsMu.Unlock()
	return pairGuards[t]
}

// requirePairGuard fails the test when it did not arm a PairGuard first. The
// last pair in "defer PairGuard(t)()" is required: without it the guard arms
// only when the test returns, and the other host waits out its bound.
func requirePairGuard(t *testing.T, what string) {
	t.Helper()
	if !pairGuardArmed(t) {
		t.Fatalf("syscontainer: %s needs the failure guard; write defer PairGuard(t)() at the start of the test", what)
	}
}

// PairGuard returns a function the test must defer, as in defer PairGuard(t)(),
// which records this host's failure in the shared directory when it runs as the
// test goroutine unwinds, before any t.Cleanup call. It recovers a panic and
// panics again because t.Failed still reports false while a panic unwinds; a
// panic outside the test goroutine or a process killed without unwinding runs
// none of this, which is why PairAwait has its own bound.
func PairGuard(t *testing.T) func() {
	t.Helper()
	role := PairRole()
	if role == "" {
		t.Fatalf("syscontainer: PairGuard runs inside a pair container as role A or B")
	}
	markPairGuard(t, true)
	return func() {
		markPairGuard(t, false)
		panicked := recover()
		if panicked == nil && !t.Failed() {
			return
		}
		message := fmt.Sprintf("host %s failed", role)
		if err := writePairFile(PairSharedDir(), pairFailureName(role), []byte(message)); err != nil {
			t.Logf("syscontainer: recording the failure of host %s: %v", role, err)
		}
		if panicked != nil {
			panic(panicked)
		}
	}
}

// PairWrite writes data to name inside the shared directory. The write goes to
// a temporary file first and is renamed into place, so the other host never
// reads a partial file.
func PairWrite(t *testing.T, name string, data []byte) {
	t.Helper()
	requirePairGuard(t, "PairWrite")
	if err := writePairFile(PairSharedDir(), name, data); err != nil {
		t.Fatalf("syscontainer: writing %s to the shared directory: %v", name, err)
	}
}

// PairAwait waits up to bound for the other host to write name and returns its
// contents; the caller passes the bound its half of the scenario accounts for.
// When the other host recorded a failure, the wait ends at once with that
// failure named.
func PairAwait(t *testing.T, name string, bound time.Duration) []byte {
	t.Helper()
	requirePairGuard(t, "PairAwait")
	dir := PairSharedDir()
	if dir == "" {
		t.Fatalf("syscontainer: PairAwait runs inside a pair container")
	}
	role := PairRole()
	if role == "" {
		t.Fatalf("syscontainer: PairAwait runs inside a pair container as role A or B")
	}
	failure := filepath.Join(dir, pairFailureName(peerRole(role)))

	var data []byte
	harness.Eventually(t, bound, "the other host to write "+name, func() (bool, string) {
		if text, err := os.ReadFile(failure); err == nil {
			t.Fatalf("the other host failed before writing %s: %s", name, text)
		}
		content, err := os.ReadFile(filepath.Join(dir, name))
		if err == nil {
			data = content
			return true, ""
		}
		if !os.IsNotExist(err) {
			return false, err.Error()
		}
		return false, name + " has not appeared"
	})
	return data
}

func writePairFile(dir, name string, data []byte) error {
	if dir == "" {
		return fmt.Errorf("the shared directory is not set")
	}
	if !filepath.IsLocal(name) || name == "." || name == ".." {
		return fmt.Errorf("%q is not a plain file name", name)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".pair-")
	if err != nil {
		return fmt.Errorf("creating a temporary file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("writing %s: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("closing %s: %w", name, err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("setting the mode of %s: %w", name, err)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("renaming %s into place: %w", name, err)
	}
	return nil
}
