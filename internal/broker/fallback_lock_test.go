package broker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunRefusesWhenFallbackLockIsHeld covers a fallback Broker that keeps
// running after the taken preferred directory disappears. A new Broker resolves
// to the preferred path, where its own lock is free, but must still refuse
// because the fallback Broker holds the account lock at the other location.
func TestRunRefusesWhenFallbackLockIsHeld(t *testing.T) {
	base := t.TempDir()
	preferred := filepath.Join(base, "preferred")
	fallback := filepath.Join(base, "fallback")
	for _, dir := range []string{preferred, fallback} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	// The fallback Broker is running and holds its account lock.
	running, err := AcquireAccountLock(filepath.Join(fallback, "broker.lock"))
	if err != nil {
		t.Fatalf("AcquireAccountLock at the fallback: %v", err)
	}
	t.Cleanup(running.Release)

	rt := newRunTestAt(t, preferred, func(rt *runTest) {
		rt.cfg.lockCandidates = []string{
			filepath.Join(preferred, "broker.lock"),
			filepath.Join(fallback, "broker.lock"),
		}
	})
	rt.start()
	err = rt.waitErr()
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("Run error = %v, want ErrAlreadyRunning", err)
	}
	if !strings.Contains(err.Error(), "account lock") {
		t.Errorf("Run error = %q, want it to name the account lock", err)
	}
	if got := rt.readyLine(); got != err.Error()+"\n" {
		t.Errorf("readiness line = %q, want %q", got, err.Error()+"\n")
	}
}
