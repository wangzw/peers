package procsig

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

// TestSendSignalRefusesGuardPids checks that SendSignal refuses -1, 0 and 1.
func TestSendSignalRefusesGuardPids(t *testing.T) {
	// SIGCONT is deliberate: a guard regression that lets the signal through
	// stays harmless, so never switch to a terminating signal.
	for _, pid := range []int{-1, 0, 1} {
		err := SendSignal(pid, syscall.SIGCONT)
		if err == nil {
			t.Fatalf("SendSignal(%d) returned nil, want a refusal", pid)
		}
		if !strings.Contains(err.Error(), "refusing to signal pid") {
			t.Fatalf("SendSignal(%d) = %v, want a refusal", pid, err)
		}
	}
}

// TestSendSignalAcceptsOwnPID checks that SendSignal accepts the caller's own
// pid.
func TestSendSignalAcceptsOwnPID(t *testing.T) {
	// SIGCONT is deliberate: a guard regression that lets the signal through
	// stays harmless to the test process, so never switch to a terminating
	// signal.
	if err := SendSignal(os.Getpid(), syscall.SIGCONT); err != nil {
		t.Fatalf("SendSignal(%d, SIGCONT) = %v, want nil", os.Getpid(), err)
	}
}

// TestAliveRefusesGuardPids checks that Alive reports false for -1, 0 and 1.
func TestAliveRefusesGuardPids(t *testing.T) {
	for _, pid := range []int{-1, 0, 1} {
		if Alive(pid) {
			t.Fatalf("Alive(%d) = true, want false", pid)
		}
	}
}

// TestAliveAcceptsOwnPID checks that Alive reports the caller's own pid as
// alive.
func TestAliveAcceptsOwnPID(t *testing.T) {
	if !Alive(os.Getpid()) {
		t.Fatalf("Alive(%d) = false, want true", os.Getpid())
	}
}

// TestAliveReportsExitedProcess checks that Alive reports false for a process
// that has exited and been reaped.
func TestAliveReportsExitedProcess(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting true: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("waiting for true: %v", err)
	}
	if Alive(pid) {
		t.Fatalf("Alive(%d) = true after the process exited", pid)
	}
}
