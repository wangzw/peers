//go:build linux

package node

import (
	"errors"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/wangzw/peers/internal/procsig"
)

// reap waits for the Broker to exit without holding the lifecycle lock, then
// marks the handle reaped and reaps the process under that lock. Waiting out of
// the lock lets a SendSignal that is holding the lock deliver the signal that
// ends the process; waiting before the lock is safe because a child that has
// exited but not been reaped is a zombie, whose pid cannot be reused. The reaped
// mark and cmd.Wait run under the lock, so no signal can be sent after the pid
// becomes reusable.
func (b *BrokerProc) reap() {
	b.awaitExit()
	b.mu.Lock()
	b.reaped = true
	b.exit = processExitCode(b.cmd.Wait())
	b.mu.Unlock()
	close(b.done)
}

// awaitExit blocks until the process has exited without reaping it. waitid with
// WNOWAIT only observes the exit, so cmd.Wait can still collect the exit status
// afterwards.
func (b *BrokerProc) awaitExit() {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, b.pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if err == unix.EINTR {
			continue
		}
		return
	}
}

// sendSignal sends sig through the guarded helper. The caller holds b.mu and
// has checked that the process is not reaped, so the pid still names the running
// or zombie process. Signalling a zombie is harmless: its pid is not reused
// until it is reaped.
func (b *BrokerProc) sendSignal(sig syscall.Signal) error {
	if b.cmd.Process == nil {
		return errors.New("node: broker process has no process handle")
	}
	b.recordSend(sig)
	if err := procsig.SendSignal(b.pid, sig); err != nil {
		return err
	}
	b.signals++
	return nil
}

// processExitCode returns the exit code cmd.Wait reports: the process's own
// code, or -1 when a signal ended it or the wait itself failed.
func processExitCode(err error) int {
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exitErr):
		return exitErr.ExitCode()
	default:
		return -1
	}
}
