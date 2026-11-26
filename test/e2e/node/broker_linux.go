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
// the lock lets a SendSignal holding it deliver the signal that ends the
// process; the mark and cmd.Wait run under the lock, so no signal is sent after
// the pid becomes reusable.
func (b *BrokerProc) reap() {
	b.awaitExit()
	b.mu.Lock()
	b.reaped = true
	b.exit = processExitCode(b.cmd.Wait())
	b.mu.Unlock()
	close(b.done)
}

// awaitExit waits without reaping, so cmd.Wait can still collect the exit status
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
// has checked that the process is not reaped; signalling a zombie is harmless
// because its pid is not reused until it is reaped.
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
