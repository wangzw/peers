//go:build !linux

package node

import (
	"errors"
	"syscall"
	"time"

	"github.com/wangzw/peers/internal/procsig"
)

// brokerReapPoll is how often the reaper polls wait4 for the Broker's exit.
// Without waitid(WNOWAIT) there is no way to observe the exit without reaping
// it, so the reaper polls with WNOHANG instead of blocking. The interval bounds
// how long a caller that has already signalled the Broker waits for the reap.
const brokerReapPoll = 50 * time.Millisecond

// reap polls wait4 with WNOHANG until it has reaped the Broker, holding the
// lifecycle lock across each poll and the mark. A child that has exited but not
// been reaped is a zombie, whose pid cannot be reused, so a SendSignal that
// wins the lock can signal it safely; once a poll reaps the process the reaped
// mark is set in the same critical section, and every later send is refused.
// WNOHANG makes each critical section a single non-blocking syscall, so a
// sender is never held off for a whole poll interval.
func (b *BrokerProc) reap() {
	ticker := time.NewTicker(brokerReapPoll)
	defer ticker.Stop()
	for {
		b.mu.Lock()
		var status syscall.WaitStatus
		wpid, err := syscall.Wait4(b.pid, &status, syscall.WNOHANG, nil)
		reaped, exit := interpretWait4(b.pid, wpid, err, status)
		if reaped {
			b.reaped = true
			b.exit = exit
			b.mu.Unlock()
			close(b.done)
			return
		}
		b.mu.Unlock()
		<-ticker.C
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
