//go:build !linux

package node

import (
	"errors"
	"syscall"
	"time"

	"github.com/wangzw/peers/internal/procsig"
)

const brokerReapPoll = 50 * time.Millisecond

// reap polls wait4 with WNOHANG until it has reaped the Broker, holding the
// lifecycle lock across each poll and the mark. A zombie's pid is not reused,
// so a sender that wins the lock can signal it safely; once a poll reaps the
// process the mark is set in the same critical section, and every later send is
// refused. WNOHANG keeps each critical section a single non-blocking syscall.
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
