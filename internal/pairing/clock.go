package pairing

import (
	"context"
	"time"
)

// Pairing never calls time.Now or time.NewTimer directly, so tests can advance
// time without sleeping.
type clock interface {
	Now() time.Time
	NewTimer(d time.Duration) timer
}

type timer interface {
	C() <-chan time.Time
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) timer { return realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }

// The timer is stopped on every exit path so a cancelled handshake does not
// leave it running.
func withClockTimeout(parent context.Context, c clock, d time.Duration) (context.Context, context.CancelFunc) {
	if c == nil {
		c = realClock{}
	}
	ctx, cancel := context.WithCancel(parent)
	t := c.NewTimer(d)
	go func() {
		select {
		case <-parent.Done():
		case <-t.C():
			cancel()
		case <-ctx.Done():
		}
		t.Stop()
	}()
	return ctx, cancel
}
