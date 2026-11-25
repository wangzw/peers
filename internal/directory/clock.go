package directory

import "time"

type clock interface {
	Now() time.Time
	NewTimer(d time.Duration) timer
}

type timer interface {
	C() <-chan time.Time
	Stop() bool
}

type realClock struct {
	now func() time.Time
}

func (c realClock) Now() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c realClock) NewTimer(d time.Duration) timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct {
	t *time.Timer
}

func (t *realTimer) C() <-chan time.Time { return t.t.C }

func (t *realTimer) Stop() bool { return t.t.Stop() }
