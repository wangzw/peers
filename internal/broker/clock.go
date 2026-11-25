package broker

import "time"

// Timer is a scheduled callback returned by Clock.AfterFunc.
type Timer interface {
	// Stop reports whether it prevented the callback from running.
	Stop() bool
}

// Clock injects the passage of time so tests can advance it instead of
// sleeping. Production code uses SystemClock.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// AfterFunc runs f after d and returns a Timer that can stop it.
	AfterFunc(d time.Duration, f func()) Timer
}

// SystemClock returns the Clock backed by the time package. Its AfterFunc runs f
// in its own goroutine.
func SystemClock() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }
