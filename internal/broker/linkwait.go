package broker

import (
	"context"
	"sync"
	"time"
)

// A linkWaiter never trusts Established: after a drop, until the state handler
// runs, it still reports true.
type linkWaiter struct {
	mu    sync.Mutex
	gen   map[peerID]uint64
	ch    map[peerID]map[chan struct{}]struct{}
	link  deliveryLink
	clock Clock
}

// newLinkWaiter must be created before the link starts so no up event is missed; a nil clock uses the system clock.
func newLinkWaiter(l deliveryLink, clock Clock) *linkWaiter {
	if clock == nil {
		clock = SystemClock()
	}
	w := &linkWaiter{
		gen:   make(map[peerID]uint64),
		ch:    make(map[peerID]map[chan struct{}]struct{}),
		link:  l,
		clock: clock,
	}
	if l != nil {
		l.Subscribe(w.onUp, func(peerID) {})
	}
	return w
}

func (w *linkWaiter) onUp(p peerID, _ []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.gen[p]++
	for ch := range w.ch[p] {
		close(ch)
	}
	delete(w.ch, p)
}

// generation returns the number of up events seen for p, which wait compares so only a later up satisfies its wait.
func (w *linkWaiter) generation(p peerID) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gen[p]
}

// wait returns true once an up event for p follows the wait, false when max
// elapses or ctx ends first. When since is given, only an up after that
// generation satisfies the wait.
func (w *linkWaiter) wait(ctx context.Context, p peerID, max time.Duration, since ...uint64) bool {
	if w.link == nil {
		return false
	}

	// The registration, the generation check and the timer share one lock, so an
	// up event either counts before the check or finds the channel.
	w.mu.Lock()
	if len(since) > 0 && w.gen[p] > since[0] {
		w.mu.Unlock()
		return true
	}
	timerCh := make(chan struct{})
	timer := w.clock.AfterFunc(max, func() { close(timerCh) })
	ch := make(chan struct{})
	if w.ch[p] == nil {
		w.ch[p] = make(map[chan struct{}]struct{})
	}
	w.ch[p][ch] = struct{}{}
	w.mu.Unlock()

	defer timer.Stop()
	defer func() {
		w.mu.Lock()
		if set := w.ch[p]; set != nil {
			delete(set, ch)
			if len(set) == 0 {
				delete(w.ch, p)
			}
		}
		w.mu.Unlock()
	}()

	select {
	case <-ch:
		return true
	case <-timerCh:
		return false
	case <-ctx.Done():
		return false
	}
}
