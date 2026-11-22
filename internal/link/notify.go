package link

import (
	"sync"

	"github.com/wangzw/peers/internal/config"
)

// subscription is one pair of link up and down callbacks.
type subscription struct {
	onUp   func(peer Fingerprint, salt []byte)
	onDown func(peer Fingerprint)
}

// event is one queued link up or down notification. delivered is non-nil only
// for up events; it is closed once every onUp callback returned, or when the
// event is discarded.
type event struct {
	up        bool
	peer      Fingerprint
	salt      []byte
	reason    FailReason
	delivered chan struct{}
}

// notifier hands link up and down events to the subscribers one at a time, in
// the order they were posted. It runs one goroutine and an unbounded queue, so
// posting never blocks and may happen while holding Link.mu; the notifier's own
// mutex is a leaf lock.
type notifier struct {
	l *Link

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []event
	started bool
	stopped bool
	done    chan struct{}
}

func newNotifier(l *Link) *notifier {
	n := &notifier{l: l, done: make(chan struct{})}
	n.cond = sync.NewCond(&n.mu)
	return n
}

// post appends ev to the queue and wakes the delivery goroutine. After stop it
// discards the event instead, closing the delivered channel of an up event so
// its waiter is released.
func (n *notifier) post(ev event) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		release(ev)
		return
	}
	n.queue = append(n.queue, ev)
	n.cond.Signal()
	n.mu.Unlock()
}

// start starts the delivery goroutine. Events posted before the start are
// delivered once it runs.
func (n *notifier) start() {
	n.mu.Lock()
	if n.started || n.stopped {
		n.mu.Unlock()
		return
	}
	n.started = true
	n.mu.Unlock()
	go n.run()
}

// stop discards the queued events, releases their waiters and waits for the
// delivery goroutine to exit. A callback already running finishes first.
func (n *notifier) stop() {
	n.mu.Lock()
	n.stopped = true
	started := n.started
	n.cond.Broadcast()
	if !started {
		rest := n.queue
		n.queue = nil
		n.mu.Unlock()
		releaseAll(rest)
		return
	}
	n.mu.Unlock()
	<-n.done
}

// run delivers queued events until stop is called.
func (n *notifier) run() {
	defer close(n.done)
	for {
		n.mu.Lock()
		for len(n.queue) == 0 && !n.stopped {
			n.cond.Wait()
		}
		if n.stopped {
			rest := n.queue
			n.queue = nil
			n.mu.Unlock()
			releaseAll(rest)
			return
		}
		ev := n.queue[0]
		n.queue = n.queue[1:]
		n.mu.Unlock()
		n.deliver(ev)
	}
}

// deliver calls every subscriber, then writes the audit event. It holds no
// Link lock, so callbacks may use Link methods.
func (n *notifier) deliver(ev event) {
	l := n.l
	l.subMu.RLock()
	subs := append([]subscription(nil), l.subs...)
	l.subMu.RUnlock()

	if ev.up {
		for _, s := range subs {
			s.onUp(ev.peer, append([]byte(nil), ev.salt...))
		}
	} else {
		for _, s := range subs {
			s.onDown(ev.peer)
		}
	}

	e := config.Event{
		Time: l.clock.Now(),
		Peer: &config.PeerRef{
			Fingerprint: ev.peer,
			Alias:       l.peers.EffectiveAlias(ev.peer),
		},
	}
	if ev.up {
		e.Event = config.EventLinkUp
	} else {
		e.Event = config.EventLinkDown
		e.Detail = ev.reason.String()
	}
	l.settings.Audit(e)

	release(ev)
}

// release closes the delivered channel of an up event. It is safe to call once
// per event.
func release(ev event) {
	if ev.up && ev.delivered != nil {
		close(ev.delivered)
	}
}

// releaseAll releases the up-event waiters of every event in events.
func releaseAll(events []event) {
	for _, ev := range events {
		release(ev)
	}
}
