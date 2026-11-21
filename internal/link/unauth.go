package link

import (
	"container/list"
	"log/slog"
	"net"
	"sync"
	"time"
)

// unauthRegistry tracks inbound connections until the peer behind them is
// authenticated. It enforces three limits from the design: a connection that
// has not sent its first frame within firstFrame is closed; the number of
// unauthenticated connections from one source never exceeds maxPerSource; and
// the total never exceeds max. When a source is over its own limit, that
// source's oldest connection is closed. When the total is over max, the oldest
// connection of the source with the most connections is closed, so a source
// that floods the pool spends its own connections before anyone else's.
type unauthRegistry struct {
	mu           sync.Mutex
	conns        list.List // of *trackedConn, oldest first
	max          int
	maxPerSource int
	firstFrame   time.Duration
	logger       *slog.Logger
	afterFunc    func(time.Duration, func()) func() bool
}

func newUnauthRegistry(max, maxPerSource int, firstFrame time.Duration, logger *slog.Logger, afterFunc func(time.Duration, func()) func() bool) *unauthRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	return &unauthRegistry{
		max:          max,
		maxPerSource: maxPerSource,
		firstFrame:   firstFrame,
		logger:       logger,
		afterFunc:    afterFunc,
	}
}

// track registers c as unauthenticated and starts its first-frame deadline.
// The caller receives the tracked wrapper and should use it in place of c.
func (r *unauthRegistry) track(c net.Conn) *trackedConn {
	source := ""
	if addr := c.RemoteAddr(); addr != nil {
		source = sourcePrefix(addr.String())
	}
	t := &trackedConn{Conn: c, reg: r, source: source}

	r.mu.Lock()
	t.elem = r.conns.PushBack(t)
	r.mu.Unlock()

	// The timer starts outside the registry lock: an injected afterFunc is
	// allowed to invoke its callback synchronously, and the callback needs the
	// lock. Production time.AfterFunc is asynchronous.
	stop := r.afterFunc(r.firstFrame, t.deadline)

	r.mu.Lock()
	if t.closed || t.seen {
		// The deadline fired before the timer was recorded, or the connection
		// was already gone. Keep no timer reference.
		r.mu.Unlock()
		if stop != nil {
			stop()
		}
	} else {
		t.timer = stop
		r.mu.Unlock()
	}

	r.evict(t)
	return t
}

func (r *unauthRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conns.Len()
}

// evict enforces the per-source and global limits after t joined the registry.
// At most one connection is closed for each limit, and the raw connections are
// closed and logged outside the registry lock.
func (r *unauthRegistry) evict(t *trackedConn) {
	type victim struct {
		source string
		cause  string
		stop   func() bool
		raw    net.Conn
	}
	var victims []victim

	r.mu.Lock()
	if r.countSourceLocked(t.source) > r.maxPerSource {
		if v := r.oldestOfSourceLocked(t.source); v != nil {
			victims = append(victims, victim{v.source, "source_limit", r.evictLocked(v), v.Conn})
		}
	}
	if r.conns.Len() > r.max {
		if v := r.oldestOfBusiestSourceLocked(); v != nil {
			victims = append(victims, victim{v.source, "global_limit", r.evictLocked(v), v.Conn})
		}
	}
	r.mu.Unlock()

	for _, v := range victims {
		if v.stop != nil {
			v.stop()
		}
		_ = v.raw.Close()
		r.logger.Debug("link: closed an unauthenticated connection", "source", v.source, "cause", v.cause)
	}
}

// evictLocked marks v closed and removes it from the registry, returning its
// timer stop function. Caller holds reg.mu and must call the stop function
// outside the lock.
func (r *unauthRegistry) evictLocked(v *trackedConn) func() bool {
	v.closed = true
	v.detachLocked()
	return v.stopTimerLocked()
}

// countSourceLocked returns the number of tracked connections from source.
// Caller holds reg.mu.
func (r *unauthRegistry) countSourceLocked(source string) int {
	n := 0
	for e := r.conns.Front(); e != nil; e = e.Next() {
		if e.Value.(*trackedConn).source == source {
			n++
		}
	}
	return n
}

// oldestOfSourceLocked returns the oldest tracked connection from source.
// Caller holds reg.mu.
func (r *unauthRegistry) oldestOfSourceLocked(source string) *trackedConn {
	for e := r.conns.Front(); e != nil; e = e.Next() {
		if t := e.Value.(*trackedConn); t.source == source {
			return t
		}
	}
	return nil
}

// oldestOfBusiestSourceLocked returns the earliest connection of the source
// with the most tracked connections. Ties go to the source whose earliest
// connection is earliest, found by taking the first element of the oldest-first
// list whose source has the maximum count. Caller holds reg.mu.
func (r *unauthRegistry) oldestOfBusiestSourceLocked() *trackedConn {
	counts := make(map[string]int)
	maxCount := 0
	for e := r.conns.Front(); e != nil; e = e.Next() {
		source := e.Value.(*trackedConn).source
		counts[source]++
		if counts[source] > maxCount {
			maxCount = counts[source]
		}
	}
	if maxCount == 0 {
		return nil
	}
	for e := r.conns.Front(); e != nil; e = e.Next() {
		if t := e.Value.(*trackedConn); counts[t.source] == maxCount {
			return t
		}
	}
	return nil
}

// trackedConn is an inbound connection under the first-frame deadline. All of
// its mutable state is guarded by reg.mu so that authentication and eviction
// cannot interleave: whichever reaches the lock first decides the outcome.
type trackedConn struct {
	net.Conn

	reg    *unauthRegistry
	elem   *list.Element // nil once detached from the registry
	source string        // sourcePrefix of the remote address; "" groups unknown sources together

	timer   func() bool // first-frame deadline; nil when none is pending
	seen    bool
	closed  bool
	claimed bool
}

// firstFrameSeen records that the peer sent its first frame. The connection
// stays counted as unauthenticated but is no longer closed by the deadline.
func (t *trackedConn) firstFrameSeen() {
	r := t.reg
	r.mu.Lock()
	if t.closed {
		r.mu.Unlock()
		return
	}
	t.seen = true
	stop := t.stopTimerLocked()
	r.mu.Unlock()

	if stop != nil {
		stop()
	}
}

// authenticated removes the connection from the registry. It is no longer
// counted or eligible for eviction, and its deadline no longer applies.
func (t *trackedConn) authenticated() {
	r := t.reg
	r.mu.Lock()
	if t.closed {
		r.mu.Unlock()
		return
	}
	t.detachLocked()
	stop := t.stopTimerLocked()
	r.mu.Unlock()

	if stop != nil {
		stop()
	}
}

// Close is idempotent. It removes the connection from the registry and closes
// the raw connection outside the registry lock.
func (t *trackedConn) Close() error {
	r := t.reg
	r.mu.Lock()
	if t.closed {
		r.mu.Unlock()
		return nil
	}
	t.closed = true
	t.detachLocked()
	stop := t.stopTimerLocked()
	r.mu.Unlock()

	if stop != nil {
		stop()
	}
	return t.Conn.Close()
}

// claimStream reports whether this is the first call. A tracked connection may
// carry only one gRPC stream, so later calls are refused before they can skip
// the first-frame check.
func (t *trackedConn) claimStream() bool {
	r := t.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.claimed {
		return false
	}
	t.claimed = true
	return true
}

// deadline runs when the first-frame timer fires.
func (t *trackedConn) deadline() {
	r := t.reg
	r.mu.Lock()
	t.timer = nil // the timer has fired; there is nothing left to stop
	if t.closed || t.seen {
		r.mu.Unlock()
		return
	}
	t.closed = true
	t.detachLocked()
	r.mu.Unlock()

	_ = t.Conn.Close()
}

// detachLocked removes t from the registry. Caller holds reg.mu.
func (t *trackedConn) detachLocked() {
	if t.elem != nil {
		t.reg.conns.Remove(t.elem)
		t.elem = nil
	}
}

// stopTimerLocked clears the recorded timer and returns its stop function.
// Caller holds reg.mu and must call the returned function outside the lock.
func (t *trackedConn) stopTimerLocked() func() bool {
	stop := t.timer
	t.timer = nil
	return stop
}

// trackingListener wraps a listener so every accepted connection is tracked.
type trackingListener struct {
	net.Listener
	reg *unauthRegistry
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return l.reg.track(c), nil
}
