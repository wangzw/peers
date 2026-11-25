package link

import (
	"container/list"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const maxKnownPerPeer = 2

const maxUnauthenticatedReadBytes = 64 << 10

type unauthRegistry struct {
	mu           sync.Mutex
	conns        list.List // of *trackedConn from stranger sources, oldest first
	known        list.List // of *trackedConn whose fingerprint is in the peer table, oldest first
	max          int
	maxPerSource int
	firstFrame   time.Duration
	logger       *slog.Logger
	afterFunc    func(time.Duration, func()) func() bool
	// onRead, when not nil, receives the bytes an accepted connection has read
	// so far; it is set before the listener starts and never changed.
	onRead func(total int64)
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

func (r *unauthRegistry) track(c net.Conn) *trackedConn {
	source := ""
	if addr := c.RemoteAddr(); addr != nil {
		source = sourcePrefix(addr.String())
	}
	t := &trackedConn{Conn: c, reg: r, source: source}
	t.counting.Store(true)

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

// evict closes the raw connections and logs outside the registry lock.
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

// Caller holds r.mu and must call the returned stop function outside it.
func (r *unauthRegistry) evictLocked(v *trackedConn) func() bool {
	v.closed = true
	v.detachLocked()
	return v.stopTimerLocked()
}

// Caller holds r.mu.
func (r *unauthRegistry) countSourceLocked(source string) int {
	n := 0
	for e := r.conns.Front(); e != nil; e = e.Next() {
		if e.Value.(*trackedConn).source == source {
			n++
		}
	}
	return n
}

// Caller holds r.mu.
func (r *unauthRegistry) oldestOfSourceLocked(source string) *trackedConn {
	for e := r.conns.Front(); e != nil; e = e.Next() {
		if t := e.Value.(*trackedConn); t.source == source {
			return t
		}
	}
	return nil
}

// Caller holds r.mu; ties go to the source whose earliest connection is oldest.
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

// The non-atomic mutable fields (elem, source, fp, known, timer, seen, closed,
// claimed) are guarded by reg.mu.
type trackedConn struct {
	net.Conn

	reg    *unauthRegistry
	elem   *list.Element // nil once detached from the registry
	source string
	fp     Fingerprint
	known  bool

	timer   func() bool // first-frame deadline; nil when none is pending
	seen    bool
	closed  bool
	claimed bool

	// Both counters are atomic so the read path never takes the registry lock
	// after authentication.
	readBytes atomic.Int64
	counting  atomic.Bool
	// observedReads is the test-only total for onRead, used only when onRead is set.
	observedReads atomic.Int64
}

// Every byte read before authentication goes through here, including the TLS
// handshake and the HTTP/2 preface.
func (t *trackedConn) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	if n > 0 && t.counting.Load() {
		if t.readBytes.Add(int64(n)) > maxUnauthenticatedReadBytes {
			t.closeForByteBudget()
		}
	}
	// The observer runs after the budget check, so a waiting test sees the
	// close already decided when the byte budget was exceeded.
	if n > 0 && t.reg.onRead != nil {
		t.reg.onRead(t.observedReads.Add(int64(n)))
	}
	return n, err
}

// closeForByteBudget closes the raw connection and logs outside the registry lock.
func (t *trackedConn) closeForByteBudget() {
	r := t.reg
	r.mu.Lock()
	if t.closed || t.elem == nil {
		r.mu.Unlock()
		return
	}
	t.closed = true
	t.detachLocked()
	stop := t.stopTimerLocked()
	source := t.source
	r.mu.Unlock()

	if stop != nil {
		stop()
	}
	_ = t.Conn.Close()
	r.logger.Debug("link: closed an unauthenticated connection", "source", source, "cause", "byte_budget")
}

// The caller decides whether fp is known before calling; this method takes the
// lock but never calls Peers.
func (t *trackedConn) promoteKnown(fp Fingerprint) {
	r := t.reg
	var (
		victim *trackedConn
		stop   func() bool
		source string
	)

	r.mu.Lock()
	if !t.closed && !t.known && t.elem != nil {
		r.conns.Remove(t.elem)
		t.known = true
		t.fp = fp
		t.elem = r.known.PushBack(t)
		if r.countKnownOfFingerprintLocked(fp) > maxKnownPerPeer {
			if v := r.oldestKnownOfFingerprintLocked(fp); v != nil {
				victim = v
				stop = r.evictLocked(v)
				source = v.source
			}
		}
	}
	r.mu.Unlock()

	if victim != nil {
		if stop != nil {
			stop()
		}
		_ = victim.Conn.Close()
		r.logger.Debug("link: closed an unauthenticated connection", "source", source, "cause", "known_peer_limit")
	}
}

// Caller holds r.mu.
func (r *unauthRegistry) countKnownOfFingerprintLocked(fp Fingerprint) int {
	n := 0
	for e := r.known.Front(); e != nil; e = e.Next() {
		if e.Value.(*trackedConn).fp == fp {
			n++
		}
	}
	return n
}

// Caller holds r.mu.
func (r *unauthRegistry) oldestKnownOfFingerprintLocked(fp Fingerprint) *trackedConn {
	for e := r.known.Front(); e != nil; e = e.Next() {
		if t := e.Value.(*trackedConn); t.fp == fp {
			return t
		}
	}
	return nil
}

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

// Close is idempotent and closes the raw connection outside the registry lock.
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

// detachLocked removes t from whichever pool holds it. Caller holds t.reg.mu.
func (t *trackedConn) detachLocked() {
	if t.elem != nil {
		if t.known {
			t.reg.known.Remove(t.elem)
		} else {
			t.reg.conns.Remove(t.elem)
		}
		t.elem = nil
	}
	// Leaving the registry ends the read budget: later reads are not counted
	// and do not take the registry lock.
	t.counting.Store(false)
}

// Caller holds t.reg.mu and must call the returned function outside it.
func (t *trackedConn) stopTimerLocked() func() bool {
	stop := t.timer
	t.timer = nil
	return stop
}

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
