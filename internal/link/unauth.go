package link

import (
	"container/list"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// maxKnownPerPeer is the number of unauthenticated connections the known-peer
// pool keeps for one fingerprint. A third connection for the same fingerprint
// closes the oldest one.
const maxKnownPerPeer = 2

// maxUnauthenticatedReadBytes is the number of bytes a connection may read
// from its raw socket before it authenticates. It covers the TLS handshake,
// the HTTP/2 preface and frames, and the first link frame. Once the connection
// leaves the registry, reads are no longer counted.
const maxUnauthenticatedReadBytes = 64 << 10

// unauthRegistry tracks inbound connections until the peer behind them is
// authenticated. It enforces three limits from the design: a connection that
// has not sent its first frame within firstFrame is closed; the number of
// unauthenticated connections from one source never exceeds maxPerSource; and
// the total never exceeds max. When a source is over its own limit, that
// source's oldest connection is closed. When the total is over max, the oldest
// connection of the source with the most connections is closed, so a source
// that floods the pool spends its own connections before anyone else's.
//
// A connection whose TLS fingerprint is already in the peer table moves to the
// known-peer pool, which neither limit counts; a stranger flood then cannot
// evict a paired peer that is reconnecting. That pool holds at most
// maxKnownPerPeer connections per fingerprint, and the first-frame deadline
// still applies to it.
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
	// so far. Tests use it to observe the read budget without polling. It is
	// set before the listener starts and never changes afterwards.
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

// track registers c as unauthenticated and starts its first-frame deadline.
// The caller receives the tracked wrapper and should use it in place of c.
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

// count returns the number of connections in the stranger-source pool. The
// known-peer pool is not included: it is bounded per fingerprint by
// maxKnownPerPeer and has no global cap. Tests read the known pool directly
// under the registry lock.
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
	fp     Fingerprint   // fingerprint once the connection moved to the known-peer pool
	known  bool          // true while the connection is in the known-peer pool

	timer   func() bool // first-frame deadline; nil when none is pending
	seen    bool
	closed  bool
	claimed bool

	// readBytes counts bytes read from the raw connection while counting is
	// true, that is, until the connection leaves the registry. Both are atomic
	// so the read path never takes the registry lock after authentication.
	readBytes atomic.Int64
	counting  atomic.Bool
	// observedReads accumulates every byte read from the raw connection for
	// the test-only onRead callback. It is used only when onRead is set.
	observedReads atomic.Int64
}

// Read counts the bytes the raw connection delivers while it is still in the
// registry. The TLS handshake, the HTTP/2 preface and every frame go through
// this method, so a connection cannot make the host buffer more than
// maxUnauthenticatedReadBytes before it authenticates. Once the connection
// leaves the registry, the check is one atomic load and the read path no
// longer takes the registry lock.
func (t *trackedConn) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	if n > 0 && t.counting.Load() {
		if t.readBytes.Add(int64(n)) > maxUnauthenticatedReadBytes {
			t.closeForByteBudget()
		}
	}
	// The observer runs after the budget check, so a test that waits for it
	// sees the close already decided when the byte budget was exceeded.
	if n > 0 && t.reg.onRead != nil {
		t.reg.onRead(t.observedReads.Add(int64(n)))
	}
	return n, err
}

// closeForByteBudget closes a connection that read past the pre-authentication
// budget. It closes the raw connection and writes the Debug record outside the
// registry lock. A connection that already left the registry, or that another
// path closed first, is left alone.
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

// promoteKnown moves t from the stranger-source pool to the known-peer pool,
// which neither the per-source nor the global limit counts. The first-frame
// deadline keeps running. When the fingerprint already holds maxKnownPerPeer
// connections, the oldest one is closed with cause known_peer_limit.
//
// The caller decides whether fp is in the peer table before calling, outside
// the registry lock: this method takes the lock but never calls Peers.
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

// countKnownOfFingerprintLocked returns the number of known-peer connections
// for fp. Caller holds reg.mu.
func (r *unauthRegistry) countKnownOfFingerprintLocked(fp Fingerprint) int {
	n := 0
	for e := r.known.Front(); e != nil; e = e.Next() {
		if e.Value.(*trackedConn).fp == fp {
			n++
		}
	}
	return n
}

// oldestKnownOfFingerprintLocked returns the oldest known-peer connection for
// fp. Caller holds reg.mu.
func (r *unauthRegistry) oldestKnownOfFingerprintLocked(fp Fingerprint) *trackedConn {
	for e := r.known.Front(); e != nil; e = e.Next() {
		if t := e.Value.(*trackedConn); t.fp == fp {
			return t
		}
	}
	return nil
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

// detachLocked removes t from whichever pool holds it. Caller holds reg.mu.
func (t *trackedConn) detachLocked() {
	if t.elem != nil {
		if t.known {
			t.reg.known.Remove(t.elem)
		} else {
			t.reg.conns.Remove(t.elem)
		}
		t.elem = nil
	}
	// Leaving the registry ends the read budget: reads after this point are
	// not counted and do not take the registry lock.
	t.counting.Store(false)
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
