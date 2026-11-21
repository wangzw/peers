package broker

import (
	"sync"
	"time"
)

// Inbound delivery quota. One peer may send inboundLimitBurst messages to one
// target Agent in a burst; the bucket then refills one token every
// inboundLimitInterval. The quota is per (source peer, target Agent) pair: a
// peer cannot drain the Claude Code inbox writer's shared token bucket and make
// every session drop other peers' messages, and its remote Agent names do not
// split its budget because the peer reports those names itself.
const (
	inboundLimitBurst = 10

	// defaultInboundLimitInterval is the release refill interval. A build
	// carrying the peers_testenv tag may lengthen it through
	// inboundLimitInterval.
	defaultInboundLimitInterval = 10 * time.Second
)

// inboundLimitKey identifies one peer-to-target quota.
type inboundLimitKey struct {
	peer   peerID
	target string
}

// inboundLimitBucket is one token bucket. tokens carries the fraction earned
// since the last refill; last is when the bucket was last refilled.
type inboundLimitBucket struct {
	tokens float64
	last   time.Time
}

// inboundLimiter holds one token bucket per (source peer, target Agent) pair.
// It is in-memory only: a restarted Broker starts every bucket full. A bucket
// refilled to its capacity is equivalent to an absent one, so the map never
// holds a full bucket. Unpairing a peer and an Agent ending delete their
// buckets, so the map is bounded by the paired peers times the local Agents.
// A link drop keeps them on purpose: a reconnect must not reset the quota.
type inboundLimiter struct {
	mu      sync.Mutex
	buckets map[inboundLimitKey]inboundLimitBucket
	now     func() time.Time
}

// newInboundLimiter builds a limiter reading time from now. A nil clock uses
// the system clock.
func newInboundLimiter(now func() time.Time) *inboundLimiter {
	if now == nil {
		now = time.Now
	}
	return &inboundLimiter{
		buckets: make(map[inboundLimitKey]inboundLimitBucket),
		now:     now,
	}
}

// take consumes one token for the pair. When the bucket is empty, ok is false
// and retryAfter is the time until the next full token, so the refusal can
// tell the sender how long to wait.
func (l *inboundLimiter) take(peer peerID, target string) (ok bool, retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	key := inboundLimitKey{peer: peer, target: target}
	b, held := l.buckets[key]
	if !held {
		b = inboundLimitBucket{tokens: inboundLimitBurst, last: now}
	}
	interval := inboundLimitInterval()
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens += elapsed.Seconds() / interval.Seconds()
		if b.tokens > inboundLimitBurst {
			b.tokens = inboundLimitBurst
		}
		b.last = now
	}
	if b.tokens < 1 {
		l.storeLocked(key, b)
		wait := (1 - b.tokens) * float64(interval)
		return false, time.Duration(wait)
	}
	b.tokens--
	l.storeLocked(key, b)
	return true, 0
}

// storeLocked writes b, dropping a bucket that is full instead of keeping it:
// a full bucket is the same as one that was never created. It must be called
// with l.mu held.
func (l *inboundLimiter) storeLocked(key inboundLimitKey, b inboundLimitBucket) {
	if b.tokens >= inboundLimitBurst {
		delete(l.buckets, key)
		return
	}
	l.buckets[key] = b
}

// dropPeer forgets every bucket of a peer that was unpaired. A later pairing
// starts with full buckets; a dropped link does not call this, so reconnecting
// cannot refresh the quota.
func (l *inboundLimiter) dropPeer(peer peerID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key := range l.buckets {
		if key.peer == peer {
			delete(l.buckets, key)
		}
	}
}

// dropTarget forgets every bucket of a local Agent that ended.
func (l *inboundLimiter) dropTarget(target string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key := range l.buckets {
		if key.target == target {
			delete(l.buckets, key)
		}
	}
}

// inboundRetrySeconds renders a wait as a whole number of seconds, rounded up,
// so a sender that waits exactly that long may retry successfully. It is never
// below one.
func inboundRetrySeconds(d time.Duration) int {
	secs := int((d + time.Second - 1) / time.Second)
	if secs < 1 {
		return 1
	}
	return secs
}
