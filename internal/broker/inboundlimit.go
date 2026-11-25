package broker

import (
	"sync"
	"time"
)

// The inbound delivery quota is a token bucket per (peer, target) of
// inboundLimitBurst tokens, refilled one token every inboundLimitInterval().
const (
	inboundLimitBurst = 10

	defaultInboundLimitInterval = 10 * time.Second
)

type inboundLimitKey struct {
	peer   peerID
	target string
}

type inboundLimitBucket struct {
	tokens float64
	last   time.Time
}

type inboundLimiter struct {
	mu      sync.Mutex
	buckets map[inboundLimitKey]inboundLimitBucket
	now     func() time.Time
}

func newInboundLimiter(now func() time.Time) *inboundLimiter {
	if now == nil {
		now = time.Now
	}
	return &inboundLimiter{
		buckets: make(map[inboundLimitKey]inboundLimitBucket),
		now:     now,
	}
}

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

// storeLocked must be called with l.mu held.
func (l *inboundLimiter) storeLocked(key inboundLimitKey, b inboundLimitBucket) {
	if b.tokens >= inboundLimitBurst {
		delete(l.buckets, key)
		return
	}
	l.buckets[key] = b
}

// dropPeer forgets an unpaired peer's buckets; a dropped link must not call it,
// or a reconnect would refresh the quota.
func (l *inboundLimiter) dropPeer(peer peerID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key := range l.buckets {
		if key.peer == peer {
			delete(l.buckets, key)
		}
	}
}

func (l *inboundLimiter) dropTarget(target string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key := range l.buckets {
		if key.target == target {
			delete(l.buckets, key)
		}
	}
}

func inboundRetrySeconds(d time.Duration) int {
	secs := int((d + time.Second - 1) / time.Second)
	if secs < 1 {
		return 1
	}
	return secs
}
