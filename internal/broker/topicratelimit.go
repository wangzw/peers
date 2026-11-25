package broker

import (
	"sync"
	"time"
)

const (
	topicRateBurst     = 64
	topicRatePerSecond = 4
)

type topicTokenBucket struct {
	tokens float64
	last   time.Time
}

// topicRateLimiter limits topic changes per account; its zero value is ready to
// use.
type topicRateLimiter struct {
	mu      sync.Mutex
	buckets map[uint32]*topicTokenBucket
}

func (rl *topicRateLimiter) allow(uid uint32, now time.Time) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.buckets == nil {
		rl.buckets = map[uint32]*topicTokenBucket{}
	}
	b := rl.buckets[uid]
	if b == nil {
		b = &topicTokenBucket{tokens: topicRateBurst, last: now}
		rl.buckets[uid] = b
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(float64(topicRateBurst), b.tokens+elapsed*topicRatePerSecond)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
