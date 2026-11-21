package broker

import (
	"sync"
	"time"
)

const (
	// topicRateBurst is the number of topic changes one account may make at
	// once.
	topicRateBurst = 64
	// topicRatePerSecond is the number of tokens one account regains each
	// second.
	topicRatePerSecond = 4
)

// topicTokenBucket holds one account's remaining topic-change tokens.
type topicTokenBucket struct {
	tokens float64
	last   time.Time
}

// topicRateLimiter limits topic changes per account with a token bucket. The
// zero value is ready to use.
type topicRateLimiter struct {
	mu      sync.Mutex
	buckets map[uint32]*topicTokenBucket
}

// allow consumes one token for uid when one is available and reports whether it
// did. It refills the bucket for the time elapsed since the last call, up to the
// burst size.
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
