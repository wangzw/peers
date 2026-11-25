package directory

import (
	"math"
	"time"
)

const (
	recordTTL              = 7 * 24 * time.Hour
	renewAfter             = 24 * time.Hour
	skewTolerance          = time.Hour
	maintainInterval       = time.Minute
	requestTimeout         = 30 * time.Second
	defaultReplaceInterval = 60 * time.Second
)

func nextVersion(now time.Time, highest uint64) uint64 {
	var fromClock uint64
	if ms := now.UnixMilli(); ms > 0 {
		fromClock = uint64(ms)
	}
	next := highest
	if next < math.MaxUint64 {
		next++
	}
	if fromClock > next {
		return fromClock
	}
	return next
}

func issuedAt(expires time.Time) time.Time {
	return expires.Add(-recordTTL)
}

func isExpired(expires, now time.Time) bool {
	return !expires.After(now)
}

func expiryTooFar(expires, now time.Time) bool {
	return expires.After(now.Add(recordTTL + skewTolerance))
}

func renewalDue(expires, now time.Time) bool {
	return !issuedAt(expires).Add(renewAfter).After(now)
}
