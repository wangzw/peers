package directory

import (
	"math"
	"time"
)

const (
	// recordTTL is how long a signed record stays valid after issuance.
	recordTTL = 7 * 24 * time.Hour
	// renewAfter is how long after issuance a record is renewed.
	renewAfter = 24 * time.Hour
	// skewTolerance is how far in the future an expiry may be before the
	// record is rejected as forged or the result of a broken clock.
	skewTolerance = time.Hour
	// maintainInterval is how often the maintenance loop wakes.
	maintainInterval = time.Minute
	// requestTimeout bounds one RecordRequest round trip.
	requestTimeout = 30 * time.Second
	// defaultReplaceInterval is how long a record accepted from a link must be
	// held before a newer version of the same identifier may replace it. It
	// bounds how fast a publisher can make the network rewrite and forward
	// records. directoryReplaceInterval returns it in release builds and an
	// override in test builds.
	defaultReplaceInterval = 60 * time.Second
)

// nextVersion returns max(now in Unix milliseconds, highest+1). Using the
// higher of the two keeps versions moving forward when the clock goes
// backwards, so freshly signed records are not treated as stale elsewhere.
// A negative Unix millisecond value counts as 0, and highest+1 saturates at
// math.MaxUint64 instead of wrapping to 0.
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

// issuedAt returns the instant a record with this expiry was issued.
func issuedAt(expires time.Time) time.Time {
	return expires.Add(-recordTTL)
}

// isExpired reports whether expires is at or before now.
func isExpired(expires, now time.Time) bool {
	return !expires.After(now)
}

// expiryTooFar reports whether expires lies beyond now plus the TTL and the
// clock skew tolerance.
func expiryTooFar(expires, now time.Time) bool {
	return expires.After(now.Add(recordTTL + skewTolerance))
}

// renewalDue reports whether a record with this expiry is due for renewal,
// that is, at least renewAfter has passed since it was issued.
func renewalDue(expires, now time.Time) bool {
	return !issuedAt(expires).Add(renewAfter).After(now)
}
