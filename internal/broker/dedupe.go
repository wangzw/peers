package broker

import (
	"context"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

// maxDedupeEntries bounds the table. A full table refuses a new message with a
// busy result instead of growing without bound.
const maxDedupeEntries = 65536

// maxDedupeEntriesPerSource bounds how many entries one source may hold. A
// source at its quota has new message ids refused with quota_exceeded, so one
// peer cannot fill the table and make every other peer's deliveries fail. It is
// a variable only so tests can lower it; the value is fixed.
var maxDedupeEntriesPerSource = 4096

// dedupeSweepPerAcquire bounds how many entries one acquire examines for
// expiry, spreading the cleanup over many acquires instead of sweeping the
// whole table on every message.
const dedupeSweepPerAcquire = 8

// dedupeKey identifies one delivered message. Source is the fingerprint of the
// Broker the message came from; the zero fingerprint means this Broker, which
// this version does not use because a local send never resends a message id.
type dedupeKey struct {
	Source    pairing.Fingerprint
	MessageID string
}

// dedupeEntry is one message in the dedupe table. done is closed when the
// processing owner records the result; result is readable only after done is
// closed. expires is zero while the entry is being processed.
type dedupeEntry struct {
	done    chan struct{}
	result  *deliverpb.DeliverResult
	expires time.Time
}

// dedupeTable maps a dedupeKey to its entry. A single mutex makes finding an
// entry and claiming it one atomic step, so a concurrent duplicate waits for
// the first copy instead of running it a second time.
type dedupeTable struct {
	mu      sync.Mutex
	entries map[dedupeKey]*dedupeEntry
	// sourceCounts holds the number of live entries per source, so a source's
	// share is checked without scanning the table. It is kept in step with
	// entries through putEntryLocked and deleteEntryLocked.
	sourceCounts map[pairing.Fingerprint]int
	now          func() time.Time
	window       func() time.Duration

	// nextExpiry is the earliest moment a held entry can expire, as recorded
	// by the last full sweep and kept a true minimum by complete. When
	// nextExpirySet is true, no entry expires before nextExpiry (a zero
	// nextExpiry means no entry can expire yet), so a full table or a source
	// at its quota refuses without scanning. When it is false, that check
	// sweeps before deciding.
	nextExpiry    time.Time
	nextExpirySet bool

	// examined counts the entries swept for expiry. A test reads it to prove
	// that a full table does not rescan on every acquire.
	examined int
}

func newDedupeTable(now func() time.Time, window func() time.Duration) *dedupeTable {
	return &dedupeTable{
		entries:      make(map[dedupeKey]*dedupeEntry),
		sourceCounts: make(map[pairing.Fingerprint]int),
		now:          now,
		window:       window,
	}
}

// lookup returns the live entry held for k without claiming anything, so a
// caller can answer a resend before running checks that only a first copy
// needs. An expired entry is removed. ok is false when no result exists yet.
func (t *dedupeTable) lookup(k dedupeKey) (e *dedupeEntry, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, held := t.entries[k]
	if !held {
		return nil, false
	}
	if entry.expires.IsZero() || entry.expires.After(t.now()) {
		return entry, true
	}
	t.deleteEntryLocked(k)
	return nil, false
}

// acquire returns the entry for k and whether the caller owns its processing.
// It returns a completed entry carrying a busy result, without writing it, when
// the table is full. A source at its own quota gets a quota_exceeded entry,
// also without writing anything. Expiry cleanup is spread over acquires: each
// one examines a bounded number of entries, and the key being acquired is
// always examined itself. A table or a source full of expired results still
// accepts new messages: the first acquire that finds it full sweeps the table
// once, and while every entry is unexpired the sweep records the earliest
// expiry and the table refuses without scanning until that moment. An entry
// that is still being processed has no expiry and is never removed.
func (t *dedupeTable) acquire(k dedupeKey) (e *dedupeEntry, owner bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	if entry, ok := t.entries[k]; ok {
		if entry.expires.IsZero() || entry.expires.After(now) {
			return entry, false
		}
		t.deleteEntryLocked(k)
	}

	// The expiry bound is a lower bound over the whole table, so it is also a
	// valid lower bound for this source: holding it means no entry of this
	// source can have expired, and the quota check needs no scan.
	if t.sourceCounts[k.Source] >= maxDedupeEntriesPerSource && !t.expiryBoundHoldsLocked(now) {
		t.sweepAllExpiredLocked(now)
	}
	if t.sourceCounts[k.Source] >= maxDedupeEntriesPerSource {
		return quotaDedupeEntry(), false
	}

	if len(t.entries) >= maxDedupeEntries && !t.expiryBoundHoldsLocked(now) {
		t.sweepAllExpiredLocked(now)
	}
	if len(t.entries) >= maxDedupeEntries {
		return fullDedupeEntry(), false
	}
	t.sweepExpiredLocked(now, dedupeSweepPerAcquire)
	entry := &dedupeEntry{done: make(chan struct{})}
	t.putEntryLocked(k, entry)
	return entry, true
}

// putEntryLocked writes e for k and keeps the source count in step. It must be
// called with t.mu held.
func (t *dedupeTable) putEntryLocked(k dedupeKey, e *dedupeEntry) {
	t.entries[k] = e
	t.sourceCounts[k.Source]++
}

// deleteEntryLocked removes k when it is held and keeps the source count in
// step. It must be called with t.mu held.
func (t *dedupeTable) deleteEntryLocked(k dedupeKey) {
	if _, ok := t.entries[k]; !ok {
		return
	}
	delete(t.entries, k)
	if n := t.sourceCounts[k.Source]; n > 1 {
		t.sourceCounts[k.Source] = n - 1
	} else {
		delete(t.sourceCounts, k.Source)
	}
}

// expiryBoundHoldsLocked reports whether the recorded bound proves no held
// entry has expired: the last full sweep recorded the earliest expiry and that
// moment has not arrived. It applies to the whole table, so it also covers one
// source's share. It must be called with t.mu held.
func (t *dedupeTable) expiryBoundHoldsLocked(now time.Time) bool {
	if !t.nextExpirySet {
		return false
	}
	return t.nextExpiry.IsZero() || now.Before(t.nextExpiry)
}

// sweepAllExpiredLocked removes every expired entry and records the earliest
// remaining expiry, so later full-table or full-source acquires refuse without
// scanning until that moment. It must be called with t.mu held.
func (t *dedupeTable) sweepAllExpiredLocked(now time.Time) {
	t.nextExpiry = t.sweepExpiredLocked(now, len(t.entries))
	t.nextExpirySet = true
}

// sweepExpiredLocked removes expired entries among at most examined entries, so
// one acquire does not pay for the whole table, and returns the earliest expiry
// among the entries it left behind. It must be called with t.mu held.
func (t *dedupeTable) sweepExpiredLocked(now time.Time, examined int) (earliest time.Time) {
	seen := 0
	for key, entry := range t.entries {
		if seen == examined {
			break
		}
		seen++
		t.examined++
		if !entry.expires.IsZero() && !entry.expires.After(now) {
			t.deleteEntryLocked(key)
			continue
		}
		if !entry.expires.IsZero() && (earliest.IsZero() || entry.expires.Before(earliest)) {
			earliest = entry.expires
		}
	}
	return earliest
}

// fullDedupeEntry is the completed entry returned for a message a full table
// refused. It is never stored, so the message is not counted and no write
// follows; the caller returns its busy result to the sender.
func fullDedupeEntry() *dedupeEntry {
	done := make(chan struct{})
	close(done)
	return &dedupeEntry{
		done: done,
		result: errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE,
			"the Broker is temporarily holding too many messages")),
	}
}

// quotaDedupeEntry is the completed entry returned for a message refused
// because its source already holds its share of the table. It is never stored,
// so the message is not counted and no write follows; the caller returns the
// quota result to the sender.
func quotaDedupeEntry() *dedupeEntry {
	done := make(chan struct{})
	close(done)
	return &dedupeEntry{
		done: done,
		result: errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
			"this Broker holds too many recent messages from your Broker")),
	}
}

// complete records r, sets the entry's expiry, and closes done. The entry is
// kept until keepUntil even when the window ends first, so a resend inside the
// sender's budget still finds the result; the zero time adds no floor. The
// processing owner calls it exactly once.
func (t *dedupeTable) complete(e *dedupeEntry, r *deliverpb.DeliverResult, keepUntil time.Time) {
	t.mu.Lock()
	e.result = r
	e.expires = t.now().Add(t.window())
	if keepUntil.After(e.expires) {
		e.expires = keepUntil
	}
	// A completion can add an expiry earlier than the recorded bound, as when
	// a different window applied to this entry. Keep the bound a true minimum
	// so the no-scan refusal stays correct.
	if t.nextExpirySet && (t.nextExpiry.IsZero() || e.expires.Before(t.nextExpiry)) {
		t.nextExpiry = e.expires
	}
	t.mu.Unlock()
	close(e.done)
}

// wait blocks until e completes or ctx ends; ok is false when ctx ended first.
// A result already recorded wins over a context that ended: the non-blocking
// check before the select makes that ordering deterministic, so a duplicate
// does not turn into an unknown result just because its context also ended.
func (e *dedupeEntry) wait(ctx context.Context) (r *deliverpb.DeliverResult, ok bool) {
	select {
	case <-e.done:
		return e.result, true
	default:
	}
	select {
	case <-e.done:
		return e.result, true
	case <-ctx.Done():
		return nil, false
	}
}

// len returns the number of entries currently held, including those being
// processed.
func (t *dedupeTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}
