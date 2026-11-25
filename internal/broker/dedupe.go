package broker

import (
	"context"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

const maxDedupeEntries = 65536

// maxDedupeEntriesPerSource is a variable only so tests can lower it; the value
// is fixed.
var maxDedupeEntriesPerSource = 4096

// dedupeSweepPerAcquire bounds how many entries one acquire examines for expiry.
const dedupeSweepPerAcquire = 8

type dedupeKey struct {
	Source    pairing.Fingerprint
	MessageID string
}

// dedupeEntry holds one message; result is readable only after done is closed,
// and expires is zero while the entry is being processed.
type dedupeEntry struct {
	done    chan struct{}
	result  *deliverpb.DeliverResult
	expires time.Time
}

// dedupeTable maps a dedupeKey to its entry. A single mutex makes finding and
// claiming an entry one atomic step, so a concurrent duplicate waits for the
// first copy instead of running it twice.
type dedupeTable struct {
	mu      sync.Mutex
	entries map[dedupeKey]*dedupeEntry
	// sourceCounts holds the number of live entries per source and must stay in
	// step with entries; putEntryLocked and deleteEntryLocked maintain it.
	sourceCounts map[pairing.Fingerprint]int
	now          func() time.Time
	window       func() time.Duration

	// nextExpiry is the earliest moment a held entry can expire, recorded by the
	// last full sweep. When nextExpirySet is true, no entry expires before it (a
	// zero value means none can expire yet), so a full table or source refuses
	// without scanning; when false, that check sweeps first.
	nextExpiry    time.Time
	nextExpirySet bool

	// examined counts the entries swept for expiry; tests read it.
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

// lookup returns the live entry for k without claiming it; an expired entry is
// removed.
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

// acquire returns the entry for k and whether the caller owns its processing; a
// refused acquire returns a completed entry without storing it. Expiry cleanup
// is spread over acquires, and the key being acquired is always examined.
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

	// The table-wide bound is also a valid bound for this source, so the quota
	// check needs no scan.
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

// putEntryLocked writes e for k; t.mu must be held.
func (t *dedupeTable) putEntryLocked(k dedupeKey, e *dedupeEntry) {
	t.entries[k] = e
	t.sourceCounts[k.Source]++
}

// deleteEntryLocked removes k if held; t.mu must be held.
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

// expiryBoundHoldsLocked reports whether the recorded bound proves no held entry
// has expired; t.mu must be held.
func (t *dedupeTable) expiryBoundHoldsLocked(now time.Time) bool {
	if !t.nextExpirySet {
		return false
	}
	return t.nextExpiry.IsZero() || now.Before(t.nextExpiry)
}

// sweepAllExpiredLocked removes every expired entry and records the earliest
// remaining expiry; t.mu must be held.
func (t *dedupeTable) sweepAllExpiredLocked(now time.Time) {
	t.nextExpiry = t.sweepExpiredLocked(now, len(t.entries))
	t.nextExpirySet = true
}

// sweepExpiredLocked removes expired entries among at most examined entries and
// returns the earliest remaining expiry; t.mu must be held.
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

// fullDedupeEntry is the completed, never-stored entry a full table returns to
// refuse a message.
func fullDedupeEntry() *dedupeEntry {
	done := make(chan struct{})
	close(done)
	return &dedupeEntry{
		done: done,
		result: errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE,
			"the Broker is temporarily holding too many messages")),
	}
}

// quotaDedupeEntry is the completed, never-stored entry a source at its quota
// returns.
func quotaDedupeEntry() *dedupeEntry {
	done := make(chan struct{})
	close(done)
	return &dedupeEntry{
		done: done,
		result: errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
			"this Broker holds too many recent messages from your Broker")),
	}
}

// complete records r, sets the entry's expiry to the window or keepUntil,
// whichever is later, and closes done; the processing owner calls it once.
func (t *dedupeTable) complete(e *dedupeEntry, r *deliverpb.DeliverResult, keepUntil time.Time) {
	t.mu.Lock()
	e.result = r
	e.expires = t.now().Add(t.window())
	if keepUntil.After(e.expires) {
		e.expires = keepUntil
	}
	// A completion can add an expiry earlier than the recorded bound, as when a
	// different window applied to this entry; keep the bound a true minimum.
	if t.nextExpirySet && (t.nextExpiry.IsZero() || e.expires.Before(t.nextExpiry)) {
		t.nextExpiry = e.expires
	}
	t.mu.Unlock()
	close(e.done)
}

// wait blocks until e completes or ctx ends; a recorded result wins over a
// context that ended.
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

func (t *dedupeTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}
