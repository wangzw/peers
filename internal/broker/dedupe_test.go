package broker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

type dedupeFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *dedupeFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *dedupeFakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func deliveredResult() *deliverpb.DeliverResult {
	return &deliverpb.DeliverResult{Result: &deliverpb.DeliverResult_Delivered{Delivered: &deliverpb.Delivered{}}}
}

// TestDedupeFirstOwns checks that a second acquire gets the first one's entry
// without ownership.
func TestDedupeFirstOwns(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	k := dedupeKey{MessageID: "m1"}

	e1, owner1 := tbl.acquire(k)
	if !owner1 {
		t.Fatal("first acquire did not own the key")
	}
	e2, owner2 := tbl.acquire(k)
	if owner2 {
		t.Fatal("second acquire owned an entry already in the table")
	}
	if e1 != e2 {
		t.Fatal("second acquire returned a different entry")
	}
	if got := tbl.len(); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}
}

// TestDedupeConcurrentSingleOwner checks that exactly one of many concurrent
// acquires owns the key and the rest wait for its result.
func TestDedupeConcurrentSingleOwner(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	k := dedupeKey{MessageID: "m1"}

	const callers = 64
	type outcome struct {
		entry *dedupeEntry
		owner bool
	}
	start := make(chan struct{})
	results := make(chan outcome, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			e, owner := tbl.acquire(k)
			results <- outcome{e, owner}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	owners := 0
	var entry *dedupeEntry
	for out := range results {
		if out.owner {
			owners++
		}
		if entry == nil {
			entry = out.entry
		} else if out.entry != entry {
			t.Fatal("concurrent acquires returned different entries")
		}
	}
	if owners != 1 {
		t.Fatalf("%d callers owned the key, want exactly 1", owners)
	}

	res := deliveredResult()
	tbl.complete(entry, res, time.Time{})
	for i := 0; i < callers-1; i++ {
		got, ok := entry.wait(context.Background())
		if !ok {
			t.Fatal("wait returned ok == false for a completed entry")
		}
		if got != res {
			t.Fatal("wait returned a different result")
		}
	}
}

// TestDedupeExpiry checks that a result is kept for the window and one acquire
// after expiry removes every expired entry.
func TestDedupeExpiry(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	k1 := dedupeKey{MessageID: "m1"}
	k2 := dedupeKey{MessageID: "m2"}

	e1, _ := tbl.acquire(k1)
	tbl.complete(e1, deliveredResult(), time.Time{})
	e2, _ := tbl.acquire(k2)
	tbl.complete(e2, deliveredResult(), time.Time{})

	clock.Advance(time.Minute - time.Nanosecond)
	if got, owner := tbl.acquire(k1); owner || got != e1 {
		t.Fatal("entry within the window was replaced")
	}

	clock.Advance(time.Nanosecond)
	e3, owner := tbl.acquire(k1)
	if !owner {
		t.Fatal("expired entry was still owned by a previous caller")
	}
	if e3 == e1 {
		t.Fatal("acquire returned the expired entry")
	}
	if got := tbl.len(); got != 1 {
		t.Fatalf("len = %d after acquiring one of two expired keys, want 1: the other expired entry was not removed", got)
	}
}

// TestDedupeInProgressNeverExpires checks that an in-progress entry is never
// removed, so a duplicate waits for the write rather than starting another.
func TestDedupeInProgressNeverExpires(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	k := dedupeKey{MessageID: "m1"}

	e1, owner := tbl.acquire(k)
	if !owner {
		t.Fatal("first acquire did not own the key")
	}
	clock.Advance(10 * time.Minute)

	e2, owner := tbl.acquire(k)
	if owner {
		t.Fatal("in-progress entry was removed and claimed again")
	}
	if e2 != e1 {
		t.Fatal("in-progress entry was replaced")
	}
}

// TestDedupeWindowReadAtCompletion checks that expiry is computed from the window
// in effect when the result is recorded.
func TestDedupeWindowReadAtCompletion(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	window := time.Minute
	tbl := newDedupeTable(clock.Now, func() time.Duration { return window })

	e, _ := tbl.acquire(dedupeKey{MessageID: "m1"})
	window = 10 * time.Minute
	tbl.complete(e, deliveredResult(), time.Time{})

	clock.Advance(5 * time.Minute)
	if got, owner := tbl.acquire(dedupeKey{MessageID: "m1"}); owner || got != e {
		t.Fatal("entry expired according to the window from acquire time")
	}
	clock.Advance(5 * time.Minute)
	if got, owner := tbl.acquire(dedupeKey{MessageID: "m1"}); !owner || got == e {
		t.Fatal("entry outlived the window in effect at completion")
	}
}

// TestDedupeWaitPrefersResult checks that a recorded result wins over a context
// that ended, so a duplicate never becomes an unknown result.
func TestDedupeWaitPrefersResult(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	e, _ := tbl.acquire(dedupeKey{MessageID: "m1"})
	res := deliveredResult()
	tbl.complete(e, res, time.Time{})

	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, ok := e.wait(ctx)
		if !ok {
			t.Fatalf("iteration %d: wait reported ok == false for a completed entry", i)
		}
		if got != res {
			t.Fatalf("iteration %d: wait returned a different result", i)
		}
	}
}

// TestDedupeWaitContextEnds checks that a waiter returns when its own context
// ends before a result is recorded.
func TestDedupeWaitContextEnds(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	e, _ := tbl.acquire(dedupeKey{MessageID: "m1"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, ok := e.wait(ctx)
	if ok {
		t.Fatal("wait returned ok == true after its context was cancelled")
	}
	if got != nil {
		t.Fatal("wait returned a result after its context was cancelled")
	}
}

// TestDedupeSourcesAreDistinct checks that the same message id from two peers is
// two different entries.
func TestDedupeSourcesAreDistinct(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })

	var sourceA, sourceB pairing.Fingerprint
	sourceA[0] = 1
	sourceB[0] = 2
	eA, ownerA := tbl.acquire(dedupeKey{Source: sourceA, MessageID: "m1"})
	eB, ownerB := tbl.acquire(dedupeKey{Source: sourceB, MessageID: "m1"})
	if !ownerA || !ownerB {
		t.Fatal("one source's message was taken for the other's")
	}
	if eA == eB {
		t.Fatal("two sources shared one entry")
	}
	if got := tbl.len(); got != 2 {
		t.Fatalf("len = %d, want 2", got)
	}
}

func fillDedupe(tbl *dedupeTable, n int, expires time.Time) {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	for i := 0; i < n; i++ {
		var source pairing.Fingerprint
		source[0] = byte(i)
		source[1] = byte(i >> 8)
		source[2] = byte(i >> 16)
		source[3] = 0x9a
		tbl.putEntryLocked(dedupeKey{Source: source, MessageID: fmt.Sprintf("filler-%d", i)}, &dedupeEntry{
			done:    make(chan struct{}),
			expires: expires,
		})
	}
}

// TestDedupeFullTableRefusesNewKey checks that a table at maxDedupeEntries
// returns a not-sent busy result without writing the new key.
func TestDedupeFullTableRefusesNewKey(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	fillDedupe(tbl, maxDedupeEntries, clock.Now().Add(time.Hour))

	if e, owner := tbl.acquire(dedupeKey{MessageID: "filler-0"}); owner || e == nil {
		t.Fatal("a full table did not return the entry for an id it already holds")
	}

	k := dedupeKey{MessageID: "new"}
	e, owner := tbl.acquire(k)
	if owner {
		t.Fatal("acquire owned a key in a full table")
	}
	if got := tbl.len(); got != maxDedupeEntries {
		t.Fatalf("len = %d after refusing a key, want %d", got, maxDedupeEntries)
	}
	if _, ok := tbl.entries[k]; ok {
		t.Fatal("the refused key was written into the table")
	}
	res, ok := e.wait(context.Background())
	if !ok {
		t.Fatal("the refused key reported no result")
	}
	got := res.GetError()
	if got.GetCode() != localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE {
		t.Fatalf("busy result code = %v, want inbox_unwritable", got.GetCode())
	}
	if want := "Not sent: the Broker is temporarily holding too many messages"; got.GetMessage() != want {
		t.Fatalf("busy result message = %q, want %q", got.GetMessage(), want)
	}
}

// TestDedupeFullTableReclaimsExpiredEntries checks that a full table of expired
// entries sweeps them once and accepts a new message.
func TestDedupeFullTableReclaimsExpiredEntries(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	fillDedupe(tbl, maxDedupeEntries, clock.Now())

	e, owner := tbl.acquire(dedupeKey{MessageID: "new"})
	if !owner || e == nil {
		t.Fatal("a full table of expired entries did not accept a new message")
	}
	if got := tbl.len(); got != 1 {
		t.Fatalf("len = %d after reclaiming a full table, want 1", got)
	}
}

// TestDedupeFullTableDoesNotRescanUnlessExpiryNears checks that a full table of
// unexpired entries refuses without rescanning until the recorded earliest
// expiry.
func TestDedupeFullTableDoesNotRescanUnlessExpiryNears(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	fillDedupe(tbl, maxDedupeEntries, clock.Now().Add(time.Hour))

	k := dedupeKey{MessageID: "new"}
	if e, owner := tbl.acquire(k); owner || e == nil {
		t.Fatal("a full table of unexpired entries accepted a new message")
	}
	first := tbl.examined
	if first < maxDedupeEntries {
		t.Fatalf("first acquire examined %d entries, want at least %d: the full table was not swept", first, maxDedupeEntries)
	}

	if e, owner := tbl.acquire(k); owner || e == nil {
		t.Fatal("a second acquire accepted a message in a full table")
	}
	if delta := tbl.examined - first; delta > dedupeSweepPerAcquire {
		t.Fatalf("second acquire examined %d entries, want at most %d: the full table was swept again", delta, dedupeSweepPerAcquire)
	}
}

// TestDedupeFullTableRescansAtEarliestExpiry checks that the recorded expiry only
// delays the sweep, which then accepts a new message.
func TestDedupeFullTableRescansAtEarliestExpiry(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	fillDedupe(tbl, maxDedupeEntries, clock.Now().Add(time.Minute))

	if e, owner := tbl.acquire(dedupeKey{MessageID: "first"}); owner || e == nil {
		t.Fatal("a full table of unexpired entries accepted a new message")
	}

	clock.Advance(time.Minute)
	e, owner := tbl.acquire(dedupeKey{MessageID: "second"})
	if !owner || e == nil {
		t.Fatal("a full table did not accept a message after its earliest expiry")
	}
	if got := tbl.len(); got != 1 {
		t.Fatalf("len = %d after reclaiming the expired table, want 1", got)
	}
}

// TestDedupeCleanupIsAmortized checks that one acquire examines only a bounded
// number of entries for expiry.
func TestDedupeCleanupIsAmortized(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	const entries = 4 * dedupeSweepPerAcquire
	fillDedupe(tbl, entries, clock.Now().Add(-time.Second))

	e, owner := tbl.acquire(dedupeKey{MessageID: "new"})
	if !owner || e == nil {
		t.Fatal("acquire did not own a key with room in the table")
	}
	if got, want := tbl.len(), entries+1-dedupeSweepPerAcquire; got != want {
		t.Fatalf("len = %d after one acquire, want %d: the sweep examined more than %d entries", got, want, dedupeSweepPerAcquire)
	}
}

// TestDedupeExpiredKeyIsNeverReturned checks that the key being acquired is
// examined itself, so a bounded sweep cannot return an expired entry.
func TestDedupeExpiredKeyIsNeverReturned(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })
	const entries = 4 * dedupeSweepPerAcquire
	fillDedupe(tbl, entries, clock.Now().Add(-time.Second))
	k := dedupeKey{MessageID: "asked"}
	old := &dedupeEntry{done: make(chan struct{}), expires: clock.Now()}
	tbl.mu.Lock()
	tbl.putEntryLocked(k, old)
	tbl.mu.Unlock()

	e, owner := tbl.acquire(k)
	if !owner {
		t.Fatal("an expired entry was returned as an in-progress duplicate")
	}
	if e == old {
		t.Fatal("acquire returned the expired entry")
	}
}

// perSourceDedupeLimit is the per-source share, spelled separately from the
// implementation constant so the test does not track it.
const perSourceDedupeLimit = 4096

// TestDedupePerSourceQuota checks that a source at its share is refused with
// quota_exceeded while another source still acquires.
func TestDedupePerSourceQuota(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Hour })

	var sourceA, sourceB pairing.Fingerprint
	sourceA[0], sourceB[0] = 1, 2

	for i := 0; i < perSourceDedupeLimit; i++ {
		e, owner := tbl.acquire(dedupeKey{Source: sourceA, MessageID: fmt.Sprintf("a-%d", i)})
		if !owner {
			t.Fatalf("acquire a-%d did not own a key below the source quota", i)
		}
		tbl.complete(e, deliveredResult(), time.Time{})
	}

	over := dedupeKey{Source: sourceA, MessageID: "a-over"}
	e, owner := tbl.acquire(over)
	if owner {
		t.Fatal("acquire owned a new id for a source at its quota")
	}
	res, ok := e.wait(context.Background())
	if !ok {
		t.Fatal("the quota refusal reported no result")
	}
	got := res.GetError()
	if got.GetCode() != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Fatalf("quota refusal code = %v, want quota_exceeded", got.GetCode())
	}
	if !strings.Contains(got.GetMessage(), "too many recent messages") {
		t.Fatalf("quota refusal message = %q, want it to name the cause", got.GetMessage())
	}
	if _, ok := tbl.entries[over]; ok {
		t.Fatal("the refused id was written into the table")
	}
	if got := tbl.len(); got != perSourceDedupeLimit {
		t.Fatalf("len = %d after the quota refusal, want %d", got, perSourceDedupeLimit)
	}

	eB, ownerB := tbl.acquire(dedupeKey{Source: sourceB, MessageID: "b-1"})
	if !ownerB || eB == nil {
		t.Fatal("a source at its quota refused another source's message")
	}
	if got := tbl.len(); got != perSourceDedupeLimit+1 {
		t.Fatalf("len = %d after another source acquired, want %d", got, perSourceDedupeLimit+1)
	}
}

// TestDedupePerSourceQuotaReleasesExpired checks that a source's expired entries
// free its quota.
func TestDedupePerSourceQuotaReleasesExpired(t *testing.T) {
	clock := &dedupeFakeClock{now: time.Unix(1000, 0)}
	tbl := newDedupeTable(clock.Now, func() time.Duration { return time.Minute })

	var source pairing.Fingerprint
	source[0] = 1
	for i := 0; i < perSourceDedupeLimit; i++ {
		e, _ := tbl.acquire(dedupeKey{Source: source, MessageID: fmt.Sprintf("a-%d", i)})
		tbl.complete(e, deliveredResult(), time.Time{})
	}

	if _, owner := tbl.acquire(dedupeKey{Source: source, MessageID: "blocked"}); owner {
		t.Fatal("acquire owned a new id for a source at its quota")
	}

	clock.Advance(time.Minute)
	if _, owner := tbl.acquire(dedupeKey{Source: source, MessageID: "after-expiry"}); !owner {
		t.Fatal("acquire refused a source whose entries had all expired")
	}
}
