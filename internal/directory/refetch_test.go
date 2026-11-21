package directory

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

// refetchClock returns the fake clock that drives n's directory.
func refetchClock(t *testing.T, n *reconcileNode) *fakeClock {
	t.Helper()
	clk, ok := n.d.clk.(*fakeClock)
	if !ok {
		t.Fatalf("clock type = %T, want *fakeClock", n.d.clk)
	}
	return clk
}

// refetchRequestsFor counts the RecordRequest frames ep sent to to that name id.
func refetchRequestsFor(t *testing.T, ep *fakeLinks, to Fingerprint, id recordID) int {
	t.Helper()
	n := 0
	for _, f := range ep.Sent() {
		if f.to != to || f.kind != directorypb.KindRecordRequest {
			continue
		}
		if reconcileRequestHasID(t, f.payload, id) {
			n++
		}
	}
	return n
}

// refetchHeldVersion returns the version n holds for id, or 0 when absent.
func refetchHeldVersion(n *reconcileNode, id recordID) uint64 {
	r, ok := reconcileLookup(n, id)
	if !ok {
		return 0
	}
	return r.version
}

// TestRefetchDeferredReplacementAfterWindow checks that a replacement dropped
// by the replacement interval is fetched once and accepted when the interval
// expires, without waiting for another reconciliation pass.
func TestRefetchDeferredReplacementAfterWindow(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	v1 := reconcileCard(t, 200, 1, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})

	clk.Advance(time.Second)
	v2 := reconcileCard(t, 200, 2, "hostX")
	reconcileInject(t, p, v2)
	clearReconcileSent(b.ep)
	b.d.acceptBatch(p.fp, []*directorypb.Record{v2.wire()})

	if got := refetchHeldVersion(b, v1.id); got != 1 {
		t.Fatalf("held version = %d, want 1 while the window is open", got)
	}
	if got := reconcileFrames(b.ep, p.fp, directorypb.KindRecordRequest); got != 0 {
		t.Fatalf("B requested %d replacements inside the window, want 0", got)
	}

	// The deferral arms one timer for the end of the window.
	clk.WaitTimers(t, 1)
	clk.Advance(directoryReplaceInterval() - time.Second)

	reconcileAwait(t, net, "B to refetch the deferred replacement at the window's end", func() bool {
		return refetchHeldVersion(b, v1.id) == 2
	})
	if got := refetchRequestsFor(t, b.ep, p.fp, v1.id); got != 1 {
		t.Fatalf("B refetched the identifier %d times, want exactly 1", got)
	}
}

// TestRefetchAtMostOncePerWindow checks that any number of newer versions
// pushed inside the window still produce exactly one refetch when it expires,
// and that the refetch takes the highest of them.
func TestRefetchAtMostOncePerWindow(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	v1 := reconcileCard(t, 200, 1, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})

	clk.Advance(time.Second)
	for version := uint64(2); version <= 4; version++ {
		r := reconcileCard(t, 200, version, "hostX")
		reconcileInject(t, p, r)
		b.d.acceptBatch(p.fp, []*directorypb.Record{r.wire()})
	}
	if got := refetchHeldVersion(b, v1.id); got != 1 {
		t.Fatalf("held version = %d, want 1 while the window is open", got)
	}

	clearReconcileSent(b.ep)
	clk.WaitTimers(t, 1)
	clk.Advance(directoryReplaceInterval() - time.Second)

	reconcileAwait(t, net, "B to refetch the highest deferred version", func() bool {
		return refetchHeldVersion(b, v1.id) == 4
	})
	if got := refetchRequestsFor(t, b.ep, p.fp, v1.id); got != 1 {
		t.Fatalf("B refetched the identifier %d times, want exactly 1", got)
	}
}

// TestRefetchSetCappedAtMaxRecords checks that the deferred set holds at most
// directory.max_records identifiers: the entries beyond the cap are not
// recorded and wait for the next reconciliation pass.
func TestRefetchSetCappedAtMaxRecords(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	const held = 3
	b.set.SetMaxRecords(2)
	clk := refetchClock(t, b)
	now := b.d.now()
	ids := make([]recordID, 0, held)
	for i := 0; i < held; i++ {
		r := reconcileCard(t, byte(200+i), 1, fmt.Sprintf("host%d", i))
		ids = append(ids, r.id)
		// A locally put record bypasses the count limit, so the store can hold
		// more records than directory.max_records and the deferred set can run
		// into its cap.
		b.d.mu.Lock()
		b.d.st.putOwn(r)
		r.acceptedAt = now
		b.d.mu.Unlock()
	}

	clk.Advance(time.Second)
	for i := 0; i < held; i++ {
		r := reconcileCard(t, byte(200+i), 2, fmt.Sprintf("host%d", i))
		reconcileInject(t, p, r)
		b.d.acceptBatch(p.fp, []*directorypb.Record{r.wire()})
	}

	b.d.mu.RLock()
	deferred := 0
	if pr := b.d.recon[p.fp]; pr != nil {
		deferred = len(pr.refetch)
	}
	b.d.mu.RUnlock()
	if deferred != 2 {
		t.Fatalf("deferred set holds %d identifiers, want at most directory.max_records (2)", deferred)
	}

	clearReconcileSent(b.ep)
	clk.WaitTimers(t, 1)
	clk.Advance(directoryReplaceInterval() - time.Second)

	reconcileAwait(t, net, "the two recorded identifiers to be refetched", func() bool {
		for _, id := range ids[:2] {
			if refetchHeldVersion(b, id) != 2 {
				return false
			}
		}
		return true
	})
	if got := refetchHeldVersion(b, ids[2]); got != 1 {
		t.Fatalf("held version of the identifier beyond the cap = %d, want 1", got)
	}
	if got := refetchRequestsFor(t, b.ep, p.fp, ids[2]); got != 0 {
		t.Fatalf("B refetched the identifier beyond the cap %d times, want 0", got)
	}
}

// TestRefetchNotLostWhenSetEmpties checks the hand-off between a refetch loop
// that finds the deferred set empty and a noteRefetch that adds an identifier
// at that same instant. The loop must clear refetchRunning in the same critical
// section as its emptiness check, so the late identifier starts a new waiter
// instead of sending a wake nobody reads.
func TestRefetchNotLostWhenSetEmpties(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	earlier := reconcileCard(t, 210, 1, "earlier")
	later := reconcileCard(t, 211, 1, "later")

	// Arm the loop with one deferred identifier; it parks on that deadline.
	b.d.noteRefetch(p.fp, map[recordID]time.Time{earlier.id: b.d.now().Add(10 * time.Minute)})
	clk.WaitTimers(t, 1)

	var once sync.Once
	hookDone := make(chan struct{})
	b.d.refetchIdle = func() {
		once.Do(func() {
			// Add an identifier at the moment the loop found the set empty.
			b.d.noteRefetch(p.fp, map[recordID]time.Time{later.id: b.d.now().Add(time.Minute)})
			close(hookDone)
		})
	}

	// Waking the loop with the set empty runs the hook at the hand-off point.
	b.d.mu.Lock()
	pr := b.d.recon[p.fp]
	clearRefetchLocked(pr, earlier.id)
	b.d.mu.Unlock()

	select {
	case <-hookDone:
	case <-time.After(3 * time.Second):
		t.Fatal("the refetch loop did not reach the empty-set hand-off")
	}
	// The late identifier must have a waiter and therefore a timer.
	clk.WaitTimers(t, 1)
	clk.Advance(time.Minute)

	reconcileAwait(t, net, "the identifier added as the set emptied to be fetched", func() bool {
		return refetchRequestsFor(t, b.ep, p.fp, later.id) >= 1
	})
	if got := refetchRequestsFor(t, b.ep, p.fp, earlier.id); got != 0 {
		t.Fatalf("the cleared identifier was fetched %d times, want 0", got)
	}
}

// TestRefetchClearedOnLinkDown checks that a link going down discards the
// peer's deferred set, so an old deadline cannot fetch through a later link.
func TestRefetchClearedOnLinkDown(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	v1 := reconcileCard(t, 200, 1, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})
	clk.Advance(time.Second)
	v2 := reconcileCard(t, 200, 2, "hostX")
	reconcileInject(t, p, v2)
	b.d.acceptBatch(p.fp, []*directorypb.Record{v2.wire()})
	clk.WaitTimers(t, 1)

	net.Disconnect(p.fp, b.fp)

	b.d.mu.RLock()
	pr := b.d.recon[p.fp]
	b.d.mu.RUnlock()
	if pr != nil {
		t.Fatal("B kept the peer's reconcile state after the link went down")
	}

	clk.Advance(directoryReplaceInterval())
	net.Settle(t)
	if got := refetchHeldVersion(b, v1.id); got != 1 {
		t.Fatalf("held version = %d after the link went down, want 1", got)
	}
}

// TestPurgedRecordStillHoldsReplacementWindow checks that a record which
// expired before its replacement interval ended is not replaced at once. The
// newer version is held back and fetched when the interval ends.
func TestPurgedRecordStillHoldsReplacementWindow(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	v1 := mustSignRecord(t, 200, recKindCard, "", 1, b.d.now().Add(2*time.Second), recTestCard("hostX"))
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})

	// The record expires two seconds later and the maintenance pass purges it.
	clk.Advance(2 * time.Second)
	b.d.maintain(b.d.now(), b.d.now())
	if _, ok := reconcileLookup(b, v1.id); ok {
		t.Fatal("the expired record was not purged")
	}

	// Six seconds after acceptance, the successor is still a replacement.
	clk.Advance(4 * time.Second)
	v2 := mustSignRecord(t, 200, recKindCard, "", 2, b.d.now().Add(time.Hour), recTestCard("hostX"))
	reconcileInject(t, p, v2)
	b.d.acceptBatch(p.fp, []*directorypb.Record{v2.wire()})
	if r, ok := reconcileLookup(b, v1.id); ok {
		t.Fatalf("held version = %d six seconds after acceptance, want none", r.version)
	}

	// The deferred fetch takes the successor once the interval ends.
	clk.WaitTimers(t, 1)
	clk.Advance(directoryReplaceInterval() - 6*time.Second)
	reconcileAwait(t, net, "the deferred successor of the purged record", func() bool {
		return refetchHeldVersion(b, v1.id) == 2
	})
}

// TestRefetchWaiterLeavesOnLinkDown checks that the link-down notification
// wakes the peer's deferred-refetch waiter, so it leaves at once rather than
// holding its slot until the deadline. A peer therefore never has an old waiter
// and a new one after reconnecting.
func TestRefetchWaiterLeavesOnLinkDown(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	v1 := reconcileCard(t, 200, 1, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})
	clk.Advance(time.Second)
	v2 := reconcileCard(t, 200, 2, "hostX")
	reconcileInject(t, p, v2)
	b.d.acceptBatch(p.fp, []*directorypb.Record{v2.wire()})
	clk.WaitTimers(t, 1)

	net.Disconnect(p.fp, b.fp)

	// The waiter must leave without the clock being advanced to its deadline.
	done := make(chan struct{})
	go func() {
		b.d.refetching.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the deferred-refetch waiter stayed after the link went down")
	}
}

// TestRefetchClearedOnUnpair checks that unpairing a peer discards its deferred
// set, just as it discards the rest of the link's reconcile state.
func TestRefetchClearedOnUnpair(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	v1 := reconcileCard(t, 200, 1, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})
	clk.Advance(time.Second)
	v2 := reconcileCard(t, 200, 2, "hostX")
	reconcileInject(t, p, v2)
	b.d.acceptBatch(p.fp, []*directorypb.Record{v2.wire()})
	clk.WaitTimers(t, 1)

	// Unpair removes the peer, and pairing drops the link, which delivers the
	// link-down notification the directory sees.
	peers, ok := b.d.peers.(*fakePeers)
	if !ok {
		t.Fatalf("peer table type = %T, want *fakePeers", b.d.peers)
	}
	peers.Remove(p.fp)
	net.Disconnect(p.fp, b.fp)

	b.d.mu.RLock()
	pr := b.d.recon[p.fp]
	b.d.mu.RUnlock()
	if pr != nil {
		t.Fatal("B kept the unpaired peer's reconcile state")
	}
	if b.d.peers.IsPeer(p.fp) {
		t.Fatal("the peer is still in the peer table after unpair")
	}

	clk.Advance(directoryReplaceInterval())
	net.Settle(t)
	if got := refetchHeldVersion(b, v1.id); got != 1 {
		t.Fatalf("held version = %d after unpair, want 1", got)
	}
}
