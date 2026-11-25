package directory

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

const (
	recordsPerRequest = 256
	requestedTTL      = 2 * requestTimeout
)

// peerReconcile is the requester-side reconcile state of one peer. Its zero
// value is not usable; newPeerReconcile returns an initialized one. Every field
// is guarded by Directory.mu.
type peerReconcile struct {
	pending   map[recordID]struct{}
	inflight  map[recordID]struct{}
	requested map[recordID]requestedEntry
	// windows holds the request windows in creation order, each with a deadline
	// no earlier than the one before it. expireWindows examines only the front
	// of the slice, so a window must be appended with a non-decreasing deadline.
	windows        []*requestWindow
	running        bool
	refetch        map[recordID]time.Time
	refetchRunning bool
	refetchWake    chan struct{}
}

type requestedEntry struct {
	deadline time.Time
	window   *requestWindow
}

type requestWindow struct {
	deadline    time.Time
	ids         []recordID
	outstanding int
}

func newPeerReconcile() *peerReconcile {
	return &peerReconcile{
		pending:     make(map[recordID]struct{}),
		inflight:    make(map[recordID]struct{}),
		requested:   make(map[recordID]requestedEntry),
		refetch:     make(map[recordID]time.Time),
		refetchWake: make(chan struct{}, 1),
	}
}

// The caller must not hold d.mu.
func (d *Directory) scheduleFetch(peer Fingerprint, ids []recordID) {
	limit := d.settings.MaxRecords()

	d.mu.Lock()
	if d.closed || !d.up[peer] {
		d.mu.Unlock()
		return
	}
	pr := d.reconcileForLocked(peer)
	for _, id := range ids {
		if !enqueuePendingLocked(pr, id, limit) {
			if len(pr.pending) >= limit {
				break
			}
			continue
		}
		clearRefetchLocked(pr, id)
	}
	start := d.startFetchLocked(pr)
	d.mu.Unlock()

	if start {
		go d.fetchLoop(peer, pr)
	}
}

// startFetchLocked reports whether a fetch loop should be started and reserves
// the inflight slot when it should. Adding the slot under the write lock keeps
// Close, which takes the write lock before waiting on the group, from missing
// the goroutine. The caller must hold d.mu.
func (d *Directory) startFetchLocked(pr *peerReconcile) bool {
	if pr.running || len(pr.pending) == 0 || len(pr.requested) >= d.settings.MaxRecords() {
		return false
	}
	pr.running = true
	d.inflight.Add(1)
	return true
}

// The caller must hold d.mu.
func (d *Directory) reconcileForLocked(peer Fingerprint) *peerReconcile {
	pr := d.recon[peer]
	if pr == nil {
		pr = newPeerReconcile()
		d.recon[peer] = pr
	}
	return pr
}

// enqueuePendingLocked adds id to pr's pending set unless it is already in
// flight or pending or the queue is full, and reports whether it was added. The
// caller must hold d.mu.
func enqueuePendingLocked(pr *peerReconcile, id recordID, limit int) bool {
	if _, ok := pr.inflight[id]; ok {
		return false
	}
	if _, ok := pr.pending[id]; ok {
		return false
	}
	if len(pr.pending) >= limit {
		return false
	}
	pr.pending[id] = struct{}{}
	return true
}

// noteRefetch records identifiers whose replacement the interval held back so
// each is fetched once when its window ends. The caller must not hold d.mu.
func (d *Directory) noteRefetch(peer Fingerprint, deadlines map[recordID]time.Time) {
	if len(deadlines) == 0 {
		return
	}
	// Read the cap before taking the lock so a config read never nests inside
	// it.
	limit := d.settings.MaxRecords()

	d.mu.Lock()
	if d.closed || !d.up[peer] {
		d.mu.Unlock()
		return
	}
	pr := d.reconcileForLocked(peer)
	start := false
	for id, deadline := range deadlines {
		if d.noteRefetchLocked(pr, id, deadline, limit) {
			start = true
		}
	}
	d.mu.Unlock()

	if start {
		go d.refetchLoop(peer, pr)
	}
}

// noteRefetchLocked adds one deferred identifier and reports whether a refetch
// loop must be started. The caller must hold d.mu.
func (d *Directory) noteRefetchLocked(pr *peerReconcile, id recordID, deadline time.Time, limit int) bool {
	if current, ok := pr.refetch[id]; ok {
		if !deadline.Before(current) {
			return false
		}
	} else if len(pr.refetch) >= limit {
		return false
	}
	pr.refetch[id] = deadline
	if !pr.refetchRunning {
		pr.refetchRunning = true
		d.refetching.Add(1)
		return true
	}
	select {
	case pr.refetchWake <- struct{}{}:
	default:
	}
	return false
}

// clearRefetchLocked drops a deferred identifier and wakes the waiting goroutine
// so it re-reads the earliest deadline. The caller must hold d.mu.
func clearRefetchLocked(pr *peerReconcile, id recordID) {
	if _, ok := pr.refetch[id]; !ok {
		return
	}
	delete(pr.refetch, id)
	if pr.refetchRunning {
		select {
		case pr.refetchWake <- struct{}{}:
		default:
		}
	}
}

func earliestRefetch(pr *peerReconcile) (time.Time, bool) {
	var earliest time.Time
	found := false
	for _, deadline := range pr.refetch {
		if !found || deadline.Before(earliest) {
			earliest = deadline
			found = true
		}
	}
	return earliest, found
}

// refetchLoop waits for the earliest deferred deadline and queues every
// identifier whose window has ended for one fetch. It counts as one
// deferred-refetch waiter for its whole life, so Close waits for it and a peer
// has at most one such loop.
func (d *Directory) refetchLoop(peer Fingerprint, pr *peerReconcile) {
	defer d.refetching.Done()

	for {
		d.mu.Lock()
		if d.closed || d.recon[peer] != pr {
			pr.refetchRunning = false
			d.mu.Unlock()
			return
		}
		deadline, ok := earliestRefetch(pr)
		if !ok {
			// Clear the flag in the same critical section as the emptiness
			// check: a noteRefetch after this unlock must see refetchRunning
			// false and start its own loop, or its wake is lost.
			pr.refetchRunning = false
		}
		d.mu.Unlock()
		if !ok {
			if d.refetchIdle != nil {
				d.refetchIdle()
			}
			return
		}

		// Serve a deadline that already passed without arming a zero timer.
		if wait := deadline.Sub(d.now()); wait > 0 {
			tm := d.clk.NewTimer(wait)
			select {
			case <-tm.C():
			case <-pr.refetchWake:
				tm.Stop()
			case <-d.ctx.Done():
				tm.Stop()
				d.mu.Lock()
				if d.recon[peer] == pr {
					pr.refetchRunning = false
				}
				d.mu.Unlock()
				return
			}
		}

		// Read the cap outside the lock so a config read never nests inside it.
		limit := d.settings.MaxRecords()
		d.mu.Lock()
		if d.closed || d.recon[peer] != pr {
			pr.refetchRunning = false
			d.mu.Unlock()
			return
		}
		now := d.now()
		var due []recordID
		for id, at := range pr.refetch {
			if !now.Before(at) {
				delete(pr.refetch, id)
				due = append(due, id)
			}
		}
		start := false
		if len(due) > 0 {
			for _, id := range due {
				enqueuePendingLocked(pr, id, limit)
			}
			start = d.startFetchLocked(pr)
		}
		d.mu.Unlock()
		if start {
			go d.fetchLoop(peer, pr)
		}
	}
}

func (d *Directory) fetchLoop(peer Fingerprint, pr *peerReconcile) {
	defer d.inflight.Done()

	for {
		d.mu.Lock()
		if d.closed || d.recon[peer] != pr || len(pr.pending) == 0 {
			if d.recon[peer] == pr {
				compactWindows(pr)
			}
			pr.running = false
			d.mu.Unlock()
			return
		}
		batch := d.takeBatch(pr)
		if len(batch) == 0 {
			// The accept window is full; leaving lets a later acceptance or a
			// window expiry start the loop again.
			if d.recon[peer] == pr {
				compactWindows(pr)
			}
			pr.running = false
			d.mu.Unlock()
			return
		}
		d.mu.Unlock()

		d.fetchOnce(peer, batch)

		d.mu.Lock()
		if d.recon[peer] == pr {
			for _, id := range batch {
				delete(pr.inflight, id)
			}
		}
		d.mu.Unlock()
	}
}

// takeBatch moves up to one request's worth of identifiers from the pending set
// into the inflight set and returns them. The caller must hold d.mu.
func (d *Directory) takeBatch(pr *peerReconcile) []recordID {
	now := d.now()
	d.expireWindows(pr, now)
	compactWindows(pr)
	if len(pr.requested) >= d.settings.MaxRecords() {
		return nil
	}
	limit := d.maxRequestIDs()
	out := make([]recordID, 0, limit)
	w := &requestWindow{deadline: now.Add(requestedTTL)}
	for id := range pr.pending {
		if len(out) >= limit {
			break
		}
		delete(pr.pending, id)
		pr.inflight[id] = struct{}{}
		pr.requested[id] = requestedEntry{deadline: w.deadline, window: w}
		out = append(out, id)
	}
	if len(out) > 0 {
		w.ids = out
		w.outstanding = len(out)
		pr.windows = append(pr.windows, w)
	}
	return out
}

// expireWindows deletes the identifiers of expired windows; an identifier asked
// for again belongs to a later window and is kept. The caller must hold d.mu.
func (d *Directory) expireWindows(pr *peerReconcile, now time.Time) {
	expired := 0
	for expired < len(pr.windows) && !now.Before(pr.windows[expired].deadline) {
		w := pr.windows[expired]
		for _, id := range w.ids {
			if entry, ok := pr.requested[id]; ok && entry.window == w {
				delete(pr.requested, id)
			}
		}
		expired++
	}
	if expired == 0 {
		return
	}
	// Compact in place so the backing array does not grow across rounds.
	n := copy(pr.windows, pr.windows[expired:])
	pr.windows = pr.windows[:n]
}

// compactWindows drops every window whose identifiers have all arrived. The
// caller must hold d.mu.
func compactWindows(pr *peerReconcile) {
	n := 0
	for _, w := range pr.windows {
		if w.outstanding > 0 {
			pr.windows[n] = w
			n++
		}
	}
	pr.windows = pr.windows[:n]
}

func (d *Directory) maxRequestIDs() int {
	n := recordsPerRequest
	if m := d.settings.MaxRecords(); m < n {
		n = m
	}
	if n < 1 {
		n = 1
	}
	return n
}

// fetchOnce asks peer for ids in one RecordRequest and accepts the reply. It
// runs outside d.mu so a slow round trip does not block the link's state frames.
func (d *Directory) fetchOnce(peer Fingerprint, ids []recordID) {
	req := make([]*directorypb.RecordId, 0, len(ids))
	for _, id := range ids {
		req = append(req, id.toProto())
	}
	payload, err := proto.Marshal(&directorypb.RecordRequest{Records: req})
	if err != nil {
		d.log.Debug("encode record request", "peer", peer.String(), "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(d.ctx, requestTimeout)
	defer cancel()

	kind, reply, _, err := d.links.Request(ctx, peer, directorypb.KindRecordRequest, payload)
	if err != nil {
		d.log.Debug("record request failed", "peer", peer.String(), "error", err)
		return
	}
	if kind != directorypb.KindRecords {
		d.log.Debug("unexpected reply to a record request", "peer", peer.String(), "kind", kind)
		return
	}
	var recs directorypb.Records
	if err := proto.Unmarshal(reply, &recs); err != nil {
		d.log.Debug("decode records reply", "peer", peer.String(), "error", err)
		return
	}
	d.acceptRecords(peer, recs.GetRecords())
}

func (d *Directory) acceptRecords(peer Fingerprint, recs []*directorypb.Record) {
	now := d.now()

	d.mu.Lock()
	pr := d.recon[peer]
	allowed := make([]*directorypb.Record, 0, len(recs))
	dropped := 0
	for _, wire := range recs {
		id, ok := wireRecordID(wire)
		if !ok || pr == nil {
			dropped++
			continue
		}
		entry, ok := pr.requested[id]
		if !ok {
			dropped++
			continue
		}
		delete(pr.requested, id)
		if entry.window != nil {
			entry.window.outstanding--
		}
		if !now.Before(entry.deadline) {
			dropped++
			continue
		}
		allowed = append(allowed, wire)
	}
	if pr != nil {
		compactWindows(pr)
	}
	d.mu.Unlock()

	if dropped > 0 {
		d.log.Warn("directory_unrequested_records", "peer", peer.String(), "count", dropped)
	}
	if len(allowed) > 0 {
		d.acceptBatch(peer, allowed)
	}

	if pr != nil {
		d.mu.Lock()
		var start bool
		if !d.closed && d.recon[peer] == pr {
			start = d.startFetchLocked(pr)
		}
		d.mu.Unlock()
		if start {
			go d.fetchLoop(peer, pr)
		}
	}
}

// wireRecordID recovers the identifier a wire record claims without verifying
// its signature; the acceptance path does that. It reports false when the raw
// bytes do not decode into a body naming a 32-byte publisher.
func wireRecordID(r *directorypb.Record) (recordID, bool) {
	raw := r.GetRaw()
	if len(raw) == 0 || len(raw) > maxRecordRaw {
		return recordID{}, false
	}
	var body directorypb.RecordBody
	if err := proto.Unmarshal(raw, &body); err != nil {
		return recordID{}, false
	}
	pub := body.GetPublisher()
	if len(pub) != 32 || len(body.GetObject()) > maxRecordObjectLen {
		return recordID{}, false
	}
	var publisher Fingerprint
	copy(publisher[:], pub)
	return recordID{publisher: publisher, kind: body.GetKind(), object: body.GetObject()}, true
}
