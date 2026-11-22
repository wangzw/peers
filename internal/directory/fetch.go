package directory

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

const (
	// recordsPerRequest is the largest number of identifiers one RecordRequest
	// asks for. The rest of a peer's pending set waits for the next request of
	// the same fetch loop, so neither a request nor its answer grows without
	// bound.
	recordsPerRequest = 256
	// requestedTTL is how long an identifier stays acceptable after the request
	// that asked for it was sent. It is twice the request deadline, so a reply
	// that arrives just before the request times out is still accepted.
	requestedTTL = 2 * requestTimeout
)

// peerReconcile is the requester-side reconcile state of one peer. The zero
// value is not usable; newPeerReconcile returns an initialized one. Every field
// is read and written under Directory.mu.
type peerReconcile struct {
	// pending holds identifiers to request on the next round.
	pending map[recordID]struct{}
	// inflight holds the identifiers of the outstanding request.
	inflight map[recordID]struct{}
	// requested maps an identifier to its accept window. An entry is removed
	// when its record arrives, when its window expires or when the link goes
	// down. Its size stays bounded: once it holds directory.max_records
	// identifiers no further batch is requested, so the window never exceeds
	// that count by more than the batch in flight.
	requested map[recordID]requestedEntry
	// windows holds the accept windows in creation order that still have an
	// identifier outstanding. Every entry added by one request shares that
	// window's deadline, so an expired window is dropped by walking only its
	// own identifiers instead of scanning the whole requested map on every
	// round. A window whose identifiers were all accepted is dropped at once,
	// so the slice is bounded by the outstanding identifiers rather than by how
	// fast a peer answers.
	windows []*requestWindow
	// running reports whether a fetch loop for this peer is alive.
	running bool
	// refetch maps an identifier to the instant its held version becomes
	// replaceable. A newer version that arrived before that instant was
	// dropped or not requested; the identifier is fetched once when the
	// window ends.
	refetch map[recordID]time.Time
	// refetchRunning reports whether a goroutine is waiting on refetch.
	refetchRunning bool
	// refetchWake wakes that goroutine so it re-reads the earliest deadline.
	refetchWake chan struct{}
}

// requestedEntry is one identifier's place in the accept window.
type requestedEntry struct {
	// deadline is the instant after which a record for the identifier is no
	// longer accepted.
	deadline time.Time
	// window is the request that asked for the identifier.
	window *requestWindow
}

// requestWindow is one request's slice of the accept window: the identifiers it
// named, the instant after which none of them is accepted, and how many of them
// have not arrived yet.
type requestWindow struct {
	deadline    time.Time
	ids         []recordID
	outstanding int
}

// newPeerReconcile returns an empty reconcile state.
func newPeerReconcile() *peerReconcile {
	return &peerReconcile{
		pending:     make(map[recordID]struct{}),
		inflight:    make(map[recordID]struct{}),
		requested:   make(map[recordID]requestedEntry),
		refetch:     make(map[recordID]time.Time),
		refetchWake: make(chan struct{}, 1),
	}
}

// scheduleFetch queues ids for peer and starts a fetch loop when none is
// running. Identifiers already in flight or already queued are skipped, and the
// queue holds at most directory.max_records identifiers; an identifier that
// does not fit is dropped and left for the next reconciliation pass. The caller
// must not hold d.mu.
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
		// The identifier is being fetched now, so its deferred entry is moot.
		clearRefetchLocked(pr, id)
	}
	start := d.startFetchLocked(pr)
	d.mu.Unlock()

	if start {
		go d.fetchLoop(peer, pr)
	}
}

// startFetchLocked reports whether a fetch loop should be started for pr, and
// reserves the inflight slot when it should. A loop is started only when none is
// running, something is pending and the accept window holds fewer than
// directory.max_records identifiers: once it reaches that count, new batches
// wait, so a peer that never answers cannot grow the set without bound. Adding the
// slot under the write lock keeps Close, which takes the write lock before
// waiting on the group, from missing the goroutine. The caller must hold d.mu.
func (d *Directory) startFetchLocked(pr *peerReconcile) bool {
	if pr.running || len(pr.pending) == 0 || len(pr.requested) >= d.settings.MaxRecords() {
		return false
	}
	pr.running = true
	d.inflight.Add(1)
	return true
}

// reconcileForLocked returns peer's reconcile state, creating an empty one when
// the peer has none. The caller must hold d.mu.
func (d *Directory) reconcileForLocked(peer Fingerprint) *peerReconcile {
	pr := d.recon[peer]
	if pr == nil {
		pr = newPeerReconcile()
		d.recon[peer] = pr
	}
	return pr
}

// enqueuePendingLocked adds id to pr's pending set unless it is already in
// flight or pending or the queue already holds limit identifiers. It reports
// whether id was added. The caller must hold d.mu.
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
// that each is fetched once when its window ends. An identifier already
// recorded keeps the earlier deadline, and the set holds at most
// directory.max_records identifiers: one that does not fit waits for the next
// reconciliation pass. The caller must not hold d.mu.
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
// loop must be started for pr. The set is capped at limit, the caller's copy of
// directory.max_records. The caller must hold d.mu.
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

// clearRefetchLocked drops a deferred identifier that no longer needs a
// refetch, waking the waiting goroutine so it re-reads the earliest deadline.
// The caller must hold d.mu.
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

// earliestRefetch returns the earliest deadline in pr's deferred set.
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

// refetchLoop waits for the earliest deferred deadline, queues every identifier
// whose window has ended for one fetch, and repeats until the set is empty, the
// link goes down or the directory closes. It counts as one deferred-refetch
// waiter for its whole life, so Close waits for it and a peer has at most one
// such loop.
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
			// check. A noteRefetch that runs after this unlock must see
			// refetchRunning false and start its own loop, because this one is
			// leaving; releasing the lock between the check and the clear would
			// let it send a wake to the departing loop and drop the identifier
			// until the next periodic reconciliation.
			pr.refetchRunning = false
		}
		d.mu.Unlock()
		if !ok {
			if d.refetchIdle != nil {
				d.refetchIdle()
			}
			return
		}

		// A deadline that has already passed is served now rather than through
		// a zero-length timer, which keeps the wait independent of how late the
		// loop got to arm it.
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

		// Read the queue cap before taking the lock so a config read never
		// nests inside it.
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
				// An identifier that does not fit waits for the next
				// reconciliation pass.
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

// fetchLoop asks peer for the queued identifiers one request at a time until
// the queue is empty, the link goes down or the directory closes. It holds one
// inflight slot for its whole life, so a peer has at most one fetch loop and
// therefore at most one outstanding request. Every failure is logged at debug
// level inside fetchOnce and swallowed: the next reconcile pass retries.
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
			// The accept window is full, so no new batch may be requested.
			// Leaving the loop lets scheduleFetch start it again once an entry
			// is accepted, and lets a future RecordList retry once a window
			// expires.
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
// into the inflight set, registers each one in the accept window and returns
// them. Expired accept windows and windows whose identifiers all arrived are
// dropped first, walking only the identifiers of each window and never the
// whole requested map. No batch is taken once the window already holds
// directory.max_records identifiers, so a peer that never answers cannot grow it
// without bound; the pending identifiers wait for a window to expire or a
// record to arrive. The caller must hold d.mu.
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

// expireWindows deletes the identifiers of every window whose deadline has
// passed. Windows are created in increasing deadline order, so only the front of
// the slice is examined; the work is proportional to the size of the expired
// windows, not to the whole requested map. An identifier whose entry points to a
// later window was asked for again, so it is kept. The caller must hold d.mu.
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

// compactWindows drops every window whose identifiers have all arrived. Without
// it, a peer that answers each request at once would leave one window per
// request for the whole accept TTL, so the slice would grow with the request
// rate rather than with the outstanding identifiers. The caller must hold d.mu.
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

// maxRequestIDs returns how many identifiers one request may name: the
// recordsPerRequest constant, capped by directory.max_records.
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
// runs outside d.mu so a slow round trip does not block the link's state
// frames.
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

// acceptRecords accepts a Records frame from peer after checking every record
// against the identifiers this direction requested and whose accept window is
// still open. Each identifier is accepted once: the first record that names it
// consumes the entry. Records that name no requested identifier are dropped,
// and the drops in one frame are merged into one warning. A record that does
// not even yield an identifier is also dropped.
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

	// Accepting a record frees its accept-window entry. If the fetch loop
	// stopped because the window was full, resume it now so the pending
	// identifiers do not wait for a window to expire or a new RecordList.
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

// wireRecordID recovers the identifier a wire record claims. It does not verify
// the signature; the acceptance path does that. It reports false when the raw
// bytes are empty or oversized or do not decode into a body naming a 32-byte
// publisher, in which case the acceptance path would drop the record anyway.
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
