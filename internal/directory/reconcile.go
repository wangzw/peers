package directory

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
)

// OnCard registers a callback invoked whenever a card published by another
// Broker is accepted from a link. Callbacks run in registration order, outside
// the directory lock, in the goroutine that handles the batch; callbacks for
// different links may run concurrently, so a callback must be safe to call
// concurrently and must not block for long. The addresses are the card's
// "direct" entries, at most eight, in the card's order. Callbacks are not
// called for records issued locally or loaded from disk.
func (d *Directory) OnCard(f func(fp Fingerprint, addresses []config.AdvertisedAddr)) {
	if f == nil {
		return
	}
	d.mu.Lock()
	d.onCard = append(d.onCard, f)
	d.mu.Unlock()
}

// recordsReplyBudget is the largest encoded Records frame the answer path
// sends. A reply holding more records is split into several frames, each at or
// below this byte budget, so no frame approaches link's single-frame limit.
const recordsReplyBudget = 1 << 20

// registerReconcile registers the handlers for the reconciliation frames: the
// RecordList state frame, the RecordRequest request frame, and the Records
// frames that carry the later batches of a split reply.
func (d *Directory) registerReconcile() {
	d.links.HandleState(directorypb.KindRecordList, d.handleRecordList)
	d.links.HandleState(directorypb.KindRecords, d.handleRecordsState)
	d.links.HandleRequest(directorypb.KindRecordRequest, d.handleRecordRequest)
}

// handleRecordList compares a peer's list of identifiers and versions with the
// local directory. Missing or older local records are queued for a fetch; for
// every identifier the local directory holds a strictly higher version of, an
// incremental RecordList is sent back. Equal versions are ignored, so an
// exchange always terminates. At most one fetch runs per peer: while it is
// outstanding, further identifiers merge into that peer's pending set, and the
// fetch loop sends them when the outstanding request finishes. Records is never
// sent here.
func (d *Directory) handleRecordList(peer Fingerprint, payload []byte) {
	d.mu.RLock()
	live := !d.closed && d.up[peer]
	d.mu.RUnlock()
	if !live {
		return
	}

	var list directorypb.RecordList
	if err := proto.Unmarshal(payload, &list); err != nil {
		d.log.Debug("dropped a malformed record list", "peer", peer.String(), "error", err)
		return
	}

	now := d.now()
	d.mu.RLock()
	if d.closed || !d.up[peer] {
		d.mu.RUnlock()
		return
	}

	var fetchIDs []recordID
	var tellBack []*directorypb.RecordVersion
	for _, item := range list.GetRecords() {
		id, ok := idFromProto(item.GetId())
		if !ok {
			continue
		}
		held, ok := d.st.get(id)
		if !ok || isExpired(held.expires, now) {
			fetchIDs = append(fetchIDs, id)
			continue
		}
		switch {
		case item.GetVersion() > held.version:
			fetchIDs = append(fetchIDs, id)
		case item.GetVersion() < held.version:
			tellBack = append(tellBack, held.recordVersion())
		}
	}
	d.mu.RUnlock()

	if len(fetchIDs) > 0 {
		d.scheduleFetch(peer, fetchIDs)
	}
	if len(tellBack) > 0 {
		d.sendList(peer, tellBack)
	}
}

// handleRecordRequest answers a RecordRequest with the held, unexpired records
// whose identifiers were asked for. Identifiers are deduplicated, so a record
// asked for twice appears once. When the answer exceeds recordsReplyBudget it
// is split into several frames: the first batch is the reply and the rest are
// sent as further Records frames, so the whole answer is delivered without
// truncation or an oversized frame. A frame from a peer whose link is not
// established gets an empty answer.
func (d *Directory) handleRecordRequest(_ context.Context, peer Fingerprint, payload []byte) (uint32, []byte) {
	d.mu.RLock()
	live := !d.closed && d.up[peer]
	d.mu.RUnlock()
	if !live {
		return directorypb.KindRecords, nil
	}

	var req directorypb.RecordRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		d.log.Debug("dropped a malformed record request", "peer", peer.String(), "error", err)
		return directorypb.KindRecords, nil
	}

	now := d.now()
	d.mu.RLock()
	if d.closed || !d.up[peer] {
		d.mu.RUnlock()
		return directorypb.KindRecords, nil
	}
	seen := make(map[recordID]bool, len(req.GetRecords()))
	var recs []*directorypb.Record
	for _, pid := range req.GetRecords() {
		id, ok := idFromProto(pid)
		if !ok || seen[id] {
			continue
		}
		seen[id] = true
		r, ok := d.st.get(id)
		if !ok || isExpired(r.expires, now) {
			continue
		}
		recs = append(recs, r.wire())
	}
	d.mu.RUnlock()

	batches := splitRecords(recs, recordsReplyBudget)
	if len(batches) == 0 {
		return directorypb.KindRecords, nil
	}
	for _, batch := range batches[1:] {
		payload, err := proto.Marshal(&directorypb.Records{Records: batch})
		if err != nil {
			d.log.Error("encode records frame", "error", err)
			continue
		}
		if err := d.links.Send(peer, directorypb.KindRecords, payload); err != nil {
			d.log.Debug("send records frame failed", "peer", peer.String(), "error", err)
		}
	}

	reply, err := proto.Marshal(&directorypb.Records{Records: batches[0]})
	if err != nil {
		d.log.Error("encode records reply", "error", err)
		return directorypb.KindRecords, nil
	}
	return directorypb.KindRecords, reply
}

// splitRecords groups recs into batches whose encoded Records frames do not
// exceed budget. A single record larger than the budget is sent on its own; the
// per-record limit makes that unreachable through the accepting paths. Order is
// preserved.
func splitRecords(recs []*directorypb.Record, budget int) [][]*directorypb.Record {
	var (
		batches [][]*directorypb.Record
		cur     []*directorypb.Record
		size    int
	)
	for _, r := range recs {
		n := proto.Size(r)
		// A repeated message field encodes as a one-byte tag, the length
		// varint and the record itself; the sum is the Records frame size.
		add := 1 + protowire.SizeVarint(uint64(n)) + n
		if len(cur) > 0 && size+add > budget {
			batches = append(batches, cur)
			cur, size = nil, 0
		}
		cur = append(cur, r)
		size += add
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}

// handleRecordsState accepts a Records frame. These carry the batches of a
// reply that did not fit in one frame, so their records are accepted only if
// this direction requested them and their accept window is still open.
func (d *Directory) handleRecordsState(peer Fingerprint, payload []byte) {
	d.mu.RLock()
	live := !d.closed && d.up[peer]
	d.mu.RUnlock()
	if !live {
		return
	}
	var recs directorypb.Records
	if err := proto.Unmarshal(payload, &recs); err != nil {
		d.log.Debug("dropped a malformed records frame", "peer", peer.String(), "error", err)
		return
	}
	d.acceptRecords(peer, recs.GetRecords())
}

// cardEvent is one accepted foreign card handed to the OnCard callbacks.
type cardEvent struct {
	fp    Fingerprint
	addrs []config.AdvertisedAddr
}

// acceptBatch verifies and accepts a batch of records received from peer, then
// pushes the newly accepted identifiers to every other established peer, runs
// the card callbacks and the own-record hook, and persists the directory. The
// records are parsed outside the lock; the acceptance and the bookkeeping run
// under one write lock.
func (d *Directory) acceptBatch(peer Fingerprint, recs []*directorypb.Record) {
	d.mu.RLock()
	live := !d.closed && d.up[peer]
	d.mu.RUnlock()
	if !live {
		return
	}

	parsed := make([]*storedRecord, 0, len(recs))
	for _, wire := range recs {
		r, err := parseRecord(wire)
		if err != nil {
			switch {
			case errors.Is(err, errRecordTooLarge):
				d.log.Warn("dropped an oversized record", "peer", peer.String(),
					"reason", "record_too_large", "limit", maxRecordRaw)
			case errors.Is(err, errBadSignature):
				d.log.Warn("dropped a record with an invalid signature", "peer", peer.String())
			default:
				d.log.Debug("dropped a malformed record", "peer", peer.String(), "error", err)
			}
			continue
		}
		r.source = peer
		parsed = append(parsed, r)
	}
	if len(parsed) == 0 {
		return
	}

	now := d.now()
	lim := d.limits()

	var (
		accepted  []*directorypb.RecordVersion
		cards     []cardEvent
		ownIDs    []recordID
		dropped   int
		reasons   []string
		callbacks []func(Fingerprint, []config.AdvertisedAddr)
		ownHook   func([]recordID)
	)

	d.mu.Lock()
	if d.closed || !d.up[peer] {
		d.mu.Unlock()
		return
	}
	for _, r := range parsed {
		switch out := d.st.accept(r, now, lim); out {
		case outcomeAccepted, outcomeReplaced:
			accepted = append(accepted, r.recordVersion())
			if r.id.publisher == d.st.self {
				ownIDs = append(ownIDs, r.id)
			} else if card := r.body.GetBrokerCard(); card != nil {
				cards = append(cards, cardEvent{fp: r.id.publisher, addrs: knownAddrs(card, 8)})
			}
		case outcomeQuotaTotal:
			dropped++
			reasons = addReason(reasons, "max_records")
		case outcomeQuotaLink:
			dropped++
			reasons = addReason(reasons, "max_records_per_link")
		case outcomeQuotaPublisher:
			dropped++
			reasons = addReason(reasons, "max_topics_per_publisher")
		}
	}
	if len(accepted) > 0 {
		d.dirty = true
	}
	callbacks = append(callbacks, d.onCard...)
	ownHook = d.ownHook
	d.mu.Unlock()

	if dropped > 0 {
		d.log.Warn("dropped records over the directory limits",
			"peer", peer.String(), "count", dropped, "limit", strings.Join(reasons, ","))
	}
	d.broadcast(accepted, peer)
	for _, ev := range cards {
		for _, cb := range callbacks {
			cb(ev.fp, ev.addrs)
		}
	}
	if len(ownIDs) > 0 && ownHook != nil {
		ownHook(ownIDs)
	}
	d.persistIfDirty()
}

// addReason appends reason to reasons unless it is already there.
func addReason(reasons []string, reason string) []string {
	for _, r := range reasons {
		if r == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

// reconcileAll sends a full RecordList to every peer whose link is established.
func (d *Directory) reconcileAll() {
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return
	}
	peers := make([]Fingerprint, 0, len(d.up))
	for peer := range d.up {
		peers = append(peers, peer)
	}
	d.mu.RUnlock()

	for _, peer := range peers {
		d.sendFullList(peer)
	}
}
