package directory

import (
	"context"
	"errors"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
)

// OnCard registers a callback invoked when another Broker's card is accepted
// from a link. Callbacks run in registration order outside the directory lock,
// and different links may call concurrently, so a callback must be safe for
// concurrent use and must not block for long. The addresses are the recognized
// direct entries, at most eight, in card order.
func (d *Directory) OnCard(f func(fp Fingerprint, addresses []config.AdvertisedAddr)) {
	if f == nil {
		return
	}
	d.mu.Lock()
	d.onCard = append(d.onCard, f)
	d.mu.Unlock()
}

const recordsReplyBudget = 1 << 20

func (d *Directory) registerReconcile() {
	d.links.HandleState(directorypb.KindRecordList, d.handleRecordList)
	d.links.HandleState(directorypb.KindRecords, d.handleRecordsState)
	d.links.HandleRequest(directorypb.KindRecordRequest, d.handleRecordRequest)
}

func (d *Directory) handleRecordList(peer Fingerprint, payload []byte) {
	if !d.linkUp(peer) {
		return
	}

	var list directorypb.RecordList
	if err := proto.Unmarshal(payload, &list); err != nil {
		d.log.Debug("dropped a malformed record list", "peer", peer.String(), "error", err)
		return
	}

	now := d.now()
	replaceInterval := directoryReplaceInterval()
	d.mu.RLock()
	if d.closed || !d.up[peer] {
		d.mu.RUnlock()
		return
	}

	var fetchIDs []recordID
	var deferred map[recordID]time.Time
	var tellBack []*directorypb.RecordVersion
	for _, item := range list.GetRecords() {
		id, ok := idFromProto(item.GetId())
		if !ok {
			continue
		}
		held, heldOK := d.st.get(id)
		if live := heldOK && !isExpired(held.expires, now); live {
			switch {
			case item.GetVersion() == held.version:
				continue
			case item.GetVersion() < held.version:
				tellBack = append(tellBack, held.recordVersion())
				continue
			}
			if id.publisher != d.st.self && !held.acceptedAt.IsZero() && now.Before(held.acceptedAt.Add(replaceInterval)) {
				if deferred == nil {
					deferred = make(map[recordID]time.Time)
				}
				deferred[id] = held.acceptedAt.Add(replaceInterval)
				continue
			}
			fetchIDs = append(fetchIDs, id)
			continue
		}
		if at, seen := d.st.replaceNotBefore(id); seen && now.Before(at.Add(replaceInterval)) {
			if deferred == nil {
				deferred = make(map[recordID]time.Time)
			}
			deferred[id] = at.Add(replaceInterval)
			continue
		}
		fetchIDs = append(fetchIDs, id)
	}
	d.mu.RUnlock()

	if len(deferred) > 0 {
		d.noteRefetch(peer, deferred)
	}
	if len(fetchIDs) > 0 {
		d.scheduleFetch(peer, fetchIDs)
	}
	if len(tellBack) > 0 {
		d.sendList(peer, tellBack)
	}
}

func (d *Directory) handleRecordRequest(_ context.Context, peer Fingerprint, payload []byte) (uint32, []byte) {
	if !d.linkUp(peer) {
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

func splitRecords(recs []*directorypb.Record, budget int) [][]*directorypb.Record {
	var (
		batches [][]*directorypb.Record
		cur     []*directorypb.Record
		size    int
	)
	for _, r := range recs {
		n := proto.Size(r)
		// A repeated message field adds a tag and a length varint per record.
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

func (d *Directory) handleRecordsState(peer Fingerprint, payload []byte) {
	if !d.linkUp(peer) {
		return
	}
	var recs directorypb.Records
	if err := proto.Unmarshal(payload, &recs); err != nil {
		d.log.Debug("dropped a malformed records frame", "peer", peer.String(), "error", err)
		return
	}
	d.acceptRecords(peer, recs.GetRecords())
}

type cardEvent struct {
	fp    Fingerprint
	addrs []config.AdvertisedAddr
}

// acceptBatch parses the records outside d.mu, then accepts and accounts for
// them under one write lock.
func (d *Directory) acceptBatch(peer Fingerprint, recs []*directorypb.Record) {
	if !d.linkUp(peer) {
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
		accepted     []*directorypb.RecordVersion
		cards        []cardEvent
		ownIDs       []recordID
		dropped      int
		reasons      []string
		callbacks    []func(Fingerprint, []config.AdvertisedAddr)
		ownHook      func([]recordID)
		pr           *peerReconcile
		startRefetch bool
	)

	d.mu.Lock()
	if d.closed || !d.up[peer] {
		d.mu.Unlock()
		return
	}
	pr = d.recon[peer]
	for _, r := range parsed {
		switch out := d.st.accept(r, now, lim); out {
		case outcomeAccepted, outcomeReplaced:
			accepted = append(accepted, r.recordVersion())
			if pr != nil {
				clearRefetchLocked(pr, r.id)
			}
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
		case outcomeReplaceTooFast:
			dropped++
			reasons = addReason(reasons, "replace_interval")
			if at, ok := d.st.replaceNotBefore(r.id); ok {
				if pr == nil {
					pr = newPeerReconcile()
					d.recon[peer] = pr
				}
				if d.noteRefetchLocked(pr, r.id, at.Add(directoryReplaceInterval()), lim.maxRecords) {
					startRefetch = true
				}
			}
		}
	}
	if len(accepted) > 0 {
		d.dirty = true
	}
	callbacks = append(callbacks, d.onCard...)
	ownHook = d.ownHook
	d.mu.Unlock()

	if startRefetch {
		go d.refetchLoop(peer, pr)
	}

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
}

func addReason(reasons []string, reason string) []string {
	for _, r := range reasons {
		if r == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

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
