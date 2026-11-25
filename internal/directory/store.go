package directory

import (
	"bytes"
	"sort"
	"time"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

type limits struct {
	maxRecords, maxPerLink, maxTopicsPerPublisher int
}

type acceptOutcome uint8

const (
	outcomeAccepted acceptOutcome = iota + 1
	outcomeReplaced
	// outcomeReplaceTooFast means the held version was accepted from a link
	// less than directoryReplaceInterval() ago, so the replacement is held back
	// and the caller records the identifier for one fetch after the interval.
	outcomeReplaceTooFast
	outcomeStale
	outcomeExpired
	// outcomeTooFar means the record's expiry lies beyond the allowed window.
	outcomeTooFar
	outcomeQuotaTotal
	outcomeQuotaLink
	outcomeQuotaPublisher
)

// store is the in-memory directory. It is not safe for concurrent use; the
// owning Directory calls every method under its lock.
type store struct {
	records      map[recordID]*storedRecord
	perLink      map[Fingerprint]int // records with a non-zero source
	perPublisher map[Fingerprint]int // records counted toward the per-publisher limit
	// lastAccepted remembers when an identifier was last accepted from a link,
	// so a record that expires before the replacement interval ends cannot come
	// back as a new record and bypass the interval. It holds at most
	// directory.max_records identifiers, dropping the oldest first.
	lastAccepted  map[recordID]time.Time
	highestIssued uint64
	self          Fingerprint
}

func newStore(self Fingerprint) *store {
	return &store{
		records:      make(map[recordID]*storedRecord),
		perLink:      make(map[Fingerprint]int),
		perPublisher: make(map[Fingerprint]int),
		lastAccepted: make(map[recordID]time.Time),
		self:         self,
	}
}

func (s *store) accept(r *storedRecord, now time.Time, lim limits) acceptOutcome {
	if isExpired(r.expires, now) {
		return outcomeExpired
	}
	if expiryTooFar(r.expires, now) {
		return outcomeTooFar
	}
	if old, ok := s.records[r.id]; ok {
		if r.version <= old.version {
			return outcomeStale
		}
		if !s.mayReplace(r, old, now) {
			return outcomeReplaceTooFast
		}
		s.uncount(old)
		s.markAccepted(r, now, lim.maxRecords)
		s.put(r)
		s.raise(r)
		return outcomeReplaced
	}
	if !r.source.IsZero() && s.withinReplaceInterval(r.id, now) {
		return outcomeReplaceTooFast
	}
	if len(s.records) >= lim.maxRecords {
		return outcomeQuotaTotal
	}
	if !r.source.IsZero() && s.perLink[r.source] >= lim.maxPerLink {
		return outcomeQuotaLink
	}
	if countsTowardPublisher(r) && s.perPublisher[r.id.publisher] >= lim.maxTopicsPerPublisher {
		return outcomeQuotaPublisher
	}
	s.markAccepted(r, now, lim.maxRecords)
	s.put(r)
	s.raise(r)
	return outcomeAccepted
}

func (s *store) mayReplace(r, old *storedRecord, now time.Time) bool {
	if r.id.publisher == s.self || old.acceptedAt.IsZero() {
		return true
	}
	return !now.Before(old.acceptedAt.Add(directoryReplaceInterval()))
}

func (s *store) withinReplaceInterval(id recordID, now time.Time) bool {
	if id.publisher == s.self {
		return false
	}
	at, ok := s.lastAccepted[id]
	return ok && now.Before(at.Add(directoryReplaceInterval()))
}

// replaceNotBefore returns the acceptance instant from which the caller
// computes the replacement deadline by adding directoryReplaceInterval(); it
// reports false when the identifier was never accepted from a link.
func (s *store) replaceNotBefore(id recordID) (time.Time, bool) {
	if old, ok := s.records[id]; ok && !old.acceptedAt.IsZero() {
		return old.acceptedAt, true
	}
	at, ok := s.lastAccepted[id]
	return at, ok
}

func (s *store) markAccepted(r *storedRecord, now time.Time, limit int) {
	if r.source.IsZero() {
		return
	}
	r.acceptedAt = now
	if r.id.publisher == s.self {
		return
	}
	s.rememberAccepted(r.id, now, limit)
}

func (s *store) rememberAccepted(id recordID, now time.Time, limit int) {
	if limit < 1 {
		limit = 1
	}
	s.lastAccepted[id] = now
	if len(s.lastAccepted) <= limit {
		return
	}
	cutoff := now.Add(-directoryReplaceInterval())
	for k, at := range s.lastAccepted {
		if !at.After(cutoff) {
			delete(s.lastAccepted, k)
		}
	}
	for len(s.lastAccepted) > limit {
		var (
			oldestID  recordID
			oldestAt  time.Time
			haveFirst bool
		)
		for k, at := range s.lastAccepted {
			if !haveFirst || at.Before(oldestAt) {
				oldestID, oldestAt, haveFirst = k, at, true
			}
		}
		delete(s.lastAccepted, oldestID)
	}
}

func (s *store) putOwn(r *storedRecord) {
	if old, ok := s.records[r.id]; ok {
		s.uncount(old)
	}
	s.put(r)
	s.raise(r)
}

func (s *store) get(id recordID) (*storedRecord, bool) {
	r, ok := s.records[id]
	return r, ok
}

func (s *store) versions(now time.Time) []*directorypb.RecordVersion {
	live := make([]*storedRecord, 0, len(s.records))
	for _, r := range s.records {
		if !isExpired(r.expires, now) {
			live = append(live, r)
		}
	}
	sortRecords(live)
	out := make([]*directorypb.RecordVersion, 0, len(live))
	for _, r := range live {
		out = append(out, r.recordVersion())
	}
	return out
}

func (s *store) purgeExpired(now time.Time) int {
	n := 0
	for _, r := range s.records {
		if isExpired(r.expires, now) {
			s.uncount(r)
			n++
		}
	}
	return n
}

// all returns every record ordered by publisher, kind and object; the caller
// must not modify them.
func (s *store) all() []*storedRecord {
	out := make([]*storedRecord, 0, len(s.records))
	for _, r := range s.records {
		out = append(out, r)
	}
	sortRecords(out)
	return out
}

// ownRecords returns the records published by the local Broker; the records
// belong to the store and the caller must not modify them.
func (s *store) ownRecords() []*storedRecord {
	out := make([]*storedRecord, 0)
	for _, r := range s.records {
		if r.id.publisher == s.self {
			out = append(out, r)
		}
	}
	sortRecords(out)
	return out
}

func (s *store) countOwnAnnouncements(now time.Time, exceptName string) int {
	n := 0
	for _, r := range s.records {
		if r.id.publisher != s.self || r.id.object == exceptName {
			continue
		}
		if isExpired(r.expires, now) || !isAnnouncement(r) {
			continue
		}
		n++
	}
	return n
}

func (s *store) load(recs []*storedRecord, highest uint64, now time.Time) int {
	s.records = make(map[recordID]*storedRecord)
	s.perLink = make(map[Fingerprint]int)
	s.perPublisher = make(map[Fingerprint]int)
	s.lastAccepted = make(map[recordID]time.Time)
	s.highestIssued = highest
	dropped := 0
	for _, r := range recs {
		if r == nil || isExpired(r.expires, now) {
			dropped++
			continue
		}
		if old, ok := s.records[r.id]; ok {
			if r.version <= old.version {
				dropped++
				continue
			}
			s.uncount(old)
		}
		s.put(r)
		s.raise(r)
	}
	return dropped
}

func (s *store) put(r *storedRecord) {
	s.records[r.id] = r
	if !r.source.IsZero() {
		s.perLink[r.source]++
	}
	if countsTowardPublisher(r) {
		s.perPublisher[r.id.publisher]++
	}
}

func (s *store) uncount(r *storedRecord) {
	delete(s.records, r.id)
	if !r.source.IsZero() {
		s.perLink[r.source]--
	}
	if countsTowardPublisher(r) {
		s.perPublisher[r.id.publisher]--
	}
}

func countsTowardPublisher(r *storedRecord) bool {
	return r.id.kind != directorypb.RecordKind_RECORD_KIND_BROKER_CARD && !r.isRevocation()
}

func liveRecord(r *storedRecord, now time.Time) bool {
	return r.body != nil && !r.isRevocation() && !isExpired(r.expires, now)
}

func (s *store) raise(r *storedRecord) {
	if r.id.publisher == s.self && r.version > s.highestIssued {
		s.highestIssued = r.version
	}
}

// isAnnouncement reports whether the record's payload is a public topic
// declaration; a revocation reuses the declaration's identifier but carries a
// Revocation payload.
func isAnnouncement(r *storedRecord) bool {
	return r.body != nil && r.body.GetTopicAnnouncement() != nil
}

func sortRecords(recs []*storedRecord) {
	sort.Slice(recs, func(i, j int) bool {
		a, b := recs[i], recs[j]
		if c := bytes.Compare(a.id.publisher[:], b.id.publisher[:]); c != 0 {
			return c < 0
		}
		if a.id.kind != b.id.kind {
			return a.id.kind < b.id.kind
		}
		return a.id.object < b.id.object
	})
}
