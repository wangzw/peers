package directory

import (
	"bytes"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

func storeFP(b byte) Fingerprint {
	var fp Fingerprint
	fp[0] = b
	return fp
}

// rec builds an unsigned stored record; the store only reads the identifier,
// version, expiry, source and payload type.
func rec(pub Fingerprint, kind directorypb.RecordKind, object string, version uint64, expires time.Time, source Fingerprint) *storedRecord {
	r := &storedRecord{
		id:      recordID{publisher: pub, kind: kind, object: object},
		version: version,
		expires: expires,
		source:  source,
	}
	switch kind {
	case directorypb.RecordKind_RECORD_KIND_BROKER_CARD:
		r.body = &directorypb.RecordBody{Payload: &directorypb.RecordBody_BrokerCard{BrokerCard: &directorypb.BrokerCard{}}}
	case directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT:
		r.body = &directorypb.RecordBody{Payload: &directorypb.RecordBody_TopicAnnouncement{TopicAnnouncement: &directorypb.TopicAnnouncement{Name: object}}}
	default:
		r.body = &directorypb.RecordBody{}
	}
	return r
}

func revocation(pub Fingerprint, kind directorypb.RecordKind, object string, version uint64, expires time.Time, source Fingerprint) *storedRecord {
	r := rec(pub, kind, object, version, expires, source)
	r.body = &directorypb.RecordBody{Payload: &directorypb.RecordBody_Revocation{Revocation: &directorypb.Revocation{RevokedKind: kind}}}
	return r
}

// TestAcceptKeepsHighestVersion checks that accept keeps the highest version
// and treats an equal or lower version as stale.
func TestAcceptKeepsHighestVersion(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	s := newStore(storeFP(99))
	pub := storeFP(1)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD

	if got := s.accept(rec(pub, card, "", 2, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept v2 = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, card, "", 1, exp, link), now, lim); got != outcomeStale {
		t.Fatalf("accept v1 after v2 = %v, want outcomeStale", got)
	}
	later := now.Add(directoryReplaceInterval())
	if got := s.accept(rec(pub, card, "", 3, exp, link), later, lim); got != outcomeReplaced {
		t.Fatalf("accept v3 after v2 = %v, want outcomeReplaced", got)
	}
	if got := s.accept(rec(pub, card, "", 3, exp, link), later, lim); got != outcomeStale {
		t.Fatalf("accept v3 again = %v, want outcomeStale", got)
	}
	r, ok := s.get(recordID{publisher: pub, kind: card})
	if !ok {
		t.Fatal("record not stored")
	}
	if r.version != 3 {
		t.Fatalf("stored version = %d, want 3", r.version)
	}
}

// TestRevocationBlocksOlderVersion checks that an accepted revocation makes a
// lower-version record of the same identifier stale.
func TestRevocationBlocksOlderVersion(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	s := newStore(storeFP(99))
	pub := storeFP(1)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
	id := recordID{publisher: pub, kind: card}

	if got := s.accept(revocation(pub, card, "", 3, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept revocation v3 = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, card, "", 2, exp, link), now, lim); got != outcomeStale {
		t.Fatalf("accept card v2 after revocation v3 = %v, want outcomeStale", got)
	}
	r, ok := s.get(id)
	if !ok {
		t.Fatal("revocation not stored")
	}
	if r.version != 3 || r.body.GetRevocation() == nil {
		t.Fatalf("stored record version = %d, revocation = %v; want version 3 with a Revocation payload", r.version, r.body.GetRevocation())
	}
}

// TestTotalLimitAllowsRenewal checks that the total limit rejects new
// identifiers but accepts renewals of held ones.
func TestTotalLimitAllowsRenewal(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 3, maxPerLink: 10, maxTopicsPerPublisher: 10}
	s := newStore(storeFP(99))
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD

	for i := byte(1); i <= 5; i++ {
		want := outcomeAccepted
		if i > 3 {
			want = outcomeQuotaTotal
		}
		if got := s.accept(rec(storeFP(i), card, "", 1, exp, link), now, lim); got != want {
			t.Fatalf("accept publisher %d = %v, want %v", i, got, want)
		}
	}
	for i := byte(1); i <= 3; i++ {
		if got := s.accept(rec(storeFP(i), card, "", 2, exp, link), now.Add(directoryReplaceInterval()), lim); got != outcomeReplaced {
			t.Fatalf("renew publisher %d = %v, want outcomeReplaced", i, got)
		}
	}
}

// TestPerLinkLimit checks that the per-link limit counts only records accepted
// from that link.
func TestPerLinkLimit(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 4, maxTopicsPerPublisher: 100}
	s := newStore(storeFP(99))
	linkP := storeFP(50)
	linkQ := storeFP(60)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD

	accepted := 0
	for i := byte(1); i <= 10; i++ {
		if got := s.accept(rec(storeFP(i), card, "", 1, exp, linkP), now, lim); got == outcomeAccepted {
			accepted++
		}
	}
	if accepted != 4 {
		t.Fatalf("accepted from link P = %d, want 4", accepted)
	}
	if got := s.accept(rec(storeFP(11), card, "", 1, exp, linkQ), now, lim); got != outcomeAccepted {
		t.Fatalf("accept from link Q = %v, want outcomeAccepted", got)
	}
}

// TestExpiryChecks checks that accept drops expired and too-distant expiries
// and that purgeExpired removes records once they expire.
func TestExpiryChecks(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
	pub := storeFP(1)
	link := storeFP(50)

	s := newStore(storeFP(99))
	if got := s.accept(rec(pub, card, "", 1, now.Add(-time.Second), link), now, lim); got != outcomeExpired {
		t.Fatalf("accept expired record = %v, want outcomeExpired", got)
	}
	tooFar := now.Add(recordTTL + skewTolerance + time.Second)
	if got := s.accept(rec(pub, card, "", 1, tooFar, link), now, lim); got != outcomeTooFar {
		t.Fatalf("accept record with expiry too far = %v, want outcomeTooFar", got)
	}

	day1 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	s2 := newStore(storeFP(99))
	if got := s2.accept(rec(pub, card, "", 1, day1.Add(recordTTL), link), day1, lim); got != outcomeAccepted {
		t.Fatalf("accept day-1 record = %v, want outcomeAccepted", got)
	}
	if n := s2.purgeExpired(day1.Add(6 * 24 * time.Hour)); n != 0 {
		t.Fatalf("purge on day 6 removed %d records, want 0", n)
	}
	if n := s2.purgeExpired(day1.Add(8 * 24 * time.Hour)); n != 1 {
		t.Fatalf("purge on day 8 removed %d records, want 1", n)
	}
	if _, ok := s2.get(recordID{publisher: pub, kind: card}); ok {
		t.Fatal("expired record still present after purge")
	}
}

// TestReplaceMovesLinkCount checks that replacing a record under a different
// source link moves the per-link count.
func TestReplaceMovesLinkCount(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	s := newStore(storeFP(99))
	pub := storeFP(1)
	linkP := storeFP(50)
	linkQ := storeFP(60)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD

	if got := s.accept(rec(pub, card, "", 1, exp, linkP), now, lim); got != outcomeAccepted {
		t.Fatalf("accept v1 from P = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, card, "", 2, exp, linkQ), now.Add(directoryReplaceInterval()), lim); got != outcomeReplaced {
		t.Fatalf("accept v2 from Q = %v, want outcomeReplaced", got)
	}
	if got := s.perLink[linkP]; got != 0 {
		t.Fatalf("perLink[P] = %d, want 0", got)
	}
	if got := s.perLink[linkQ]; got != 1 {
		t.Fatalf("perLink[Q] = %d, want 1", got)
	}
}

// TestOwnRecordRaisesHighest checks that accepting a record published by the
// local Broker raises highestIssued.
func TestOwnRecordRaisesHighest(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	self := storeFP(7)
	s := newStore(self)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
	const topic = directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT

	if got := s.accept(rec(self, card, "", 1000, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept own v1000 = %v, want outcomeAccepted", got)
	}
	if s.highestIssued != 1000 {
		t.Fatalf("highestIssued = %d, want 1000", s.highestIssued)
	}
	if got := s.accept(rec(self, topic, "lower", 400, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept own v400 = %v, want outcomeAccepted", got)
	}
	if s.highestIssued != 1000 {
		t.Fatalf("highestIssued = %d after lower version, want 1000", s.highestIssued)
	}
}

// TestVersionsSkipExpired checks that versions returns every live record
// ordered by publisher, kind and object.
func TestVersionsSkipExpired(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	s := newStore(storeFP(99))
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
	const topic = directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT

	pub1 := storeFP(1)
	pub2 := storeFP(2)
	pub3 := storeFP(3)

	if got := s.accept(rec(pub2, card, "", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept pub2 card = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub1, topic, "b", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept pub1 topic = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub1, card, "", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept pub1 card = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub3, card, "", 1, now.Add(time.Hour), link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept soon-expiring record = %v, want outcomeAccepted", got)
	}

	later := now.Add(2 * time.Hour)
	got := s.versions(later)
	want := []recordID{
		{publisher: pub1, kind: card, object: ""},
		{publisher: pub1, kind: topic, object: "b"},
		{publisher: pub2, kind: card, object: ""},
	}
	if len(got) != len(want) {
		t.Fatalf("versions returned %d entries, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if !bytes.Equal(got[i].Id.Publisher, w.publisher[:]) || got[i].Id.Kind != w.kind || got[i].Id.Object != w.object {
			t.Fatalf("versions[%d] = %v/%v/%q, want %v/%v/%q", i,
				got[i].Id.Publisher, got[i].Id.Kind, got[i].Id.Object, w.publisher[:], w.kind, w.object)
		}
	}
}

// TestLoadKeepsOverLimitAndRaisesHighest checks that load keeps every parsed
// record regardless of the limits, drops expired ones and takes the higher
// highest_issued.
func TestLoadKeepsOverLimitAndRaisesHighest(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	self := storeFP(7)
	linkP := storeFP(50)
	linkQ := storeFP(60)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD

	recs := []*storedRecord{
		rec(storeFP(1), card, "", 1, exp, linkP),
		rec(storeFP(2), card, "", 1, exp, linkP),
		rec(storeFP(3), card, "", 1, exp, linkQ),
		rec(storeFP(4), card, "", 1, exp, linkQ),
		rec(self, card, "", 1000, exp, Fingerprint{}),
	}

	s := newStore(self)
	s.load(recs, 50, now)
	if got := len(s.all()); got != 5 {
		t.Fatalf("loaded %d records, want 5", got)
	}
	if s.highestIssued != 1000 {
		t.Fatalf("highestIssued = %d, want 1000", s.highestIssued)
	}
	if got := s.perLink[linkP]; got != 2 {
		t.Fatalf("perLink[P] = %d, want 2", got)
	}
	if got := s.perLink[linkQ]; got != 2 {
		t.Fatalf("perLink[Q] = %d, want 2", got)
	}

	higherFile := newStore(self)
	higherFile.load(recs, 2000, now)
	if higherFile.highestIssued != 2000 {
		t.Fatalf("highestIssued with higher file value = %d, want 2000", higherFile.highestIssued)
	}

	withExpired := append(recs, rec(storeFP(5), card, "", 1, now.Add(-time.Second), linkP))
	dropped := newStore(self)
	dropped.load(withExpired, 0, now)
	if got := len(dropped.all()); got != 5 {
		t.Fatalf("loaded %d records with one expired, want 5", got)
	}
}

// TestPurgedRecordStillWaitsOutReplaceInterval checks that an identifier keeps
// its replacement interval after its record has expired and been purged.
func TestPurgedRecordStillWaitsOutReplaceInterval(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	pub := storeFP(1)
	link := storeFP(50)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
	id := recordID{publisher: pub, kind: card}

	s := newStore(storeFP(99))
	if got := s.accept(rec(pub, card, "", 1, now.Add(2*time.Second), link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept v1 = %v, want outcomeAccepted", got)
	}
	if n := s.purgeExpired(now.Add(2 * time.Second)); n != 1 {
		t.Fatalf("purge removed %d records, want 1", n)
	}
	if _, ok := s.get(id); ok {
		t.Fatal("the expired record was not purged")
	}

	if got := s.accept(rec(pub, card, "", 2, now.Add(time.Hour), link), now.Add(6*time.Second), lim); got != outcomeReplaceTooFast {
		t.Fatalf("accept v2 six seconds later = %v, want outcomeReplaceTooFast", got)
	}
	if _, ok := s.get(id); ok {
		t.Fatal("v2 was stored inside the replacement interval")
	}

	if got := s.accept(rec(pub, card, "", 2, now.Add(time.Hour), link), now.Add(directoryReplaceInterval()), lim); got != outcomeAccepted {
		t.Fatalf("accept v2 at the interval = %v, want outcomeAccepted", got)
	}
}

// TestLastAcceptedSetCappedAtMaxRecords checks that the remembered acceptance
// instants are bounded by directory.max_records.
func TestLastAcceptedSetCappedAtMaxRecords(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 2, maxPerLink: 10, maxTopicsPerPublisher: 10}
	link := storeFP(50)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD

	s := newStore(storeFP(99))
	for i := byte(1); i <= 2; i++ {
		if got := s.accept(rec(storeFP(i), card, "", 1, now.Add(time.Second), link), now, lim); got != outcomeAccepted {
			t.Fatalf("accept publisher %d = %v, want outcomeAccepted", i, got)
		}
	}
	s.purgeExpired(now.Add(time.Second))

	for i := byte(3); i <= 4; i++ {
		if got := s.accept(rec(storeFP(i), card, "", 1, now.Add(time.Hour), link), now.Add(2*time.Second), lim); got != outcomeAccepted {
			t.Fatalf("accept publisher %d after the purge = %v, want outcomeAccepted", i, got)
		}
	}
	if got := len(s.lastAccepted); got > lim.maxRecords {
		t.Fatalf("remembered %d identifiers, want at most max_records (%d)", got, lim.maxRecords)
	}
}

// TestLastAcceptedDropsOldestEntry checks that evicting beyond
// directory.max_records drops the earliest acceptance instant and keeps the
// newest.
func TestLastAcceptedDropsOldestEntry(t *testing.T) {
	base := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	const limit = 4
	const trials = 50
	// The map is walked in varying order, so only repeated trials tell apart
	// evicting the oldest entry from evicting the last visited.
	for trial := 0; trial < trials; trial++ {
		s := newStore(storeFP(99))
		for i := 0; i <= limit; i++ {
			id := recordID{publisher: storeFP(byte(i + 1)), kind: recKindCard}
			s.rememberAccepted(id, base.Add(time.Duration(i)*time.Second), limit)
		}
		if got := len(s.lastAccepted); got != limit {
			t.Fatalf("trial %d: remembered %d identifiers, want %d", trial, got, limit)
		}
		first := recordID{publisher: storeFP(1), kind: recKindCard}
		if _, ok := s.lastAccepted[first]; ok {
			t.Fatalf("trial %d: the earliest entry survived eviction: %v", trial, s.lastAccepted)
		}
		last := recordID{publisher: storeFP(byte(limit + 1)), kind: recKindCard}
		if _, ok := s.lastAccepted[last]; !ok {
			t.Fatalf("trial %d: the newest entry was evicted: %v", trial, s.lastAccepted)
		}
	}
}

// TestReplaceIntervalLimitsLinkReplacements checks that a link replacement
// waits out the replacement interval, while a local record or a record without
// a source may be replaced at once.
func TestReplaceIntervalLimitsLinkReplacements(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 10}
	pub := storeFP(1)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const card = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
	id := recordID{publisher: pub, kind: card}

	s := newStore(storeFP(99))
	if got := s.accept(rec(pub, card, "", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept v1 = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, card, "", 2, exp, link), now.Add(time.Second), lim); got == outcomeReplaced {
		t.Fatal("replaced a record one second after accepting the old version")
	}
	held, ok := s.get(id)
	if !ok || held.version != 1 {
		t.Fatalf("held record = %v (present %v), want version 1", held, ok)
	}
	if got := s.accept(rec(pub, card, "", 3, exp, link), now.Add(60*time.Second), lim); got != outcomeReplaced {
		t.Fatalf("accept v3 at the interval = %v, want outcomeReplaced", got)
	}

	self := storeFP(7)
	own := newStore(self)
	if got := own.accept(rec(self, card, "", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept own v1 = %v, want outcomeAccepted", got)
	}
	if got := own.accept(rec(self, card, "", 2, exp, link), now.Add(time.Second), lim); got != outcomeReplaced {
		t.Fatalf("replace own v1 with v2 = %v, want outcomeReplaced", got)
	}

	loaded := newStore(storeFP(99))
	if got := loaded.accept(rec(pub, card, "", 1, exp, Fingerprint{}), now, lim); got != outcomeAccepted {
		t.Fatalf("accept a record without a source = %v, want outcomeAccepted", got)
	}
	if got := loaded.accept(rec(pub, card, "", 2, exp, link), now.Add(time.Second), lim); got != outcomeReplaced {
		t.Fatalf("replace a record without a source = %v, want outcomeReplaced", got)
	}
}
