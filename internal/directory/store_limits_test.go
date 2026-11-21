package directory

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

// TestPublisherLimitExcludesCard checks that a Broker card does not count
// against the per-publisher limit: a publisher holding its full quota of public
// declarations still has its card accepted, while one more declaration is
// rejected.
func TestPublisherLimitExcludesCard(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 100, maxPerLink: 100, maxTopicsPerPublisher: 16}
	s := newStore(storeFP(99))
	pub := storeFP(1)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const (
		card  = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
		topic = directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT
	)

	for i := 0; i < lim.maxTopicsPerPublisher; i++ {
		if got := s.accept(rec(pub, topic, fmt.Sprintf("topic-%02d", i), 1, exp, link), now, lim); got != outcomeAccepted {
			t.Fatalf("accept declaration %d = %v, want outcomeAccepted", i, got)
		}
	}
	if got := s.accept(rec(pub, card, "", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept the card at the declaration limit = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, topic, "one-too-many", 1, exp, link), now, lim); got != outcomeQuotaPublisher {
		t.Fatalf("accept a declaration past the limit = %v, want outcomeQuotaPublisher", got)
	}
}

// TestPublisherLimitCountsDeclarationsAndUnknownKinds checks that the
// per-publisher limit counts public topic declarations and records of kinds
// this version does not understand, so a publisher cannot bypass it with
// another kind. Broker cards and revocations do not count, and a replacement
// is never subject to the limit.
func TestPublisherLimitCountsDeclarationsAndUnknownKinds(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 2}
	s := newStore(storeFP(99))
	pub := storeFP(1)
	other := storeFP(2)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const (
		card    = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
		topic   = directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT
		unknown = directorypb.RecordKind(7)
	)

	if got := s.accept(rec(pub, card, "", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept the card = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, topic, "a", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept the first declaration = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, topic, "b", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept the second declaration = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, unknown, "x", 1, exp, link), now, lim); got != outcomeQuotaPublisher {
		t.Fatalf("accept an unknown kind at the limit = %v, want outcomeQuotaPublisher", got)
	}
	first := now.Add(directoryReplaceInterval())
	second := first.Add(directoryReplaceInterval())
	if got := s.accept(rec(pub, topic, "b", 2, exp, link), first, lim); got != outcomeReplaced {
		t.Fatalf("replace a declaration at the limit = %v, want outcomeReplaced", got)
	}
	if got := s.accept(revocation(pub, topic, "a", 2, exp, link), first, lim); got != outcomeReplaced {
		t.Fatalf("replace the declaration with a revocation = %v, want outcomeReplaced", got)
	}
	if got := s.accept(rec(pub, unknown, "x", 1, exp, link), first, lim); got != outcomeAccepted {
		t.Fatalf("accept an unknown kind after a revocation freed a slot = %v, want outcomeAccepted", got)
	}
	if got := s.accept(revocation(pub, card, "", 2, exp, link), first, lim); got != outcomeReplaced {
		t.Fatalf("replace the card with a revocation = %v, want outcomeReplaced", got)
	}
	if got := s.accept(rec(pub, card, "", 3, exp, link), second, lim); got != outcomeReplaced {
		t.Fatalf("replace the card with a newer card = %v, want outcomeReplaced", got)
	}
	if got := s.accept(rec(pub, topic, "c", 1, exp, link), second, lim); got != outcomeQuotaPublisher {
		t.Fatalf("accept a declaration when the limit is full again = %v, want outcomeQuotaPublisher", got)
	}
	if got := s.accept(rec(other, card, "", 1, exp, link), second, lim); got != outcomeAccepted {
		t.Fatalf("accept a record from another publisher = %v, want outcomeAccepted", got)
	}
}

// TestPublisherTopicLimitIgnoresRevocations checks that a revocation does not
// occupy the slot of the declaration it replaces: with a limit of two, two
// declarations are accepted, a third is rejected, and after one declaration is
// revoked the third is accepted again.
func TestPublisherTopicLimitIgnoresRevocations(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 2}
	s := newStore(storeFP(99))
	pub := storeFP(1)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const topic = directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT

	if got := s.accept(rec(pub, topic, "a", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept a = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, topic, "b", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept b = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, topic, "c", 1, exp, link), now, lim); got != outcomeQuotaPublisher {
		t.Fatalf("accept c at quota = %v, want outcomeQuotaPublisher", got)
	}
	if got := s.accept(revocation(pub, topic, "a", 2, exp, link), now.Add(directoryReplaceInterval()), lim); got != outcomeReplaced {
		t.Fatalf("revoke a = %v, want outcomeReplaced", got)
	}
	if got := s.accept(rec(pub, topic, "c", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept c after the revocation = %v, want outcomeAccepted", got)
	}
	if got := s.perPublisher[pub]; got != 2 {
		t.Fatalf("perPublisher[pub] = %d, want 2", got)
	}
}

// TestPublisherTopicLimitCardFirst checks that a card accepted before any
// declaration does not take one of the publisher's slots: with a limit of two,
// the card and two declarations are all accepted.
func TestPublisherTopicLimitCardFirst(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	lim := limits{maxRecords: 10, maxPerLink: 10, maxTopicsPerPublisher: 2}
	s := newStore(storeFP(99))
	pub := storeFP(1)
	link := storeFP(50)
	exp := now.Add(recordTTL)
	const (
		card  = directorypb.RecordKind_RECORD_KIND_BROKER_CARD
		topic = directorypb.RecordKind_RECORD_KIND_TOPIC_ANNOUNCEMENT
	)

	if got := s.accept(rec(pub, card, "", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept the card = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, topic, "a", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept the first declaration = %v, want outcomeAccepted", got)
	}
	if got := s.accept(rec(pub, topic, "b", 1, exp, link), now, lim); got != outcomeAccepted {
		t.Fatalf("accept the second declaration = %v, want outcomeAccepted", got)
	}
}

// TestOpenWarnsOnExpiredLoadedRecords checks that a record dropped at load time
// because it has expired is counted in the "dropped invalid records" warning.
func TestOpenWarnsOnExpiredLoadedRecords(t *testing.T) {
	dir := t.TempDir()
	expired := mustSignRecord(t, 61, recKindCard, "", 1, dirTestNow().Add(-time.Hour), recTestCard("hostGone"))
	if err := saveFile(realFileOps{}, dir, 0, toFileRecords([]*storedRecord{expired})); err != nil {
		t.Fatalf("save directory file: %v", err)
	}

	logger, logs := newLogBuffer()
	deps, _ := dirTestDeps(t, 1, dir, newFakeNet(), logger)
	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("open directory: %v", err)
	}
	t.Cleanup(d.Close)

	if got := len(d.st.all()); got != 0 {
		t.Fatalf("loaded %d records, want 0", got)
	}
	out := logs.String()
	if !strings.Contains(out, "dropped invalid records") {
		t.Fatalf("log does not carry the dropped-records warning: %s", out)
	}
	if !strings.Contains(out, "count=1") {
		t.Fatalf("log does not carry count=1: %s", out)
	}
}
