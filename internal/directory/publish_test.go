package directory

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
)

type pubFixture struct {
	t        *testing.T
	d        *Directory
	clock    *fakeClock
	settings *fakeSettings
	ep       *fakeLinks
	net      *fakeNet
	id       Identity
	self     Fingerprint
	log      *lockedBuffer
}

func newPubFixture(t *testing.T) *pubFixture {
	t.Helper()
	return newPubFixtureWithIdentity(t, newTestIdentity(1))
}

func newPubFixtureWithIdentity(t *testing.T, id Identity) *pubFixture {
	t.Helper()
	dir := t.TempDir()
	net := newFakeNet()
	settings := newFakeSettings(dir)
	settings.SetAlias("hostA")
	ep := net.Endpoint(id.Fingerprint())
	logger, logs := newLogBuffer()
	clock := newFakeClock(dirTestNow())
	d, err := openWith(Deps{
		Identity: id,
		Peers:    newFakePeers(),
		Links:    ep,
		Settings: settings,
		Logger:   logger,
	}, clock, realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	t.Cleanup(d.Close)
	return &pubFixture{t: t, d: d, clock: clock, settings: settings, ep: ep, net: net, id: id, self: id.Fingerprint(), log: logs}
}

func (f *pubFixture) now() time.Time { return f.clock.Now() }

func (f *pubFixture) ownRecord(kind directorypb.RecordKind, object string) (*storedRecord, bool) {
	f.d.mu.RLock()
	defer f.d.mu.RUnlock()
	return f.d.st.get(recordID{publisher: f.self, kind: kind, object: object})
}

func (f *pubFixture) mustOwnRecord(kind directorypb.RecordKind, object string) *storedRecord {
	f.t.Helper()
	r, ok := f.ownRecord(kind, object)
	if !ok {
		f.t.Fatalf("no local record for kind %v object %q", kind, object)
	}
	return r
}

func (f *pubFixture) recordCount() int {
	f.d.mu.RLock()
	defer f.d.mu.RUnlock()
	return len(f.d.st.records)
}

func (f *pubFixture) activeContains(name string) bool {
	f.d.mu.RLock()
	defer f.d.mu.RUnlock()
	_, ok := f.d.active[name]
	return ok
}

func (f *pubFixture) activeLen() int {
	f.d.mu.RLock()
	defer f.d.mu.RUnlock()
	return len(f.d.active)
}

func pubOwnRecord(t *testing.T, d *Directory, kind directorypb.RecordKind, object string, version uint64, expires time.Time, payload any) *storedRecord {
	t.Helper()
	body := recTestBody(d.id.PublicKey(), kind, object, version, expires, payload)
	r, err := signRecord(body, d.id.Sign)
	if err != nil {
		t.Fatalf("sign own record: %v", err)
	}
	return r
}

func pubSeedOwn(t *testing.T, d *Directory, r *storedRecord) {
	t.Helper()
	d.mu.Lock()
	d.st.putOwn(r)
	d.dirty = true
	d.mu.Unlock()
}

func pubAcceptReturned(t *testing.T, f *pubFixture, r *storedRecord) {
	t.Helper()
	f.d.mu.Lock()
	out := f.d.st.accept(r, f.now(), f.d.limits())
	f.d.mu.Unlock()
	if out != outcomeAccepted && out != outcomeReplaced {
		t.Fatalf("accept returned %v, want accepted or replaced", out)
	}
}

type failingIdentity struct {
	Identity
	mu     sync.Mutex
	failed bool
}

func (id *failingIdentity) Sign(data []byte) []byte {
	id.mu.Lock()
	failed := id.failed
	id.mu.Unlock()
	if failed {
		return nil
	}
	return id.Identity.Sign(data)
}

func (id *failingIdentity) SetFailSign(failed bool) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.failed = failed
}

// TestPrivateByDefaultIssuesNothing checks that a Broker that was not made public issues nothing.
func TestPrivateByDefaultIssuesNothing(t *testing.T) {
	f := newPubFixture(t)
	peer := newTestIdentity(2).Fingerprint()
	f.net.Endpoint(peer)
	f.d.trackLinks()
	f.net.Connect(f.self, peer)
	f.net.Settle(t)

	f.d.startupCheck(f.now())
	f.net.Settle(t)

	if n := f.recordCount(); n != 0 {
		t.Fatalf("startup issued %d records while private, want 0", n)
	}
	f.d.mu.RLock()
	public := f.d.public
	f.d.mu.RUnlock()
	if public {
		t.Fatal("startupCheck made the directory public")
	}
	for _, fr := range f.ep.Sent() {
		if fr.kind != directorypb.KindRecordList {
			t.Fatalf("sent frame kind %d, want only RecordList", fr.kind)
		}
		for _, v := range decodeRecordList(t, fr.payload).Records {
			var publisher Fingerprint
			copy(publisher[:], v.GetId().GetPublisher())
			if publisher == f.self {
				t.Fatal("a sent frame carries the local publisher")
			}
		}
	}
}

// TestSetPublicIssuesCard checks that the card carries the configured alias and addresses.
func TestSetPublicIssuesCard(t *testing.T) {
	f := newPubFixture(t)
	addrs := []config.AdvertisedAddr{
		{Kind: "direct", Addr: "b.example:443"},
		{Kind: "direct", Addr: "b.vpn.example:17443"},
	}
	f.settings.SetAdvertised(addrs)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}

	r := f.mustOwnRecord(recKindCard, "")
	if r.id.publisher != f.self {
		t.Fatalf("card publisher = %v, want %v", r.id.publisher, f.self)
	}
	if !bytes.Equal(r.body.GetPublicKey(), f.id.PublicKey()) {
		t.Fatal("card public key differs from the identity's")
	}
	card := r.body.GetBrokerCard()
	if card == nil {
		t.Fatal("card payload is missing")
	}
	if card.GetAlias() != "hostA" {
		t.Fatalf("card alias = %q, want %q", card.GetAlias(), "hostA")
	}
	got := card.GetAddresses()
	if len(got) != len(addrs) {
		t.Fatalf("card has %d addresses, want %d", len(got), len(addrs))
	}
	for i, want := range addrs {
		if got[i].GetKind() != want.Kind || got[i].GetAddr() != want.Addr {
			t.Errorf("address %d = {%q %q}, want {%q %q}", i,
				got[i].GetKind(), got[i].GetAddr(), want.Kind, want.Addr)
		}
	}

	t.Run("empty address list", func(t *testing.T) {
		f := newPubFixture(t)
		if err := f.d.SetPublic(true); err != nil {
			t.Fatalf("SetPublic(true): %v", err)
		}
		r := f.mustOwnRecord(recKindCard, "")
		card := r.body.GetBrokerCard()
		if card == nil {
			t.Fatal("card payload is missing")
		}
		if n := len(card.GetAddresses()); n != 0 {
			t.Fatalf("card has %d addresses, want 0", n)
		}
	})
}

// TestSetPublicTwiceIsNoop checks that a second SetPublic(true) issues no new version.
func TestSetPublicTwiceIsNoop(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	first := f.mustOwnRecord(recKindCard, "").version
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("second SetPublic(true): %v", err)
	}
	if got := f.mustOwnRecord(recKindCard, "").version; got != first {
		t.Fatalf("second SetPublic(true) issued version %d, want the unchanged %d", got, first)
	}
	if n := f.recordCount(); n != 1 {
		t.Fatalf("directory holds %d records, want 1", n)
	}
}

// TestSetPublicFalseRevokesAll checks that going private revokes the card and every public topic declaration.
func TestSetPublicFalseRevokesAll(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if err := f.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic(infra-help): %v", err)
	}
	if err := f.d.DeclarePublicTopic("ops", ""); err != nil {
		t.Fatalf("DeclarePublicTopic(ops): %v", err)
	}

	ids := []recordID{
		{publisher: f.self, kind: recKindCard},
		{publisher: f.self, kind: recKindTopic, object: "infra-help"},
		{publisher: f.self, kind: recKindTopic, object: "ops"},
	}
	before := make(map[recordID]uint64, len(ids))
	for _, id := range ids {
		before[id] = f.mustOwnRecord(id.kind, id.object).version
	}

	if err := f.d.SetPublic(false); err != nil {
		t.Fatalf("SetPublic(false): %v", err)
	}
	now := f.now()
	for _, id := range ids {
		r := f.mustOwnRecord(id.kind, id.object)
		if !r.isRevocation() {
			t.Errorf("record %v is not a revocation after going private", id)
		}
		if r.version <= before[id] {
			t.Errorf("revocation version %d is not above the original %d", r.version, before[id])
		}
		if !r.expires.Equal(now.Add(recordTTL)) {
			t.Errorf("revocation expires at %v, want %v", r.expires, now.Add(recordTTL))
		}
	}
	if n := f.activeLen(); n != 0 {
		t.Fatalf("active set has %d entries after going private, want 0", n)
	}
}

// TestSetPublicStaysPrivateWhenSigningFails checks that a card that cannot be signed leaves the Broker private and returns no error.
func TestSetPublicStaysPrivateWhenSigningFails(t *testing.T) {
	f := newPubFixture(t)
	f.settings.SetAlias("")
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true) with an unsignable card returned %v, want nil", err)
	}
	f.d.mu.RLock()
	public := f.d.public
	f.d.mu.RUnlock()
	if public {
		t.Fatal("the directory is public although no card was issued")
	}
	if _, ok := f.ownRecord(recKindCard, ""); ok {
		t.Fatal("a card was stored although signing failed")
	}
	if got := f.log.String(); !strings.Contains(got, "level=WARN") {
		t.Fatalf("no warning was logged for the failed issuance: %s", got)
	}

	f.settings.SetAlias("hostA")
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true) after fixing the alias: %v", err)
	}
	card := f.mustOwnRecord(recKindCard, "")
	if card.isRevocation() || card.body.GetBrokerCard().GetAlias() != "hostA" {
		t.Fatalf("stored record is not the published card: %v", card.body)
	}
}

// TestSetPublicFalseRetriesRevocations checks that a revocation that failed to sign is issued by a later maintenance pass.
func TestSetPublicFalseRetriesRevocations(t *testing.T) {
	id := &failingIdentity{Identity: newTestIdentity(1)}
	f := newPubFixtureWithIdentity(t, id)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if err := f.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic: %v", err)
	}
	cardBefore := f.mustOwnRecord(recKindCard, "").version
	annBefore := f.mustOwnRecord(recKindTopic, "infra-help").version

	id.SetFailSign(true)
	if err := f.d.SetPublic(false); err != nil {
		t.Fatalf("SetPublic(false): %v", err)
	}
	if r := f.mustOwnRecord(recKindCard, ""); r.isRevocation() {
		t.Fatal("a card revocation was stored although signing failed")
	}
	if r := f.mustOwnRecord(recKindTopic, "infra-help"); r.isRevocation() {
		t.Fatal("a declaration revocation was stored although signing failed")
	}

	id.SetFailSign(false)
	f.d.maintain(f.now(), f.now())

	card := f.mustOwnRecord(recKindCard, "")
	if !card.isRevocation() || card.version <= cardBefore {
		t.Fatalf("card after one maintenance pass: revocation=%v version=%d, want a revocation above %d",
			card.isRevocation(), card.version, cardBefore)
	}
	ann := f.mustOwnRecord(recKindTopic, "infra-help")
	if !ann.isRevocation() || ann.version <= annBefore {
		t.Fatalf("declaration after one maintenance pass: revocation=%v version=%d, want a revocation above %d",
			ann.isRevocation(), ann.version, annBefore)
	}
}

// TestRevocationOutlivesRevokedRecord checks that a revocation does not expire before the record it revokes.
func TestRevocationOutlivesRevokedRecord(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	card := f.mustOwnRecord(recKindCard, "")
	oldVersion, oldExpires := card.version, card.expires

	f.clock.Advance(24 * time.Hour)
	f.clock.Advance(-48 * time.Hour)
	if err := f.d.SetPublic(false); err != nil {
		t.Fatalf("SetPublic(false): %v", err)
	}

	rev := f.mustOwnRecord(recKindCard, "")
	if !rev.isRevocation() {
		t.Fatal("the card was not revoked")
	}
	if !rev.expires.Equal(oldExpires) {
		t.Fatalf("revocation expires at %v, want the revoked card's expiry %v", rev.expires, oldExpires)
	}

	f.clock.Advance(rev.expires.Sub(f.now()))
	f.d.maintain(f.now(), f.now())

	old := pubOwnRecord(t, f.d, recKindCard, "", oldVersion, oldExpires, recTestCard("hostA"))
	f.d.mu.Lock()
	out := f.d.st.accept(old, f.now(), f.d.limits())
	f.d.mu.Unlock()
	if out != outcomeExpired {
		t.Fatalf("accepting the old card after the revocation expired returned %v, want it rejected", out)
	}
}

// TestDeclareWhenPrivateFails checks that a private Broker cannot declare a public topic.
func TestDeclareWhenPrivateFails(t *testing.T) {
	f := newPubFixture(t)
	before := f.recordCount()
	err := f.d.DeclarePublicTopic("infra-help", "ask here")
	if !errors.Is(err, ErrNotPublic) {
		t.Fatalf("DeclarePublicTopic error = %v, want ErrNotPublic", err)
	}
	if n := f.recordCount(); n != before {
		t.Fatalf("directory holds %d records, want the unchanged %d", n, before)
	}
	if _, ok := f.ownRecord(recKindTopic, "infra-help"); ok {
		t.Fatal("a declaration was stored while private")
	}
	if f.activeContains("infra-help") {
		t.Fatal("the active set changed while private")
	}
}

// TestDeclareQuota checks the per-publisher quota, re-declaration and reuse after retiring.
func TestDeclareQuota(t *testing.T) {
	f := newPubFixture(t)
	f.settings.SetMaxTopicsPerPublisher(2)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if err := f.d.DeclarePublicTopic("a", "one"); err != nil {
		t.Fatalf("DeclarePublicTopic(a): %v", err)
	}
	if err := f.d.DeclarePublicTopic("b", "two"); err != nil {
		t.Fatalf("DeclarePublicTopic(b): %v", err)
	}
	if err := f.d.DeclarePublicTopic("c", "three"); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("DeclarePublicTopic(c) error = %v, want ErrQuotaExceeded", err)
	}

	aBefore := f.mustOwnRecord(recKindTopic, "a")
	if err := f.d.DeclarePublicTopic("a", "one-updated"); err != nil {
		t.Fatalf("re-declaring a: %v", err)
	}
	aAfter := f.mustOwnRecord(recKindTopic, "a")
	if aAfter.version <= aBefore.version {
		t.Fatalf("re-declared a version %d is not above %d", aAfter.version, aBefore.version)
	}
	if got := aAfter.body.GetTopicAnnouncement().GetDescription(); got != "one-updated" {
		t.Fatalf("re-declared description = %q, want %q", got, "one-updated")
	}

	f.d.RetirePublicTopic("b")
	if err := f.d.DeclarePublicTopic("c", "three"); err != nil {
		t.Fatalf("DeclarePublicTopic(c) after retiring b: %v", err)
	}
}

// TestDeclareEmptyName checks that an empty topic name is rejected.
func TestDeclareEmptyName(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.DeclarePublicTopic("", "description"); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("DeclarePublicTopic(\"\") error = %v, want ErrInvalidTopic", err)
	}
	if n := f.activeLen(); n != 0 {
		t.Fatalf("active set has %d entries, want 0", n)
	}
}

// TestDeclareInvalidDescription checks that an invalid description is rejected and a valid one accepted.
func TestDeclareInvalidDescription(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	base := f.recordCount()

	invalid := []string{
		strings.Repeat("a", 257),
		"a\x00b",
		"\xff",
		"a\u0085b",
	}
	for _, description := range invalid {
		if err := f.d.DeclarePublicTopic("topic", description); !errors.Is(err, ErrInvalidDescription) {
			t.Fatalf("DeclarePublicTopic(%q) error = %v, want ErrInvalidDescription", description, err)
		}
		if n := f.recordCount(); n != base {
			t.Fatalf("directory holds %d records after an invalid description, want %d", n, base)
		}
		if f.activeContains("topic") {
			t.Fatal("an invalid description entered the active set")
		}
	}

	valid := []struct {
		name        string
		description string
	}{
		{"topic-256", strings.Repeat("a", 256)},
		{"topic-unicode", "with space and 中文"},
		{"topic-empty", ""},
	}
	for _, tc := range valid {
		if err := f.d.DeclarePublicTopic(tc.name, tc.description); err != nil {
			t.Fatalf("DeclarePublicTopic(%q): %v", tc.name, err)
		}
		if !f.activeContains(tc.name) {
			t.Fatalf("%q is missing from the active set", tc.name)
		}
		r := f.mustOwnRecord(recKindTopic, tc.name)
		if got := r.body.GetTopicAnnouncement().GetDescription(); got != tc.description {
			t.Fatalf("stored description for %q = %q, want %q", tc.name, got, tc.description)
		}
	}
}

// TestRetireTopic checks that retiring revokes a declaration and re-declaring issues a new version.
func TestRetireTopic(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if err := f.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic: %v", err)
	}
	annVersion := f.mustOwnRecord(recKindTopic, "infra-help").version

	f.d.RetirePublicTopic("infra-help")
	rev := f.mustOwnRecord(recKindTopic, "infra-help")
	if !rev.isRevocation() {
		t.Fatal("the retired declaration is not a revocation")
	}
	if rev.version <= annVersion {
		t.Fatalf("revocation version %d is not above the declaration's %d", rev.version, annVersion)
	}
	if f.activeContains("infra-help") {
		t.Fatal("the retired topic is still active")
	}

	if err := f.d.DeclarePublicTopic("infra-help", "ask again"); err != nil {
		t.Fatalf("re-declaring the topic: %v", err)
	}
	again := f.mustOwnRecord(recKindTopic, "infra-help")
	if again.isRevocation() {
		t.Fatal("the re-declared topic is still a revocation")
	}
	if again.version <= rev.version {
		t.Fatalf("re-declared version %d is not above the revocation's %d", again.version, rev.version)
	}

	before := f.recordCount()
	f.d.RetirePublicTopic("never-declared")
	if n := f.recordCount(); n != before {
		t.Fatalf("retiring an undeclared name changed the record count from %d to %d", before, n)
	}
}

// TestReissueOnAddressChange checks that Reissue publishes the new address list.
func TestReissueOnAddressChange(t *testing.T) {
	f := newPubFixture(t)
	f.settings.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}})
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	first := f.mustOwnRecord(recKindCard, "")

	f.settings.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.9:17443"}})
	f.d.Reissue()
	second := f.mustOwnRecord(recKindCard, "")
	if second.version <= first.version {
		t.Fatalf("reissued version %d is not above the original %d", second.version, first.version)
	}
	addrs := second.body.GetBrokerCard().GetAddresses()
	if len(addrs) != 1 || addrs[0].GetAddr() != "198.51.100.9:17443" {
		t.Fatalf("reissued card addresses = %v, want the new address", addrs)
	}

	t.Run("private", func(t *testing.T) {
		f := newPubFixture(t)
		f.d.Reissue()
		if n := f.recordCount(); n != 0 {
			t.Fatalf("Reissue while private stored %d records, want 0", n)
		}
	})
}

// TestReissueRetriesAfterSigningFailure checks that a re-sign that failed is retried by the maintenance loop.
func TestReissueRetriesAfterSigningFailure(t *testing.T) {
	id := &failingIdentity{Identity: newTestIdentity(1)}
	f := newPubFixtureWithIdentity(t, id)
	f.settings.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}})
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	first := f.mustOwnRecord(recKindCard, "")

	f.settings.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.9:17443"}})
	id.SetFailSign(true)
	f.d.Reissue()
	if got := f.mustOwnRecord(recKindCard, "").version; got != first.version {
		t.Fatalf("Reissue stored version %d although signing failed, want the old %d", got, first.version)
	}

	id.SetFailSign(false)
	f.d.renewDue(f.now())

	card := f.mustOwnRecord(recKindCard, "")
	if card.version <= first.version {
		t.Fatalf("card version %d after the retry, want above %d", card.version, first.version)
	}
	addrs := card.body.GetBrokerCard().GetAddresses()
	if len(addrs) != 1 || addrs[0].GetAddr() != "198.51.100.9:17443" {
		t.Fatalf("retried card addresses = %v, want the address that failed to publish", addrs)
	}

	stable := card.version
	f.d.renewDue(f.now())
	if got := f.mustOwnRecord(recKindCard, "").version; got != stable {
		t.Fatalf("renewDue reissued version %d without a pending re-sign, want %d", got, stable)
	}
}

// TestRenewDue checks that a card and an active declaration renew after a day while inactive and private ones do not.
func TestRenewDue(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if err := f.d.DeclarePublicTopic("topic-a", "desc-a"); err != nil {
		t.Fatalf("DeclarePublicTopic: %v", err)
	}
	loaded := pubOwnRecord(t, f.d, recKindTopic, "loaded", 500, f.now().Add(recordTTL),
		&directorypb.TopicAnnouncement{Name: "loaded", Description: "old"})
	pubSeedOwn(t, f.d, loaded)

	cardVersion := f.mustOwnRecord(recKindCard, "").version
	annVersion := f.mustOwnRecord(recKindTopic, "topic-a").version

	f.clock.Advance(23 * time.Hour)
	f.d.renewDue(f.now())
	if got := f.mustOwnRecord(recKindCard, "").version; got != cardVersion {
		t.Fatalf("card was renewed after 23 hours (version %d, want %d)", got, cardVersion)
	}
	if got := f.mustOwnRecord(recKindTopic, "topic-a").version; got != annVersion {
		t.Fatalf("declaration was renewed after 23 hours (version %d, want %d)", got, annVersion)
	}

	f.clock.Advance(time.Hour)
	now := f.now()
	f.d.renewDue(now)

	card := f.mustOwnRecord(recKindCard, "")
	if card.version <= cardVersion {
		t.Fatalf("card version %d was not renewed", card.version)
	}
	if !card.expires.Equal(now.Add(recordTTL)) {
		t.Fatalf("renewed card expires at %v, want %v", card.expires, now.Add(recordTTL))
	}
	ann := f.mustOwnRecord(recKindTopic, "topic-a")
	if ann.version <= annVersion {
		t.Fatalf("declaration version %d was not renewed", ann.version)
	}
	if got := ann.body.GetTopicAnnouncement().GetDescription(); got != "desc-a" {
		t.Fatalf("renewed declaration description = %q, want %q", got, "desc-a")
	}
	if !ann.expires.Equal(now.Add(recordTTL)) {
		t.Fatalf("renewed declaration expires at %v, want %v", ann.expires, now.Add(recordTTL))
	}
	if got := f.mustOwnRecord(recKindTopic, "loaded").version; got != 500 {
		t.Fatalf("a declaration outside the active set was renewed (version %d)", got)
	}

	t.Run("private", func(t *testing.T) {
		f := newPubFixture(t)
		card := pubOwnRecord(t, f.d, recKindCard, "", 500, f.now().Add(recordTTL), recTestCard("hostA"))
		pubSeedOwn(t, f.d, card)
		f.clock.Advance(24 * time.Hour)
		f.d.renewDue(f.now())
		if got := f.mustOwnRecord(recKindCard, "").version; got != 500 {
			t.Fatalf("a private Broker renewed its card (version %d)", got)
		}
	})
}

// TestStartupRevokesWhenPrivate checks that a private startup revokes every live local record.
func TestStartupRevokesWhenPrivate(t *testing.T) {
	f := newPubFixture(t)
	now := f.now()
	card := pubOwnRecord(t, f.d, recKindCard, "", 100, now.Add(recordTTL), recTestCard("hostA"))
	ann := pubOwnRecord(t, f.d, recKindTopic, "infra-help", 101, now.Add(recordTTL),
		&directorypb.TopicAnnouncement{Name: "infra-help", Description: "ask here"})
	pubSeedOwn(t, f.d, card)
	pubSeedOwn(t, f.d, ann)

	f.settings.SetPublic(false)
	f.d.startupCheck(now)

	for _, tc := range []struct {
		id      recordID
		version uint64
	}{
		{card.id, 100},
		{ann.id, 101},
	} {
		r := f.mustOwnRecord(tc.id.kind, tc.id.object)
		if !r.isRevocation() {
			t.Errorf("record %v is not a revocation after a private startup", tc.id)
		}
		if r.version <= tc.version {
			t.Errorf("revocation of %v has version %d, want above the original %d", tc.id, r.version, tc.version)
		}
	}
}

// TestStartupReissuesWhenPublic checks that a public startup issues a fresh card and leaves loaded declarations alone.
func TestStartupReissuesWhenPublic(t *testing.T) {
	f := newPubFixture(t)
	now := f.now()
	issued := now.Add(-48 * time.Hour)
	card := pubOwnRecord(t, f.d, recKindCard, "", 100, issued.Add(recordTTL), recTestCard("old"))
	loaded := pubOwnRecord(t, f.d, recKindTopic, "loaded", 101, now.Add(recordTTL),
		&directorypb.TopicAnnouncement{Name: "loaded", Description: "from disk"})
	pubSeedOwn(t, f.d, card)
	pubSeedOwn(t, f.d, loaded)

	f.settings.SetPublic(true)
	f.d.startupCheck(now)

	cur := f.mustOwnRecord(recKindCard, "")
	if cur.isRevocation() {
		t.Fatal("a public startup revoked the card")
	}
	if cur.version <= 100 {
		t.Fatalf("startup card version %d is not above the old %d", cur.version, 100)
	}
	if got := cur.body.GetBrokerCard().GetAlias(); got != "hostA" {
		t.Fatalf("startup card alias = %q, want %q", got, "hostA")
	}
	if !cur.expires.Equal(now.Add(recordTTL)) {
		t.Fatalf("startup card expires at %v, want %v", cur.expires, now.Add(recordTTL))
	}

	loadedCur := f.mustOwnRecord(recKindTopic, "loaded")
	if loadedCur.isRevocation() {
		t.Fatal("a public startup revoked a loaded declaration")
	}
	if loadedCur.version != 101 {
		t.Fatalf("a loaded declaration was reissued (version %d, want 101)", loadedCur.version)
	}
	if n := f.activeLen(); n != 0 {
		t.Fatalf("the active set has %d entries after a restart, want 0", n)
	}
}

// TestRetireInactiveTopics checks that declarations outside the active set are revoked.
func TestRetireInactiveTopics(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if err := f.d.DeclarePublicTopic("y", "desc-y"); err != nil {
		t.Fatalf("DeclarePublicTopic(y): %v", err)
	}
	loaded := pubOwnRecord(t, f.d, recKindTopic, "x", 500, f.now().Add(recordTTL),
		&directorypb.TopicAnnouncement{Name: "x", Description: "from disk"})
	pubSeedOwn(t, f.d, loaded)

	f.d.RetireInactiveTopics()

	x := f.mustOwnRecord(recKindTopic, "x")
	if !x.isRevocation() {
		t.Fatal("the loaded declaration was not revoked")
	}
	if x.version <= 500 {
		t.Fatalf("revocation version %d is not above the loaded %d", x.version, 500)
	}
	y := f.mustOwnRecord(recKindTopic, "y")
	if y.isRevocation() {
		t.Fatal("the active declaration was revoked")
	}
	if got := y.body.GetTopicAnnouncement().GetDescription(); got != "desc-y" {
		t.Fatalf("active declaration description = %q, want %q", got, "desc-y")
	}
}

// TestReconcileOwnTable checks each returned-record case against the local state.
func TestReconcileOwnTable(t *testing.T) {
	cardID := func(f *pubFixture) recordID {
		return recordID{publisher: f.self, kind: recKindCard}
	}
	topicID := func(f *pubFixture, name string) recordID {
		return recordID{publisher: f.self, kind: recKindTopic, object: name}
	}
	returnedVersion := func(f *pubFixture) uint64 {
		return uint64(f.now().UnixMilli()) + 1_000_000
	}
	acceptCard := func(t *testing.T, f *pubFixture) {
		r := pubOwnRecord(t, f.d, recKindCard, "", returnedVersion(f), f.now().Add(recordTTL), recTestCard("from-backup"))
		pubAcceptReturned(t, f, r)
	}
	acceptTopic := func(t *testing.T, f *pubFixture, name, description string) {
		r := pubOwnRecord(t, f.d, recKindTopic, name, returnedVersion(f), f.now().Add(recordTTL),
			&directorypb.TopicAnnouncement{Name: name, Description: description})
		pubAcceptReturned(t, f, r)
	}
	acceptRevocation := func(t *testing.T, f *pubFixture, id recordID) {
		r := pubOwnRecord(t, f.d, id.kind, id.object, returnedVersion(f), f.now().Add(recordTTL),
			&directorypb.Revocation{RevokedKind: id.kind})
		pubAcceptReturned(t, f, r)
	}

	cases := []struct {
		name            string
		setup           func(t *testing.T, f *pubFixture) recordID
		wantIssued      bool
		wantRevoked     bool
		wantCard        bool
		wantDescription string
	}{
		{
			name: "card while public",
			setup: func(t *testing.T, f *pubFixture) recordID {
				f.settings.SetPublic(true)
				f.d.startupCheck(f.now())
				acceptCard(t, f)
				return cardID(f)
			},
			wantIssued: true,
			wantCard:   true,
		},
		{
			name: "card while private",
			setup: func(t *testing.T, f *pubFixture) recordID {
				acceptCard(t, f)
				return cardID(f)
			},
			wantIssued:  true,
			wantRevoked: true,
		},
		{
			name: "announcement while public and active",
			setup: func(t *testing.T, f *pubFixture) recordID {
				if err := f.d.SetPublic(true); err != nil {
					t.Fatalf("SetPublic(true): %v", err)
				}
				if err := f.d.DeclarePublicTopic("topic-a", "desc-a"); err != nil {
					t.Fatalf("DeclarePublicTopic: %v", err)
				}
				acceptTopic(t, f, "topic-a", "stale")
				return topicID(f, "topic-a")
			},
			wantIssued:      true,
			wantDescription: "desc-a",
		},
		{
			name: "announcement while public and inactive",
			setup: func(t *testing.T, f *pubFixture) recordID {
				if err := f.d.SetPublic(true); err != nil {
					t.Fatalf("SetPublic(true): %v", err)
				}
				acceptTopic(t, f, "ghost", "stale")
				return topicID(f, "ghost")
			},
			wantIssued:  true,
			wantRevoked: true,
		},
		{
			name: "announcement while private",
			setup: func(t *testing.T, f *pubFixture) recordID {
				acceptTopic(t, f, "topic-a", "stale")
				return topicID(f, "topic-a")
			},
			wantIssued:  true,
			wantRevoked: true,
		},
		{
			name: "revocation of the card while public",
			setup: func(t *testing.T, f *pubFixture) recordID {
				if err := f.d.SetPublic(true); err != nil {
					t.Fatalf("SetPublic(true): %v", err)
				}
				id := cardID(f)
				acceptRevocation(t, f, id)
				return id
			},
			wantIssued: true,
			wantCard:   true,
		},
		{
			name: "revocation of an active announcement while public",
			setup: func(t *testing.T, f *pubFixture) recordID {
				if err := f.d.SetPublic(true); err != nil {
					t.Fatalf("SetPublic(true): %v", err)
				}
				if err := f.d.DeclarePublicTopic("topic-a", "desc-a"); err != nil {
					t.Fatalf("DeclarePublicTopic: %v", err)
				}
				id := topicID(f, "topic-a")
				acceptRevocation(t, f, id)
				return id
			},
			wantIssued:      true,
			wantDescription: "desc-a",
		},
		{
			name: "revocation of the card while private",
			setup: func(t *testing.T, f *pubFixture) recordID {
				id := cardID(f)
				acceptRevocation(t, f, id)
				return id
			},
			wantRevoked: true,
		},
		{
			name: "revocation of an inactive announcement while public",
			setup: func(t *testing.T, f *pubFixture) recordID {
				if err := f.d.SetPublic(true); err != nil {
					t.Fatalf("SetPublic(true): %v", err)
				}
				id := topicID(f, "ghost")
				acceptRevocation(t, f, id)
				return id
			},
			wantRevoked: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := newPubFixture(t)
			id := tc.setup(t, f)
			before := f.mustOwnRecord(id.kind, id.object).version

			f.d.reconcileOwn([]recordID{id})
			after := f.mustOwnRecord(id.kind, id.object)

			if tc.wantIssued {
				if after.version <= before {
					t.Errorf("version stayed at %d, want a new record", before)
				}
			} else if after.version != before {
				t.Errorf("version changed from %d to %d, want no issuance", before, after.version)
			}
			if got := after.isRevocation(); got != tc.wantRevoked {
				t.Errorf("isRevocation = %v, want %v", got, tc.wantRevoked)
			}
			if tc.wantCard && after.body.GetBrokerCard() == nil {
				t.Error("record is not a card")
			}
			if tc.wantDescription != "" {
				ann := after.body.GetTopicAnnouncement()
				if ann == nil || ann.GetDescription() != tc.wantDescription {
					t.Errorf("announcement = %v, want description %q", ann, tc.wantDescription)
				}
			}
		})
	}
}

// TestVersionMonotonicAcrossClockRollback checks that a clock rollback does not lower the next version.
func TestVersionMonotonicAcrossClockRollback(t *testing.T) {
	f := newPubFixture(t)
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	first := f.mustOwnRecord(recKindCard, "").version

	f.clock.Advance(-time.Hour)
	f.d.Reissue()
	second := f.mustOwnRecord(recKindCard, "").version
	if second <= first {
		t.Fatalf("version after a clock rollback is %d, want above %d", second, first)
	}
}

// TestPublishBroadcastsOutsideLock checks that publishing calls Send without holding the directory lock.
func TestPublishBroadcastsOutsideLock(t *testing.T) {
	f := newPubFixture(t)
	peer := newTestIdentity(2).Fingerprint()
	f.net.Endpoint(peer)
	f.d.trackLinks()
	f.net.Connect(f.self, peer)
	f.net.Settle(t)

	var hooked bool
	f.ep.SetSendHook(func(Fingerprint, uint32) {
		hooked = true
		if !f.d.mu.TryLock() {
			t.Error("Send called while the directory lock is held")
			return
		}
		f.d.mu.Unlock()
	})
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if !hooked {
		t.Fatal("SetPublic did not send the new card")
	}
}

// TestPublishPersistsBeforeBroadcast checks that a locally issued record is written to disk before it is announced.
func TestPublishPersistsBeforeBroadcast(t *testing.T) {
	f := newPubFixture(t)
	peer := newTestIdentity(2).Fingerprint()
	f.net.Endpoint(peer)
	f.d.trackLinks()
	f.net.Connect(f.self, peer)
	f.net.Settle(t)

	var checked bool
	f.ep.SetSendHook(func(_ Fingerprint, kind uint32) {
		if kind != directorypb.KindRecordList || checked {
			return
		}
		checked = true
		recs, _, err := loadFile(realFileOps{}, f.settings.dir)
		if err != nil {
			t.Errorf("load directory file: %v", err)
			return
		}
		if len(recs) == 0 {
			t.Error("the local record was broadcast before it was written to disk")
		}
	})
	if err := f.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	if !checked {
		t.Fatal("no RecordList was sent for the new card")
	}
}
