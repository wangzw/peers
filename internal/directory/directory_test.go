package directory

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

// eventTimeout is the package-wide fallback for a wait on an event that the
// production code signals on a channel, or on a real timer, and that the test
// cannot force. It only decides how long a wait that will never complete takes
// to fail: the success path selects on the event itself and never spends this
// time, so a larger value costs nothing. Thirty seconds is far above the worst
// normal latency under load; it is not a claim that an event should take that
// long.
const eventTimeout = 30 * time.Second

// dirTestNow is the fixed instant most tests run at.
func dirTestNow() time.Time {
	return time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
}

// mustSignRecord signs a structurally valid record with the key derived from
// seed. The result has no source; tests set it when the record is accepted from
// a link.
func mustSignRecord(t *testing.T, seed byte, kind directorypb.RecordKind, object string, version uint64, expires time.Time, payload any) *storedRecord {
	t.Helper()
	pub, priv := recTestKey(seed)
	r, err := signRecord(recTestBody(pub, kind, object, version, expires, payload), func(data []byte) []byte {
		return ed25519.Sign(priv, data)
	})
	if err != nil {
		t.Fatalf("sign record: %v", err)
	}
	return r
}

// dirTestDeps builds Deps for a directory identified by seed, backed by the fake
// network endpoint for that fingerprint.
func dirTestDeps(t *testing.T, seed byte, dir string, net *fakeNet, logger *slog.Logger) (Deps, *fakeLinks) {
	t.Helper()
	id := newTestIdentity(seed)
	if logger == nil {
		logger, _ = newLogBuffer()
	}
	return Deps{
		Identity: id,
		Peers:    newFakePeers(),
		Links:    net.Endpoint(id.Fingerprint()),
		Settings: newFakeSettings(dir),
		Logger:   logger,
	}, net.Endpoint(id.Fingerprint())
}

// decodeRecordList unmarshals a RecordList frame.
func decodeRecordList(t *testing.T, payload []byte) *directorypb.RecordList {
	t.Helper()
	var list directorypb.RecordList
	if err := proto.Unmarshal(payload, &list); err != nil {
		t.Fatalf("unmarshal RecordList: %v", err)
	}
	return &list
}

func TestOpenLoadsAndPurges(t *testing.T) {
	dir := t.TempDir()
	day := func(n int) time.Time { return time.Date(2026, 11, n, 12, 0, 0, 0, time.UTC) }
	first := mustSignRecord(t, 2, recKindCard, "", 1, day(1).Add(recordTTL), recTestCard("first"))
	second := mustSignRecord(t, 3, recKindCard, "", 1, day(7).Add(recordTTL), recTestCard("second"))
	mustSaveFile(t, dir, 0, toFileRecords([]*storedRecord{first, second}))

	deps, _ := dirTestDeps(t, 1, dir, newFakeNet(), nil)
	d, err := openWith(deps, newFakeClock(day(9)), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	defer d.Close()

	d.mu.RLock()
	_, hasFirst := d.st.get(first.id)
	_, hasSecond := d.st.get(second.id)
	n := len(d.st.records)
	d.mu.RUnlock()
	if hasFirst || !hasSecond || n != 1 {
		t.Fatalf("loaded %d records (first=%v second=%v), want only the second", n, hasFirst, hasSecond)
	}

	d.persistIfDirty()
	frs, _, err := loadFile(realFileOps{}, dir)
	if err != nil {
		t.Fatalf("loadFile: %v", err)
	}
	if len(frs) != 1 || !bytes.Equal(frs[0].Raw, second.raw) {
		t.Fatalf("persisted %d records, want only the second", len(frs))
	}
}

func TestFileRecordsRoundTrip(t *testing.T) {
	exp := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	card := mustSignRecord(t, 1, recKindCard, "", 10, exp, recTestCard("hostA"))
	ann := mustSignRecord(t, 2, recKindTopic, "news", 11, exp, &directorypb.TopicAnnouncement{Name: "news", Description: "updates"})
	rev := mustSignRecord(t, 2, recKindCard, "", 12, exp, &directorypb.Revocation{RevokedKind: recKindCard})
	ann.source = newTestIdentity(3).Fingerprint()

	input := []*storedRecord{card, ann, rev}
	frs := toFileRecords(input)
	recs, dropped := fromFileRecords(frs)
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if len(recs) != len(input) {
		t.Fatalf("got %d records, want %d", len(recs), len(input))
	}

	byID := make(map[recordID]*storedRecord, len(recs))
	for _, r := range recs {
		byID[r.id] = r
	}
	for _, want := range input {
		got, ok := byID[want.id]
		if !ok {
			t.Fatalf("record %v is missing after the round trip", want.id)
		}
		if !bytes.Equal(got.raw, want.raw) {
			t.Errorf("raw for %v changed across the round trip", want.id)
		}
		if !bytes.Equal(got.sig, want.sig) {
			t.Errorf("signature for %v changed across the round trip", want.id)
		}
		if got.source != want.source {
			t.Errorf("source for %v = %v, want %v", want.id, got.source, want.source)
		}
	}

	reversed := toFileRecords([]*storedRecord{rev, ann, card})
	if !reflect.DeepEqual(frs, reversed) {
		t.Fatal("toFileRecords output depends on the input order")
	}
}

func TestFromFileRecordsDropsTampered(t *testing.T) {
	r := mustSignRecord(t, 2, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("hostP"))
	frs := toFileRecords([]*storedRecord{r})
	frs[0].Raw = append([]byte(nil), frs[0].Raw...)
	frs[0].Raw[0] ^= 0xff

	recs, dropped := fromFileRecords(frs)
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	if len(recs) != 0 {
		t.Fatalf("kept %d tampered records, want 0", len(recs))
	}
}

func TestFromFileRecordsDropsBadSource(t *testing.T) {
	r := mustSignRecord(t, 2, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("hostP"))
	frs := toFileRecords([]*storedRecord{r})
	frs[0].Source = "zz"

	recs, dropped := fromFileRecords(frs)
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	if len(recs) != 0 {
		t.Fatalf("kept %d records with a bad source, want 0", len(recs))
	}
}

func TestOpenUnreadableFileStartsEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte("not json"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	logger, logs := newLogBuffer()
	deps, _ := dirTestDeps(t, 1, dir, newFakeNet(), nil)
	deps.Logger = logger

	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	defer d.Close()

	d.mu.RLock()
	n := len(d.st.records)
	d.mu.RUnlock()
	if n != 0 {
		t.Fatalf("loaded %d records from a corrupt file, want 0", n)
	}
	if got := strings.Count(logs.String(), "level=ERROR"); got != 1 {
		t.Fatalf("error log lines = %d, want 1", got)
	}
}

func TestOpenReadErrorFails(t *testing.T) {
	deps, _ := dirTestDeps(t, 1, t.TempDir(), newFakeNet(), nil)

	_, err := openWith(deps, newFakeClock(dirTestNow()), persistReadErrorOps{err: fs.ErrPermission})
	if err == nil {
		t.Fatal("openWith succeeded after a read error")
	}
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("openWith error = %v, want fs.ErrPermission", err)
	}
}

// zeroIdentity is an Identity with no fingerprint.
type zeroIdentity struct{}

func (zeroIdentity) Fingerprint() Fingerprint { return Fingerprint{} }
func (zeroIdentity) PublicKey() []byte        { return make([]byte, ed25519.PublicKeySize) }
func (zeroIdentity) Sign([]byte) []byte       { return nil }

func TestOpenRejectsZeroIdentity(t *testing.T) {
	deps, _ := dirTestDeps(t, 1, t.TempDir(), newFakeNet(), nil)
	deps.Identity = zeroIdentity{}

	if _, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{}); !errors.Is(err, ErrNoIdentity) {
		t.Fatalf("openWith error = %v, want ErrNoIdentity", err)
	}
}

func TestOpenUsesDepsNow(t *testing.T) {
	dir := t.TempDir()
	issued := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	r := mustSignRecord(t, 2, recKindCard, "", 1, issued.Add(recordTTL), recTestCard("old"))
	mustSaveFile(t, dir, 0, toFileRecords([]*storedRecord{r}))

	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	deps, _ := dirTestDeps(t, 1, dir, newFakeNet(), nil)
	deps.Now = func() time.Time { return now }

	d, err := Open(deps)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	d.mu.RLock()
	n := len(d.st.records)
	d.mu.RUnlock()
	if n != 0 {
		t.Fatalf("kept %d expired records, want 0", n)
	}
	if got := d.now(); !got.Equal(now) {
		t.Fatalf("now() = %v, want %v", got, now)
	}
}

func TestOpenDoesNotTouchLinks(t *testing.T) {
	deps, ep := dirTestDeps(t, 1, t.TempDir(), newFakeNet(), nil)
	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	defer d.Close()

	ep.mu.Lock()
	stateHandlers := len(ep.stateHandlers)
	requestHandlers := len(ep.requestHandlers)
	onUp, onDown := ep.onUp, ep.onDown
	ep.mu.Unlock()

	if stateHandlers != 0 || requestHandlers != 0 || onUp != nil || onDown != nil {
		t.Fatal("Open registered link handlers or subscriptions")
	}
	if sent := ep.Sent(); len(sent) != 0 {
		t.Fatalf("Open sent %d frames, want 0", len(sent))
	}
}

func TestTrackLinksSendsFullList(t *testing.T) {
	dir := t.TempDir()
	now := dirTestNow()
	r := mustSignRecord(t, 2, recKindCard, "", 4, now.Add(recordTTL), recTestCard("hostP"))
	mustSaveFile(t, dir, 0, toFileRecords([]*storedRecord{r}))

	net := newFakeNet()
	deps, ea := dirTestDeps(t, 1, dir, net, nil)
	peer := newTestIdentity(2).Fingerprint()
	net.Endpoint(peer)

	d, err := openWith(deps, newFakeClock(now), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	defer d.Close()

	d.trackLinks()
	net.Connect(deps.Identity.Fingerprint(), peer)
	net.Settle(t)

	var lists []*directorypb.RecordList
	for _, f := range ea.Sent() {
		if f.to != peer {
			t.Fatalf("sent frame to %v, want %v", f.to, peer)
		}
		switch f.kind {
		case directorypb.KindRecordList:
			lists = append(lists, decodeRecordList(t, f.payload))
		case directorypb.KindRecords:
			t.Fatal("sent a Records frame instead of a RecordList")
		default:
			t.Fatalf("sent frame kind %d, want %d", f.kind, directorypb.KindRecordList)
		}
	}
	if len(lists) != 1 {
		t.Fatalf("sent %d RecordList frames, want 1", len(lists))
	}
	if len(lists[0].Records) != 1 || !proto.Equal(lists[0].Records[0], r.recordVersion()) {
		t.Fatalf("full list = %v, want the one held record", lists[0].Records)
	}
}

func TestBroadcastSkipsSourceAndDownPeers(t *testing.T) {
	net := newFakeNet()
	deps, ea := dirTestDeps(t, 1, t.TempDir(), net, nil)
	self := deps.Identity.Fingerprint()
	p := newTestIdentity(2).Fingerprint()
	q := newTestIdentity(3).Fingerprint()
	r := newTestIdentity(4).Fingerprint()

	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	defer d.Close()

	d.trackLinks()
	net.Endpoint(p)
	net.Endpoint(q)
	net.Endpoint(r)
	net.Connect(self, p)
	net.Connect(self, q)
	net.Settle(t)
	before := len(ea.Sent())

	vs := []*directorypb.RecordVersion{{
		Id:      &directorypb.RecordId{Publisher: bytes.Repeat([]byte{9}, 32), Kind: recKindCard},
		Version: 7,
	}}
	d.broadcast(vs, p)

	sent := ea.Sent()[before:]
	if len(sent) != 1 {
		t.Fatalf("broadcast sent %d frames, want 1", len(sent))
	}
	if sent[0].to != q {
		t.Fatalf("broadcast sent to %v, want %v", sent[0].to, q)
	}
	if sent[0].kind != directorypb.KindRecordList {
		t.Fatalf("broadcast kind = %d, want %d", sent[0].kind, directorypb.KindRecordList)
	}
	list := decodeRecordList(t, sent[0].payload)
	if len(list.Records) != 1 || !proto.Equal(list.Records[0], vs[0]) {
		t.Fatalf("broadcast list = %v, want only the given version", list.Records)
	}
}

func TestBroadcastSendsOutsideLock(t *testing.T) {
	net := newFakeNet()
	deps, ea := dirTestDeps(t, 1, t.TempDir(), net, nil)
	self := deps.Identity.Fingerprint()
	peer := newTestIdentity(2).Fingerprint()
	net.Endpoint(peer)

	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	defer d.Close()

	d.trackLinks()
	net.Connect(self, peer)
	net.Settle(t)

	var hooked bool
	ea.SetSendHook(func(Fingerprint, uint32) {
		hooked = true
		if !d.mu.TryLock() {
			t.Error("Send called while the directory lock is held")
			return
		}
		d.mu.Unlock()
	})

	d.broadcast([]*directorypb.RecordVersion{{
		Id:      &directorypb.RecordId{Publisher: bytes.Repeat([]byte{9}, 32), Kind: recKindCard},
		Version: 1,
	}}, Fingerprint{})
	if !hooked {
		t.Fatal("broadcast did not call Send")
	}
}

// failOnceRenameOps fails the first rename and succeeds afterwards.
type failOnceRenameOps struct {
	realFileOps
	mu     sync.Mutex
	failed bool
}

func (o *failOnceRenameOps) Rename(oldpath, newpath string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.failed {
		o.failed = true
		return errors.New("injected rename failure")
	}
	return o.realFileOps.Rename(oldpath, newpath)
}

func TestPersistFailureKeepsMemoryAndRetries(t *testing.T) {
	dir := t.TempDir()
	logger, logs := newLogBuffer()
	deps, _ := dirTestDeps(t, 1, dir, newFakeNet(), nil)
	deps.Logger = logger
	ops := &failOnceRenameOps{}

	d, err := openWith(deps, newFakeClock(dirTestNow()), ops)
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	defer d.Close()

	r := mustSignRecord(t, 2, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("hostP"))
	d.mu.Lock()
	d.st.put(r)
	d.dirty = true
	d.mu.Unlock()

	d.persistIfDirty()

	d.mu.RLock()
	_, kept := d.st.get(r.id)
	dirty := d.dirty
	d.mu.RUnlock()
	if !kept {
		t.Fatal("the record disappeared from memory after a failed write")
	}
	if !dirty {
		t.Fatal("dirty was cleared after a failed write")
	}
	if got := strings.Count(logs.String(), "level=ERROR"); got != 1 {
		t.Fatalf("error log lines = %d, want 1", got)
	}
	if _, err := os.Stat(filepath.Join(dir, fileName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("directory file exists after a failed rename: %v", err)
	}

	d.persistIfDirty()

	d.mu.RLock()
	dirty = d.dirty
	d.mu.RUnlock()
	if dirty {
		t.Fatal("dirty is still set after a successful retry")
	}
	frs, _, err := loadFile(realFileOps{}, dir)
	if err != nil {
		t.Fatalf("loadFile: %v", err)
	}
	if len(frs) != 1 || !bytes.Equal(frs[0].Raw, r.raw) {
		t.Fatalf("persisted %d records, want the accepted one", len(frs))
	}
}

// TestClosePersistsChangeMadeDuringWrite checks that a change made while a
// write is in flight is persisted before Close returns. The persistHook holds
// the first write between its snapshot and saveFile; meanwhile a second
// goroutine changes the directory and closes it. The afterPersistLock seam
// reports when a caller holds persistMu, and the dirty flag read at that
// instant shows whether the caller took its snapshot only after the lock.
func TestClosePersistsChangeMadeDuringWrite(t *testing.T) {
	dir := t.TempDir()
	deps, _ := dirTestDeps(t, 1, dir, newFakeNet(), nil)
	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}

	first := mustSignRecord(t, 2, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("first"))
	second := mustSignRecord(t, 3, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("second"))

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	var hookCalls int
	var hookMu sync.Mutex
	d.persistHook = func() {
		hookMu.Lock()
		hookCalls++
		n := hookCalls
		hookMu.Unlock()
		if n != 1 {
			return
		}
		close(entered)
		<-release
	}

	// afterPersistLock fires once a persistIfDirty call holds persistMu. The
	// first call is the write started below, which reaches the seam before it
	// blocks in persistHook; the second is Close. Blocking on the second call
	// keeps the dirty flag still while the test reads it.
	closeAtLock := make(chan struct{})
	releaseLock := make(chan struct{})
	var releaseLockOnce sync.Once
	unblockLock := func() { releaseLockOnce.Do(func() { close(releaseLock) }) }
	defer unblockLock()

	var lockCalls int
	var lockMu sync.Mutex
	afterPersistLock = func() {
		lockMu.Lock()
		lockCalls++
		n := lockCalls
		lockMu.Unlock()
		if n != 2 {
			return
		}
		close(closeAtLock)
		<-releaseLock
	}
	defer func() { afterPersistLock = nil }()

	d.mu.Lock()
	d.st.put(first)
	d.dirty = true
	d.mu.Unlock()

	persistDone := make(chan struct{})
	go func() {
		d.persistIfDirty()
		close(persistDone)
	}()
	<-entered

	d.mu.Lock()
	d.st.put(second)
	d.dirty = true
	d.mu.Unlock()

	closeDone := make(chan struct{})
	go func() {
		d.Close()
		close(closeDone)
	}()

	t.Cleanup(func() {
		unblock()
		unblockLock()
		for _, ch := range []chan struct{}{persistDone, closeDone} {
			select {
			case <-ch:
			case <-time.After(eventTimeout):
				t.Errorf("a persistence goroutine did not finish during cleanup")
			}
		}
	})

	// Let the held write finish so Close can acquire persistMu and reach the
	// seam. An implementation that clears dirty before acquiring persistMu has
	// already taken the newer snapshot by then; the held older write lands on
	// top after the release.
	unblock()

	select {
	case <-closeAtLock:
	case <-time.After(eventTimeout):
		t.Fatal("Close did not reach persistMu")
	}
	d.mu.RLock()
	dirty := d.dirty
	d.mu.RUnlock()
	if !dirty {
		t.Fatal("Close cleared the dirty flag before acquiring persistMu")
	}

	unblockLock()

	waitDone := func(ch chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(eventTimeout):
			t.Fatalf("%s did not finish", what)
		}
	}
	waitDone(persistDone, "persistIfDirty")
	waitDone(closeDone, "Close")

	frs, _, err := loadFile(realFileOps{}, dir)
	if err != nil {
		t.Fatalf("loadFile: %v", err)
	}
	if len(frs) != 2 {
		t.Fatalf("persisted %d records after Close, want the 2 in memory", len(frs))
	}
}

func TestCloseIsIdempotentAndStopsHandlers(t *testing.T) {
	deps, ep := dirTestDeps(t, 1, t.TempDir(), newFakeNet(), nil)
	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("openWith: %v", err)
	}
	d.trackLinks()
	up := ep.upCallback()
	if up == nil {
		t.Fatal("trackLinks did not subscribe")
	}

	d.Close()
	d.Close()

	up(newTestIdentity(2).Fingerprint(), make([]byte, 32))
	if sent := ep.Sent(); len(sent) != 0 {
		t.Fatalf("sent %d frames after Close, want 0", len(sent))
	}
}
