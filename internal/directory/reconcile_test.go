package directory

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
)

type reconcileNode struct {
	d    *Directory
	ep   *fakeLinks
	fp   Fingerprint
	set  *fakeSettings
	logs *lockedBuffer
}

func newReconcileNode(t *testing.T, net *fakeNet, seed byte) *reconcileNode {
	t.Helper()
	id := newTestIdentity(seed)
	logger, logs := newLogBuffer()
	set := newFakeSettings(t.TempDir())
	deps := Deps{
		Identity: id,
		Peers:    newFakePeers(),
		Links:    net.Endpoint(id.Fingerprint()),
		Settings: set,
		Logger:   logger,
	}
	d, err := openWith(deps, newFakeClock(dirTestNow()), realFileOps{})
	if err != nil {
		t.Fatalf("open directory for seed %d: %v", seed, err)
	}
	t.Cleanup(d.Close)
	d.trackLinks()
	d.registerReconcile()
	return &reconcileNode{
		d:    d,
		ep:   net.Endpoint(id.Fingerprint()),
		fp:   id.Fingerprint(),
		set:  set,
		logs: logs,
	}
}

func reconcileInject(t *testing.T, n *reconcileNode, r *storedRecord) {
	t.Helper()
	n.d.mu.Lock()
	n.d.st.putOwn(r)
	n.d.dirty = true
	n.d.mu.Unlock()
}

func reconcileLookup(n *reconcileNode, id recordID) (*storedRecord, bool) {
	n.d.mu.RLock()
	defer n.d.mu.RUnlock()
	return n.d.st.get(id)
}

func clearReconcileSent(ep *fakeLinks) {
	ep.mu.Lock()
	ep.sent = nil
	ep.mu.Unlock()
}

// reconcileAwait drains the fake network until cond reports true. It polls
// instead of passing the inflight groups to Settle: a fetch goroutine can Add on
// a zero counter while Settle waits, which the race detector reports.
func reconcileAwait(t *testing.T, net *fakeNet, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		net.Settle(t)
		if cond() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		runtime.Gosched()
	}
}

func reconcileListPayload(t *testing.T, vs ...*directorypb.RecordVersion) []byte {
	t.Helper()
	p, err := proto.Marshal(&directorypb.RecordList{Records: vs})
	if err != nil {
		t.Fatalf("marshal RecordList: %v", err)
	}
	return p
}

func reconcileRequestPayload(t *testing.T, ids ...*directorypb.RecordId) []byte {
	t.Helper()
	p, err := proto.Marshal(&directorypb.RecordRequest{Records: ids})
	if err != nil {
		t.Fatalf("marshal RecordRequest: %v", err)
	}
	return p
}

func reconcileListHasID(t *testing.T, payload []byte, id recordID) bool {
	t.Helper()
	var list directorypb.RecordList
	if err := proto.Unmarshal(payload, &list); err != nil {
		t.Fatalf("unmarshal RecordList: %v", err)
	}
	for _, v := range list.Records {
		got, ok := idFromProto(v.GetId())
		if ok && got == id {
			return true
		}
	}
	return false
}

func reconcileRequestHasID(t *testing.T, payload []byte, id recordID) bool {
	t.Helper()
	var req directorypb.RecordRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		t.Fatalf("unmarshal RecordRequest: %v", err)
	}
	for _, pid := range req.Records {
		got, ok := idFromProto(pid)
		if ok && got == id {
			return true
		}
	}
	return false
}

func reconcileListCount(t *testing.T, ep *fakeLinks, to Fingerprint, id recordID) int {
	t.Helper()
	n := 0
	for _, f := range ep.Sent() {
		if f.to != to || f.kind != directorypb.KindRecordList {
			continue
		}
		if reconcileListHasID(t, f.payload, id) {
			n++
		}
	}
	return n
}

func reconcileFrames(ep *fakeLinks, to Fingerprint, kind uint32) int {
	n := 0
	for _, f := range ep.Sent() {
		if f.to == to && f.kind == kind {
			n++
		}
	}
	return n
}

func reconcileDirectAddr(addr string) *directorypb.AdvertisedAddress {
	return &directorypb.AdvertisedAddress{Kind: "direct", Addr: addr}
}

func reconcileRelayAddr(addr string) *directorypb.AdvertisedAddress {
	return &directorypb.AdvertisedAddress{Kind: "relay", Addr: addr}
}

func reconcileCard(t *testing.T, seed byte, version uint64, alias string, addrs ...*directorypb.AdvertisedAddress) *storedRecord {
	t.Helper()
	return mustSignRecord(t, seed, recKindCard, "", version, dirTestNow().Add(recordTTL), recTestCard(alias, addrs...))
}

func reconcileAnnouncement(t *testing.T, seed byte, name, description string, version uint64) *storedRecord {
	t.Helper()
	return mustSignRecord(t, seed, recKindTopic, name, version, dirTestNow().Add(recordTTL),
		&directorypb.TopicAnnouncement{Name: name, Description: description})
}

// TestReconcileOnLinkUp checks that a card held by one directory reaches a peer
// on link-up.
func TestReconcileOnLinkUp(t *testing.T) {
	net := newFakeNet()
	a := newReconcileNode(t, net, 1)
	b := newReconcileNode(t, net, 2)

	x := reconcileCard(t, 200, 1, "hostX", reconcileDirectAddr("198.51.100.4:17443"))
	reconcileInject(t, a, x)

	net.Connect(a.fp, b.fp)
	reconcileAwait(t, net, "B to accept the card held by A", func() bool {
		got, ok := reconcileLookup(b, x.id)
		return ok && bytes.Equal(got.raw, x.raw)
	})
}

// TestReconcileFetchesNewerOnly checks that a peer fetches a newer version and
// never requests the older one it already holds.
func TestReconcileFetchesNewerOnly(t *testing.T) {
	net := newFakeNet()
	a := newReconcileNode(t, net, 1)
	b := newReconcileNode(t, net, 2)

	x1 := reconcileCard(t, 200, 1, "hostX")
	x2 := reconcileCard(t, 200, 2, "hostX")
	y1 := reconcileCard(t, 201, 1, "hostY")
	reconcileInject(t, a, x2)
	reconcileInject(t, b, x1)
	reconcileInject(t, b, y1)

	net.Connect(a.fp, b.fp)
	reconcileAwait(t, net, "both directories to reconcile", func() bool {
		for _, n := range []*reconcileNode{a, b} {
			r, ok := reconcileLookup(n, x1.id)
			if !ok || !bytes.Equal(r.raw, x2.raw) {
				return false
			}
			if _, ok := reconcileLookup(n, y1.id); !ok {
				return false
			}
		}
		return true
	})

	for _, f := range a.ep.Sent() {
		if f.to != b.fp || f.kind != directorypb.KindRecordRequest {
			continue
		}
		if reconcileRequestHasID(t, f.payload, x1.id) {
			t.Fatal("A requested the older X from B")
		}
	}
}

// TestHopByHopPush checks that a newly accepted record is pushed to other peers
// and never back to its source.
func TestHopByHopPush(t *testing.T) {
	net := newFakeNet()
	a := newReconcileNode(t, net, 1)
	b := newReconcileNode(t, net, 2)
	c := newReconcileNode(t, net, 3)
	p := newReconcileNode(t, net, 4)

	net.Connect(a.fp, b.fp)
	net.Connect(b.fp, c.fp)
	net.Settle(t)

	x := reconcileCard(t, 200, 1, "hostX")
	reconcileInject(t, p, x)
	net.Connect(p.fp, a.fp)
	reconcileAwait(t, net, "C to receive the record A accepted from P", func() bool {
		_, ok := reconcileLookup(c, x.id)
		return ok
	})

	if got := reconcileListCount(t, a.ep, p.fp, x.id); got != 0 {
		t.Fatalf("A sent the record back to its source %d times", got)
	}
}

// TestHigherVersionToldBackByIncrementalList checks that a peer holding a higher
// version answers a lower one with an incremental RecordList, not the record.
func TestHigherVersionToldBackByIncrementalList(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	x1 := reconcileCard(t, 200, 1, "hostX")
	x3 := reconcileCard(t, 200, 3, "hostX")
	reconcileInject(t, b, x3)
	clearReconcileSent(b.ep)

	if err := p.ep.Send(b.fp, directorypb.KindRecordList, reconcileListPayload(t, x1.recordVersion())); err != nil {
		t.Fatalf("send incremental list: %v", err)
	}
	reconcileAwait(t, net, "P to obtain X v3 after the tell-back", func() bool {
		got, ok := reconcileLookup(p, x3.id)
		return ok && bytes.Equal(got.raw, x3.raw)
	})

	sent := b.ep.Sent()
	if len(sent) != 1 {
		t.Fatalf("B sent %d frames, want exactly one incremental list", len(sent))
	}
	if sent[0].kind != directorypb.KindRecordList {
		t.Fatalf("B sent kind %d, want %d", sent[0].kind, directorypb.KindRecordList)
	}
	list := decodeRecordList(t, sent[0].payload)
	if len(list.Records) != 1 || !proto.Equal(list.Records[0], x3.recordVersion()) {
		t.Fatalf("B told back %v, want only X v3", list.Records)
	}
	if got := reconcileFrames(b.ep, p.fp, directorypb.KindRecords); got != 0 {
		t.Fatalf("B sent %d Records frames with Send, want 0", got)
	}
}

// TestEqualVersionsNotToldBack checks that equal versions produce no incremental
// list on either side.
func TestEqualVersionsNotToldBack(t *testing.T) {
	net := newFakeNet()
	a := newReconcileNode(t, net, 1)
	b := newReconcileNode(t, net, 2)

	x2a := reconcileCard(t, 200, 2, "hostX")
	x2b := reconcileCard(t, 200, 2, "hostX")
	reconcileInject(t, a, x2a)
	reconcileInject(t, b, x2b)

	net.Connect(a.fp, b.fp)
	net.Settle(t)
	clearReconcileSent(a.ep)
	clearReconcileSent(b.ep)
	a.d.reconcileAll()
	b.d.reconcileAll()
	net.Settle(t)

	for _, pair := range []struct {
		from *reconcileNode
		to   *reconcileNode
	}{{a, b}, {b, a}} {
		sent := pair.from.ep.Sent()
		if len(sent) != 1 {
			t.Fatalf("%v sent %d frames to %v, want only the full list", pair.from.fp, len(sent), pair.to.fp)
		}
		if sent[0].kind != directorypb.KindRecordList || !reconcileListHasID(t, sent[0].payload, x2a.id) {
			t.Fatalf("%v sent kind %d, want one RecordList holding X v2", pair.from.fp, sent[0].kind)
		}
	}
}

// TestRingTerminates checks that a record propagated around a ring is sent to
// each neighbor at most once.
func TestRingTerminates(t *testing.T) {
	net := newFakeNet()
	a := newReconcileNode(t, net, 1)
	b := newReconcileNode(t, net, 2)
	c := newReconcileNode(t, net, 3)
	p := newReconcileNode(t, net, 4)

	net.Connect(a.fp, b.fp)
	net.Connect(b.fp, c.fp)
	net.Connect(c.fp, a.fp)
	net.Settle(t)

	for _, n := range []*reconcileNode{a, b, c} {
		clearReconcileSent(n.ep)
	}

	x := reconcileCard(t, 200, 1, "hostX")
	reconcileInject(t, p, x)
	net.Connect(p.fp, a.fp)
	reconcileAwait(t, net, "the record to reach every node in the ring", func() bool {
		for _, n := range []*reconcileNode{a, b, c} {
			if _, ok := reconcileLookup(n, x.id); !ok {
				return false
			}
		}
		return true
	})

	neighbors := map[*reconcileNode][]*reconcileNode{
		a: {b, c},
		b: {a, c},
		c: {a, b},
	}
	for from, tos := range neighbors {
		for _, to := range tos {
			if got := reconcileListCount(t, from.ep, to.fp, x.id); got > 1 {
				t.Fatalf("%v sent %d RecordLists holding the record to %v, want at most 1", from.fp, got, to.fp)
			}
		}
	}
}

// TestRejectsTamperedAndForged checks that tampered and forged records are
// dropped with a warning naming the source.
func TestRejectsTamperedAndForged(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	valid := reconcileCard(t, 200, 1, "hostX", reconcileDirectAddr("198.51.100.4:17443"))
	tampered := valid.wire()
	tampered.Raw = append([]byte(nil), tampered.Raw...)
	at := bytes.Index(tampered.Raw, []byte("198.51.100.4:17443"))
	if at < 0 {
		t.Fatal("address not found in the encoded card")
	}
	tampered.Raw[at] ^= 0x01

	pubB, privB := recTestKey(2)
	forgedBody := recTestBody(pubB, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("hostX"))
	forgedPublisher := newTestIdentity(200).Fingerprint()
	forgedBody.Publisher = forgedPublisher[:]
	forged := recTestWire(t, forgedBody, privB)

	b.d.acceptBatch(p.fp, []*directorypb.Record{tampered, forged})

	if _, ok := reconcileLookup(b, valid.id); ok {
		t.Fatal("B accepted a record it should have rejected")
	}

	var warns []string
	for _, line := range strings.Split(b.logs.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, "invalid signature") {
			warns = append(warns, line)
		}
	}
	if len(warns) != 2 {
		t.Fatalf("invalid-signature warnings = %d, want 2", len(warns))
	}
	for _, line := range warns {
		if !strings.Contains(line, p.fp.String()) {
			t.Fatalf("warning does not name the source peer: %s", line)
		}
	}
}

// TestIgnoresFramesFromNonEstablishedSource checks that frames and record
// requests from a fingerprint without an established link change nothing.
func TestIgnoresFramesFromNonEstablishedSource(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	z := newTestIdentity(9).Fingerprint()

	x := reconcileCard(t, 200, 1, "hostX")
	b.d.handleRecordList(z, reconcileListPayload(t, x.recordVersion()))
	b.d.acceptBatch(z, []*directorypb.Record{x.wire()})

	if _, ok := reconcileLookup(b, x.id); ok {
		t.Fatal("accepted a record from a peer without an established link")
	}
	if sent := b.ep.Sent(); len(sent) != 0 {
		t.Fatalf("sent %d frames after ignoring a non-established source", len(sent))
	}

	held := reconcileCard(t, 201, 1, "hostHeld")
	b.d.mu.Lock()
	out := b.d.st.accept(held, b.d.now(), b.d.limits())
	b.d.mu.Unlock()
	if out != outcomeAccepted {
		t.Fatalf("seed the held record: accept = %v, want outcomeAccepted", out)
	}

	kind, reply := b.d.handleRecordRequest(context.Background(), z, reconcileRequestPayload(t, held.id.toProto()))
	if kind != directorypb.KindRecords {
		t.Fatalf("reply kind = %d, want KindRecords", kind)
	}
	var answered directorypb.Records
	if err := proto.Unmarshal(reply, &answered); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if n := len(answered.GetRecords()); n != 0 {
		t.Fatalf("answered a non-established source with %d records, want none", n)
	}
}

// TestQuotaDropLoggedOncePerBatch checks that records over the per-link limit are
// dropped and reported once for the batch.
func TestQuotaDropLoggedOncePerBatch(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)
	b.set.SetMaxRecordsPerLink(4)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	recs := make([]*directorypb.Record, 0, 10)
	for i := 0; i < 10; i++ {
		r := reconcileCard(t, byte(100+i), 1, "hostX")
		recs = append(recs, r.wire())
	}
	b.d.acceptBatch(p.fp, recs)

	b.d.mu.RLock()
	held := len(b.d.st.records)
	b.d.mu.RUnlock()
	if held != 4 {
		t.Fatalf("held %d records, want 4", held)
	}

	logs := b.logs.String()
	if got := strings.Count(logs, "dropped records over the directory limits"); got != 1 {
		t.Fatalf("quota warnings = %d, want 1", got)
	}
	if !strings.Contains(logs, "count=6") {
		t.Fatalf("quota warning does not carry the dropped count: %s", logs)
	}
}

// TestOnCardCalledWithKnownAddresses checks that OnCard receives the publisher
// and the recognized addresses of a forwarded card.
func TestOnCardCalledWithKnownAddresses(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	c := newReconcileNode(t, net, 2)
	p := newReconcileNode(t, net, 3)

	net.Connect(b.fp, c.fp)
	net.Connect(p.fp, b.fp)
	net.Settle(t)

	type call struct {
		fp    Fingerprint
		addrs []config.AdvertisedAddr
	}
	var mu sync.Mutex
	var calls []call
	b.d.OnCard(func(fp Fingerprint, addrs []config.AdvertisedAddr) {
		mu.Lock()
		calls = append(calls, call{fp: fp, addrs: append([]config.AdvertisedAddr(nil), addrs...)})
		mu.Unlock()
	})

	x := reconcileCard(t, 200, 1, "hostX",
		reconcileRelayAddr("r1"), reconcileDirectAddr("198.51.100.4:17443"))
	reconcileInject(t, p, x)
	p.d.broadcast([]*directorypb.RecordVersion{x.recordVersion()}, Fingerprint{})
	reconcileAwait(t, net, "B to forward the card to C", func() bool {
		_, ok := reconcileLookup(c, x.id)
		return ok
	})

	mu.Lock()
	got := append([]call(nil), calls...)
	mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("OnCard was called %d times, want 1", len(got))
	}
	if got[0].fp != x.id.publisher {
		t.Fatalf("OnCard fingerprint = %v, want %v", got[0].fp, x.id.publisher)
	}
	wantAddrs := []config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}}
	if !reflect.DeepEqual(got[0].addrs, wantAddrs) {
		t.Fatalf("OnCard addresses = %v, want %v", got[0].addrs, wantAddrs)
	}

	forwarded, ok := reconcileLookup(c, x.id)
	if !ok {
		t.Fatal("C did not receive the forwarded card")
	}
	if !bytes.Equal(forwarded.raw, x.raw) {
		t.Fatal("C stored a card without the unknown address kind")
	}
}

// TestAcceptsAnnouncementWithLongDescription checks that a declaration with an
// over-long description is accepted and propagated.
func TestAcceptsAnnouncementWithLongDescription(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	c := newReconcileNode(t, net, 2)
	p := newReconcileNode(t, net, 3)

	net.Connect(b.fp, c.fp)
	net.Connect(p.fp, b.fp)
	net.Settle(t)

	desc := strings.Repeat("d", 300)
	ann := reconcileAnnouncement(t, 200, "news", desc, 1)
	reconcileInject(t, p, ann)
	p.d.broadcast([]*directorypb.RecordVersion{ann.recordVersion()}, Fingerprint{})
	reconcileAwait(t, net, "B and C to accept the long-description declaration", func() bool {
		for _, n := range []*reconcileNode{b, c} {
			got, ok := reconcileLookup(n, ann.id)
			if !ok || !bytes.Equal(got.raw, ann.raw) {
				return false
			}
		}
		return true
	})
}

// TestOnCardNotCalledForOwnOrNonCard checks that OnCard is not called for the
// local Broker's own card, a declaration or a revocation.
func TestOnCardNotCalledForOwnOrNonCard(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	var calls int32
	b.d.OnCard(func(Fingerprint, []config.AdvertisedAddr) { atomic.AddInt32(&calls, 1) })

	ann := reconcileAnnouncement(t, 200, "news", "", 1)
	rev := mustSignRecord(t, 201, recKindCard, "", 2, dirTestNow().Add(recordTTL),
		&directorypb.Revocation{RevokedKind: recKindCard})
	own := mustSignRecord(t, 1, recKindCard, "", 5, dirTestNow().Add(recordTTL), recTestCard("self"))

	b.d.acceptBatch(p.fp, []*directorypb.Record{ann.wire(), rev.wire(), own.wire()})

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("OnCard was called %d times for non-foreign-card records, want 0", got)
	}
}

// TestOwnRecordFromNetworkRaisesAndHooks checks that the local Broker's own
// record arriving from the network raises the issued version and calls the
// own-record hook.
func TestOwnRecordFromNetworkRaisesAndHooks(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	own := mustSignRecord(t, 1, recKindCard, "", 5000, dirTestNow().Add(recordTTL), recTestCard("self"))
	reconcileInject(t, p, own)

	var hooked atomic.Bool
	b.d.mu.Lock()
	b.d.st.highestIssued = 10
	b.d.ownHook = func(ids []recordID) {
		if len(ids) == 1 && ids[0] == own.id {
			hooked.Store(true)
		}
	}
	b.d.mu.Unlock()

	net.Connect(p.fp, b.fp)
	reconcileAwait(t, net, "the own-record hook", hooked.Load)

	b.d.mu.RLock()
	highest := b.d.st.highestIssued
	b.d.mu.RUnlock()
	if highest != 5000 {
		t.Fatalf("highestIssued = %d, want 5000", highest)
	}
}

// TestRecordListFetchesOwnHigherVersionAtOnce checks that a higher version of the
// local Broker's own record is fetched at once rather than deferred.
func TestRecordListFetchesOwnHigherVersionAtOnce(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	v1 := mustSignRecord(t, 1, recKindCard, "", 1, dirTestNow().Add(recordTTL), recTestCard("self"))
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})

	v2 := mustSignRecord(t, 1, recKindCard, "", 2, dirTestNow().Add(recordTTL), recTestCard("self"))
	reconcileInject(t, p, v2)
	b.d.handleRecordList(p.fp, reconcileListPayload(t, v2.recordVersion()))

	reconcileAwait(t, net, "B to fetch its own higher version at once", func() bool {
		return refetchHeldVersion(b, v2.id) == 2
	})

	b.d.mu.RLock()
	deferred := 0
	if pr := b.d.recon[p.fp]; pr != nil {
		deferred = len(pr.refetch)
	}
	b.d.mu.RUnlock()
	if deferred != 0 {
		t.Fatalf("B deferred %d identifiers for its own higher version, want 0", deferred)
	}
}

// TestRecordListDefersExpiredOwnRecordAcceptedFromLink checks that the
// replacement window applies to the local Broker's own record once it came back
// from a link, even after it expires.
func TestRecordListDefersExpiredOwnRecordAcceptedFromLink(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	clk := refetchClock(t, b)
	v1 := mustSignRecord(t, 1, recKindCard, "", 1, dirTestNow().Add(2*time.Second), recTestCard("self"))
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})

	held, ok := reconcileLookup(b, v1.id)
	if !ok {
		t.Fatalf("B does not hold its own record after accepting it from a link")
	}
	if held.source.IsZero() || held.acceptedAt.IsZero() {
		t.Fatalf("own record accepted from a link has source=%v acceptedAt=%v, want both non-zero",
			held.source, held.acceptedAt)
	}

	clk.Advance(3 * time.Second)

	v2 := mustSignRecord(t, 1, recKindCard, "", 2, dirTestNow().Add(recordTTL), recTestCard("self"))
	reconcileInject(t, p, v2)
	clearReconcileSent(b.ep)

	b.d.handleRecordList(p.fp, reconcileListPayload(t, v2.recordVersion()))
	b.d.inflight.Wait()
	net.Settle(t)
	if got := reconcileFrames(b.ep, p.fp, directorypb.KindRecordRequest); got != 0 {
		t.Fatalf("B requested the expired own record %d times inside the window, want 0", got)
	}
	if got := refetchHeldVersion(b, v1.id); got != 1 {
		t.Fatalf("held version = %d inside the window, want the expired v1 to stay", got)
	}

	clk.WaitTimers(t, 1)
	clk.Advance(directoryReplaceInterval() - 3*time.Second)
	reconcileAwait(t, net, "B to fetch the expired own record once its window ends", func() bool {
		return refetchHeldVersion(b, v1.id) == 2
	})
	if got := refetchRequestsFor(t, b.ep, p.fp, v1.id); got != 1 {
		t.Fatalf("B refetched the identifier %d times, want exactly 1", got)
	}
}

// TestRequestHandlerReturnsHeldOnly checks that a record request is answered with
// the held, unexpired records only.
func TestRequestHandlerReturnsHeldOnly(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	live := reconcileCard(t, 200, 1, "hostLive")
	expired := mustSignRecord(t, 201, recKindCard, "", 1, dirTestNow().Add(-time.Hour), recTestCard("hostGone"))
	missing := recordID{publisher: newTestIdentity(202).Fingerprint(), kind: recKindCard}
	reconcileInject(t, b, live)
	reconcileInject(t, b, expired)

	payload := reconcileRequestPayload(t, live.id.toProto(), expired.id.toProto(), missing.toProto())
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	kind, reply, sent, err := p.ep.Request(ctx, b.fp, directorypb.KindRecordRequest, payload)
	if err != nil || !sent {
		t.Fatalf("request failed: sent=%v err=%v", sent, err)
	}
	if kind != directorypb.KindRecords {
		t.Fatalf("reply kind = %d, want %d", kind, directorypb.KindRecords)
	}
	var recs directorypb.Records
	if err := proto.Unmarshal(reply, &recs); err != nil {
		t.Fatalf("unmarshal Records: %v", err)
	}
	if len(recs.Records) != 1 {
		t.Fatalf("reply holds %d records, want 1", len(recs.Records))
	}
	if !bytes.Equal(recs.Records[0].GetRaw(), live.raw) {
		t.Fatal("reply holds a different record than the held one")
	}
}

// TestFetchFailureIsSilent checks that a failed record request stores no record
// and sends no further request.
func TestFetchFailureIsSilent(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	pfp := newTestIdentity(2).Fingerprint()
	net.Endpoint(pfp) // an endpoint with no handlers, so a request is unsupported

	net.Connect(b.fp, pfp)
	net.Settle(t)

	x := reconcileCard(t, 200, 1, "hostX")
	b.d.handleRecordList(pfp, reconcileListPayload(t, x.recordVersion()))
	reconcileAwait(t, net, "the failed record request", func() bool {
		return strings.Contains(b.logs.String(), "record request failed")
	})
	b.d.inflight.Wait()

	if _, ok := reconcileLookup(b, x.id); ok {
		t.Fatal("B accepted a record that it never received")
	}
	if got := reconcileFrames(b.ep, pfp, directorypb.KindRecordRequest); got != 1 {
		t.Fatalf("B sent %d record requests, want 1", got)
	}
}

func reconcileRecordsPayload(t *testing.T, recs ...*directorypb.Record) []byte {
	t.Helper()
	p, err := proto.Marshal(&directorypb.Records{Records: recs})
	if err != nil {
		t.Fatalf("marshal Records: %v", err)
	}
	return p
}

func reconcileRequestBatchSizes(t *testing.T, ep *fakeLinks, to Fingerprint) []int {
	t.Helper()
	var sizes []int
	for _, f := range ep.Sent() {
		if f.to != to || f.kind != directorypb.KindRecordRequest {
			continue
		}
		var req directorypb.RecordRequest
		if err := proto.Unmarshal(f.payload, &req); err != nil {
			t.Fatalf("unmarshal RecordRequest: %v", err)
		}
		sizes = append(sizes, len(req.GetRecords()))
	}
	return sizes
}

// TestReconcileCapsInflightFetchPerPeer checks that repeated RecordList frames
// for an identifier already in flight start no second request.
func TestReconcileCapsInflightFetchPerPeer(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	pfp := newTestIdentity(2).Fingerprint()
	p := net.Endpoint(pfp)

	net.Connect(b.fp, pfp)
	net.Settle(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	p.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return directorypb.KindRecords, nil
		})

	x := reconcileCard(t, 200, 1, "hostX")
	payload := reconcileListPayload(t, x.recordVersion())
	b.d.handleRecordList(pfp, payload)
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("B never sent the record request")
	}
	for i := 0; i < 99; i++ {
		b.d.handleRecordList(pfp, payload)
	}
	close(release)
	b.d.inflight.Wait()

	if got := reconcileFrames(b.ep, pfp, directorypb.KindRecordRequest); got != 1 {
		t.Fatalf("B sent %d record requests for one identifier while one was in flight, want 1", got)
	}
}

// TestReconcileMergesDistinctIDsWhileFetching checks that a RecordList naming a
// different identifier while a fetch is in flight queues it instead of starting a
// second fetch.
func TestReconcileMergesDistinctIDsWhileFetching(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	pfp := newTestIdentity(2).Fingerprint()
	p := net.Endpoint(pfp)

	net.Connect(b.fp, pfp)
	net.Settle(t)

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	p.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			started <- struct{}{}
			<-release
			return directorypb.KindRecords, nil
		})

	first := reconcileCard(t, 200, 1, "hostX")
	second := reconcileCard(t, 201, 1, "hostY")
	b.d.handleRecordList(pfp, reconcileListPayload(t, first.recordVersion()))
	select {
	case <-started:
	case <-time.After(eventTimeout):
		close(release)
		t.Fatal("B never sent the first record request")
	}

	b.d.handleRecordList(pfp, reconcileListPayload(t, second.recordVersion()))

	assertOneFetch := func() {
		b.d.mu.RLock()
		pr := b.d.recon[pfp]
		_, firstInflight := pr.inflight[first.id]
		_, secondInflight := pr.inflight[second.id]
		_, secondPending := pr.pending[second.id]
		pendingLen := len(pr.pending)
		b.d.mu.RUnlock()
		if !firstInflight {
			t.Fatal("B dropped the first fetch from the in-flight set")
		}
		if secondInflight {
			t.Fatal("B put the second identifier in flight while one was in flight")
		}
		if !secondPending || pendingLen != 1 {
			t.Fatalf("B holds %d pending identifiers (second queued: %v), want only the second", pendingLen, secondPending)
		}
	}
	assertOneFetch()
	select {
	case <-started:
		close(release)
		t.Fatal("B started a second fetch while one was in flight")
	case <-time.After(time.Second):
		assertOneFetch()
	}
	close(release)
	b.d.inflight.Wait()
}

// TestRejectsUnrequestedRecords checks that a record never requested is dropped
// and logged as directory_unrequested_records.
func TestRejectsUnrequestedRecords(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)
	clearReconcileSent(b.ep)

	x := reconcileCard(t, 200, 1, "hostX")
	b.d.handleRecordsState(p.fp, reconcileRecordsPayload(t, x.wire()))

	if _, ok := reconcileLookup(b, x.id); ok {
		t.Fatal("B accepted a record it never requested")
	}
	logs := b.logs.String()
	if !strings.Contains(logs, "directory_unrequested_records") {
		t.Fatalf("missing directory_unrequested_records warning: %s", logs)
	}
	if !strings.Contains(logs, "count=1") {
		t.Fatalf("warning does not carry count=1: %s", logs)
	}
	if !strings.Contains(logs, p.fp.String()) {
		t.Fatalf("warning does not name the peer: %s", logs)
	}
}

// TestUnrequestedRecordsMergeIntoOneWarning checks that all unrequested records
// in one frame produce a single warning with the total count.
func TestUnrequestedRecordsMergeIntoOneWarning(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)
	clearReconcileSent(b.ep)

	x := reconcileCard(t, 200, 1, "hostX")
	y := reconcileCard(t, 201, 1, "hostY")
	b.d.handleRecordsState(p.fp, reconcileRecordsPayload(t, x.wire(), y.wire()))

	logs := b.logs.String()
	if got := strings.Count(logs, "directory_unrequested_records"); got != 1 {
		t.Fatalf("directory_unrequested_records warnings = %d, want 1: %s", got, logs)
	}
	if !strings.Contains(logs, "count=2") {
		t.Fatalf("warning does not merge both drops into count=2: %s", logs)
	}
}

// TestRequestedRecordWindowExpires checks that a record is accepted only while
// the request that asked for it is still in its window.
func TestRequestedRecordWindowExpires(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	started := make(chan struct{}, 1)
	p.ep.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			select {
			case started <- struct{}{}:
			default:
			}
			return directorypb.KindRecords, nil
		})

	x := reconcileCard(t, 200, 1, "hostX")
	b.d.handleRecordList(p.fp, reconcileListPayload(t, x.recordVersion()))
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("B never sent the record request")
	}
	b.d.inflight.Wait()

	clk, ok := b.d.clk.(*fakeClock)
	if !ok {
		t.Fatalf("clock type = %T, want *fakeClock", b.d.clk)
	}
	clk.Advance(2*requestTimeout + time.Second)

	b.d.handleRecordsState(p.fp, reconcileRecordsPayload(t, x.wire()))
	if _, ok := reconcileLookup(b, x.id); ok {
		t.Fatal("B accepted a record after its request window expired")
	}
}

// TestDisconnectClearsReconcileState checks that a peer's pending set and accept
// window are dropped when its link goes down.
func TestDisconnectClearsReconcileState(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	p.ep.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return directorypb.KindRecords, nil
		})

	x := reconcileCard(t, 200, 1, "hostX")
	b.d.handleRecordList(p.fp, reconcileListPayload(t, x.recordVersion()))
	select {
	case <-started:
	case <-time.After(eventTimeout):
		close(release)
		t.Fatal("B never sent the record request")
	}

	net.Disconnect(p.fp, b.fp)

	b.d.mu.RLock()
	_, ok := b.d.recon[p.fp]
	b.d.mu.RUnlock()
	if ok {
		close(release)
		t.Fatal("the reconcile state survived the link going down")
	}

	close(release)
	b.d.inflight.Wait()
}

// TestReconnectRejectsOldWindowRecords checks that an accept window of the
// previous link does not accept records after reconnect.
func TestReconnectRejectsOldWindowRecords(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	pfp := newTestIdentity(2).Fingerprint()
	p := net.Endpoint(pfp)

	net.Connect(b.fp, pfp)
	net.Settle(t)
	p.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			return directorypb.KindRecords, nil
		})

	x := reconcileCard(t, 200, 1, "hostX")
	b.d.handleRecordList(pfp, reconcileListPayload(t, x.recordVersion()))
	b.d.inflight.Wait()

	b.d.mu.RLock()
	pr := b.d.recon[pfp]
	precondition := false
	if pr != nil {
		_, precondition = pr.requested[x.id]
	}
	b.d.mu.RUnlock()
	if !precondition {
		t.Fatal("B's accept window does not hold the requested identifier")
	}

	net.Disconnect(pfp, b.fp)
	net.Connect(b.fp, pfp)
	net.Settle(t)

	y := reconcileCard(t, 201, 1, "hostY")
	b.d.handleRecordsState(pfp, reconcileRecordsPayload(t, x.wire(), y.wire()))

	if _, ok := reconcileLookup(b, x.id); ok {
		t.Fatal("B accepted a record from the accept window of the previous link")
	}
	if _, ok := reconcileLookup(b, y.id); ok {
		t.Fatal("B accepted a record it never requested")
	}
	logs := b.logs.String()
	if got := strings.Count(logs, "directory_unrequested_records"); got != 1 {
		t.Fatalf("directory_unrequested_records warnings = %d, want 1: %s", got, logs)
	}
	if !strings.Contains(logs, "count=2") {
		t.Fatalf("warning does not merge both drops into count=2: %s", logs)
	}
}

// TestRequestedWindowCappedAtMaxRecords checks that the accept window stays
// bounded by directory.max_records against a peer that answers every request
// with no records.
func TestRequestedWindowCappedAtMaxRecords(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	b.set.SetMaxRecords(4)

	pfp := newTestIdentity(2).Fingerprint()
	p := net.Endpoint(pfp)
	p.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			return directorypb.KindRecords, nil
		})

	net.Connect(b.fp, pfp)
	net.Settle(t)

	const rounds = 20
	const perRound = 4
	for i := 0; i < rounds; i++ {
		vs := make([]*directorypb.RecordVersion, 0, perRound)
		for j := 0; j < perRound; j++ {
			seed := byte(100 + i*perRound + j)
			vs = append(vs, reconcileCard(t, seed, 1, fmt.Sprintf("host-%d-%d", i, j)).recordVersion())
		}
		b.d.handleRecordList(pfp, reconcileListPayload(t, vs...))
		net.Settle(t)
		b.d.inflight.Wait()
	}

	b.d.mu.RLock()
	held := -1
	if pr := b.d.recon[pfp]; pr != nil {
		held = len(pr.requested)
	}
	b.d.mu.RUnlock()
	if held > 4 {
		t.Fatalf("accept window holds %d identifiers, want at most directory.max_records (4)", held)
	}
}

// TestCompletedRequestWindowsAreDropped checks that a request window whose
// identifiers all arrived is dropped at once.
func TestCompletedRequestWindowsAreDropped(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	b.set.SetMaxRecords(64)

	pfp := newTestIdentity(2).Fingerprint()
	p := net.Endpoint(pfp)

	records := make(map[recordID]*directorypb.Record)
	p.HandleRequest(directorypb.KindRecordRequest,
		func(_ context.Context, _ Fingerprint, payload []byte) (uint32, []byte) {
			var req directorypb.RecordRequest
			if err := proto.Unmarshal(payload, &req); err != nil {
				return directorypb.KindRecords, nil
			}
			out := make([]*directorypb.Record, 0, len(req.GetRecords()))
			for _, pid := range req.GetRecords() {
				id, ok := idFromProto(pid)
				if !ok {
					continue
				}
				if r, ok := records[id]; ok {
					out = append(out, r)
				}
			}
			reply, err := proto.Marshal(&directorypb.Records{Records: out})
			if err != nil {
				return directorypb.KindRecords, nil
			}
			return directorypb.KindRecords, reply
		})

	net.Connect(b.fp, pfp)
	net.Settle(t)

	const (
		rounds   = 5
		perRound = 4
	)
	for i := 0; i < rounds; i++ {
		vs := make([]*directorypb.RecordVersion, 0, perRound)
		ids := make([]recordID, 0, perRound)
		for j := 0; j < perRound; j++ {
			r := reconcileCard(t, byte(100+i*perRound+j), 1, fmt.Sprintf("win-%d-%d", i, j))
			records[r.id] = r.wire()
			vs = append(vs, r.recordVersion())
			ids = append(ids, r.id)
		}
		b.d.handleRecordList(pfp, reconcileListPayload(t, vs...))
		reconcileAwait(t, net, fmt.Sprintf("round %d to be fully answered", i), func() bool {
			for _, id := range ids {
				if _, ok := reconcileLookup(b, id); !ok {
					return false
				}
			}
			b.d.mu.RLock()
			defer b.d.mu.RUnlock()
			pr := b.d.recon[pfp]
			return pr != nil && len(pr.requested) == 0 && len(pr.windows) == 0
		})
	}
}

// TestScheduleFetchCapsPendingAtMaxRecords checks that the pending set stays
// bounded by directory.max_records while a fetch is in flight.
func TestScheduleFetchCapsPendingAtMaxRecords(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	b.set.SetMaxRecords(3)
	pfp := newTestIdentity(2).Fingerprint()
	p := net.Endpoint(pfp)

	net.Connect(b.fp, pfp)
	net.Settle(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	p.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return directorypb.KindRecords, nil
		})

	first := make([]*directorypb.RecordVersion, 0, 3)
	for i := 0; i < 3; i++ {
		first = append(first, reconcileCard(t, byte(200+i), 1, fmt.Sprintf("first-%d", i)).recordVersion())
	}
	b.d.handleRecordList(pfp, reconcileListPayload(t, first...))
	select {
	case <-started:
	case <-time.After(eventTimeout):
		close(release)
		t.Fatal("B never sent the first record request")
	}

	second := make([]*directorypb.RecordVersion, 0, 10)
	for i := 0; i < 10; i++ {
		second = append(second, reconcileCard(t, byte(220+i), 1, fmt.Sprintf("second-%d", i)).recordVersion())
	}
	b.d.handleRecordList(pfp, reconcileListPayload(t, second...))

	b.d.mu.RLock()
	pending := -1
	if pr := b.d.recon[pfp]; pr != nil {
		pending = len(pr.pending)
	}
	b.d.mu.RUnlock()
	close(release)
	b.d.inflight.Wait()

	if pending > 3 {
		t.Fatalf("pending set holds %d identifiers, want at most directory.max_records (3)", pending)
	}
}

// TestRequestedRecordAcceptedOnce checks that a requested identifier is accepted
// once and a second copy is dropped and reported.
func TestRequestedRecordAcceptedOnce(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	pfp := newTestIdentity(2).Fingerprint()
	p := net.Endpoint(pfp)

	net.Connect(b.fp, pfp)
	net.Settle(t)
	p.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			return directorypb.KindRecords, nil
		})

	x := reconcileCard(t, 200, 1, "hostX")
	b.d.handleRecordList(pfp, reconcileListPayload(t, x.recordVersion()))
	b.d.inflight.Wait()

	b.d.handleRecordsState(pfp, reconcileRecordsPayload(t, x.wire()))
	if _, ok := reconcileLookup(b, x.id); !ok {
		t.Fatal("B did not accept the requested record")
	}

	b.d.handleRecordsState(pfp, reconcileRecordsPayload(t, x.wire()))
	logs := b.logs.String()
	if got := strings.Count(logs, "directory_unrequested_records"); got != 1 {
		t.Fatalf("directory_unrequested_records warnings = %d, want 1: %s", got, logs)
	}
}

// TestTakeBatchCapsRequestByMaxRecords checks that one RecordRequest never names
// more identifiers than directory.max_records.
func TestTakeBatchCapsRequestByMaxRecords(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	b.set.SetMaxRecords(3)

	b.d.mu.Lock()
	pr := newPeerReconcile()
	for i := 0; i < 10; i++ {
		pr.pending[recordID{publisher: newTestIdentity(byte(150 + i)).Fingerprint(), kind: recKindCard}] = struct{}{}
	}
	batch := b.d.takeBatch(pr)
	b.d.mu.Unlock()

	if len(batch) > 3 {
		t.Fatalf("one record request named %d identifiers, want at most directory.max_records (3)", len(batch))
	}
}

// TestNoFramesToNonPeers checks that accepting a card from a non-peer sends no
// frame outside the established links.
func TestNoFramesToNonPeers(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	x := reconcileCard(t, 200, 1, "hostX", reconcileDirectAddr("198.51.100.4:17443"))
	b.d.acceptBatch(p.fp, []*directorypb.Record{x.wire()})
	net.Settle(t)

	for _, f := range b.ep.Sent() {
		if f.to != p.fp {
			t.Fatalf("B sent a frame to %v, which is not an established peer", f.to)
		}
	}

	b.d.mu.RLock()
	ups := len(b.d.up)
	_, isUp := b.d.up[p.fp]
	b.d.mu.RUnlock()
	if ups != 1 || !isUp {
		t.Fatalf("B's established set has %d entries (P up: %v), want only P", ups, isUp)
	}
}

// TestReplaceIntervalDropsNewerVersion checks that a too-fast replacement is
// dropped and not forwarded, and a version after the interval replaces it and is
// forwarded.
func TestReplaceIntervalDropsNewerVersion(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)
	c := newReconcileNode(t, net, 3)

	net.Connect(p.fp, b.fp)
	net.Connect(b.fp, c.fp)
	net.Settle(t)

	v1 := reconcileCard(t, 200, 1, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})
	clearReconcileSent(b.ep)

	clk, ok := b.d.clk.(*fakeClock)
	if !ok {
		t.Fatalf("clock type = %T, want *fakeClock", b.d.clk)
	}
	clk.Advance(time.Second)
	v2 := reconcileCard(t, 200, 2, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v2.wire()})

	held, present := reconcileLookup(b, v1.id)
	if !present || held.version != 1 {
		t.Fatalf("held version = %d (present %v), want 1 after a replacement one second later", heldVersion(held), present)
	}
	if got := reconcileFrames(b.ep, c.fp, directorypb.KindRecordList); got != 0 {
		t.Fatalf("B forwarded %d lists for the dropped version, want 0", got)
	}
	if logs := b.logs.String(); !strings.Contains(logs, "limit=replace_interval") {
		t.Fatalf("drop warning does not name replace_interval: %s", logs)
	}

	clk.Advance(60 * time.Second)
	v3 := reconcileCard(t, 200, 3, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v3.wire()})

	held, present = reconcileLookup(b, v1.id)
	if !present || held.version != 3 {
		t.Fatalf("held version = %d (present %v), want 3 after the interval", heldVersion(held), present)
	}
	if got := reconcileFrames(b.ep, c.fp, directorypb.KindRecordList); got != 1 {
		t.Fatalf("B forwarded %d lists after the replacement, want 1", got)
	}
}

func heldVersion(r *storedRecord) uint64 {
	if r == nil {
		return 0
	}
	return r.version
}

// TestRecordListDoesNotRequestWithinReplaceInterval checks that a newer version
// is not requested within the replacement interval and is fetched after it.
func TestRecordListDoesNotRequestWithinReplaceInterval(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	v1 := reconcileCard(t, 200, 1, "hostX")
	b.d.acceptBatch(p.fp, []*directorypb.Record{v1.wire()})

	clk, ok := b.d.clk.(*fakeClock)
	if !ok {
		t.Fatalf("clock type = %T, want *fakeClock", b.d.clk)
	}
	clk.Advance(time.Second)

	v2 := reconcileCard(t, 200, 2, "hostX")
	reconcileInject(t, p, v2)
	clearReconcileSent(b.ep)

	b.d.handleRecordList(p.fp, reconcileListPayload(t, v2.recordVersion()))
	b.d.inflight.Wait()
	net.Settle(t)
	if got := reconcileFrames(b.ep, p.fp, directorypb.KindRecordRequest); got != 0 {
		t.Fatalf("B requested a replacement %d times within the interval, want 0", got)
	}

	clk.Advance(60 * time.Second)
	b.d.handleRecordList(p.fp, reconcileListPayload(t, v2.recordVersion()))
	reconcileAwait(t, net, "B to fetch v2 after the interval", func() bool {
		held, ok := reconcileLookup(b, v1.id)
		return ok && held.version == 2
	})
}
