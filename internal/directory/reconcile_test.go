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

// reconcileNode is one Directory wired to a fake network endpoint. The helpers
// below are prefixed with "reconcile" so they cannot collide with helpers added
// by the other work items that share this package.
type reconcileNode struct {
	d    *Directory
	ep   *fakeLinks
	fp   Fingerprint
	set  *fakeSettings
	logs *lockedBuffer
}

// newReconcileNode opens a directory for seed, subscribes it to link
// notifications and registers the reconciliation handlers.
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

// reconcileInject puts r into n's directory without going through a link.
func reconcileInject(t *testing.T, n *reconcileNode, r *storedRecord) {
	t.Helper()
	n.d.mu.Lock()
	n.d.st.putOwn(r)
	n.d.dirty = true
	n.d.mu.Unlock()
}

// reconcileLookup returns the record held for id.
func reconcileLookup(n *reconcileNode, id recordID) (*storedRecord, bool) {
	n.d.mu.RLock()
	defer n.d.mu.RUnlock()
	return n.d.st.get(id)
}

// clearReconcileSent empties the endpoint's record of sent frames.
func clearReconcileSent(ep *fakeLinks) {
	ep.mu.Lock()
	ep.sent = nil
	ep.mu.Unlock()
}

// reconcileAwait drains the fake network until cond reports true. It polls the
// observable state instead of passing the directories' inflight groups to
// fakeNet.Settle: Settle's Wait can run while a fetch goroutine spawned by an
// already finished delivery performs its first Add on a zero counter, which the
// race detector reports as WaitGroup misuse. Polling does not sleep, so the
// three-second bound still applies.
func reconcileAwait(t *testing.T, net *fakeNet, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
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

// reconcileListPayload encodes a RecordList frame body.
func reconcileListPayload(t *testing.T, vs ...*directorypb.RecordVersion) []byte {
	t.Helper()
	p, err := proto.Marshal(&directorypb.RecordList{Records: vs})
	if err != nil {
		t.Fatalf("marshal RecordList: %v", err)
	}
	return p
}

// reconcileRequestPayload encodes a RecordRequest frame body.
func reconcileRequestPayload(t *testing.T, ids ...*directorypb.RecordId) []byte {
	t.Helper()
	p, err := proto.Marshal(&directorypb.RecordRequest{Records: ids})
	if err != nil {
		t.Fatalf("marshal RecordRequest: %v", err)
	}
	return p
}

// reconcileListHasID reports whether an encoded RecordList mentions id.
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

// reconcileRequestHasID reports whether an encoded RecordRequest asks for id.
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

// reconcileListCount counts the RecordList frames ep sent to to that mention id.
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

// reconcileFrames counts the frames ep sent to to with kind.
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

// reconcileCard signs a card with the key derived from seed.
func reconcileCard(t *testing.T, seed byte, version uint64, alias string, addrs ...*directorypb.AdvertisedAddress) *storedRecord {
	t.Helper()
	return mustSignRecord(t, seed, recKindCard, "", version, dirTestNow().Add(recordTTL), recTestCard(alias, addrs...))
}

// reconcileAnnouncement signs a public topic declaration with the key derived
// from seed.
func reconcileAnnouncement(t *testing.T, seed byte, name, description string, version uint64) *storedRecord {
	t.Helper()
	return mustSignRecord(t, seed, recKindTopic, name, version, dirTestNow().Add(recordTTL),
		&directorypb.TopicAnnouncement{Name: name, Description: description})
}

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

	// A holds the higher version of X, so it must not ask B for the older one.
	for _, f := range a.ep.Sent() {
		if f.to != b.fp || f.kind != directorypb.KindRecordRequest {
			continue
		}
		if reconcileRequestHasID(t, f.payload, x1.id) {
			t.Fatal("A requested the older X from B")
		}
	}
}

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

// REQ-broker-discovery-006: tampered records and records that claim another
// publisher's fingerprint are dropped with a warning naming the source.
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

// REQ-broker-discovery-010: frames from a fingerprint without an established
// link change nothing, and a record request from it is answered with nothing.
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

	// A RecordRequest from the same fingerprint must get an empty answer even
	// when the directory holds the record it asks for.
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
	// No further frame can arrive, so the fetch is the last work the group holds.
	b.d.inflight.Wait()

	if _, ok := reconcileLookup(b, x.id); ok {
		t.Fatal("B accepted a record that it never received")
	}
	if got := reconcileFrames(b.ep, pfp, directorypb.KindRecordRequest); got != 1 {
		t.Fatalf("B sent %d record requests, want 1", got)
	}
}

// reconcileRecordsPayload encodes a Records frame body.
func reconcileRecordsPayload(t *testing.T, recs ...*directorypb.Record) []byte {
	t.Helper()
	p, err := proto.Marshal(&directorypb.Records{Records: recs})
	if err != nil {
		t.Fatalf("marshal Records: %v", err)
	}
	return p
}

// reconcileRequestBatchSizes returns the number of identifiers in every
// RecordRequest frame ep sent to to, in send order.
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

// TestReconcileCapsInflightFetchPerPeer checks that a peer cannot stack fetch
// goroutines: while one request is in flight, further RecordList frames for the
// same identifier merge into the pending set instead of starting another.
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
	case <-time.After(3 * time.Second):
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

// TestReconcileMergesDistinctIDsWhileFetching checks that a RecordList naming
// an identifier other than the one in flight joins the pending set instead of
// starting a second fetch.
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
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("B never sent the first record request")
	}

	b.d.handleRecordList(pfp, reconcileListPayload(t, second.recordVersion()))

	select {
	case <-started:
		close(release)
		t.Fatal("B started a second fetch while one was in flight")
	case <-time.After(time.Second):
	}
	close(release)
	b.d.inflight.Wait()
}

// TestRejectsUnrequestedRecords checks that a Records state frame carrying a
// record this direction never asked for is dropped as a whole and logged.
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
// in one frame produce a single warning carrying the total count.
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
// the request that asked for it is still inside its window.
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
	case <-time.After(3 * time.Second):
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

// TestDisconnectClearsReconcileState checks that the pending set and the accept
// window of a peer are dropped when its link goes down.
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
	case <-time.After(3 * time.Second):
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

// TestReconnectRejectsOldWindowRecords checks that the accept window belongs to
// the link: a record whose request was sent before the link went down is
// dropped as a whole, and reported, after the link comes back. Without the
// per-link reset a reconnected peer could deliver a record requested on the
// previous link.
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

	// x names the identifier requested on the previous link; y was never
	// requested at all. Both are dropped, and the whole frame is reported once.
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

// TestRequestedWindowCappedAtMaxRecords checks that the identifiers this
// direction has asked a peer for but not yet accepted stay bounded by
// directory.max_records. A peer that keeps sending fresh RecordList frames and
// answers every request with an empty Records frame must not be able to grow the
// accept window without bound: each round would otherwise retain a full batch
// of new identifiers for the whole 60-second window.
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

// TestScheduleFetchCapsPendingAtMaxRecords checks that the identifiers waiting
// for a request stay bounded by directory.max_records even when more arrive
// while a request is in flight.
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
	case <-time.After(3 * time.Second):
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
// only once: the first record consumes its accept entry, so a second copy of the
// same record names an identifier that is no longer requested and is dropped and
// reported.
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
// more identifiers than directory.max_records, even when the pending set holds
// more.
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

// REQ-broker-pairing-014: accepting a card from a Broker that is not a peer
// never makes this package send a frame outside the established links.
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

// TestCompletedRequestWindowsAreDropped checks that an accept window whose
// identifiers all arrived is dropped at once. A peer that answers every request
// immediately must not leave one window per request behind for the whole accept
// TTL, which would tie memory to the request rate rather than to the
// outstanding identifiers.
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
