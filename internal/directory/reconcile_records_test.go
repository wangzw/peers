package directory

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

const replyBudget = 1 << 20

// TestAcceptsOversizedRecordLogsReason checks that a record over the 64 KiB raw
// limit is dropped and the warning names the reason.
func TestAcceptsOversizedRecordLogsReason(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	b.d.acceptBatch(p.fp, []*directorypb.Record{oversizedRecord(t, 43)})

	b.d.mu.RLock()
	held := len(b.d.st.records)
	b.d.mu.RUnlock()
	if held != 0 {
		t.Fatalf("B holds %d records, want 0", held)
	}

	out := b.logs.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("no warning was logged: %s", out)
	}
	if !strings.Contains(out, "reason=record_too_large") {
		t.Fatalf("warning does not carry the reason: %s", out)
	}
}

// TestRecordRequestDedupesIDs checks that asking twice for the same identifier
// returns the record once.
func TestRecordRequestDedupesIDs(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	x := reconcileCard(t, 200, 1, "hostX")
	reconcileInject(t, b, x)
	clearReconcileSent(b.ep)

	kind, reply := b.d.handleRecordRequest(context.Background(), p.fp,
		reconcileRequestPayload(t, x.id.toProto(), x.id.toProto()))
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
}

func largeAnnouncements(t *testing.T, count int) []*storedRecord {
	t.Helper()
	description := strings.Repeat("d", 60*1024)
	out := make([]*storedRecord, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, reconcileAnnouncement(t, byte(100+i), "news", description, 1))
	}
	return out
}

// TestRecordRequestSplitsReplyUnderBudget checks that a reply over the 1 MiB
// budget is split into frames under the budget, each record appearing once.
func TestRecordRequestSplitsReplyUnderBudget(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	const count = 20
	recs := largeAnnouncements(t, count)
	ids := make([]*directorypb.RecordId, 0, count+1)
	wantRaw := make(map[string]bool, count)
	for _, r := range recs {
		reconcileInject(t, b, r)
		ids = append(ids, r.id.toProto())
		wantRaw[string(r.raw)] = true
	}
	ids = append(ids, ids[0]) // ask for the first record twice
	clearReconcileSent(b.ep)

	kind, reply := b.d.handleRecordRequest(context.Background(), p.fp, reconcileRequestPayload(t, ids...))
	if kind != directorypb.KindRecords {
		t.Fatalf("reply kind = %d, want %d", kind, directorypb.KindRecords)
	}

	frames := [][]byte{reply}
	for _, f := range b.ep.Sent() {
		if f.to == p.fp && f.kind == directorypb.KindRecords {
			frames = append(frames, f.payload)
		}
	}
	if len(frames) < 2 {
		t.Fatalf("reply was sent in %d frame(s), want at least 2", len(frames))
	}

	seen := make(map[string]int, count)
	for i, payload := range frames {
		if len(payload) > replyBudget {
			t.Fatalf("frame %d is %d bytes, over the %d-byte budget", i, len(payload), replyBudget)
		}
		var batch directorypb.Records
		if err := proto.Unmarshal(payload, &batch); err != nil {
			t.Fatalf("unmarshal frame %d: %v", i, err)
		}
		for _, r := range batch.GetRecords() {
			key := string(r.GetRaw())
			if !wantRaw[key] {
				t.Fatalf("frame %d holds an unrequested record", i)
			}
			seen[key]++
		}
	}
	if len(seen) != count {
		t.Fatalf("reply holds %d distinct records, want %d", len(seen), count)
	}
	for raw, n := range seen {
		if n != 1 {
			t.Fatalf("a record appears %d times in the reply, want 1", n)
		}
		_ = raw
	}
}

// TestRecordRequestDeliversAllSplitFrames checks that a peer ends up holding
// every record when the reply spans several frames.
func TestRecordRequestDeliversAllSplitFrames(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	const count = 20
	recs := largeAnnouncements(t, count)
	for _, r := range recs {
		reconcileInject(t, b, r)
	}

	clearReconcileSent(b.ep)
	net.Connect(p.fp, b.fp)
	reconcileAwait(t, net, "P to hold every record from the split reply", func() bool {
		for _, r := range recs {
			got, ok := reconcileLookup(p, r.id)
			if !ok || !bytes.Equal(got.raw, r.raw) {
				return false
			}
		}
		return true
	})
	if got := reconcileFrames(b.ep, p.fp, directorypb.KindRecords); got < 1 {
		t.Fatalf("B sent %d Records continuation frames, want at least 1", got)
	}
}

// TestReconcileRequestsInBatches checks that 600 identifiers are requested in
// three requests of at most 256 each and all accepted.
func TestReconcileRequestsInBatches(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)
	p.set.SetMaxRecords(1000)
	p.set.SetMaxRecordsPerLink(1000)
	p.set.SetMaxTopicsPerPublisher(1000)

	const count = 600
	for i := 0; i < count; i++ {
		r := reconcileAnnouncement(t, 200, fmt.Sprintf("news-%03d", i), "", 1)
		reconcileInject(t, b, r)
	}

	clearReconcileSent(b.ep)
	net.Connect(p.fp, b.fp)
	reconcileAwait(t, net, "P to hold all 600 records", func() bool {
		p.d.mu.RLock()
		defer p.d.mu.RUnlock()
		return len(p.d.st.records) == count
	})

	sizes := reconcileRequestBatchSizes(t, p.ep, b.fp)
	if len(sizes) != 3 {
		t.Fatalf("P sent %d record requests for 600 identifiers, want 3 (sizes %v)", len(sizes), sizes)
	}
	for i, n := range sizes {
		if n > 256 {
			t.Fatalf("request %d asked for %d identifiers, want at most 256", i, n)
		}
	}
}

// TestCloseCancelsBlockedFetch checks that Close cancels an in-flight record
// request instead of waiting for its deadline.
func TestCloseCancelsBlockedFetch(t *testing.T) {
	net := newFakeNet()
	b := newReconcileNode(t, net, 1)
	p := newReconcileNode(t, net, 2)

	net.Connect(p.fp, b.fp)
	net.Settle(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	p.ep.HandleRequest(directorypb.KindRecordRequest,
		func(context.Context, Fingerprint, []byte) (uint32, []byte) {
			started <- struct{}{}
			<-release
			return directorypb.KindRecords, nil
		})

	x := reconcileCard(t, 200, 1, "hostX")
	if err := p.ep.Send(b.fp, directorypb.KindRecordList, reconcileListPayload(t, x.recordVersion())); err != nil {
		t.Fatalf("send record list: %v", err)
	}
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("B never sent the blocking record request")
	}

	done := make(chan struct{})
	go func() {
		b.d.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("Close did not return while a fetch was blocked")
	}
}
