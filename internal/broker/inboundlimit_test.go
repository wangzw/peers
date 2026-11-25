package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
)

// TestReceivePerPeerSessionQuota checks that one peer cannot write more than the per-target burst to one Agent, and a resend of the refused id gets the same result.
func TestReceivePerPeerSessionQuota(t *testing.T) {
	target := testAgent("frontend", "/tmp/ap-receive-burst.sock")
	f := newReceiveFixture(t, defaultReceiveSettings(), target)
	f.keepReread(target.ID)
	sender := remoteMember{Name: "api-worker", Ref: "1a2b3c"}
	f.member(sender, "t1")
	f.share(target.ID, "t1")

	rec := newRecordingInbox()
	f.setInbox(target.ID, rec)

	const burst = 10
	for i := 0; i < burst; i++ {
		frame := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(i))
		wantDelivered(t, f.handle(frame))
	}

	over := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(burst))
	detail := wantReceiveError(t, f.handle(over), localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
		"Not sent: frontend is receiving too many messages from your Broker")
	if !strings.Contains(detail.GetMessage(), "try again in") {
		t.Fatalf("message = %q, want a retry hint", detail.GetMessage())
	}
	if got := rec.Count(); got != burst {
		t.Fatalf("inbox calls = %d, want %d", got, burst)
	}

	// A resend of the refused id gets the recorded quota result, not a write.
	wantReceiveError(t, f.handle(over), localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "")
	if got := rec.Count(); got != burst {
		t.Fatalf("inbox calls after a resend = %d, want %d", got, burst)
	}

	if logs := f.logs.String(); !strings.Contains(logs, `"result":"quota_exceeded"`) {
		t.Fatalf("the refusal is not logged with result quota_exceeded: %s", logs)
	}
}

// TestReceiveQuotaSurvivesReconnect checks that dropping and re-establishing the link does not hand the peer a fresh quota.
func TestReceiveQuotaSurvivesReconnect(t *testing.T) {
	target := testAgent("frontend", "/tmp/ap-receive-reconnect.sock")
	f := newReceiveFixture(t, defaultReceiveSettings(), target)
	f.keepReread(target.ID)
	sender := remoteMember{Name: "api-worker", Ref: "1a2b3c"}
	f.member(sender, "t1")
	f.share(target.ID, "t1")

	rec := newRecordingInbox()
	f.setInbox(target.ID, rec)

	f.net.setUp(f.self, f.peer, true)
	for i := 0; i < inboundLimitBurst; i++ {
		frame := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(i))
		wantDelivered(t, f.handle(frame))
	}

	f.net.setUp(f.self, f.peer, false)
	f.net.setUp(f.self, f.peer, true)

	over := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(inboundLimitBurst))
	wantReceiveError(t, f.handle(over), localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "")
	if got := rec.Count(); got != inboundLimitBurst {
		t.Fatalf("inbox calls after the reconnect = %d, want %d", got, inboundLimitBurst)
	}
}

// TestReceiveQuotaIsPerPeerAndTarget checks that an exhausted quota affects only its own peer and target pair.
func TestReceiveQuotaIsPerPeerAndTarget(t *testing.T) {
	targetA := testAgent("frontend", "/tmp/ap-receive-quota-a.sock")
	targetB := testAgent("backend", "/tmp/ap-receive-quota-b.sock")
	f := newReceiveFixture(t, defaultReceiveSettings(), targetA, targetB)
	f.keepReread(targetA.ID)
	f.keepReread(targetB.ID)

	sender := remoteMember{Name: "api-worker", Ref: "1a2b3c"}
	f.member(sender, "t1")
	f.topics.remote[f.other] = append(f.topics.remote[f.other], deliveryRemoteEntry{
		Member: sender,
		Topics: []string{"t1"},
	})
	f.peers.effective[f.other] = "hostB"
	f.share(targetA.ID, "t1")
	f.share(targetB.ID, "t1")

	recA := newRecordingInbox()
	recB := newRecordingInbox()
	f.setInbox(targetA.ID, recA)
	f.setInbox(targetB.ID, recB)

	for i := 0; i < inboundLimitBurst; i++ {
		frame := f.requestFrameWithID(sender, targetA, "hello", time.Second, receiveMsgIDN(i))
		wantDelivered(t, f.handle(frame))
	}
	over := f.requestFrameWithID(sender, targetA, "hello", time.Second, receiveMsgIDN(inboundLimitBurst))
	wantReceiveError(t, f.handle(over), localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "")

	other := f.requestFrameWithID(sender, targetA, "hello", time.Second, receiveMsgIDN(100))
	wantDelivered(t, f.d.handleDeliver(context.Background(), f.other, other))

	elsewhere := f.requestFrameWithID(sender, targetB, "hello", time.Second, receiveMsgIDN(200))
	wantDelivered(t, f.handle(elsewhere))

	if got := recA.Count(); got != inboundLimitBurst+1 {
		t.Fatalf("inbox calls for the first target = %d, want %d", got, inboundLimitBurst+1)
	}
	if got := recB.Count(); got != 1 {
		t.Fatalf("inbox calls for the second target = %d, want 1", got)
	}
}

// TestReceiveQuotaRefillsWithClock checks that a bucket refills one token per interval on the delivery's injected clock.
func TestReceiveQuotaRefillsWithClock(t *testing.T) {
	target := testAgent("frontend", "/tmp/ap-receive-refill.sock")
	f := newReceiveFixture(t, defaultReceiveSettings(), target)
	f.keepReread(target.ID)
	sender := remoteMember{Name: "api-worker", Ref: "1a2b3c"}
	f.member(sender, "t1")
	f.share(target.ID, "t1")

	rec := newRecordingInbox()
	f.setInbox(target.ID, rec)

	clock := &dedupeFakeClock{now: time.Unix(0, 0)}
	f.d.inbound = newInboundLimiter(clock.Now)

	for i := 0; i < inboundLimitBurst; i++ {
		frame := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(i))
		wantDelivered(t, f.handle(frame))
	}
	over := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(inboundLimitBurst))
	wantReceiveError(t, f.handle(over), localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "")

	clock.Advance(9 * time.Second)
	detail := wantReceiveError(t, f.handle(f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(101))),
		localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "")
	if want := "Not sent: frontend is receiving too many messages from your Broker; try again in 1 second"; detail.GetMessage() != want {
		t.Fatalf("reply message = %q, want %q", detail.GetMessage(), want)
	}

	clock.Advance(2 * time.Second)
	wantDelivered(t, f.handle(f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(102))))

	if got := rec.Count(); got != inboundLimitBurst+1 {
		t.Fatalf("inbox calls = %d, want %d", got, inboundLimitBurst+1)
	}
}

// TestReceiveQuotaChargedOncePerMessage checks that the inbox retries of one message share the one token it took.
func TestReceiveQuotaChargedOncePerMessage(t *testing.T) {
	target := testAgent("frontend", "/tmp/ap-receive-retry.sock")
	settings := defaultReceiveSettings()
	settings.InboxRetries = 2
	f := newReceiveFixture(t, settings, target)
	f.keepReread(target.ID)
	sender := remoteMember{Name: "api-worker", Ref: "1a2b3c"}
	f.member(sender, "t1")
	f.share(target.ID, "t1")

	clock := &dedupeFakeClock{now: time.Unix(0, 0)}
	f.d.inbound = newInboundLimiter(clock.Now)

	rec := newRecordingInbox(inbox.ErrNotWritten, inbox.ErrNotWritten, nil)
	f.setInbox(target.ID, rec)

	first := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(0))
	wantDelivered(t, f.handle(first))
	if got := rec.Count(); got != 3 {
		t.Fatalf("inbox calls for the retried message = %d, want 3", got)
	}

	for i := 1; i < inboundLimitBurst; i++ {
		frame := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(i))
		wantDelivered(t, f.handle(frame))
	}
	over := f.requestFrameWithID(sender, target, "hello", time.Second, receiveMsgIDN(inboundLimitBurst))
	wantReceiveError(t, f.handle(over), localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "")
}

// TestInboundLimiterDropsBucketsOnUnpair checks that an unpaired peer or an ended target starts again with a full bucket.
func TestInboundLimiterDropsBucketsOnUnpair(t *testing.T) {
	peerA, peerB := fakenetPeer(20), fakenetPeer(30)
	clock := &dedupeFakeClock{now: time.Unix(0, 0)}
	l := newInboundLimiter(clock.Now)

	exhaust := func(peer peerID, target string) {
		t.Helper()
		for i := 0; i < inboundLimitBurst; i++ {
			if ok, _ := l.take(peer, target); !ok {
				t.Fatalf("take %d for %v/%s failed, want a full burst", i, peer, target)
			}
		}
		if ok, _ := l.take(peer, target); ok {
			t.Fatalf("take after the burst for %v/%s succeeded, want refusal", peer, target)
		}
	}

	exhaust(peerA, "a")
	exhaust(peerB, "a")
	exhaust(peerA, "b")

	l.dropPeer(peerA)
	if ok, _ := l.take(peerA, "a"); !ok {
		t.Fatal("an unpaired peer got no fresh bucket")
	}
	if ok, _ := l.take(peerA, "b"); !ok {
		t.Fatal("an unpaired peer got no fresh bucket for its other target")
	}
	if ok, _ := l.take(peerB, "a"); ok {
		t.Fatal("unpairing one peer refilled another peer's bucket")
	}

	l.dropTarget("a")
	if ok, _ := l.take(peerB, "a"); !ok {
		t.Fatal("a dropped target got no fresh bucket")
	}
}

// TestInboundLimiterKeepsOneBucketPerPair checks that a refill adds no second bucket and a full bucket is never stored.
func TestInboundLimiterKeepsOneBucketPerPair(t *testing.T) {
	peer := fakenetPeer(20)
	clock := &dedupeFakeClock{now: time.Unix(0, 0)}
	l := newInboundLimiter(clock.Now)

	if ok, _ := l.take(peer, "a"); !ok {
		t.Fatal("the first take failed, want a full bucket")
	}
	clock.Advance(defaultInboundLimitInterval * inboundLimitBurst)
	if ok, _ := l.take(peer, "a"); !ok {
		t.Fatal("the take after a full refill failed")
	}

	l.mu.Lock()
	held := len(l.buckets)
	l.mu.Unlock()
	if held != 1 {
		t.Fatalf("buckets held = %d, want 1", held)
	}
}
