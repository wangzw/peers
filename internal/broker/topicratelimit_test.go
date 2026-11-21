package broker

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// TestTopicRateLimiter checks the token bucket directly: a burst of 64, a refill
// of 4 each second, and independent buckets per account.
func TestTopicRateLimiter(t *testing.T) {
	rl := &topicRateLimiter{}
	now := time.Unix(1700000000, 0)

	for i := 0; i < topicRateBurst; i++ {
		if !rl.allow(1, now) {
			t.Fatalf("change %d was refused inside the burst", i)
		}
	}
	if rl.allow(1, now) {
		t.Fatal("change past the burst was allowed")
	}
	if !rl.allow(2, now) {
		t.Fatal("another account was refused")
	}

	now = now.Add(time.Second)
	for i := 0; i < topicRatePerSecond; i++ {
		if !rl.allow(1, now) {
			t.Fatalf("refilled change %d was refused", i)
		}
	}
	if rl.allow(1, now) {
		t.Fatal("a change past the refilled tokens was allowed")
	}
}

// TestLeaveTopicRateLimited checks the LeaveTopic path: leaving a topic the
// agent is not in still consumes a token, the 65th leave in a burst is refused
// with quota_exceeded, and the refilled tokens work a second later.
func TestLeaveTopicRateLimited(t *testing.T) {
	tb := newTestBroker(t)
	id, _ := registerTopicAgent(t, tb, "leavelimit")
	ctx := topicCallContext(t)

	leave := func(name string) error {
		_, err := tb.client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{AgentId: id, Name: name})
		return err
	}
	for i := 0; i < topicRateBurst; i++ {
		if err := leave(fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("LeaveTopic %d: %v", i, err)
		}
	}
	err := leave("one-too-many")
	code, wire, _ := topicErrorDetail(t, err)
	if code != codes.ResourceExhausted || wire != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Fatalf("LeaveTopic past the burst = %v/%v, want ResourceExhausted/QUOTA_EXCEEDED", code, wire)
	}

	tb.clock.Advance(time.Second)
	for i := 0; i < topicRatePerSecond; i++ {
		if err := leave(fmt.Sprintf("refilled%d", i)); err != nil {
			t.Fatalf("refilled LeaveTopic %d: %v", i, err)
		}
	}
}

// TestJoinTopicRateLimited checks the RPC path: the 65th join from one account
// in a burst is refused with quota_exceeded and changes no membership, and a
// second later four more succeed.
func TestJoinTopicRateLimited(t *testing.T) {
	tb := newTestBroker(t)
	id, rs := registerTopicAgent(t, tb, "ratelimit")
	ctx := topicCallContext(t)

	join := func(name string) error {
		_, err := tb.client.JoinTopic(ctx, &localpb.JoinTopicRequest{AgentId: id, Name: name})
		return err
	}
	for i := 0; i < topicRateBurst; i++ {
		if err := join(fmt.Sprintf("t%d", i)); err != nil {
			t.Fatalf("JoinTopic %d: %v", i, err)
		}
	}
	err := join("one-too-many")
	code, wire, _ := topicErrorDetail(t, err)
	if code != codes.ResourceExhausted || wire != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Fatalf("JoinTopic past the burst = %v/%v, want ResourceExhausted/QUOTA_EXCEEDED", code, wire)
	}
	if got := tb.topics.TopicsOf(AgentID(id)); len(got) != topicRateBurst {
		t.Fatalf("after the refused join the agent has %d topics, want %d", len(got), topicRateBurst)
	}

	tb.clock.Advance(time.Second)
	for i := 0; i < topicRatePerSecond; i++ {
		if err := join(fmt.Sprintf("refilled%d", i)); err != nil {
			t.Fatalf("refilled JoinTopic %d: %v", i, err)
		}
	}
	if err := join("refilled-too-many"); err == nil {
		t.Fatal("a join past the refilled tokens succeeded")
	}

	// Unregistering is not limited: the exhausted account still leaves the
	// table and every membership goes with it.
	rs.cancel()
	waitForReg(t, "the agent to leave the topic table", func() bool {
		tb.topics.mu.Lock()
		defer tb.topics.mu.Unlock()
		return len(tb.topics.members) == 0
	})
}
