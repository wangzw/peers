//go:build e2e

package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/harness"
)

// The size-limit case is defined against the product's own computation. A
// signature change breaks this test at compile time, which is the intent of the
// plan's assertion.
var _ func(name, ref, text string) int = broker.MaxSendFrameSize

const (
	// srTopic is the topic the two sessions share.
	srTopic = "t"

	// srSendBudget is send.request_timeout (2s) plus the 500ms allowance the
	// plan puts on one send in these scenarios.
	srSendBudget = 2500 * time.Millisecond

	// srReplyTimeout is the send budget of the reply-loss case, not the
	// shortened FastTimers value: the fault injection that loses the reply
	// (Cut, Release, Restore) runs inside this budget, and each of its steps
	// is a docker compose exec with no product timer behind it. The injection
	// itself measured 1.4-1.5s, so ten seconds leaves about six times that
	// measurement, enough to absorb a loaded host. Do not raise it to trade
	// stability for a longer timeout, and do not grow it further.
	srReplyTimeout = 10 * time.Second

	// srReplyBudget is srReplyTimeout plus the 500ms allowance the plan puts
	// on one send.
	srReplyBudget = srReplyTimeout + 500*time.Millisecond

	// srRelinkBound is reconnect_max (400ms) + dial_timeout (500ms) + 3s.
	srRelinkBound = 3900 * time.Millisecond

	// srWaitBound bounds a wait that no product timer governs.
	srWaitBound = 3 * time.Second

	// srQuietWindow is send.request_timeout: after the link is back, a message
	// that was not delivered must not surface within the sender's own budget.
	// It is a deliberate observation, as the plan requires for negative checks.
	srQuietWindow = 2 * time.Second

	// srFrameLimit is the encoded-frame limit of one inbox message.
	srFrameLimit = 1_000_000
)

// TestRemoteSendFaults covers cross-host sending under a NAT: both directions
// behind the router, an unreachable peer, the guarantee that a failed send is
// not delivered later, an unknown result after the reply is lost, the reply-loss
// duplicate, and the size limit. A is the acceptor and B the dialer.
func TestRemoteSendFaults(t *testing.T) {
	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	srStartBroker(t, aNode, srConfig())
	srStartBroker(t, bNode, srConfig())
	opA, opB := srOperator(t, aNode), srOperator(t, bNode)

	a1 := srSession(t, aNode, "a1")
	b1 := srSession(t, bNode, "b1")
	defer a1.End(t)
	defer b1.End(t)

	key := srCreateKey(t, opA)
	srConnect(t, opB, srShownAddress(t, c, "hostA", key.Addresses), key.Key).MustOK(t)
	srWaitConnected(t, bNode, "hostA.peer", srRelinkBound)
	srWaitConnected(t, aNode, "hostB.peer", srRelinkBound)
	srWaitRoster(t, a1, "a1", b1, "b1")

	t.Run("REQ-broker-pairing-020 both directions behind NAT", func(t *testing.T) {
		fromA := srMark("nat-a-")
		r := a1.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": fromA})
		r.MustOK(t)
		frame := b1.WaitFrame(t, srWaitBound)
		if body := srEnvelopeBody(t, frame.Content); body != fromA {
			t.Fatalf("B received %q, want %q", body, fromA)
		}

		fromB := srMark("nat-b-")
		r = b1.Call(t, "send_message", map[string]any{"to": "a1@hostA.peer", "message": fromB})
		r.MustOK(t)
		frame = a1.WaitFrame(t, srWaitBound)
		if body := srEnvelopeBody(t, frame.Content); body != fromB {
			t.Fatalf("A received %q, want %q", body, fromB)
		}
	})

	// The two bodies below must never surface after the cut is restored; the
	// next two cases check both that they do not arrive before the sentinel and
	// that they do not arrive in the sender's whole budget afterwards.
	var failedFromA, failedFromB string

	t.Run("REQ-agent-messaging-017 unreachable within budget", func(t *testing.T) {
		srCutAndWaitDown(t, c, aNode, bNode)

		// The dialer side stops after its retries are spent; the acceptor side
		// waits out the rest of the budget. Both must answer inside the budget.
		failedFromB = srMark("unreachable-b-")
		start := time.Now()
		r := b1.Call(t, "send_message", map[string]any{"to": "a1@hostA.peer", "message": failedFromB})
		elapsed := time.Since(start)
		r.MustCode(t, "peer_unreachable")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the dialer's error is %q, want it to start with Not sent", r.Text)
		}
		if elapsed > srSendBudget {
			t.Fatalf("the dialer's send returned after %s, want at most %s", elapsed, srSendBudget)
		}

		failedFromA = srMark("unreachable-a-")
		start = time.Now()
		r = a1.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": failedFromA})
		elapsed = time.Since(start)
		r.MustCode(t, "peer_unreachable")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the acceptor's error is %q, want it to start with Not sent", r.Text)
		}
		if elapsed > srSendBudget {
			t.Fatalf("the acceptor's send returned after %s, want at most %s", elapsed, srSendBudget)
		}
	})

	t.Run("REQ-agent-messaging-019 never delivered later", func(t *testing.T) {
		c.Restore(t)
		srWaitConnected(t, bNode, "hostA.peer", srRelinkBound)
		srWaitConnected(t, aNode, "hostB.peer", srRelinkBound)
		// The restored roster is what makes each sentinel's target resolvable.
		srWaitRoster(t, a1, "a1", b1, "b1")

		// Sentinels prove the direction works again before the old bodies are
		// declared absent: a queued message would have to sit in front of them.
		sentinelToB := srMark("sentinel-to-b-")
		r := a1.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": sentinelToB})
		r.MustOK(t)
		b1.WaitFrame(t, srWaitBound)

		sentinelToA := srMark("sentinel-to-a-")
		r = b1.Call(t, "send_message", map[string]any{"to": "a1@hostA.peer", "message": sentinelToA})
		r.MustOK(t)
		a1.WaitFrame(t, srWaitBound)

		if frames := b1.Fake().Frames(); srCountMark(frames, failedFromA) != 0 || srCountMark(frames, failedFromB) != 0 {
			t.Fatalf("B received a message that was reported Not sent")
		}
		if frames := a1.Fake().Frames(); srCountMark(frames, failedFromA) != 0 || srCountMark(frames, failedFromB) != 0 {
			t.Fatalf("A received a message that was reported Not sent")
		}

		baseA, baseB := len(a1.Fake().Frames()), len(b1.Fake().Frames())
		harness.Consistently(t, srQuietWindow, "the failed messages never arrive", func() (bool, string) {
			framesA, framesB := a1.Fake().Frames(), b1.Fake().Frames()
			if len(framesA) != baseA || len(framesB) != baseB {
				return false, fmt.Sprintf("frame counts grew from A=%d B=%d to A=%d B=%d", baseA, baseB, len(framesA), len(framesB))
			}
			if srCountMark(framesA, failedFromA) != 0 || srCountMark(framesB, failedFromB) != 0 {
				return false, "a message reported Not sent was delivered"
			}
			return true, ""
		})
	})

	t.Run("REQ-agent-messaging-027 delivery unknown", func(t *testing.T) {
		// HoldOpen makes the receiver's Broker block while writing, so the frame
		// being recorded proves the request is in flight. Cutting the link then
		// loses the reply while the write itself succeeds.
		a1.Fake().SetMode(fakesession.HoldOpen)
		t.Cleanup(func() { a1.Fake().SetMode(fakesession.CloseAfterRead) })
		marker := srMark("unknown-")
		ch := srSendAsync(t, b1, "a1@hostA.peer", marker)
		// The frame proves the delivery request reached the receiver's inbox
		// before the link is cut. The sender's send.request_timeout (2s here)
		// is the product timer that governs it: a frame recorded after that
		// budget belongs to a request the sender had already given up on, and
		// the cut could no longer prove a lost reply. srSendBudget is that
		// timer plus the plan's 500ms allowance.
		frame := a1.WaitFrame(t, srSendBudget)
		if !strings.Contains(frame.Content, marker) {
			t.Fatalf("the in-flight frame carries %q, want the marker %q", frame.Content, marker)
		}

		c.Cut(t, "hostA", "hostB")
		a1.Fake().Release()

		r := srAwait(t, ch, srSendBudget, "the delivery-unknown send")
		r.MustCode(t, "delivery_unknown")
		if !r.HasPrefix("Delivery unknown") {
			t.Fatalf("the error is %q, want it to start with Delivery unknown", r.Text)
		}

		c.Restore(t)
		if got := srCountMark(a1.Fake().Frames(), marker); got != 1 {
			t.Fatalf("A received the message %d times, want exactly once", got)
		}
		srWaitConnected(t, bNode, "hostA.peer", srRelinkBound)
		srWaitConnected(t, aNode, "hostB.peer", srRelinkBound)
		// The next case sends before it cuts anything, so it needs the
		// restored roster too.
		srWaitRoster(t, a1, "a1", b1, "b1")
	})

	t.Run("REQ-agent-messaging-020 reply lost, one copy", func(t *testing.T) {
		// The receiver bounds its inbox write by the smaller of the sender's
		// remaining budget and its own send.request_timeout (design-broker.md),
		// so both Brokers get the case's ten-second value. Raising only the
		// sender's value leaves the write on the two-second FastTimers deadline,
		// and the fault injection below runs inside that deadline.
		oldA := srSetSendTimeout(t, opA, srReplyTimeout.String())
		oldB := srSetSendTimeout(t, opB, srReplyTimeout.String())
		t.Cleanup(func() {
			srSetSendTimeout(t, opA, oldA)
			srSetSendTimeout(t, opB, oldB)
		})

		// A failed send must not leave the hold in place. The later subtests
		// share a1, and a held connection would turn their writes into an
		// uncertain result as well.
		t.Cleanup(func() { a1.Fake().SetMode(fakesession.CloseAfterRead) })

		a1.Fake().SetMode(fakesession.HoldOpen)
		marker := srMark("reply-lost-")
		ch := srSendAsync(t, b1, "a1@hostA.peer", marker)
		frame := a1.WaitFrame(t, srWaitBound)
		if !strings.Contains(frame.Content, marker) {
			t.Fatalf("the in-flight frame carries %q, want the marker %q", frame.Content, marker)
		}

		c.Cut(t, "hostA", "hostB")
		a1.Fake().Release()
		c.Restore(t)

		r := srAwait(t, ch, srReplyBudget, "the reply-loss send")
		r.MustOK(t)

		// The sentinel below re-resolves its target, which needs the roster
		// the restore dropped.
		srWaitRoster(t, a1, "a1", b1, "b1")

		// A second message proves the link is usable again. The first message must
		// still exist exactly once: the resend reuses its identifier and the
		// receiver's dedupe table must not write it again.
		a1.Fake().SetMode(fakesession.CloseAfterRead)
		sentinel := srMark("reply-lost-sentinel-")
		r = b1.Call(t, "send_message", map[string]any{"to": "a1@hostA.peer", "message": sentinel})
		r.MustOK(t)
		a1.WaitFrame(t, srWaitBound)

		if got := srCountMark(a1.Fake().Frames(), marker); got != 1 {
			t.Fatalf("A received the reply-loss message %d times, want exactly once", got)
		}
	})

	t.Run("REQ-agent-messaging-018 inbox unwritable across hosts", func(t *testing.T) {
		// The receiving Broker still holds the Agent's record; only its inbox
		// socket is gone, so the failure has to come from over the link.
		gone := srSession(t, bNode, "gone")
		defer gone.End(t)
		srWaitSessionVisible(t, a1, "gone")

		gone.Fake().RemoveSocket()
		start := time.Now()
		r := a1.Call(t, "send_message", map[string]any{"to": "gone@hostB.peer", "message": srMark("gone-")})
		elapsed := time.Since(start)
		r.MustCode(t, "inbox_unwritable")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the inbox_unwritable error is %q, want it to start with Not sent", r.Text)
		}
		if elapsed > srSendBudget {
			t.Fatalf("the send returned after %s, want at most %s", elapsed, srSendBudget)
		}
	})

	t.Run("REQ-agent-messaging-026 size limit", func(t *testing.T) {
		// A second session named b1 makes b1's own address carry its ref, which
		// is the value the sender uses when it computes the frame size. The
		// duplicate stays alive for the whole case so the ref does not change
		// back under the sends.
		duplicate := srSession(t, bNode, "b1")
		defer duplicate.End(t)

		var ref string
		harness.EventuallyPoll(t, srWaitBound, "b1's address carries its ref", func(deadline time.Time) (bool, string) {
			out, state := srListAgentsPoll(t, deadline, b1)
			if state != "" {
				return false, state
			}
			name, gotRef, alias := srSplitAddress(out.Self)
			if name != "b1" || gotRef == "" || alias != "hostB.peer" {
				return false, "self_address is " + out.Self
			}
			ref = gotRef
			return true, ""
		})

		// The body is ASCII, so FrameSize grows by exactly one per byte. Probe
		// the empty body to get the base, then assert both sides of the boundary
		// with the product's function.
		base := broker.MaxSendFrameSize("b1", ref, "")
		atLimit := srFrameLimit - base
		overLimit := atLimit + 1
		if got := broker.MaxSendFrameSize("b1", ref, strings.Repeat("x", atLimit)); got != srFrameLimit {
			t.Fatalf("the computed boundary sizes to %d, want %d", got, srFrameLimit)
		}
		if got := broker.MaxSendFrameSize("b1", ref, strings.Repeat("x", overLimit)); got != srFrameLimit+1 {
			t.Fatalf("one byte past the boundary sizes to %d, want %d", got, srFrameLimit+1)
		}

		bytesIn := c.ByteCounter(t, "hostA", c.Addr("router", "wan"), 17443)
		before := bytesIn()
		framesBefore := len(a1.Fake().Frames())

		oversize := strings.Repeat("x", overLimit)
		r := b1.Call(t, "send_message", map[string]any{"to": "a1@hostA.peer", "message": oversize})
		r.MustCode(t, "too_large")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the oversize error is %q, want it to start with Not sent", r.Text)
		}
		if got := r.Origin(); got != "" {
			t.Fatalf("the oversize error carries origin %q, want the sender's own rejection", got)
		}
		if grew := bytesIn() - before; grew >= 100_000 {
			t.Fatalf("the rejected message put %d bytes on the link, want far less than the body", grew)
		}
		if got := len(a1.Fake().Frames()); got != framesBefore {
			t.Fatalf("A received %d new frames for a rejected message", got-framesBefore)
		}

		atBoundary := strings.Repeat("x", atLimit)
		boundaryBefore := bytesIn()
		r = b1.Call(t, "send_message", map[string]any{"to": "a1@hostA.peer", "message": atBoundary})
		r.MustOK(t)
		frame := a1.WaitFrame(t, srWaitBound)
		if body := srEnvelopeBody(t, frame.Content); body != atBoundary {
			t.Fatalf("the boundary message arrived with %d bytes, want %d", len(body), len(atBoundary))
		}
		// The body crossed the link: prove the counter sees its traffic, so the
		// small growth asserted for the rejected send above means something.
		if grew := bytesIn() - boundaryBefore; grew < int64(atLimit) {
			t.Fatalf("the boundary message put %d bytes on the link, want at least the %d-byte body", grew, atLimit)
		}
	})

	t.Run("REQ-agent-messaging-046 inbound quota per peer", func(t *testing.T) {
		// A dedicated target and a local sender keep this case's quota bucket
		// apart from a1, which the cases above share: the bucket is keyed by
		// (source peer, target Agent), and q1 has its own from the start.
		q1 := srSession(t, aNode, "q1")
		defer q1.End(t)
		local := srSession(t, aNode, "a2")
		defer local.End(t)

		// b1 must see q1 before it can address it, and a2 must see q1 before it
		// can send locally. The local address is the one q1 lists for itself:
		// every address carries an alias.
		srWaitSessionVisible(t, b1, "q1")
		srWaitSessionVisible(t, local, "q1")
		q1Out, state := srListAgents(t, q1)
		if state != "" {
			t.Fatalf("listing q1: %s", state)
		}

		// The burst is exactly the bucket's capacity, so the next message has no
		// token and must be refused. The sends are sequential and finish well
		// inside the ten seconds one refilled token takes.
		const burst = 10
		for i := 0; i < burst; i++ {
			r := b1.Call(t, "send_message", map[string]any{"to": "q1@hostA.peer", "message": srMark("quota-")})
			r.MustOK(t)
		}
		r := b1.Call(t, "send_message", map[string]any{"to": "q1@hostA.peer", "message": srMark("quota-over-")})
		r.MustCode(t, "quota_exceeded")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the quota error is %q, want it to start with Not sent", r.Text)
		}
		if got := len(q1.Fake().Frames()); got != burst {
			t.Fatalf("q1 received %d frames, want %d after the eleventh was refused", got, burst)
		}

		// The target still takes a message from another source: a2 sends to q1
		// on the same host, the local path that does not pass the inbound quota.
		fromLocal := srMark("quota-local-")
		r = local.Call(t, "send_message", map[string]any{"to": q1Out.Self, "message": fromLocal})
		r.MustOK(t)
		harness.Eventually(t, srWaitBound, "the local sender reaches q1", func() (bool, string) {
			if srCountMark(q1.Fake().Frames(), fromLocal) != 1 {
				return false, "q1 has not received the local message"
			}
			return true, ""
		})
	})
}

// TestRemoteSendInboundQuota proves that the inbound quota is per source peer:
// one peer spending its whole burst on a session must not stop a third host,
// paired with the same target, from reaching that session. REQ-agent-messaging-046.
func TestRemoteSendInboundQuota(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(3))
	aNode, bNode, cNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
	srStartBroker(t, aNode, srConfig())
	srStartBroker(t, bNode, srConfig())
	srStartBroker(t, cNode, srConfig())

	opA, opB, opC := srOperator(t, aNode), srOperator(t, bNode), srOperator(t, cNode)
	defer opA.End(t)
	defer opB.End(t)
	defer opC.End(t)

	// Both other hosts pair with the target's host, each through its own key.
	aAddr := net.JoinHostPort(c.Addr("hostA", "net"), "17443")
	for _, pair := range []struct {
		op    *harness.Session
		node  *harness.Node
		alias string
	}{
		{opB, bNode, "hostB.peer"},
		{opC, cNode, "hostC.peer"},
	} {
		key := srCreateKey(t, opA)
		srConnect(t, pair.op, aAddr, key.Key).MustOK(t)
		srWaitConnected(t, pair.node, "hostA.peer", srRelinkBound)
		srWaitConnected(t, aNode, pair.alias, srRelinkBound)
	}

	q1 := srSession(t, aNode, "q1")
	b1 := srSession(t, bNode, "b1")
	c1 := srSession(t, cNode, "c1")
	defer q1.End(t)
	defer b1.End(t)
	defer c1.End(t)

	t.Run("REQ-agent-messaging-046 another peer still reaches the session", func(t *testing.T) {
		srWaitSessionVisible(t, b1, "q1")
		srWaitSessionVisible(t, c1, "q1")

		// b1 spends the whole burst on q1; its next message has no token.
		const burst = 10
		for i := 0; i < burst; i++ {
			b1.Call(t, "send_message", map[string]any{"to": "q1@hostA.peer", "message": srMark("cross-a-")}).MustOK(t)
		}
		r := b1.Call(t, "send_message", map[string]any{"to": "q1@hostA.peer", "message": srMark("cross-over-")})
		r.MustCode(t, "quota_exceeded")
		if got := len(q1.Fake().Frames()); got != burst {
			t.Fatalf("q1 received %d frames, want %d after the eleventh was refused", got, burst)
		}

		// c1 is a different source peer, so its own bucket is full and the
		// delivery must reach the same session while b1 is still limited.
		fromOther := srMark("cross-b-")
		c1.Call(t, "send_message", map[string]any{"to": "q1@hostA.peer", "message": fromOther}).MustOK(t)
		harness.Eventually(t, srWaitBound, "the other peer reaches q1", func() (bool, string) {
			if srCountMark(q1.Fake().Frames(), fromOther) != 1 {
				return false, "q1 has not received the other peer's message"
			}
			return true, ""
		})
	})
}

// srConfig merges the shortened timers with the given fragments.
func srConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

// srSetSendTimeout sets send.request_timeout on the running Broker through
// set_config, which applies to the next send without a restart, and returns the
// value it replaced so the caller can restore it.
func srSetSendTimeout(t *testing.T, op *harness.Session, value string) string {
	t.Helper()
	r := op.Call(t, "set_config", map[string]any{"key": "send.request_timeout", "value": value})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.SetConfigOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding set_config send.request_timeout: %v", err)
	}
	return out.Old
}

// srStartBroker starts a Broker and fails when it does not come up.
func srStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// srOperator starts a session that never registers as an Agent.
func srOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, srWaitBound)
	return s
}

// srSession starts a registered session in srTopic.
func srSession(t *testing.T, node *harness.Node, name string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	srWaitRegistered(t, s, name)
	srJoin(t, s, srTopic)
	return s
}

// srWaitRegistered waits until the session lists itself under name.
func srWaitRegistered(t *testing.T, s *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, srWaitBound, "session "+name+" is registered", func(deadline time.Time) (bool, string) {
		out, state := srListAgentsPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		if got, _, _ := srSplitAddress(out.Self); got != name {
			return false, "self_address is " + out.Self
		}
		return true, ""
	})
}

// srWaitSessionVisible waits until the session's listing contains name.
func srWaitSessionVisible(t *testing.T, s *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, srRelinkBound+srWaitBound, "the session "+name+" is visible again", func(deadline time.Time) (bool, string) {
		out, state := srListAgentsPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		if _, ok := srFindAgent(out, name); !ok {
			return false, name + " is not listed"
		}
		return true, ""
	})
}

// srWaitRoster waits until each session's listing contains the other. A link
// reports itself established before topic exchanges the peer's roster, and a
// send in that window resolves against an empty roster and returns not_found
// (design-broker.md). Call it after a link comes up, before a send that needs
// the peer resolved.
func srWaitRoster(t *testing.T, a *harness.Session, aName string, b *harness.Session, bName string) {
	t.Helper()
	srWaitSessionVisible(t, a, bName)
	srWaitSessionVisible(t, b, aName)
}

// srJoin joins name, retrying while the session is still registering.
// join_topic's handler may spend topicRPCTimeout before the update hint runs,
// so the poll uses harness.TopicCallPollBound.
func srJoin(t *testing.T, s *harness.Session, topic string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "join_topic "+topic, func(deadline time.Time) (bool, string) {
		r, err := s.PollCallWithin(t, deadline, harness.TopicCallPollBound, "join_topic", map[string]any{"name": topic})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

// srCreateKey creates one pairing key.
func srCreateKey(t *testing.T, op *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := op.Call(t, "create_pairing_key", map[string]any{})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

// srConnect calls connect_broker.
func srConnect(t *testing.T, op *harness.Session, address, key string) harness.Result {
	t.Helper()
	return op.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
}

// srShownAddress picks the shown address of node on the wan network.
func srShownAddress(t *testing.T, c *harness.Cluster, node string, shown []string) string {
	t.Helper()
	want := net.JoinHostPort(c.Addr(node, "wan"), "17443")
	for _, address := range shown {
		if address == want {
			return address
		}
	}
	if len(shown) > 0 {
		return shown[0]
	}
	t.Fatalf("create_pairing_key on %s returned no shown address", node)
	return ""
}

// srWaitConnected waits until node lists alias as connected.
func srWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the link to "+alias+" is connected", func() (bool, string) {
		resp, detail := node.Oracle().ListBrokers(t)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, broker := range resp.GetBrokers() {
			if broker.GetAlias() == alias {
				return broker.GetStatus() == localpb.BrokerStatus_BROKER_STATUS_CONNECTED, fmt.Sprintf("status is %s", broker.GetStatus())
			}
		}
		return false, "the peer is not listed"
	})
}

// srCutAndWaitDown cuts the network and waits until neither side reports the
// link as connected.
func srCutAndWaitDown(t *testing.T, c *harness.Cluster, aNode, bNode *harness.Node) {
	t.Helper()
	c.Cut(t, "hostA", "hostB")
	harness.Eventually(t, srWaitBound, "both sides notice the cut", func() (bool, string) {
		if srPeerConnected(t, aNode, "hostB.peer") || srPeerConnected(t, bNode, "hostA.peer") {
			return false, "a side still reports the link connected"
		}
		return true, ""
	})
}

// srPeerConnected reports whether node lists alias as connected.
func srPeerConnected(t *testing.T, node *harness.Node, alias string) bool {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		return false
	}
	for _, broker := range resp.GetBrokers() {
		if broker.GetAlias() == alias {
			return broker.GetStatus() == localpb.BrokerStatus_BROKER_STATUS_CONNECTED
		}
	}
	return false
}

// srListAgents calls list_agents and returns the decoded answer, with a tool
// error reported as a state string.
func srListAgents(t *testing.T, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	r := s.Call(t, "list_agents", nil)
	if r.IsError {
		return mcpserver.ListAgentsOut{}, r.Text
	}
	out, err := harness.Decode[mcpserver.ListAgentsOut](string(r.Structured))
	if err != nil {
		return mcpserver.ListAgentsOut{}, "decoding list_agents: " + err.Error()
	}
	return out, ""
}

// srListAgentsPoll is srListAgents for a wait callback: the call is bounded by
// the wait's deadline and a transport error is returned as state.
func srListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", nil)
}

// srFindAgent returns the entry whose name part is name.
func srFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if got, _, _ := srSplitAddress(agent.Address); got == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

// srSplitAddress splits "name [ref]@alias" into its three parts. ref is empty
// when the address carries none.
func srSplitAddress(address string) (name, ref, alias string) {
	at := strings.LastIndexByte(address, '@')
	if at < 0 {
		return strings.TrimSpace(address), "", ""
	}
	name, alias = address[:at], address[at+1:]
	if open := strings.LastIndexByte(name, '['); open >= 0 && strings.HasSuffix(name, "]") {
		ref = name[open+1 : len(name)-1]
		name = name[:open]
	}
	return strings.TrimSpace(name), strings.TrimSpace(ref), alias
}

// srEnvelopeBody returns the message body from the attribution envelope the
// transport wraps it in.
func srEnvelopeBody(t *testing.T, content string) string {
	t.Helper()
	openEnd := strings.Index(content, ">\n")
	if openEnd < 0 {
		t.Fatalf("the delivered content has no envelope opening line: %.80q", content)
	}
	body := content[openEnd+2:]
	const closing = "\n</cross-session-message>"
	if !strings.HasSuffix(body, closing) {
		t.Fatalf("the delivered content has no envelope closing tag: %.80q", content)
	}
	return strings.TrimSuffix(body, closing)
}

// srCountMark counts the frames whose content carries marker.
func srCountMark(frames []fakesession.Frame, marker string) int {
	n := 0
	for _, frame := range frames {
		if strings.Contains(frame.Content, marker) {
			n++
		}
	}
	return n
}

// srAsyncResult carries one asynchronous tool result.
type srAsyncResult struct {
	res harness.Result
	ok  bool
}

// srSendAsync sends one message in the background. The result channel is
// buffered and closed after the call returns; a closed channel with no value
// means the goroutine ended inside the harness failure path.
func srSendAsync(t *testing.T, s *harness.Session, to, message string) <-chan srAsyncResult {
	t.Helper()
	ch := make(chan srAsyncResult, 1)
	go func() {
		defer close(ch)
		res := s.Call(t, "send_message", map[string]any{"to": to, "message": message})
		ch <- srAsyncResult{res: res, ok: true}
	}()
	return ch
}

// srAwait waits for an asynchronous result and fails the test when it does not
// arrive in bound.
func srAwait(t *testing.T, ch <-chan srAsyncResult, bound time.Duration, what string) harness.Result {
	t.Helper()
	select {
	case result, open := <-ch:
		if !open || !result.ok {
			t.Fatalf("%s: the asynchronous call ended without a result", what)
		}
		return result.res
	case <-time.After(bound):
		t.Fatalf("%s: no result within %s", what, bound)
		return harness.Result{}
	}
}

// srMark returns a unique ASCII marker that cannot collide with envelope text.
func srMark(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("send_remote_test: reading random bytes: %v", err))
	}
	return prefix + hex.EncodeToString(b[:])
}
