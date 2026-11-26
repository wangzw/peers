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

// The size-limit case is defined against the product's own computation; this
// assertion is deliberate, so a signature change breaks the test at compile
// time instead of at runtime.
var _ func(name, ref, text string) int = broker.MaxSendFrameSize

const (
	srTopic = "t"

	srShortRequestTimeout = 2 * time.Second

	srSendBudget = srShortRequestTimeout + harness.EventTimeout

	// srSendMargin is measurement overhead on top of the send time limit, not a
	// second product allowance.
	srSendMargin = 500 * time.Millisecond

	srSendAssertBound = srShortRequestTimeout + srSendMargin

	// srReplyTimeout is the send budget of the reply-loss case; the fault
	// injection measured 1.4-1.5s and runs inside it, so do not raise it to
	// trade stability for a longer timeout or grow it further.
	srReplyTimeout = 10 * time.Second

	// srReplyBudget is srReplyTimeout plus srSendMargin; the 500ms here is a
	// literal copy of srSendMargin, so the two must change together.
	srReplyBudget = srReplyTimeout + 500*time.Millisecond

	srRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout

	srWaitBound = harness.EventTimeout

	// srQuietWindow must equal the sender's short send.request_timeout: after
	// the link is back, a message that was not delivered must not surface
	// within the sender's own budget.
	srQuietWindow = 2 * time.Second

	srFrameLimit = 1_000_000
)

// TestRemoteSendFaults covers cross-host sending under a NAT: both directions,
// an unreachable peer, no later delivery after a failed send, an unknown result
// after a lost reply, the reply-loss duplicate and the size limit.
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

	// failedFromA and failedFromB cross subtests: the unreachable case below
	// fills them and the never-delivered case reads them after its own restore.
	var failedFromA, failedFromB string

	t.Run("REQ-agent-messaging-017 unreachable within budget", func(t *testing.T) {
		srUseShortRequestTimeout(t, opA, opB)
		srCutAndWaitDown(t, c, aNode, bNode)

		failedFromB = srMark("unreachable-b-")
		r, elapsed := srSendWithin(t, b1, "a1@hostA.peer", failedFromB)
		r.MustCode(t, "peer_unreachable")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the dialer's error is %q, want it to start with Not sent", r.Text)
		}
		if elapsed > srSendAssertBound {
			t.Fatalf("the send returned after %s, want at most %s (configured send.request_timeout %s + measurement margin %s; the wait fallback is %s)",
				elapsed, srSendAssertBound, srShortRequestTimeout, srSendMargin, srSendBudget)
		}

		failedFromA = srMark("unreachable-a-")
		r, elapsed = srSendWithin(t, a1, "b1@hostB.peer", failedFromA)
		r.MustCode(t, "peer_unreachable")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the acceptor's error is %q, want it to start with Not sent", r.Text)
		}
		if elapsed > srSendAssertBound {
			t.Fatalf("the acceptor's send returned after %s, want at most %s (configured send.request_timeout %s + measurement margin %s; the wait fallback is %s)",
				elapsed, srSendAssertBound, srShortRequestTimeout, srSendMargin, srSendBudget)
		}
	})

	t.Run("REQ-agent-messaging-019 never delivered later", func(t *testing.T) {
		c.Restore(t)
		srWaitConnected(t, bNode, "hostA.peer", srRelinkBound)
		srWaitConnected(t, aNode, "hostB.peer", srRelinkBound)
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
		srUseShortRequestTimeout(t, opA, opB)
		// HoldOpen blocks the receiver's Broker while writing, so a recorded
		// frame proves the request is in flight; cutting the link then loses
		// the reply while the write itself succeeds.
		a1.Fake().SetMode(fakesession.HoldOpen)
		t.Cleanup(func() { a1.Fake().SetMode(fakesession.CloseAfterRead) })
		marker := srMark("unknown-")
		ch := srSendAsync(t, b1, "a1@hostA.peer", marker)
		// Wait for the frame before cutting: one recorded after the sender's
		// budget belongs to a request it had already given up on.
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
		srWaitRoster(t, a1, "a1", b1, "b1")
	})

	t.Run("REQ-agent-messaging-020 reply lost, one copy", func(t *testing.T) {
		// The receiver bounds its inbox write by the smaller of the sender's
		// remaining budget and its own send.request_timeout, so both Brokers
		// get the case's ten-second value.
		oldA := srSetSendTimeout(t, opA, srReplyTimeout.String())
		oldB := srSetSendTimeout(t, opB, srReplyTimeout.String())
		t.Cleanup(func() {
			srSetSendTimeout(t, opA, oldA)
			srSetSendTimeout(t, opB, oldB)
		})

		// A failed send must not leave the hold in place: the later subtests
		// share a1, and a held connection would make their writes uncertain.
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

		srWaitRoster(t, a1, "a1", b1, "b1")

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
		srUseShortRequestTimeout(t, opA, opB)
		gone := srSession(t, bNode, "gone")
		defer gone.End(t)
		srWaitSessionVisible(t, a1, "gone")

		gone.Fake().RemoveSocket()
		r, elapsed := srSendWithin(t, a1, "gone@hostB.peer", srMark("gone-"))
		r.MustCode(t, "inbox_unwritable")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the inbox_unwritable error is %q, want it to start with Not sent", r.Text)
		}
		if elapsed > srSendAssertBound {
			t.Fatalf("the send returned after %s, want at most %s (configured send.request_timeout %s + measurement margin %s; the wait fallback is %s)",
				elapsed, srSendAssertBound, srShortRequestTimeout, srSendMargin, srSendBudget)
		}
	})

	t.Run("REQ-agent-messaging-026 size limit", func(t *testing.T) {
		// A second session named b1 gives b1 a disambiguation ref, which the
		// sender includes in the frame size; the duplicate stays alive so the
		// ref does not change under the sends.
		duplicate := srSession(t, bNode, "b1")
		defer duplicate.End(t)

		var ref string
		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "b1's address carries its ref", func(deadline time.Time) (bool, string) {
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

		// The body is ASCII, so the frame grows by one byte per character; probe
		// the empty body for the base, then assert both sides of the boundary.
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
		if grew := bytesIn() - boundaryBefore; grew < int64(atLimit) {
			t.Fatalf("the boundary message put %d bytes on the link, want at least the %d-byte body", grew, atLimit)
		}
	})

	t.Run("REQ-agent-messaging-046 inbound quota per peer", func(t *testing.T) {
		// A dedicated target and a local sender keep this case's quota bucket
		// apart from a1's: the bucket is keyed by both source peer and target
		// agent, so tokens are shared only when both match.
		q1 := srSession(t, aNode, "q1")
		defer q1.End(t)
		local := srSession(t, aNode, "a2")
		defer local.End(t)

		srWaitSessionVisible(t, b1, "q1")
		srWaitSessionVisible(t, local, "q1")
		q1Out, state := srListAgents(t, q1)
		if state != "" {
			t.Fatalf("listing q1: %s", state)
		}

		// The burst equals the bucket's capacity, so the next message has no
		// token and must be refused; the sequential sends finish well inside
		// the ten seconds one refilled token takes.
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

// TestRemoteSendInboundQuota checks that one peer spending its whole burst on a
// session does not stop another host paired with the same target from reaching
// it.
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

		const burst = 10
		for i := 0; i < burst; i++ {
			b1.Call(t, "send_message", map[string]any{"to": "q1@hostA.peer", "message": srMark("cross-a-")}).MustOK(t)
		}
		r := b1.Call(t, "send_message", map[string]any{"to": "q1@hostA.peer", "message": srMark("cross-over-")})
		r.MustCode(t, "quota_exceeded")
		if got := len(q1.Fake().Frames()); got != burst {
			t.Fatalf("q1 received %d frames, want %d after the eleventh was refused", got, burst)
		}

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

func srConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

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

func srUseShortRequestTimeout(t *testing.T, brokers ...*harness.Session) {
	t.Helper()
	old := make([]string, len(brokers))
	for i, op := range brokers {
		old[i] = srSetSendTimeout(t, op, srShortRequestTimeout.String())
	}
	t.Cleanup(func() {
		for i, op := range brokers {
			srSetSendTimeout(t, op, old[i])
		}
	})
}

func srStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

func srOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, srWaitBound)
	return s
}

func srSession(t *testing.T, node *harness.Node, name string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	srWaitRegistered(t, s, name)
	srJoin(t, s, srTopic)
	return s
}

func srWaitRegistered(t *testing.T, s *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "session "+name+" is registered", func(deadline time.Time) (bool, string) {
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

// srWaitRoster waits until each session's listing contains the other: a link
// reports itself established before the peer's roster arrives, and a send in
// that window returns not_found.
func srWaitRoster(t *testing.T, a *harness.Session, aName string, b *harness.Session, bName string) {
	t.Helper()
	srWaitSessionVisible(t, a, bName)
	srWaitSessionVisible(t, b, aName)
}

// srJoin retries while the session is still registering. The poll needs
// harness.TopicCallPollBound because join_topic's handler may spend
// topicRPCTimeout before the update hint runs.
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

func srConnect(t *testing.T, op *harness.Session, address, key string) harness.Result {
	t.Helper()
	return op.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
}

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

func srListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", nil)
}

func srFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if got, _, _ := srSplitAddress(agent.Address); got == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

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

func srCountMark(frames []fakesession.Frame, marker string) int {
	n := 0
	for _, frame := range frames {
		if strings.Contains(frame.Content, marker) {
			n++
		}
	}
	return n
}

func srSendWithin(t *testing.T, s *harness.Session, to, message string) (harness.Result, time.Duration) {
	t.Helper()
	start := time.Now()
	r, err := s.CallErrWithin(srSendBudget, "send_message", map[string]any{"to": to, "message": message})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("send_message to %s: %v", to, err)
	}
	return r, elapsed
}

type srAsyncResult struct {
	res harness.Result
	ok  bool
}

// srSendAsync's channel is closed after the call; a closed channel with no
// value means the harness failure path ended the goroutine.
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

func srMark(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("send_remote_test: reading random bytes: %v", err))
	}
	return prefix + hex.EncodeToString(b[:])
}
