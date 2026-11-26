//go:build e2e

package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	vrTopic = "t"

	vrWaitBound = harness.EventTimeout

	vrRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout

	// vrRestoreBound is a wait bound only: the measured span is asserted
	// against vrRestoreAssertBound, so the fallback never enters an assertion.
	vrRestoreBound = vrRelinkBound

	// vrObserveMargin is test-side polling and round-trip overhead, not a
	// product allowance: four intervals cover one interval of granularity, two
	// round trips and one interval of scheduler slack, and 200ms is 4% of the
	// five-second removal requirement.
	vrObserveMargin = 4 * harness.PollInterval

	// vrRemovalRequirement is the copied five-second requirement value the
	// removal span is asserted against.
	vrRemovalRequirement = 5 * time.Second

	vrRemovalAssertBound = vrRemovalRequirement + vrObserveMargin

	vrRestoreAssertBound = harness.FastReconnectMax + harness.FastDialTimeout + vrObserveMargin

	// vrQuietWindow is a deliberate negative observation: three seconds is long
	// enough for a wrong delivery to surface on the remote path, while a
	// shorter window can only miss a violation.
	vrQuietWindow = 3 * time.Second
)

// TestRemoteVisibility covers what two paired hosts show each other: addresses
// with refs, receiver-side signatures, replies by signature, cleaned and long
// names, session removal and the topic intersection that decides visibility.
func TestRemoteVisibility(t *testing.T) {
	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	vrStartBroker(t, aNode, vrConfig())
	vrStartBroker(t, bNode, vrConfig())
	opA, opB := vrOperator(t, aNode), vrOperator(t, bNode)

	key := vrCreateKey(t, opA)
	vrConnect(t, opB, vrShownAddress(t, c, "hostA", key.Addresses), key.Key, "").MustOK(t)
	vrWaitConnected(t, bNode, "hostA.peer", vrRelinkBound)
	vrWaitConnected(t, aNode, "hostB.peer", vrRelinkBound)

	a1 := vrStartSession(t, aNode, "a1", vrTopic)
	b1 := vrStartSession(t, bNode, "b1", vrTopic)
	defer a1.End(t)
	defer b1.End(t)

	t.Run("REQ-agent-messaging-008 lists remote", func(t *testing.T) {
		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "a1 sees b1 in the shared topic", func(deadline time.Time) (bool, string) {
			out, state := vrListAgentsPoll(t, deadline, a1)
			if state != "" {
				return false, state
			}
			entry, ok := vrFindAgent(out, "b1")
			if !ok {
				return false, "b1 is not listed"
			}
			name, ref, alias := vrSplitAddress(entry.Address)
			if name != "b1" || ref != "" || alias != "hostB.peer" {
				return false, fmt.Sprintf("b1's address is %q, want b1@hostB.peer", entry.Address)
			}
			if !vrSameSet(entry.SharedTopics, []string{vrTopic}) {
				return false, fmt.Sprintf("shared topics are %v, want [%s]", entry.SharedTopics, vrTopic)
			}
			return true, ""
		})

		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "b1 sees a1 in the shared topic", func(deadline time.Time) (bool, string) {
			out, state := vrListAgentsPoll(t, deadline, b1)
			if state != "" {
				return false, state
			}
			entry, ok := vrFindAgent(out, "a1")
			if !ok {
				return false, "a1 is not listed"
			}
			if entry.Address != "a1@hostA.peer" {
				return false, fmt.Sprintf("a1's address is %q, want a1@hostA.peer", entry.Address)
			}
			return true, ""
		})

		if !vrSees(t, a1, "b1") || !vrSees(t, b1, "a1") {
			t.Fatal("vrSees does not find the pair that list_agents shows")
		}
	})

	t.Run("REQ-topics-007 same name across brokers", func(t *testing.T) {
		topic := "cross-" + vrRandom()
		a2 := vrStartSession(t, aNode, "a2", topic)
		b2 := vrStartSession(t, bNode, "b2", topic)
		defer a2.End(t)
		defer b2.End(t)

		harness.Eventually(t, vrWaitBound, "a2 and b2 share the new topic", func() (bool, string) {
			if !vrSees(t, a2, "b2") {
				return false, "a2 does not list b2"
			}
			if !vrSees(t, b2, "a2") {
				return false, "b2 does not list a2"
			}
			return true, ""
		})
	})

	t.Run("REQ-topics-005 iff shared and connected", func(t *testing.T) {
		topic := "cross-" + vrRandom()
		a2 := vrStartSession(t, aNode, "a2", topic)
		b2 := vrStartSession(t, bNode, "b2", topic)
		defer a2.End(t)
		defer b2.End(t)
		harness.Eventually(t, vrWaitBound, "the new pair is visible", func() (bool, string) {
			return vrSees(t, a2, "b2") && vrSees(t, b2, "a2"), "the pair is not visible"
		})

		b2.Call(t, "leave_topic", map[string]any{"name": topic}).MustOK(t)
		harness.Eventually(t, vrWaitBound, "leaving the topic hides b2 from a2", func() (bool, string) {
			if vrSees(t, a2, "b2") {
				return false, "a2 still lists b2"
			}
			if vrSees(t, b2, "a2") {
				return false, "b2 still lists a2"
			}
			return true, ""
		})

		vrJoin(t, b2, topic)
		harness.Eventually(t, vrWaitBound, "joining again makes the pair visible", func() (bool, string) {
			return vrSees(t, a2, "b2") && vrSees(t, b2, "a2"), "the pair is not visible"
		})

		c.Cut(t, "hostA", "hostB")
		harness.Eventually(t, vrWaitBound, "the cut hides both remote sides", func() (bool, string) {
			if vrSees(t, a2, "b2") {
				return false, "a2 still lists b2 while the link is cut"
			}
			if vrSees(t, b2, "a2") {
				return false, "b2 still lists a2 while the link is cut"
			}
			return true, ""
		})

		c.Restore(t)
		// Restore is synchronous and returns once the network rules are gone,
		// so the span starts just after it.
		restoreStart := time.Now()
		harness.Eventually(t, vrRestoreBound, "the restored link makes the pair visible again", func() (bool, string) {
			return vrSees(t, a2, "b2") && vrSees(t, b2, "a2"), "the pair is not visible"
		})
		if elapsed := time.Since(restoreStart); elapsed > vrRestoreAssertBound {
			t.Fatalf("the restored link made the pair visible after %s, want at most %s (link.reconnect_max %s + link.dial_timeout %s + observation margin %s; the wait fallback is %s)",
				elapsed, vrRestoreAssertBound, harness.FastReconnectMax, harness.FastDialTimeout, vrObserveMargin, vrRestoreBound)
		}
		vrWaitConnected(t, bNode, "hostA.peer", vrRelinkBound)
		vrWaitConnected(t, aNode, "hostB.peer", vrRelinkBound)
	})

	t.Run("REQ-topics-006 no topic invisible", func(t *testing.T) {
		lonely := vrStartSession(t, bNode, "lonely")
		defer lonely.End(t)
		harness.ConsistentlyPoll(t, vrQuietWindow, harness.ListAgentsPollBound, "the topicless session stays invisible", lonely.LogBrokerStack, func(deadline time.Time) harness.Observation {
			for _, check := range []struct {
				session *harness.Session
				name    string
			}{{a1, "lonely"}, {lonely, "a1"}} {
				out, state := vrListAgentsPoll(t, deadline, check.session)
				if state != "" {
					return harness.Unobservable(state)
				}
				if _, ok := vrFindAgent(out, check.name); ok {
					return harness.Observed(false, "lists "+check.name)
				}
			}
			return harness.Observed(true, "")
		})

		// The keeper keeps topic t present on A so B's roster still lists b1;
		// only a1 shares no topic with it.
		keeper := vrStartSession(t, aNode, "a-keep", vrTopic)
		defer keeper.End(t)
		a1.Call(t, "leave_topic", map[string]any{"name": vrTopic}).MustOK(t)
		harness.Eventually(t, vrWaitBound, "a1 no longer sees b1 after leaving the topic", func() (bool, string) {
			return !vrSees(t, a1, "b1"), "a1 still lists b1"
		})
		r := a1.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": vrMark("invisible-")})
		r.MustCode(t, "not_visible")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the not-visible error is %q, want it to start with Not sent", r.Text)
		}

		vrJoin(t, a1, vrTopic)
		harness.Eventually(t, vrWaitBound, "a1 sees b1 again after rejoining", func() (bool, string) {
			return vrSees(t, a1, "b1"), "a1 does not list b1"
		})
	})

	t.Run("REQ-agent-messaging-004 remote refs", func(t *testing.T) {
		w1 := vrStartSession(t, bNode, "api-worker", vrTopic)
		w2 := vrStartSession(t, bNode, "api-worker", vrTopic)
		defer w1.End(t)
		defer w2.End(t)

		ref1 := vrWaitSelfRef(t, w1, "api-worker")
		ref2 := vrWaitSelfRef(t, w2, "api-worker")
		if ref1 == ref2 {
			t.Fatalf("the two api-worker sessions share the ref %q", ref1)
		}
		addr1 := "api-worker [" + ref1 + "]@hostB.peer"
		addr2 := "api-worker [" + ref2 + "]@hostB.peer"

		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "a1 lists both api-worker addresses with refs", func(deadline time.Time) (bool, string) {
			out, state := vrListAgentsPoll(t, deadline, a1)
			if state != "" {
				return false, state
			}
			seen := map[string]bool{}
			for _, agent := range out.Agents {
				if name, _, _ := vrSplitAddress(agent.Address); name == "api-worker" {
					seen[agent.Address] = true
				}
			}
			if !seen[addr1] || !seen[addr2] || len(seen) != 2 {
				return false, fmt.Sprintf("a1 lists api-worker addresses %v", vrKeys(seen))
			}
			return true, ""
		})

		ambiguousMark := vrMark("ambiguous-")
		r := a1.Call(t, "send_message", map[string]any{"to": "api-worker@hostB.peer", "message": ambiguousMark})
		r.MustCode(t, "ambiguous")
		if !r.HasPrefix("Not sent") {
			t.Fatalf("the ambiguous error is %q, want it to start with Not sent", r.Text)
		}
		for _, session := range []*harness.Session{w1, w2} {
			if got := vrCountMark(session.Fake().Frames(), ambiguousMark); got != 0 {
				t.Fatalf("an ambiguous send reached a session %d time(s)", got)
			}
		}

		markOne := vrMark("to-first-")
		a1.Call(t, "send_message", map[string]any{"to": addr1, "message": markOne}).MustOK(t)
		frame := w1.WaitFrame(t, vrWaitBound)
		if body := vrEnvelopeBody(t, frame.Content); body != markOne {
			t.Fatalf("w1 received %q, want %q", body, markOne)
		}
		if vrCountMark(w2.Fake().Frames(), markOne) != 0 {
			t.Fatal("the message addressed to the first api-worker reached the second one")
		}

		markTwo := vrMark("to-second-")
		a1.Call(t, "send_message", map[string]any{"to": addr2, "message": markTwo}).MustOK(t)
		frame = w2.WaitFrame(t, vrWaitBound)
		if body := vrEnvelopeBody(t, frame.Content); body != markTwo {
			t.Fatalf("w2 received %q, want %q", body, markTwo)
		}
		if vrCountMark(w1.Fake().Frames(), markTwo) != 0 {
			t.Fatal("the message addressed to the second api-worker reached the first one")
		}
	})

	t.Run("REQ-agent-messaging-022 signature from receiver view", func(t *testing.T) {
		marker := vrMark("signed-")
		a1.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": marker}).MustOK(t)
		frame := b1.WaitFrame(t, vrWaitBound)
		if frame.FromName != "a1@hostA.peer" {
			t.Fatalf("the message is signed %q, want a1@hostA.peer", frame.FromName)
		}

		c2 := harness.Up(t, harness.TopoNAT)
		aNode2, bNode2 := c2.Node("hostA"), c2.Node("hostB")
		vrStartBroker(t, aNode2, vrConfig())
		vrStartBroker(t, bNode2, vrConfig())
		opA2, opB2 := vrOperator(t, aNode2), vrOperator(t, bNode2)
		a3 := vrStartSession(t, aNode2, "a1", vrTopic)
		b3 := vrStartSession(t, bNode2, "b1", vrTopic)
		defer a3.End(t)
		defer b3.End(t)

		key2 := vrCreateKey(t, opA2)
		vrConnect(t, opB2, vrShownAddress(t, c2, "hostA", key2.Addresses), key2.Key, "alpha").MustOK(t)
		vrWaitConnected(t, bNode2, "alpha", vrRelinkBound)
		vrWaitConnected(t, aNode2, "hostB.peer", vrRelinkBound)
		harness.Eventually(t, vrWaitBound, "a1 sees b1 in the second subproject", func() (bool, string) {
			return vrSees(t, a3, "b1"), "a1 does not list b1"
		})

		a3.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": marker}).MustOK(t)
		frame = b3.WaitFrame(t, vrWaitBound)
		if frame.FromName != "a1@alpha" {
			t.Fatalf("the message is signed %q, want a1@alpha after the local alias", frame.FromName)
		}
	})

	t.Run("REQ-agent-messaging-025 reply by signature", func(t *testing.T) {
		duplicate := vrStartSession(t, aNode, "b1", vrTopic)
		defer duplicate.End(t)

		marker := vrMark("reply-target-")
		a1.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": marker}).MustOK(t)
		frame := b1.WaitFrame(t, vrWaitBound)
		signature := frame.FromName
		if signature != "a1@hostA.peer" {
			t.Fatalf("the message is signed %q, want a1@hostA.peer", signature)
		}

		reply := vrMark("reply-body-")
		b1.Call(t, "send_message", map[string]any{"to": signature, "message": reply}).MustOK(t)
		frame = a1.WaitFrame(t, vrWaitBound)
		if body := vrEnvelopeBody(t, frame.Content); body != reply {
			t.Fatalf("a1 received %q, want %q", body, reply)
		}
		if vrCountMark(duplicate.Fake().Frames(), reply) != 0 {
			t.Fatal("the reply reached the local session that shares the replier's name")
		}
	})

	t.Run("REQ-agent-messaging-029 cleaned name gets ref", func(t *testing.T) {
		cleaned := vrStartSession(t, aNode, "a<b", vrTopic)
		plain := vrStartSession(t, aNode, "ab", vrTopic)
		defer cleaned.End(t)
		defer plain.End(t)

		marker := vrMark("cleaned-")
		cleaned.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": marker}).MustOK(t)
		frame := b1.WaitFrame(t, vrWaitBound)
		signature := frame.FromName
		if !strings.HasPrefix(signature, "ab [") || !strings.HasSuffix(signature, "]@hostA.peer") {
			t.Fatalf("the cleaned name is signed %q, want ab [<ref>]@hostA.peer", signature)
		}
		if inbox.CleanSignature(signature) != signature {
			t.Fatalf("the receiver would rewrite the signature %q", signature)
		}

		reply := vrMark("cleaned-reply-")
		b1.Call(t, "send_message", map[string]any{"to": signature, "message": reply}).MustOK(t)
		frame = cleaned.WaitFrame(t, vrWaitBound)
		if body := vrEnvelopeBody(t, frame.Content); body != reply {
			t.Fatalf("a<b received %q, want %q", body, reply)
		}
		if vrCountMark(plain.Fake().Frames(), reply) != 0 {
			t.Fatal("the reply reached ab instead of a<b")
		}
	})

	t.Run("REQ-agent-messaging-037 truncated signature", func(t *testing.T) {
		longName := strings.Repeat("n", 70)
		long := vrStartSession(t, aNode, longName, vrTopic)
		defer long.End(t)

		marker := vrMark("long-name-")
		long.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": marker}).MustOK(t)
		frame := b1.WaitFrame(t, vrWaitBound)
		signature := frame.FromName
		if got := utf8.RuneCountInString(signature); got > 64 {
			t.Fatalf("the signature is %d runes, want at most 64: %q", got, signature)
		}
		if !strings.Contains(signature, " [") || !strings.HasSuffix(signature, "]@hostA.peer") {
			t.Fatalf("the long name is signed %q, want a truncated name with a ref", signature)
		}
		namePart := signature[:strings.Index(signature, " [")]
		if namePart == "" || !strings.HasPrefix(longName, namePart) {
			t.Fatalf("the signature's name part %q is not a prefix of %q", namePart, longName)
		}

		reply := vrMark("long-reply-")
		b1.Call(t, "send_message", map[string]any{"to": signature, "message": reply}).MustOK(t)
		frame = long.WaitFrame(t, vrWaitBound)
		if body := vrEnvelopeBody(t, frame.Content); body != reply {
			t.Fatalf("the long-named session received %q, want %q", body, reply)
		}
	})

	t.Run("REQ-agent-messaging-030 long name is truncated for both sides", func(t *testing.T) {
		longName := strings.Repeat("q", 300)
		want := strings.Repeat("q", inbox.MaxNameBytes)
		long := vrStartSession(t, aNode, longName, vrTopic)
		defer long.End(t)

		harness.Eventually(t, vrWaitBound, "the Broker registers the long name cut to the limit", func() (bool, string) {
			got := vrBrokerRegisteredName(t, aNode, "qq")
			if got == "" {
				return false, "no agent_registered record for the long name yet"
			}
			if len(got) != inbox.MaxNameBytes || got != want {
				return false, fmt.Sprintf("the registered name is %d bytes, want %d", len(got), inbox.MaxNameBytes)
			}
			return true, ""
		})

		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "a1 lists the long name truncated", func(deadline time.Time) (bool, string) {
			out, state := vrListAgentsPoll(t, deadline, a1)
			if state != "" {
				return false, state
			}
			name, _, _, ok := vrFindNamePrefix(out, want)
			if !ok {
				return false, "no entry carries a prefix of the long name"
			}
			if !utf8.ValidString(name) {
				return false, "the displayed name is not valid UTF-8"
			}
			return true, ""
		})

		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the peer lists the long name truncated", func(deadline time.Time) (bool, string) {
			out, state := vrListAgentsPoll(t, deadline, b1)
			if state != "" {
				return false, state
			}
			name, _, alias, ok := vrFindNamePrefix(out, want)
			if !ok {
				return false, "the peer lists no entry carrying a prefix of the long name"
			}
			if !utf8.ValidString(name) {
				return false, "the peer's displayed name is not valid UTF-8"
			}
			if alias != "hostA.peer" {
				return false, "the peer entry's alias is " + alias
			}
			return true, ""
		})
	})

	t.Run("REQ-agent-messaging-012 removal reaches peer", func(t *testing.T) {
		gone := vrStartSession(t, bNode, "b3", vrTopic)
		defer gone.End(t)
		harness.Eventually(t, vrWaitBound, "a1 sees b3 before it ends", func() (bool, string) {
			return vrSees(t, a1, "b3"), "a1 does not list b3"
		})

		// The requirement's clock starts when the session ends, so the span is
		// measured from just before End; End's own signal escalation runs
		// inside it.
		start := time.Now()
		gone.End(t)
		harness.Eventually(t, vrWaitBound, "the ended session leaves both lists", func() (bool, string) {
			if vrSees(t, a1, "b3") {
				return false, "a1 still lists b3"
			}
			if vrSees(t, b1, "b3") {
				return false, "B still lists b3"
			}
			return true, ""
		})
		if elapsed := time.Since(start); elapsed > vrRemovalAssertBound {
			t.Fatalf("the ended session left both lists after %s, want at most %s (requirement %s + observation margin %s; the wait fallback is %s)",
				elapsed, vrRemovalAssertBound, vrRemovalRequirement, vrObserveMargin, vrWaitBound)
		}
	})

	t.Run("REQ-topics-008 and 019 negative", func(t *testing.T) {
		const defaultTopic = "github.com/org/e2e-default"
		ws := aNode.Workspace(t, "git", "https://github.com/org/e2e-default", "")
		auto := aNode.StartSession(t, harness.SessionSpec{Name: "auto", StartMCP: true, Workdir: ws})
		defer auto.End(t)
		vrWaitListable(t, auto)
		harness.EventuallyPoll(t, vrWaitBound, "the git workspace joins its default topic", func(deadline time.Time) (bool, string) {
			out, state := vrListTopicsPoll(t, deadline, auto)
			if state != "" {
				return false, state
			}
			if !vrContains(out.Joined, defaultTopic) {
				return false, fmt.Sprintf("joined topics are %v, want %s", out.Joined, defaultTopic)
			}
			return true, ""
		})

		out, state := vrListTopics(t, a1)
		if state != "" {
			t.Fatal(state)
		}
		for _, public := range out.Public {
			if public.Name == vrTopic || public.Name == defaultTopic {
				t.Fatalf("the public topic list exposes %q; neither topic was declared public", public.Name)
			}
		}
	})
}

func vrConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

func vrStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

func vrOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, vrWaitBound)
	return s
}

func vrStartSession(t *testing.T, node *harness.Node, name string, topics ...string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	vrWaitListable(t, s)
	for _, topic := range topics {
		vrJoin(t, s, topic)
	}
	return s
}

func vrWaitListable(t *testing.T, s *harness.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the session is registered", func(deadline time.Time) (bool, string) {
		_, state := vrListAgentsPoll(t, deadline, s)
		return state == "", state
	})
}

// vrJoin retries while the session is still registering. The poll needs
// harness.TopicCallPollBound because join_topic's handler may spend
// topicRPCTimeout before the update hint runs.
func vrJoin(t *testing.T, s *harness.Session, topic string) {
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

func vrCreateKey(t *testing.T, op *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := op.Call(t, "create_pairing_key", map[string]any{})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

func vrConnect(t *testing.T, op *harness.Session, address, key, localAlias string) harness.Result {
	t.Helper()
	args := map[string]any{"address": address, "key": key}
	if localAlias != "" {
		args["local_alias"] = localAlias
	}
	return op.Call(t, "connect_broker", args)
}

func vrShownAddress(t *testing.T, c *harness.Cluster, node string, shown []string) string {
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

func vrWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the link to "+alias+" is connected", func() (bool, string) {
		resp, detail := node.Oracle().ListBrokers(t)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, broker := range resp.GetBrokers() {
			if broker.GetAlias() == alias {
				return broker.GetStatus() == localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
					fmt.Sprintf("status is %s", broker.GetStatus())
			}
		}
		return false, "the peer is not listed"
	})
}

func vrListAgents(t *testing.T, s *harness.Session) (mcpserver.ListAgentsOut, string) {
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

func vrListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", nil)
}

func vrListTopics(t *testing.T, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	r := s.Call(t, "list_topics", nil)
	if r.IsError {
		return mcpserver.ListTopicsOut{}, r.Text
	}
	out, err := harness.Decode[mcpserver.ListTopicsOut](string(r.Structured))
	if err != nil {
		return mcpserver.ListTopicsOut{}, "decoding list_topics: " + err.Error()
	}
	return out, ""
}

func vrListTopicsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListTopicsOut](t, deadline, s, "list_topics", nil)
}

// vrSees fails the test on a list_agents error: treating it as not visible
// would let a broken Broker satisfy every negative assertion.
func vrSees(t *testing.T, s *harness.Session, name string) bool {
	t.Helper()
	out, state := vrListAgents(t, s)
	if state != "" {
		t.Fatalf("list_agents failed while looking for %q: %s", name, state)
	}
	_, ok := vrFindAgent(out, name)
	return ok
}

func vrFindNamePrefix(out mcpserver.ListAgentsOut, want string) (name, ref, alias string, ok bool) {
	for _, agent := range out.Agents {
		n, r, a := vrSplitAddress(agent.Address)
		if n != "" && strings.HasPrefix(want, n) {
			return n, r, a, true
		}
	}
	return "", "", "", false
}

func vrBrokerRegisteredName(t *testing.T, node *harness.Node, prefix string) string {
	t.Helper()
	for _, line := range strings.Split(string(node.BrokerLog(t)), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Event string `json:"event"`
			Name  string `json:"name"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec.Event == "agent_registered" && strings.HasPrefix(rec.Name, prefix) {
			return rec.Name
		}
	}
	return ""
}

func vrFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if got, _, _ := vrSplitAddress(agent.Address); got == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

func vrWaitSelfRef(t *testing.T, s *harness.Session, name string) string {
	t.Helper()
	var ref string
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the address of "+name+" carries a ref", func(deadline time.Time) (bool, string) {
		out, state := vrListAgentsPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		got, gotRef, _ := vrSplitAddress(out.Self)
		if got != name || gotRef == "" {
			return false, "self_address is " + out.Self
		}
		ref = gotRef
		return true, ""
	})
	return ref
}

func vrSplitAddress(address string) (name, ref, alias string) {
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

func vrEnvelopeBody(t *testing.T, content string) string {
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

func vrCountMark(frames []fakesession.Frame, marker string) int {
	n := 0
	for _, frame := range frames {
		if strings.Contains(frame.Content, marker) {
			n++
		}
	}
	return n
}

func vrSameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, value := range got {
		seen[value] = true
	}
	for _, value := range want {
		if !seen[value] {
			return false
		}
	}
	return true
}

func vrContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func vrKeys(seen map[string]bool) []string {
	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func vrMark(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("visibility_remote_test: reading random bytes: %v", err))
	}
	return prefix + hex.EncodeToString(b[:])
}

func vrRandom() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("visibility_remote_test: reading random bytes: %v", err))
	}
	return hex.EncodeToString(b[:])
}
