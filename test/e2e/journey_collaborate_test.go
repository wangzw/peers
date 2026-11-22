//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// collabTopic is the default topic both differently written origins normalize
// to.
const collabTopic = "github.com/org/repo"

// TestJourneyCollaborateInTopic walks the "collaborate across hosts in one
// topic" journey: two paired hosts whose workspaces name the same repository
// in two spellings, sessions on both sides made visible through the automatic
// topic, and a message and its reply delivered with the receiving side's
// address.
func TestJourneyCollaborateInTopic(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoNAT)
	hostA := c.Node("hostA")
	hostB := c.Node("hostB")
	collabStartBroker(t, hostA)
	collabStartBroker(t, hostB)

	// The precondition is a paired and connected link, arranged by operator
	// sessions exactly as the previous journey does: hostA creates the key and
	// hostB dials.
	operatorA := collabOperator(t, hostA, hostA.Workspace(t, "plain", "", ""))
	defer operatorA.End(t)
	operatorB := collabOperator(t, hostB, hostB.Workspace(t, "plain", "", ""))
	defer operatorB.End(t)
	key := collabCreateKey(t, operatorA)
	collabConnect(t, operatorB, key.Addresses[0], key.Key)
	collabWaitPeer(t, hostA, "hostB.peer", localpb.PeerRole_PEER_ROLE_ACCEPTOR, harness.EventTimeout)
	collabWaitPeer(t, hostB, "hostA.peer", localpb.PeerRole_PEER_ROLE_DIALER, harness.EventTimeout)

	// Steps 1 and 2: both sessions work in a repository named by a different
	// origin spelling; no project configuration exists.
	wsA := hostA.Workspace(t, "git", "git@github.com:org/repo.git", "")
	wsB := hostB.Workspace(t, "git", "https://github.com/org/repo", "")
	api := collabSession(t, hostA, "api-worker", wsA)
	defer api.End(t)
	frontend := collabSession(t, hostB, "frontend", wsB)
	defer frontend.End(t)
	collabWaitRegistered(t, api, "api-worker")
	collabWaitRegistered(t, frontend, "frontend")

	// Step 3: the automatic topic makes the other session visible, under the
	// alias each Broker uses for its peer.
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "api-worker sees frontend in the shared default topic", func(deadline time.Time) (bool, string) {
		out, state := collabListAgentsPoll(t, deadline, api)
		if state != "" {
			return false, state
		}
		entry, ok := collabFindAgent(out, "frontend")
		if !ok {
			return false, "frontend is not listed"
		}
		name, alias := collabSplitAddress(entry.Address)
		if name != "frontend" || alias != "hostB.peer" {
			return false, fmt.Sprintf("frontend's address is %q, want frontend@hostB.peer", entry.Address)
		}
		if !collabSameSet(entry.SharedTopics, []string{collabTopic}) {
			return false, fmt.Sprintf("shared topics are %v, want [%s]", entry.SharedTopics, collabTopic)
		}
		return true, ""
	})

	// Steps 4, 5 and 6: the message is signed by the receiving Broker. The
	// address in the envelope is the one hostB uses for hostA, not the one
	// hostA uses for itself.
	const request = "API /v2/users changed"
	r := api.Call(t, "send_message", map[string]any{"to": "frontend@hostB.peer", "message": request})
	r.MustOK(t)
	frame := frontend.WaitFrame(t, harness.EventTimeout)
	if frame.FromName != "api-worker@hostA.peer" {
		t.Fatalf("the message is signed %q, want api-worker@hostA.peer", frame.FromName)
	}
	if frame.Auth {
		t.Fatal("the delivered frame carries an authentication line")
	}
	if body := collabEnvelopeBody(t, frame.Content); body != request {
		t.Fatalf("the delivered body is %q, want %q", body, request)
	}

	// Steps 7 and 8: the reply uses the signature verbatim as the target.
	const replyText = "thanks, the frontend is updated"
	r = frontend.Call(t, "send_message", map[string]any{"to": frame.FromName, "message": replyText})
	r.MustOK(t)
	reply := api.WaitFrame(t, harness.EventTimeout)
	if reply.FromName != "frontend@hostB.peer" {
		t.Fatalf("the reply is signed %q, want frontend@hostB.peer", reply.FromName)
	}
	if body := collabEnvelopeBody(t, reply.Content); body != replyText {
		t.Fatalf("the reply body is %q, want %q", body, replyText)
	}

	t.Run("busy target", func(t *testing.T) {
		// A busy session delays reading; the send still succeeds and only
		// returns once the frame has been written to its inbox.
		frontend.Fake().SetBusy(true)
		frontend.Fake().SetBusyDelay(time.Second)
		r := api.Call(t, "send_message", map[string]any{"to": "frontend@hostB.peer", "message": "while busy"})
		r.MustOK(t)
		if r.Elapsed < 900*time.Millisecond {
			t.Fatalf("the send returned after %s, want the one-second busy delay to elapse first", r.Elapsed)
		}
		frame := frontend.WaitFrame(t, harness.EventTimeout)
		if frame.FromName != "api-worker@hostA.peer" {
			t.Fatalf("the busy-target message is signed %q", frame.FromName)
		}
		if body := collabEnvelopeBody(t, frame.Content); body != "while busy" {
			t.Fatalf("the busy-target body is %q", body)
		}
	})

	t.Run("different origins then join", func(t *testing.T) {
		// A workspace whose origin names another repository shares no topic
		// with api-worker. Joining the repository topic explicitly makes it
		// visible and the exchange works from step 3 on.
		wsOther := hostB.Workspace(t, "git", "https://github.com/org/other", "")
		frontend2 := collabSession(t, hostB, "frontend2", wsOther)
		defer frontend2.End(t)
		collabWaitRegistered(t, frontend2, "frontend2")

		// The negative check is a short observation: the directory is already
		// synchronised, so one second without an entry is the intended result.
		harness.ConsistentlyPoll(t, 1*time.Second, harness.ListAgentsPollBound, "api-worker does not see frontend2 before it joins", nil, func(deadline time.Time) harness.Observation {
			out, state := collabListAgentsPoll(t, deadline, api)
			if state != "" {
				return harness.Unobservable(state)
			}
			if _, ok := collabFindAgent(out, "frontend2"); ok {
				return harness.Observed(false, "frontend2 is listed")
			}
			return harness.Observed(true, "")
		})

		collabJoin(t, frontend2, collabTopic)
		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "frontend2 appears after joining the repository topic", func(deadline time.Time) (bool, string) {
			out, state := collabListAgentsPoll(t, deadline, api)
			if state != "" {
				return false, state
			}
			entry, ok := collabFindAgent(out, "frontend2")
			if !ok {
				return false, "frontend2 is not listed"
			}
			name, alias := collabSplitAddress(entry.Address)
			if name != "frontend2" || alias != "hostB.peer" {
				return false, fmt.Sprintf("frontend2's address is %q, want frontend2@hostB.peer", entry.Address)
			}
			return true, ""
		})

		const joined = "after joining the topic"
		r := api.Call(t, "send_message", map[string]any{"to": "frontend2@hostB.peer", "message": joined})
		r.MustOK(t)
		frame := frontend2.WaitFrame(t, harness.EventTimeout)
		if frame.FromName != "api-worker@hostA.peer" {
			t.Fatalf("the joined-topic message is signed %q", frame.FromName)
		}
		if body := collabEnvelopeBody(t, frame.Content); body != joined {
			t.Fatalf("the joined-topic body is %q, want %q", body, joined)
		}
	})
}

// collabStartBroker starts the node's Broker with the default configuration.
func collabStartBroker(t *testing.T, node *harness.Node) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// collabSession starts one simulated session with a real MCP server process.
func collabSession(t *testing.T, node *harness.Node, name, workdir string) *harness.Session {
	t.Helper()
	return node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, Workdir: workdir})
}

// collabOperator starts a session that never registers as an Agent but can
// call the pairing tools. It waits for the Broker connection before the
// caller's first tool call.
func collabOperator(t *testing.T, node *harness.Node, workdir string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true, Workdir: workdir})
	harness.WaitConnected(t, s, harness.EventTimeout)
	return s
}

// collabCreateKey creates a one-time pairing key and decodes the answer.
func collabCreateKey(t *testing.T, session *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := session.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

// collabConnect dials the paired Broker and requires success.
func collabConnect(t *testing.T, session *harness.Session, address, key string) mcpserver.ConnectBrokerOut {
	t.Helper()
	r := session.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	return out
}

// collabWaitRegistered waits until the session is registered and lists itself.
func collabWaitRegistered(t *testing.T, session *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "session "+name+" is registered", func(deadline time.Time) (bool, string) {
		out, state := collabListAgentsPoll(t, deadline, session)
		if state != "" {
			return false, state
		}
		if !strings.HasPrefix(out.Self, name+"@") {
			return false, "self_address is " + out.Self
		}
		return true, ""
	})
}

// collabListAgentsPoll calls list_agents from a wait and returns the decoded
// answer: the call is bounded by the wait's deadline and a transport error is
// returned as state.
func collabListAgentsPoll(t *testing.T, deadline time.Time, session *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, session, "list_agents", nil)
}

// collabFindAgent returns the entry whose name part is name.
func collabFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		got, _ := collabSplitAddress(agent.Address)
		if got == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

// collabSplitAddress splits "name@alias" into its two parts.
func collabSplitAddress(address string) (name, alias string) {
	at := strings.LastIndexByte(address, '@')
	if at < 0 {
		return address, ""
	}
	return address[:at], address[at+1:]
}

// collabSameSet reports whether got and want hold the same strings.
func collabSameSet(got, want []string) bool {
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

// collabJoin joins a topic, retrying while the session is still registering.
// join_topic's handler may spend topicRPCTimeout before the update hint runs,
// so the poll uses harness.TopicCallPollBound.
func collabJoin(t *testing.T, session *harness.Session, topic string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "join_topic "+topic, func(deadline time.Time) (bool, string) {
		r, err := session.PollCallWithin(t, deadline, harness.TopicCallPollBound, "join_topic", map[string]any{"name": topic})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		return true, ""
	})
}

// collabEnvelopeBody returns the message body from the attribution envelope
// the transport wraps it in.
func collabEnvelopeBody(t *testing.T, content string) string {
	t.Helper()
	openEnd := strings.Index(content, ">\n")
	if openEnd < 0 {
		t.Fatalf("the delivered content has no envelope opening line: %q", content)
	}
	body := content[openEnd+2:]
	const closing = "\n</cross-session-message>"
	if !strings.HasSuffix(body, closing) {
		t.Fatalf("the delivered content has no envelope closing tag: %q", content)
	}
	return strings.TrimSuffix(body, closing)
}

// collabWaitPeer waits until node's local interface reports alias connected
// with the given role.
func collabWaitPeer(t *testing.T, node *harness.Node, alias string, role localpb.PeerRole, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the local interface reports "+alias+" connected", func() (bool, string) {
		resp, detail := node.Oracle().ListBrokers(t)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, broker := range resp.GetBrokers() {
			if broker.GetAlias() != alias {
				continue
			}
			if broker.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, "status is not CONNECTED"
			}
			if broker.GetRole() != role {
				return false, fmt.Sprintf("role is %v, want %v", broker.GetRole(), role)
			}
			return true, ""
		}
		return false, "the peer is not listed"
	})
}
