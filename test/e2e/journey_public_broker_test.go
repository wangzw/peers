//go:build e2e

package e2e

import (
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// journeyBrokerPort is the port a Broker listens on with the default
// configuration.
const journeyBrokerPort = "17443"

// journeyConfig is the configuration every Broker in the journey uses.
func journeyConfig(public bool, noListen bool) string {
	broker := "broker:"
	if public {
		broker += "\n  public: true"
	}
	if noListen {
		broker += "\n  listen: \"\""
	}
	parts := []string{
		harness.FastTimers,
		"directory:\n  reconcile_interval: 1s",
	}
	if broker != "broker:" {
		parts = append(parts, broker)
	}
	return harness.ConfigYAML(parts...)
}

// journeyNodeAddr returns the node's address on the flat network and its
// Broker port.
func journeyNodeAddr(c *harness.Cluster, node string) string {
	return net.JoinHostPort(c.Addr(node, "net"), journeyBrokerPort)
}

// journeyCallOK retries a tool that must succeed, covering the short window in
// which a fresh MCP server process is still connecting. A read-only or
// idempotent tool polls with Session.PollCallWithin, so a transport error is
// retried; any other tool is a side effect and goes through
// Session.CallUntilOK, which fails on a transport error instead of repeating a
// call the server may already have applied. A topic tool spends
// topicRPCTimeout inside the handler before the update hint runs, so it polls
// with harness.TopicCallPollBound and the wait gets harness.TopicCallWaitBound.
func journeyCallOK(t *testing.T, session *harness.Session, tool string, args map[string]any, bound time.Duration) harness.Result {
	t.Helper()
	if !pollableTool(tool) {
		return session.CallUntilOK(t, bound, tool, args)
	}
	pollBound, waitBound := harness.ToolCallPollBound, bound
	if brokerTopicTool(tool) {
		pollBound, waitBound = harness.TopicCallPollBound, harness.TopicCallWaitBound
	}
	var result harness.Result
	harness.EventuallyPoll(t, waitBound, "calling "+tool, func(deadline time.Time) (bool, string) {
		var err error
		result, err = session.PollCallWithin(t, deadline, pollBound, tool, args)
		if err != nil {
			return false, err.Error()
		}
		if result.IsError {
			return false, result.Text
		}
		return true, ""
	})
	return result
}

// journeyCreateKey creates a one-time pairing key.
func journeyCreateKey(t *testing.T, session *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := journeyCallOK(t, session, "create_pairing_key", nil, harness.EventTimeout)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

// journeyConnect dials a Broker with a pairing key.
func journeyConnect(t *testing.T, session *harness.Session, address, key string) mcpserver.ConnectBrokerOut {
	t.Helper()
	r := journeyCallOK(t, session, "connect_broker", map[string]any{"address": address, "key": key}, harness.EventTimeout)
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	return out
}

// journeyWorkspace creates a plain workspace inside the node and returns its
// path, which a session in that node uses as its working directory.
func journeyWorkspace(t *testing.T, node *harness.Node) string {
	t.Helper()
	return node.Workspace(t, "plain", "", "")
}

// journeyPair has acceptor create a key and dialer use it.
func journeyPair(t *testing.T, acceptor, dialer *harness.Node, acceptorAddr string) mcpserver.ConnectBrokerOut {
	t.Helper()
	acceptorOperator := pairOperator(t, acceptor, journeyWorkspace(t, acceptor))
	defer acceptorOperator.End(t)
	key := journeyCreateKey(t, acceptorOperator)
	dialerOperator := pairOperator(t, dialer, journeyWorkspace(t, dialer))
	defer dialerOperator.End(t)
	return journeyConnect(t, dialerOperator, acceptorAddr, key.Key)
}

// journeyWaitPeer waits until the session lists alias as a connected peer with
// the given role.
func journeyWaitPeer(t *testing.T, session *harness.Session, alias, role string, bound time.Duration) {
	t.Helper()
	harness.EventuallyPoll(t, bound, "the session reports "+alias+" connected as "+role, func(deadline time.Time) (bool, string) {
		out, state := pairListBrokersPoll(t, deadline, session)
		if state != "" {
			return false, state
		}
		broker, ok := pairBrokerByAlias(out, alias)
		if !ok {
			return false, "the peer is not listed"
		}
		if !broker.IsPeer {
			return false, "the entry is not a peer"
		}
		if broker.Status != "connected" {
			return false, "status is " + broker.Status
		}
		if broker.LocalRole != role {
			return false, "role is " + broker.LocalRole
		}
		return true, ""
	})
}

// TestJourneyConnectPublicBroker walks the "discover and connect to a public
// Broker" journey on real containers: discover a public Broker from its card,
// see its public topic, pair with it, join the topic, list its Agent and send
// it a message. Two alternatives cover a fingerprint mismatch warning and an
// acceptor that does not listen.
func TestJourneyConnectPublicBroker(t *testing.T) {
	t.Run("main", func(t *testing.T) {
		c := harness.Up(t, harness.TopoFlat(3))
		hostA, hostB, hostC := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
		pairStartBroker(t, hostA, journeyConfig(true, false))
		pairStartBroker(t, hostB, journeyConfig(false, false))
		pairStartBroker(t, hostC, journeyConfig(false, false))

		aAddr := journeyNodeAddr(c, "hostA")
		journeyPair(t, hostB, hostA, journeyNodeAddr(c, "hostB"))
		journeyPair(t, hostB, hostC, journeyNodeAddr(c, "hostB"))

		infra := pairSession(t, hostA, "infra-agent", journeyWorkspace(t, hostA))
		defer infra.End(t)
		scout := pairSession(t, hostC, "scout", journeyWorkspace(t, hostC))
		defer scout.End(t)
		pairWaitRegistered(t, infra, "infra-agent")
		pairWaitRegistered(t, scout, "scout")

		// Precondition: A declares infra-help publicly.
		journeyCallOK(t, infra, "join_topic", map[string]any{
			"name": "infra-help", "public": true, "description": "ask infra",
		}, harness.EventTimeout)

		// Step 1: C discovers A's card and its public topic.
		var discovered mcpserver.BrokerOut
		harness.EventuallyPoll(t, discBound(2), "C discovers A's card", func(deadline time.Time) (bool, string) {
			out, state := pairListBrokersPoll(t, deadline, scout)
			if state != "" {
				return false, state
			}
			broker, ok := pairBrokerByAlias(out, "hostA.peer")
			if !ok {
				return false, "hostA.peer is not listed"
			}
			if broker.IsPeer {
				return false, "A is listed as a peer before pairing"
			}
			if broker.Status != "connectable" {
				return false, "status is " + broker.Status
			}
			if len(broker.Addresses) != 1 || broker.Addresses[0].Kind != "direct" || broker.Addresses[0].Addr != aAddr {
				return false, fmt.Sprintf("addresses are %v, want [{direct %s}]", broker.Addresses, aAddr)
			}
			if !pairIsFingerprint(broker.Fingerprint) {
				return false, "fingerprint is " + broker.Fingerprint
			}
			discovered = broker
			return true, ""
		})

		// Step 2: C sees A's public declaration.
		harness.EventuallyPoll(t, harness.TopicCallWaitBound, "C sees A's public topic", func(deadline time.Time) (bool, string) {
			r, err := scout.PollCall(t, deadline, "list_topics", nil)
			if err != nil {
				return false, err.Error()
			}
			if r.IsError {
				return false, r.Text
			}
			out, err := harness.Decode[mcpserver.ListTopicsOut](string(r.Structured))
			if err != nil {
				return false, "decoding list_topics: " + err.Error()
			}
			for _, topic := range out.Public {
				if topic.Name == "infra-help" && topic.Description == "ask infra" {
					return true, ""
				}
			}
			return false, fmt.Sprintf("public topics are %v", out.Public)
		})

		// Step 4: A creates a pairing key.
		key := journeyCreateKey(t, infra)

		// Step 5: C connects with the address in A's card. Its fingerprint is
		// the one the card announced and no warning is attached.
		connected := journeyConnect(t, scout, aAddr, key.Key)
		if connected.Fingerprint != discovered.Fingerprint {
			t.Fatalf("connect_broker fingerprint is %q, want the card's %q", connected.Fingerprint, discovered.Fingerprint)
		}
		if len(connected.Warnings) != 0 {
			t.Fatalf("connect_broker warnings are %v, want none", connected.Warnings)
		}

		// Step 6: C joins the same topic, which makes the agents visible to
		// each other. Step 7: list_agents shows A's Agent.
		journeyCallOK(t, scout, "join_topic", map[string]any{"name": "infra-help"}, harness.EventTimeout)
		var target string
		harness.EventuallyPoll(t, discBound(1), "C sees A's Agent in infra-help", func(deadline time.Time) (bool, string) {
			out, state := pairListAgentsPoll(t, deadline, scout)
			if state != "" {
				return false, state
			}
			for _, agent := range out.Agents {
				name, alias := pairSplitAddress(agent.Address)
				if name == "infra-agent" && alias == "hostA.peer" {
					target = agent.Address
					return true, ""
				}
			}
			return false, fmt.Sprintf("agents are %v", out.Agents)
		})

		// Step 8: C sends a message and A's simulated session receives it.
		const request = "can you look at the deploy?"
		journeyCallOK(t, scout, "send_message", map[string]any{"to": target, "message": request}, harness.EventTimeout)
		frame := infra.WaitFrame(t, harness.EventTimeout)
		if frame.FromName != "scout@hostC.peer" {
			t.Fatalf("the message is signed %q, want scout@hostC.peer", frame.FromName)
		}
		if body := collabEnvelopeBody(t, frame.Content); body != request {
			t.Fatalf("the delivered body is %q, want %q", body, request)
		}

		// Result: A is now C's peer, as the Broker it dialed.
		harness.EventuallyPoll(t, harness.EventTimeout, "C lists A as its connected peer", func(deadline time.Time) (bool, string) {
			out, state := pairListBrokersPoll(t, deadline, scout)
			if state != "" {
				return false, state
			}
			broker, ok := pairBrokerByAlias(out, "hostA.peer")
			if !ok {
				return false, "hostA.peer is not listed"
			}
			if !broker.IsPeer || broker.Status != "connected" || broker.LocalRole != "dialer" {
				return false, fmt.Sprintf("is_peer=%v status=%s role=%s", broker.IsPeer, broker.Status, broker.LocalRole)
			}
			return true, ""
		})

		// The reverse view: A accepted C's connection and sees its Agent.
		journeyWaitPeer(t, infra, "hostC.peer", "acceptor", discBound(1))
		var scoutAddr string
		harness.EventuallyPoll(t, discBound(1), "A sees scout in infra-help", func(deadline time.Time) (bool, string) {
			out, state := pairListAgentsPoll(t, deadline, infra)
			if state != "" {
				return false, state
			}
			for _, agent := range out.Agents {
				name, alias := pairSplitAddress(agent.Address)
				if name == "scout" && alias == "hostC.peer" {
					scoutAddr = agent.Address
					return true, ""
				}
			}
			return false, fmt.Sprintf("agents are %v", out.Agents)
		})

		// The reverse message: A's Agent answers the request it received.
		const reply = "yes, I am on it"
		journeyCallOK(t, infra, "send_message", map[string]any{"to": scoutAddr, "message": reply}, harness.EventTimeout)
		frame = scout.WaitFrame(t, harness.EventTimeout)
		if frame.FromName != "infra-agent@hostA.peer" {
			t.Fatalf("the reply is signed %q, want infra-agent@hostA.peer", frame.FromName)
		}
		if body := collabEnvelopeBody(t, frame.Content); body != reply {
			t.Fatalf("the reply body is %q, want %q", body, reply)
		}
	})

	t.Run("fingerprint_mismatch", func(t *testing.T) {
		c := harness.Up(t, harness.TopoFlat(3))
		hostA, hostB, hostC := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
		pairStartBroker(t, hostA, journeyConfig(true, false))
		pairStartBroker(t, hostB, journeyConfig(false, false))
		pairStartBroker(t, hostC, journeyConfig(false, false))

		journeyPair(t, hostB, hostA, journeyNodeAddr(c, "hostB"))
		journeyPair(t, hostB, hostC, journeyNodeAddr(c, "hostB"))

		scout := pairSession(t, hostC, "scout", journeyWorkspace(t, hostC))
		defer scout.End(t)
		pairWaitRegistered(t, scout, "scout")

		var oldFingerprint string
		harness.EventuallyPoll(t, discBound(2), "C sees A's card before A changes identity", func(deadline time.Time) (bool, string) {
			out, state := pairListBrokersPoll(t, deadline, scout)
			if state != "" {
				return false, state
			}
			broker, ok := pairBrokerByAlias(out, "hostA.peer")
			if !ok {
				return false, "hostA.peer is not listed"
			}
			oldFingerprint = broker.Fingerprint
			return true, ""
		})

		// A restarts with a fresh identity at the same address. Its old card
		// is still in the directory.
		hostA.StopBroker(t, syscall.SIGTERM)
		c.Root(t, "hostA", "rm", "-rf", "/home/peer/.peers")
		pairStartBroker(t, hostA, journeyConfig(true, false))

		operator := pairOperator(t, hostA, journeyWorkspace(t, hostA))
		defer operator.End(t)
		key := journeyCreateKey(t, operator)

		connected := journeyConnect(t, scout, journeyNodeAddr(c, "hostA"), key.Key)
		if connected.Fingerprint == oldFingerprint {
			t.Fatalf("A kept fingerprint %q across the identity reset", oldFingerprint)
		}
		if len(connected.Warnings) != 1 {
			t.Fatalf("connect_broker warnings are %v, want exactly one", connected.Warnings)
		}
		warning := connected.Warnings[0]
		if !strings.HasPrefix(warning, "fingerprint mismatch") {
			t.Fatalf("the warning is %q, want it to start with \"fingerprint mismatch\"", warning)
		}
		if !strings.Contains(warning, oldFingerprint) || !strings.Contains(warning, connected.Fingerprint) {
			t.Fatalf("the warning %q does not name both fingerprints %q and %q", warning, oldFingerprint, connected.Fingerprint)
		}
	})

	t.Run("acceptor_not_listening", func(t *testing.T) {
		c := harness.Up(t, harness.TopoFlat(3))
		hostA, hostB, hostC := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
		pairStartBroker(t, hostA, journeyConfig(true, true))
		pairStartBroker(t, hostB, journeyConfig(false, false))
		pairStartBroker(t, hostC, journeyConfig(false, false))

		// A has no listener, so it dials B; C dials B as well. A's card still
		// reaches C, with an empty address list.
		journeyPair(t, hostB, hostA, journeyNodeAddr(c, "hostB"))
		journeyPair(t, hostB, hostC, journeyNodeAddr(c, "hostB"))

		scout := pairSession(t, hostC, "scout", journeyWorkspace(t, hostC))
		defer scout.End(t)
		pairWaitRegistered(t, scout, "scout")

		harness.EventuallyPoll(t, discBound(2), "C sees A as not directly reachable", func(deadline time.Time) (bool, string) {
			out, state := pairListBrokersPoll(t, deadline, scout)
			if state != "" {
				return false, state
			}
			broker, ok := pairBrokerByAlias(out, "hostA.peer")
			if !ok {
				return false, "hostA.peer is not listed"
			}
			if broker.IsPeer {
				return false, "A is listed as a peer"
			}
			if len(broker.Addresses) != 0 {
				return false, fmt.Sprintf("addresses are %v, want none", broker.Addresses)
			}
			if broker.Status != "not_directly_reachable" {
				return false, "status is " + broker.Status
			}
			return true, ""
		})

		// C creates the key and A dials C.
		key := journeyCreateKey(t, scout)
		operator := pairOperator(t, hostA, journeyWorkspace(t, hostA))
		defer operator.End(t)
		connected := journeyConnect(t, operator, journeyNodeAddr(c, "hostC"), key.Key)
		if connected.Alias != "hostC.peer" {
			t.Fatalf("A paired with %q, want hostC.peer", connected.Alias)
		}

		journeyWaitPeer(t, operator, "hostC.peer", "dialer", harness.EventTimeout)
		journeyWaitPeer(t, scout, "hostA.peer", "acceptor", harness.EventTimeout)
	})
}
