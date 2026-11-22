//go:build e2e

package e2e

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// pairBrokerPort is the port a Broker listens on with the default
// configuration, and the port the topologies forward.
const pairBrokerPort = "17443"

// pairReconnectBound waits for both Brokers to relink after a cut on the
// default timers. The cut is noticed within three seconds; the dialer's
// backoff has then grown to at most four seconds, one dial and the accept
// side's catch-up follow, and the project-wide three-second margin brings the
// bound to ten seconds.
const pairReconnectBound = 10 * time.Second

// TestJourneyPairBrokers walks the "two hosts pair for the first time" journey
// on real containers: two Brokers with the default configuration, two real MCP
// server processes, and a NAT that lets only hostB open a connection to hostA.
// It also covers the alias-conflict, unreachable-display-address and network
// outage alternatives.
func TestJourneyPairBrokers(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoNAT)
	hostA := c.Node("hostA")
	hostB := c.Node("hostB")
	pairStartBroker(t, hostA, "")
	pairStartBroker(t, hostB, "")

	// The M2 completion condition: hostB listens, yet hostA cannot connect to
	// it, while hostB can reach hostA.
	aWan := net.JoinHostPort(c.Addr("hostA", "wan"), pairBrokerPort)
	bLanb := net.JoinHostPort(c.Addr("hostB", "lanb"), pairBrokerPort)
	if c.TCPReachable(t, "hostA", bLanb, time.Second) {
		t.Fatalf("hostA reached hostB at %s; the two hosts must not be mutually reachable", bLanb)
	}
	if !c.TCPReachable(t, "hostB", aWan, harness.EventTimeout) {
		t.Fatalf("hostB cannot reach hostA at %s; the displayed address would not work either", aWan)
	}
	// hostA and hostB are on different Docker networks, so the hostname shown
	// with the pairing key does not resolve on the dialing side. An
	// implementation that dials it instead of the address in the parentheses
	// fails the connect below.
	if result := hostB.Exec(t, []string{"getent", "hosts", "hostA"}, nil); result.ExitCode == 0 {
		t.Fatalf("hostB resolves hostA (%q); the displayed hostname check would not prove anything", strings.TrimSpace(result.Stdout))
	}

	wsA := hostA.Workspace(t, "plain", "", "")
	wsB := hostB.Workspace(t, "plain", "", "")
	alice := pairSession(t, hostA, "alice", wsA)
	defer alice.End(t)
	bob := pairSession(t, hostB, "bob", wsB)
	defer bob.End(t)
	pairWaitRegistered(t, alice, "alice")
	pairWaitRegistered(t, bob, "bob")

	// Both sides know no Broker before the pairing.
	for name, session := range map[string]*harness.Session{"alice": alice, "bob": bob} {
		out, state := pairListBrokers(t, session)
		if state != "" {
			t.Fatalf("%s: list_brokers failed: %s", name, state)
		}
		if len(out.Brokers) != 0 {
			t.Fatalf("%s knows %v before any pairing, want none", name, out.Brokers)
		}
	}

	// Step 1 and 2: alice creates a one-time key. The shown address is hostA
	// and the address of its default-route interface; neither is part of the
	// key.
	key := pairCreateKey(t, alice)
	expires, err := time.Parse(time.RFC3339, key.ExpiresAt)
	if err != nil {
		t.Fatalf("create_pairing_key returned expires_at %q: %v", key.ExpiresAt, err)
	}
	if remaining := time.Until(expires); remaining < 9*time.Minute || remaining > 10*time.Minute {
		t.Fatalf("the key expires in %s, want between 9 and 10 minutes", remaining)
	}
	wantShown := []string{"hostA(" + aWan + ")"}
	if len(key.Addresses) != 1 || key.Addresses[0] != wantShown[0] {
		t.Fatalf("shown_addresses = %v, want %v", key.Addresses, wantShown)
	}
	if strings.Contains(key.Key, "hostA") || strings.Contains(key.Key, c.Addr("hostA", "wan")) {
		t.Fatalf("the key %q embeds the shown hostname or address", key.Key)
	}

	// Steps 3 and 4: bob dials with the displayed address verbatim and the
	// key. The parenthesis content is what gets dialed.
	r := bob.Call(t, "connect_broker", map[string]any{"address": key.Addresses[0], "key": key.Key})
	r.MustOK(t)
	connected, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	if connected.Alias != "hostA.peer" {
		t.Fatalf("connect_broker alias = %q, want hostA.peer", connected.Alias)
	}
	if connected.Host != "hostA" {
		t.Fatalf("connect_broker shown_host = %q, want hostA", connected.Host)
	}
	if !pairIsFingerprint(connected.Fingerprint) {
		t.Fatalf("connect_broker fingerprint = %q, want 64 lowercase hex digits", connected.Fingerprint)
	}
	// A's identity, derived from its certificate, is what both sides must
	// report; comparing the two reported values alone would only prove that
	// they agree with each other.
	aIdentity := pkIdentityFingerprint(t, hostA)
	if connected.Fingerprint != aIdentity {
		t.Fatalf("connect_broker fingerprint = %q, want A's identity %q", connected.Fingerprint, aIdentity)
	}

	// Step 5 and 6: both sides list the other as a peer and report the link.
	var bobView mcpserver.BrokerOut
	harness.EventuallyPoll(t, harness.EventTimeout, "both Brokers report the established link", func(deadline time.Time) (bool, string) {
		bobOut, state := pairListBrokersPoll(t, deadline, bob)
		if state != "" {
			return false, "bob: " + state
		}
		peerOfBob, ok := pairBrokerByAlias(bobOut, "hostA.peer")
		if !ok {
			return false, "bob does not list hostA.peer yet"
		}
		if !peerOfBob.IsPeer {
			return false, "bob's entry for hostA.peer is not a peer"
		}
		if peerOfBob.Status != "connected" {
			return false, "bob sees status " + peerOfBob.Status
		}
		if peerOfBob.LocalRole != "dialer" {
			return false, "bob's role is " + peerOfBob.LocalRole
		}
		if len(peerOfBob.Addresses) != 1 || peerOfBob.Addresses[0].Kind != "direct" || peerOfBob.Addresses[0].Addr != aWan {
			return false, fmt.Sprintf("bob sees advertised addresses %v, want [direct %s]", peerOfBob.Addresses, aWan)
		}
		bobView = peerOfBob

		aliceOut, state := pairListBrokersPoll(t, deadline, alice)
		if state != "" {
			return false, "alice: " + state
		}
		peerOfAlice, ok := pairBrokerByAlias(aliceOut, "hostB.peer")
		if !ok {
			return false, "alice does not list hostB.peer yet"
		}
		if !peerOfAlice.IsPeer {
			return false, "alice's entry for hostB.peer is not a peer"
		}
		if peerOfAlice.Status != "connected" {
			return false, "alice sees status " + peerOfAlice.Status
		}
		if peerOfAlice.LocalRole != "acceptor" {
			return false, "alice's role is " + peerOfAlice.LocalRole
		}
		if len(peerOfAlice.Addresses) != 0 {
			return false, fmt.Sprintf("alice sees advertised addresses %v for hostB, want none", peerOfAlice.Addresses)
		}
		return true, ""
	})

	// The acceptor's fingerprint is A's own identity, not merely the value the
	// dialer reported back.
	if bobView.Fingerprint != aIdentity {
		t.Fatalf("bob lists hostA with fingerprint %q, want A's identity %q", bobView.Fingerprint, aIdentity)
	}
	pairCheckOraclePeer(t, hostA, "hostB.peer", localpb.PeerRole_PEER_ROLE_ACCEPTOR)
	pairCheckOraclePeer(t, hostB, "hostA.peer", localpb.PeerRole_PEER_ROLE_DIALER)

	t.Run("alias conflict needs local alias", func(t *testing.T) {
		// hostB and hostC both call themselves macbook. Once hostA has paired
		// with hostB, initiating a pairing to hostC must ask for a local alias
		// before it records anything.
		flat := harness.Up(t, harness.TopoFlat(3))
		hostA := flat.Node("hostA")
		hostB := flat.Node("hostB")
		hostC := flat.Node("hostC")
		pairStartBroker(t, hostA, "")
		pairStartBroker(t, hostB, "broker:\n  alias: macbook\n")
		pairStartBroker(t, hostC, "broker:\n  alias: macbook\n")

		wsA := hostA.Workspace(t, "plain", "", "")
		wsB := hostB.Workspace(t, "plain", "", "")
		wsC := hostC.Workspace(t, "plain", "", "")
		opA := pairOperator(t, hostA, wsA)
		defer opA.End(t)
		opB := pairOperator(t, hostB, wsB)
		defer opB.End(t)
		opC := pairOperator(t, hostC, wsC)
		defer opC.End(t)

		keyAB := pairCreateKey(t, opA)
		pairConnect(t, opB, keyAB.Addresses[0], keyAB.Key)
		pairWaitOraclePeer(t, hostA, "macbook", localpb.PeerRole_PEER_ROLE_ACCEPTOR, harness.EventTimeout)

		keyC := pairCreateKey(t, opC)
		r := opA.Call(t, "connect_broker", map[string]any{"address": keyC.Addresses[0], "key": keyC.Key})
		r.MustCode(t, "needs_local_alias")
		if !r.HasPrefix("Not connected") {
			t.Fatalf("the alias-conflict error %q does not start with Not connected", r.Text)
		}
		// Neither side recorded the other on the failed attempt.
		if got := pairOracleBrokerCount(t, hostA); got != 1 {
			t.Fatalf("hostA knows %d Brokers after the failed attempt, want only hostB", got)
		}
		if got := pairOracleBrokerCount(t, hostC); got != 0 {
			t.Fatalf("hostC knows %d Brokers after the failed attempt, want none", got)
		}

		pairConnectWithAlias(t, opA, keyC.Addresses[0], keyC.Key, "macbook-2", "macbook-2")
		pairWaitOraclePeer(t, hostA, "macbook-2", localpb.PeerRole_PEER_ROLE_DIALER, harness.EventTimeout)

		// Both peers' sessions are visible to a session on hostA, and each
		// address carries the alias hostA uses for that peer.
		sa := pairSession(t, hostA, "a1", wsA)
		defer sa.End(t)
		sb := pairSession(t, hostB, "b1", wsB)
		defer sb.End(t)
		sc := pairSession(t, hostC, "c1", wsC)
		defer sc.End(t)
		pairJoin(t, sa, "t")
		pairJoin(t, sb, "t")
		pairJoin(t, sc, "t")

		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "a1 sees b1@macbook and c1@macbook-2", func(deadline time.Time) (bool, string) {
			out, state := pairListAgentsPoll(t, deadline, sa)
			if state != "" {
				return false, state
			}
			found := map[string]string{}
			for _, agent := range out.Agents {
				name, alias := pairSplitAddress(agent.Address)
				found[name] = alias
			}
			if found["b1"] != "macbook" {
				return false, fmt.Sprintf("b1's address is %q, want the alias macbook", out.Agents)
			}
			if found["c1"] != "macbook-2" {
				return false, fmt.Sprintf("c1's address is %q, want the alias macbook-2", out.Agents)
			}
			return true, ""
		})
	})

	t.Run("displayed address unreachable", func(t *testing.T) {
		// hostA sits behind a port forward. Its displayed address is the
		// private one, which hostB cannot dial; the forwarded address works.
		c := harness.Up(t, harness.TopoPortForward)
		hostA := c.Node("hostA")
		hostB := c.Node("hostB")
		pairStartBroker(t, hostA, "")
		pairStartBroker(t, hostB, "")

		wsA := hostA.Workspace(t, "plain", "", "")
		wsB := hostB.Workspace(t, "plain", "", "")
		opA := pairOperator(t, hostA, wsA)
		defer opA.End(t)
		opB := pairOperator(t, hostB, wsB)
		defer opB.End(t)

		key := pairCreateKey(t, opA)
		aLana := net.JoinHostPort(c.Addr("hostA", "lana"), pairBrokerPort)
		if len(key.Addresses) != 1 || key.Addresses[0] != "hostA("+aLana+")" {
			t.Fatalf("shown_addresses = %v, want [hostA(%s)]", key.Addresses, aLana)
		}

		r := opB.Call(t, "connect_broker", map[string]any{"address": key.Addresses[0], "key": key.Key})
		r.MustCode(t, "peer_unreachable")
		if !r.HasPrefix("Not connected") {
			t.Fatalf("the unreachable-address error %q does not start with Not connected", r.Text)
		}

		// The user hands over the address that works. The failed dial never
		// reached hostA, so the key is still usable.
		forwarded := net.JoinHostPort(c.Addr("router", "wan"), pairBrokerPort)
		pairConnect(t, opB, forwarded, key.Key)
		pairWaitOraclePeer(t, hostB, "hostA.peer", localpb.PeerRole_PEER_ROLE_DIALER, harness.EventTimeout)
	})

	t.Run("network outage", func(t *testing.T) {
		// The dialer reconnects on its own after the network comes back; no
		// tool or management call happens in between, only local-interface
		// probes.
		c := harness.Up(t, harness.TopoNAT)
		hostA := c.Node("hostA")
		hostB := c.Node("hostB")
		pairStartBroker(t, hostA, "")
		pairStartBroker(t, hostB, "")

		wsA := hostA.Workspace(t, "plain", "", "")
		wsB := hostB.Workspace(t, "plain", "", "")
		opA := pairOperator(t, hostA, wsA)
		defer opA.End(t)
		opB := pairOperator(t, hostB, wsB)
		defer opB.End(t)
		key := pairCreateKey(t, opA)
		pairConnect(t, opB, key.Addresses[0], key.Key)
		pairWaitOraclePeer(t, hostA, "hostB.peer", localpb.PeerRole_PEER_ROLE_ACCEPTOR, harness.EventTimeout)
		pairWaitOraclePeer(t, hostB, "hostA.peer", localpb.PeerRole_PEER_ROLE_DIALER, harness.EventTimeout)

		c.Cut(t, "hostA", "hostB")
		harness.Eventually(t, harness.EventTimeout, "both Brokers notice the cut", func() (bool, string) {
			statusA, state := pairOracleStatus(t, hostA, "hostB.peer")
			if state != "" {
				return false, "hostA: " + state
			}
			statusB, state := pairOracleStatus(t, hostB, "hostA.peer")
			if state != "" {
				return false, "hostB: " + state
			}
			if statusA == localpb.BrokerStatus_BROKER_STATUS_CONNECTED || statusB == localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, "a Broker still reports CONNECTED"
			}
			return true, ""
		})

		c.Restore(t)
		harness.Eventually(t, pairReconnectBound, "both Brokers reconnect without any tool call", func() (bool, string) {
			statusA, state := pairOracleStatus(t, hostA, "hostB.peer")
			if state != "" {
				return false, "hostA: " + state
			}
			statusB, state := pairOracleStatus(t, hostB, "hostA.peer")
			if state != "" {
				return false, "hostB: " + state
			}
			if statusA != localpb.BrokerStatus_BROKER_STATUS_CONNECTED || statusB != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, "both sides are not CONNECTED again"
			}
			return true, ""
		})
	})
}

// pairStartBroker starts a Broker on node with the given configuration and
// fails when it does not come up.
func pairStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// pairSession starts one simulated session with a real MCP server process.
func pairSession(t *testing.T, node *harness.Node, name, workdir string) *harness.Session {
	t.Helper()
	return node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, Workdir: workdir})
}

// pairOperator starts a session that never registers as an Agent: it carries
// no inbox socket, yet its MCP server process can call the management tools.
// It waits for the Broker connection before the caller's first tool call.
func pairOperator(t *testing.T, node *harness.Node, workdir string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true, Workdir: workdir})
	harness.WaitConnected(t, s, harness.EventTimeout)
	return s
}

// pairWaitRegistered waits until the session is registered and lists itself.
func pairWaitRegistered(t *testing.T, session *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "session "+name+" is registered", func(deadline time.Time) (bool, string) {
		out, state := pairListAgentsPoll(t, deadline, session)
		if state != "" {
			return false, state
		}
		if !strings.HasPrefix(out.Self, name+"@") {
			return false, "self_address is " + out.Self
		}
		return true, ""
	})
}

// pairCreateKey calls create_pairing_key and decodes the result.
func pairCreateKey(t *testing.T, session *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := session.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

// pairConnect calls connect_broker, requires success, and checks the alias.
func pairConnect(t *testing.T, session *harness.Session, address, key string) mcpserver.ConnectBrokerOut {
	t.Helper()
	return pairConnectWithAlias(t, session, address, key, "", "")
}

// pairConnectWithAlias calls connect_broker with an optional local alias and
// checks the alias the Broker reports.
func pairConnectWithAlias(t *testing.T, session *harness.Session, address, key, localAlias, wantAlias string) mcpserver.ConnectBrokerOut {
	t.Helper()
	args := map[string]any{"address": address, "key": key}
	if localAlias != "" {
		args["local_alias"] = localAlias
	}
	r := session.Call(t, "connect_broker", args)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	if wantAlias != "" && out.Alias != wantAlias {
		t.Fatalf("connect_broker alias = %q, want %q", out.Alias, wantAlias)
	}
	return out
}

// pairListBrokers calls list_brokers and returns the decoded answer.
func pairListBrokers(t *testing.T, session *harness.Session) (mcpserver.ListBrokersOut, string) {
	t.Helper()
	r := session.Call(t, "list_brokers", nil)
	if r.IsError {
		return mcpserver.ListBrokersOut{}, r.Text
	}
	out, err := harness.Decode[mcpserver.ListBrokersOut](string(r.Structured))
	if err != nil {
		return mcpserver.ListBrokersOut{}, "decoding list_brokers: " + err.Error()
	}
	return out, ""
}

// pairListBrokersPoll is pairListBrokers for a wait callback: the call is
// bounded by the wait's deadline and a transport error is returned as state.
func pairListBrokersPoll(t *testing.T, deadline time.Time, session *harness.Session) (mcpserver.ListBrokersOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListBrokersOut](t, deadline, session, "list_brokers", nil)
}

// pairListAgentsPoll calls list_agents from a wait and returns the decoded
// answer. The call is bounded by the wait's deadline and a transport error is
// returned as state.
func pairListAgentsPoll(t *testing.T, deadline time.Time, session *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, session, "list_agents", nil)
}

// pairJoin joins a topic, retrying while the session is still registering.
// join_topic's handler may spend topicRPCTimeout before the update hint runs,
// so the poll uses harness.TopicCallPollBound.
func pairJoin(t *testing.T, session *harness.Session, topic string) {
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

// pairBrokerByAlias returns the Broker entry with the given alias.
func pairBrokerByAlias(out mcpserver.ListBrokersOut, alias string) (mcpserver.BrokerOut, bool) {
	for _, broker := range out.Brokers {
		if broker.Alias == alias {
			return broker, true
		}
	}
	return mcpserver.BrokerOut{}, false
}

// pairSplitAddress splits "name@alias" into its two parts.
func pairSplitAddress(address string) (name, alias string) {
	at := strings.LastIndexByte(address, '@')
	if at < 0 {
		return address, ""
	}
	return address[:at], address[at+1:]
}

// pairIsFingerprint reports whether s is 64 lowercase hexadecimal characters.
func pairIsFingerprint(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// pairCheckOraclePeer checks one peer through the node's local interface, the
// read-only cross-check used alongside the tool results.
func pairCheckOraclePeer(t *testing.T, node *harness.Node, alias string, role localpb.PeerRole) {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("listing Brokers through the local interface: %s", detail.GetMessage())
	}
	for _, broker := range resp.GetBrokers() {
		if broker.GetAlias() != alias {
			continue
		}
		if !broker.GetIsPeer() {
			t.Fatalf("the local interface does not mark %s as a peer", alias)
		}
		if broker.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
			t.Fatalf("the local interface reports %s as %v, want CONNECTED", alias, broker.GetStatus())
		}
		if broker.GetRole() != role {
			t.Fatalf("the local interface reports role %v for %s, want %v", broker.GetRole(), alias, role)
		}
		return
	}
	t.Fatalf("the local interface does not list %s", alias)
}

// pairOracleBrokerCount counts the Brokers the local interface knows.
func pairOracleBrokerCount(t *testing.T, node *harness.Node) int {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("listing Brokers through the local interface: %s", detail.GetMessage())
	}
	return len(resp.GetBrokers())
}

// pairOracleStatus returns the status the local interface reports for alias.
// The second value is empty on success, "missing" when no such entry exists,
// and an error text otherwise.
func pairOracleStatus(t *testing.T, node *harness.Node, alias string) (localpb.BrokerStatus, string) {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		return localpb.BrokerStatus_BROKER_STATUS_UNSPECIFIED, detail.GetMessage()
	}
	for _, broker := range resp.GetBrokers() {
		if broker.GetAlias() == alias {
			return broker.GetStatus(), ""
		}
	}
	return localpb.BrokerStatus_BROKER_STATUS_UNSPECIFIED, "missing"
}

// pairWaitOraclePeer waits until the local interface lists alias with the link
// connected and this Broker's role as given.
func pairWaitOraclePeer(t *testing.T, node *harness.Node, alias string, role localpb.PeerRole, bound time.Duration) {
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
