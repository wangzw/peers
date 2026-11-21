//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/ctl"
	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	// secWaitBound is the bound of a wait that no product timer governs.
	secWaitBound = 3 * time.Second
	// secRelinkBound is reconnect_max (400ms) + dial_timeout (500ms) + 3s, the
	// bound the plan fixes for reconnecting under FastTimers.
	secRelinkBound = 3900 * time.Millisecond
	// secBrokerPort is the default Broker port these scenarios use.
	secBrokerPort = "17443"
	// secLeakWindow is a deliberate observation window for the negative
	// assertions: more than two address-check intervals (500ms each), so an
	// implementation that broadcasts private topic names on a periodic sync
	// has several chances to do so. The checks inside it make no product call
	// that expects a change.
	secLeakWindow = 1500 * time.Millisecond
	// secRelayStartBound bounds starting the relay through the node control
	// protocol. The request only binds a listener and dials on demand, so no
	// product timer governs it and the project-wide test wait applies.
	secRelayStartBound = 3 * time.Second
	// secCaptureWriteBound bounds the wait for tcpdump to write the bytes the
	// test has already sent. tcpdump flushes on packet activity and no product
	// timer governs it, so it uses the project-wide three-second bound.
	secCaptureWriteBound = 3 * time.Second
)

// secFingerprintPattern matches a stored peer fingerprint of the form the peer
// table writes. A failed pairing must leave no such value behind.
var secFingerprintPattern = regexp.MustCompile(`[0-9a-f]{64}`)

// TestMITM places a relay on the router between hostB and hostA. The relay
// terminates TLS with its own certificate and dials hostA with a second one;
// it therefore sees every frame while holding none of the pairing key. A
// pairing that succeeded through it would mean the authentication code does
// not bind both sides' certificate fingerprints.
func TestMITM(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoMITM)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	secStartBroker(t, aNode, "")
	secStartBroker(t, bNode, "")

	aWan := net.JoinHostPort(c.Addr("hostA", "wan"), secBrokerPort)

	// The node agent in the router runs the relay; it listens on the port the
	// REDIRECT rule sends hostB's connection to and dials hostA upstream.
	router := secStartRelayAgent(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), secRelayStartBound)
	defer cancel()
	if err := router.RelayStart(ctx, ctl.RelayStartRequest{Listen: ":" + secBrokerPort, Upstream: aWan}); err != nil {
		t.Fatalf("starting the relay on the router: %v", err)
	}

	opA := secOperator(t, aNode)
	defer opA.End(t)
	opB := secOperator(t, bNode)
	defer opB.End(t)

	t.Run("REQ-broker-pairing-012 man in the middle", func(t *testing.T) {
		key := secCreateKey(t, opA)
		address := secShownAddress(t, key.Addresses, aWan)
		// hostB dials the displayed address verbatim. The REDIRECT rule sends
		// that connection to the relay, whose certificate is not hostA's.
		r := secConnect(t, opB, address, key.Key)
		r.MustCode(t, "pairing_rejected")
		if !r.HasPrefix("Not connected") {
			t.Fatalf("the MITM pairing error %q does not start with Not connected", r.Text)
		}

		// The traffic really crossed the relay. A relay that never accepted a
		// stream would make the rejection above prove nothing about a MITM.
		harness.Eventually(t, secWaitBound, "the relay carried the pairing attempt in both directions", func() (bool, string) {
			stats, err := router.RelayStats(context.Background())
			if err != nil {
				return false, err.Error()
			}
			if stats.Streams < 1 {
				return false, fmt.Sprintf("the relay accepted %d streams", stats.Streams)
			}
			if stats.FramesUp == 0 || stats.FramesDown == 0 {
				return false, fmt.Sprintf("frames up %d, down %d", stats.FramesUp, stats.FramesDown)
			}
			return true, ""
		})

		// Neither side recorded anything: not the other Broker, not the relay
		// that actually presented a certificate.
		for name, node := range map[string]*harness.Node{"hostA": aNode, "hostB": bNode} {
			resp, detail := node.Oracle().ListBrokers(t)
			if detail != nil {
				t.Fatalf("%s: list_brokers through the local interface: %s", name, detail.GetMessage())
			}
			if len(resp.GetBrokers()) != 0 {
				t.Fatalf("%s knows %d Brokers after the rejected MITM pairing, want none", name, len(resp.GetBrokers()))
			}
			session := map[string]*harness.Session{"hostA": opA, "hostB": opB}[name]
			if out, state := secListBrokers(t, session); state != "" {
				t.Fatalf("%s: list_brokers through a session: %s", name, state)
			} else if len(out.Brokers) != 0 {
				t.Fatalf("%s lists %v after the rejected MITM pairing, want none", name, out.Brokers)
			}

			data := secPeersJSON(t, node)
			if matches := secFingerprintPattern.FindAll(data, -1); len(matches) > 0 {
				t.Fatalf("%s stored a peer fingerprint after the rejected pairing: %s", name, data)
			}
			for _, marker := range []string{"hostA.peer", "hostB.peer"} {
				if bytes.Contains(data, []byte(marker)) {
					t.Fatalf("%s stored %q in peers.json after the rejected pairing: %s", name, marker, data)
				}
			}
		}
	})
}

// TestLinkEncrypted captures the wire traffic between two paired Brokers and
// checks that it is TLS and that neither a session name, a topic name nor a
// message body appears in it in any common encoding.
func TestLinkEncrypted(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")

	// Capture returns only after tcpdump has attached, so the first frame is
	// never sent into a capture that is not running yet.
	stop := c.Capture(t, "hostA", 17443)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()

	secStartBroker(t, aNode, secConfig())
	secStartBroker(t, bNode, secConfig())

	opA := secOperator(t, aNode)
	defer opA.End(t)
	opB := secOperator(t, bNode)
	defer opB.End(t)

	t.Run("REQ-broker-pairing-013 encrypted", func(t *testing.T) {
		key := secCreateKey(t, opA)
		aWan := net.JoinHostPort(c.Addr("hostA", "wan"), secBrokerPort)
		secConnect(t, opB, secShownAddress(t, key.Addresses, aWan), key.Key).MustOK(t)
		secWaitConnected(t, bNode, "hostA.peer", secRelinkBound)
		secWaitConnected(t, aNode, "hostB.peer", secRelinkBound)

		token := secToken(t)
		nameA := "secret-agent-" + token
		nameB := "secret-agent-" + secToken(t)
		topic := "secret-topic-" + token
		body := "secret-body-" + token

		wsA := aNode.Workspace(t, "plain", "", "")
		wsB := bNode.Workspace(t, "plain", "", "")
		a1 := aNode.StartSession(t, harness.SessionSpec{Name: nameA, StartMCP: true, Workdir: wsA})
		defer a1.End(t)
		b1 := bNode.StartSession(t, harness.SessionSpec{Name: nameB, StartMCP: true, Workdir: wsB})
		defer b1.End(t)
		secWaitRegistered(t, a1, nameA)
		secWaitRegistered(t, b1, nameB)
		secJoin(t, a1, topic)
		secJoin(t, b1, topic)

		// Both sessions must be visible to each other before a message can be
		// addressed; this also proves the name travelled over the link.
		harness.EventuallyPoll(t, secWaitBound, "the two sessions see each other", func(deadline time.Time) (bool, string) {
			out, state := secListAgentsPoll(t, deadline, a1)
			if state != "" {
				return false, state
			}
			if _, ok := secFindAgent(out, nameB); !ok {
				return false, nameB + " is not listed yet"
			}
			return true, ""
		})
		addressB := secAgentAddress(t, a1, nameB)

		a1.Call(t, "send_message", map[string]any{"to": addressB, "message": body}).MustOK(t)
		frame := b1.WaitFrame(t, secWaitBound)
		if content := secEnvelopeBody(t, frame.Content); content != body {
			t.Fatalf("hostB received body %q, want %q", content, body)
		}

		// The reply keeps application data flowing in the other direction too.
		b1.Call(t, "send_message", map[string]any{"to": frame.FromName, "message": body}).MustOK(t)
		reply := a1.WaitFrame(t, secWaitBound)
		if content := secEnvelopeBody(t, reply.Content); content != body {
			t.Fatalf("hostA received reply body %q, want %q", content, body)
		}

		// tcpdump services its ring on a timer and loses queued packets when
		// it is interrupted, so wait until it has actually written the
		// capture instead of stopping right after the last frame.
		secWaitCaptureBytes(t, c, "hostA", 17443, 2048)
		stopped = true
		pcap := stop()
		if len(pcap) <= 2048 {
			t.Fatalf("the capture is %d bytes, want more than 2048", len(pcap))
		}
		if !bytes.Contains(pcap, []byte{0x17, 0x03, 0x03}) {
			t.Fatal("the capture contains no TLS application-data record header (17 03 03)")
		}
		for _, marker := range []string{nameA, nameB, topic, body} {
			for _, form := range secForbiddenForms(marker) {
				if bytes.Contains(pcap, form) {
					t.Fatalf("the capture contains %q (one of its plain, base64 or UTF-16 forms)", marker)
				}
			}
		}
	})
}

// TestPrivateTopicNamesStayHome checks that a topic without a member on the
// peer's side does not reach that peer at all: neither its name nor the list
// of its members appears in any tool result, the status output, the run log or
// the audit file. Once the peer joins the same topic, the member appears.
func TestPrivateTopicNamesStayHome(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	secStartBroker(t, aNode, secConfig())
	// B runs at debug level so a roster frame the topic Registry ignores or
	// drops leaves a record. A leaked roster would otherwise be invisible here.
	secStartBroker(t, bNode, secConfig("log:\n  level: debug\n"))

	opA := secOperator(t, aNode)
	defer opA.End(t)
	opB := secOperator(t, bNode)
	defer opB.End(t)

	key := secCreateKey(t, opA)
	aWan := net.JoinHostPort(c.Addr("hostA", "wan"), secBrokerPort)
	secConnect(t, opB, secShownAddress(t, key.Addresses, aWan), key.Key).MustOK(t)
	secWaitConnected(t, bNode, "hostA.peer", secRelinkBound)
	secWaitConnected(t, aNode, "hostB.peer", secRelinkBound)

	token := secToken(t)
	workerName := "api-worker-" + token
	secretTopic := "secret-x-" + token
	observerName := "b-observer-" + token
	openTopic := "open-" + token

	worker := aNode.StartSession(t, harness.SessionSpec{Name: workerName, StartMCP: true})
	defer worker.End(t)
	secWaitRegistered(t, worker, workerName)

	observer := bNode.StartSession(t, harness.SessionSpec{Name: observerName, StartMCP: true})
	defer observer.End(t)
	secWaitRegistered(t, observer, observerName)
	secJoin(t, observer, openTopic)

	// B has no member of secretTopic. Its Operator and its registered session
	// together exercise every read-only tool; an implementation that broadcasts
	// topic names or member lists to every peer fails here.
	assertQuiet := func(t *testing.T) {
		t.Helper()
		for name, session := range map[string]*harness.Session{"operator": opB, "session": observer} {
			for _, tool := range secReadOnlyTools(name) {
				r := session.Call(t, tool, nil)
				if r.IsError {
					t.Fatalf("%s: %s failed: %s", name, tool, r.Text)
				}
				for _, marker := range []string{secretTopic, workerName} {
					if strings.Contains(r.Text, marker) || strings.Contains(string(r.Structured), marker) {
						t.Fatalf("hostB's %s result for %s contains %q: %s", name, tool, marker, r.Text)
					}
				}
			}
		}
	}

	t.Run("REQ-topics-009 private topic name", func(t *testing.T) {
		// The window opens with the join that creates the private topic, so a
		// roster frame produced by that join is inside it. The log offset taken
		// here turns every record appended afterwards into evidence.
		aFingerprint := alIdentityFingerprint(t, aNode)
		logStart := len(bNode.BrokerLog(t))
		secJoin(t, worker, secretTopic)

		// Negative observations run over a window rather than a single sample:
		// a periodic sync can leak on any later round.
		harness.Consistently(t, secLeakWindow, "hostB never learns the private topic name or member", func() (bool, string) {
			for name, session := range map[string]*harness.Session{"operator": opB, "session": observer} {
				for _, tool := range secReadOnlyTools(name) {
					r := session.Call(t, tool, nil)
					if r.IsError {
						return false, name + ": " + tool + ": " + r.Text
					}
					for _, marker := range []string{secretTopic, workerName} {
						if strings.Contains(r.Text, marker) || strings.Contains(string(r.Structured), marker) {
							return false, fmt.Sprintf("%s: %s leaks %q", name, tool, marker)
						}
					}
				}
			}
			return true, ""
		})

		// A roster or roster_delta frame from A that B had to ignore or drop is
		// the wire-level form of the leak: the topic was not in the
		// intersection, so its name had no business leaving A.
		brokerLog := bNode.BrokerLog(t)
		secAssertNoRosterFrames(t, brokerLog[logStart:], aFingerprint)

		// Positive control for the log checks below: the same log records B's
		// own session name, so a log that is empty or unreadable fails here
		// instead of passing the absence checks because it was never read.
		if !strings.Contains(string(brokerLog), observerName) {
			t.Fatalf("hostB's Broker log does not record its own session %q; the absence checks are blind", observerName)
		}

		status := bNode.Exec(t, []string{"/usr/local/bin/peers", "service", "status"}, nil)
		if status.ExitCode != 0 {
			t.Fatalf("peers service status on hostB exited %d: %s", status.ExitCode, status.Stderr)
		}
		secAssertAbsent(t, "hostB status output", status.Stdout+status.Stderr, secretTopic, workerName)
		secAssertAbsent(t, "hostB Broker log", string(brokerLog), secretTopic, workerName)
		secAssertAbsent(t, "hostB audit log", string(secReadIfExists(t, bNode, "/home/peer/.peers/audit.log")), secretTopic, workerName)
	})

	t.Run("REQ-topics-014 member list", func(t *testing.T) {
		// The member is still hidden while B has no membership in the topic.
		assertQuiet(t)

		secJoin(t, observer, secretTopic)
		harness.EventuallyPoll(t, secWaitBound, "hostB sees the member after joining the topic", func(deadline time.Time) (bool, string) {
			out, state := secListAgentsPoll(t, deadline, observer)
			if state != "" {
				return false, state
			}
			entry, ok := secFindAgent(out, workerName)
			if !ok {
				return false, workerName + " is not listed"
			}
			want := workerName + "@hostA.peer"
			if entry.Address != want {
				return false, fmt.Sprintf("the member address is %q, want %q", entry.Address, want)
			}
			return true, ""
		})
	})
}

// TestEchoedTopicDigestsRevealNothing proves REQ-topics-014 against a real
// Broker: a peer that echoes a Broker's own TopicDigests, then forges a Roster
// under an echoed digest, learns and injects nothing. A digest binds the
// sender's fingerprint (ADR-salted-topic-digest), so one direction's digest
// never matches the other and the echoed set intersects nothing. A fresh local
// topic then provides a fence: its Roster can only come from the peer's own
// digest set, so every Roster received before it must be the shared-t one.
func TestEchoedTopicDigestsRevealNothing(t *testing.T) {
	t.Parallel()

	a := discNewNode(t, "digest-a", "log:\n  level: debug\n", false)

	token := secToken(t)
	workerName := "echo-worker-" + token
	forgedName := "intruder-" + token
	const (
		secretTopic = "secret-x"
		sharedTopic = "shared-t"
		freshTopic  = "fresh-t"
	)

	worker := discAgent(t, a, workerName, t.TempDir())
	defer worker.End(t)
	secWaitRegistered(t, worker, workerName)
	secJoin(t, worker, secretTopic)
	secJoin(t, worker, sharedTopic)

	// The window opens before pairing, so every frame the attack interaction
	// produces lands in the log tail below.
	logStart := len(a.BrokerLog(t))

	f := startFakePeer(t)
	f.Pair(t, a.addr, hostilePairKey(t, a))
	f.WaitLinked(t, secWaitBound)

	t.Run("REQ-topics-014 echoed digests", func(t *testing.T) {
		// On link up A sends one digest per local topic. Only the worker holds
		// any, and it holds exactly the two private topics.
		digests := f.TopicDigests(t)
		if len(digests.Digests) != 2 {
			t.Fatalf("A sent %d topic digests, want 2 (secret-x and shared-t)", len(digests.Digests))
		}

		// Echo A's set verbatim, then forge a Roster under one of the echoed
		// digests while that set is still A's latest. A knows the digest, so the
		// frame reaches the direction check instead of the branch that drops a
		// digest outside the latest set.
		f.EchoDigests(t)
		f.SendRoster(t, digests.Digests[0], fakePeerWireMember{Name: forgedName, Ref: "abcdef"})

		// The peer then sends its own direction for shared-t. A answers with a
		// Roster under the digest it sends for shared-t, which differs from
		// every echoed digest.
		sent := f.SendTopicDigests(t, sharedTopic)
		if len(sent.Want) != 1 {
			t.Fatalf("digests_sent carried %d expected digests, want 1", len(sent.Want))
		}
		sharedDigest := sent.Want[0]

		// Joining a fresh topic adds a digest the echo never carried. When the
		// peer claims that topic, A's Roster under its digest is a fence the
		// echo cannot produce, so every Roster received before it has already
		// been recorded. The fence requires each of them to be the shared-t
		// Roster; anything else is the echoed set matching secret-x.
		secJoin(t, worker, freshTopic)
		f.TopicDigests(t)
		fresh := f.SendTopicDigests(t, freshTopic)
		if len(fresh.Want) != 1 {
			t.Fatalf("digests_sent carried %d expected digests, want 1", len(fresh.Want))
		}
		f.WaitTopicRosterFence(t, fresh.Want[0], sharedDigest, secWaitBound)

		out, state := secListAgents(t, worker)
		if state != "" {
			t.Fatalf("list_agents on A: %s", state)
		}
		if entry, ok := secFindAgent(out, forgedName); ok {
			t.Fatalf("A lists the peer's forged member %q at %q", forgedName, entry.Address)
		}

		// A never writes a private topic's name or a member name from a dropped
		// frame to its own log or audit.
		secAssertAbsent(t, "hostA Broker log", string(a.BrokerLog(t)[logStart:]), secretTopic, forgedName)
		audit := secReadIfExists(t, a.Node, filepath.Join(a.AccountHome(), ".peers", "audit.log"))
		secAssertAbsent(t, "hostA audit log", string(audit), secretTopic, forgedName)
	})
}

// secStartRelayAgent starts a second node-agent process on the router and
// returns a client for it.
//
// The relay's gRPC server uses credentials.NewTLS, and grpc-go v1.85 enforces
// ALPN on such a server by default, while the product's Link client completes
// TLS without advertising h2. A relay served by grpc's TLS credentials
// therefore closes the connection after the handshake and never creates a
// stream. The relay is test infrastructure and must accept the product client
// as it is, so this helper starts the agent with ALPN enforcement disabled.
// The alternative, changing the relay's credentials, is a harness change and
// belongs to that work item; nothing outside this test file is modified.
func secStartRelayAgent(t *testing.T, c *harness.Cluster) *ctl.Client {
	t.Helper()
	ctlDir := c.CTLDir(t, "router")
	const socket = "/ctl/relay/ctl.sock"
	script := "install -d -o peer -g peer /ctl/relay && " +
		"setsid setpriv --reuid=peer --regid=peer --init-groups env GRPC_ENFORCE_ALPN_ENABLED=false " +
		"/usr/local/bin/e2e-node serve --ctl " + socket + " --home /home/peer --bin /usr/local/bin/peers " +
		">/dev/null 2>&1 </dev/null &"
	c.Root(t, "router", "sh", "-c", script)

	client := ctl.Dial(filepath.Join(ctlDir, "relay", "ctl.sock"))
	t.Cleanup(client.Close)
	harness.Eventually(t, secWaitBound, "the relay node agent answers", func() (bool, string) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := client.Procs(ctx, ctl.ProcsRequest{Kind: "all"}); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
	return client
}

// secReadOnlyTools returns the read-only tools a session may call. An
// Operator never registers as an Agent, so the tools that require an Agent
// identity are unavailable to it; list_brokers is the one it can use.
func secReadOnlyTools(sessionName string) []string {
	if sessionName == "operator" {
		return []string{"list_brokers"}
	}
	return []string{"list_agents", "list_topics", "list_brokers"}
}

// secWaitCaptureBytes waits until tcpdump has written more than want bytes to
// the capture file. A tcpdump that has received packets may still hold them in
// its ring; this wait makes the stop deterministic.
func secWaitCaptureBytes(t *testing.T, c *harness.Cluster, node string, port, want int) []byte {
	t.Helper()
	path := fmt.Sprintf("/tmp/e2e-capture-%d.pcap", port)
	var data []byte
	harness.Eventually(t, secCaptureWriteBound, fmt.Sprintf("tcpdump on %s writes more than %d bytes", node, want), func() (bool, string) {
		data = c.Node(node).ReadFile(t, path)
		if len(data) > want {
			return true, ""
		}
		return false, fmt.Sprintf("the capture file is %d bytes", len(data))
	})
	return data
}

// secStartBroker starts a Broker with config and fails when it does not come
// up.
func secStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// secConfig merges the shortened timers with the given fragments.
func secConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

// secOperator starts an MCP server process without an inbox socket, which
// never registers as an Agent but can call the management tools.
func secOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, secWaitBound)
	return s
}

// secWaitRegistered waits until the session is registered and lists itself.
func secWaitRegistered(t *testing.T, session *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, secWaitBound, "session "+name+" is registered", func(deadline time.Time) (bool, string) {
		out, state := secListAgentsPoll(t, deadline, session)
		if state != "" {
			return false, state
		}
		if !strings.HasPrefix(out.Self, name+"@") {
			return false, "self_address is " + out.Self
		}
		return true, ""
	})
}

// secCreateKey calls create_pairing_key and decodes the result.
func secCreateKey(t *testing.T, session *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := session.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

// secConnect calls connect_broker.
func secConnect(t *testing.T, session *harness.Session, address, key string) harness.Result {
	t.Helper()
	return session.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
}

// secWaitConnected waits until the local interface reports alias connected.
func secWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the local interface reports "+alias+" connected", func() (bool, string) {
		resp, detail := node.Oracle().ListBrokers(t)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, broker := range resp.GetBrokers() {
			if broker.GetAlias() == alias {
				if broker.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
					return false, "status is not CONNECTED yet"
				}
				return true, ""
			}
		}
		return false, "the peer is not listed"
	})
}

// secJoin joins a topic, retrying while the session is still registering.
// join_topic's handler may spend topicRPCTimeout before the update hint runs,
// so the poll uses harness.TopicCallPollBound.
func secJoin(t *testing.T, session *harness.Session, topic string) {
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

// secListAgents calls list_agents and returns the decoded answer.
func secListAgents(t *testing.T, session *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	r := session.Call(t, "list_agents", nil)
	if r.IsError {
		return mcpserver.ListAgentsOut{}, r.Text
	}
	out, err := harness.Decode[mcpserver.ListAgentsOut](string(r.Structured))
	if err != nil {
		return mcpserver.ListAgentsOut{}, "decoding list_agents: " + err.Error()
	}
	return out, ""
}

// secListAgentsPoll is secListAgents for a wait callback: the call is bounded
// by the wait's deadline and a transport error is returned as state.
func secListAgentsPoll(t *testing.T, deadline time.Time, session *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, session, "list_agents", nil)
}

// secListBrokers calls list_brokers and returns the decoded answer.
func secListBrokers(t *testing.T, session *harness.Session) (mcpserver.ListBrokersOut, string) {
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

// secFindAgent returns the listed entry named name.
func secFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if secAgentName(agent.Address) == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

// secAgentAddress returns the full address of the listed agent named name.
func secAgentAddress(t *testing.T, session *harness.Session, name string) string {
	t.Helper()
	out, state := secListAgents(t, session)
	if state != "" {
		t.Fatalf("list_agents: %s", state)
	}
	agent, ok := secFindAgent(out, name)
	if !ok {
		t.Fatalf("agent %s is not listed", name)
	}
	return agent.Address
}

// secAgentName returns the name part of an address, before "@".
func secAgentName(address string) string {
	if at := strings.LastIndexByte(address, '@'); at >= 0 {
		return address[:at]
	}
	return address
}

// secShownAddress returns the shown address that carries wantAddr. The shown
// form wraps the address in parentheses after the hostname, for example
// "hostA(10.0.0.2:17443)".
func secShownAddress(t *testing.T, shown []string, wantAddr string) string {
	t.Helper()
	for _, address := range shown {
		if strings.Contains(address, wantAddr) {
			return address
		}
	}
	t.Fatalf("create_pairing_key returned %v, want one entry containing %s", shown, wantAddr)
	return ""
}

// secEnvelopeBody returns the message body from the attribution envelope the
// transport wraps it in.
func secEnvelopeBody(t *testing.T, content string) string {
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

// secPeersJSON returns the peer table's bytes, or nil when it does not exist.
func secPeersJSON(t *testing.T, node *harness.Node) []byte {
	t.Helper()
	return secReadIfExists(t, node, filepath.Join(node.AccountHome(), ".peers", "peers.json"))
}

// secReadIfExists reads path on node and returns nil when it is absent.
func secReadIfExists(t *testing.T, node *harness.Node, path string) []byte {
	t.Helper()
	res := node.Exec(t, []string{"sh", "-c", "if [ -f " + shellQuote(path) + " ]; then cat " + shellQuote(path) + "; fi"}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("reading %s: exit %d: %s", path, res.ExitCode, res.Stderr)
	}
	return []byte(res.Stdout)
}

// secAssertAbsent fails when any marker appears in what.
func secAssertAbsent(t *testing.T, what, haystack string, markers ...string) {
	t.Helper()
	for _, marker := range markers {
		if strings.Contains(haystack, marker) {
			t.Fatalf("%s contains %q: %s", what, marker, haystack)
		}
	}
}

// secAssertNoRosterFrames fails when the Broker log holds a record of a roster
// or roster_delta frame from peer that the topic Registry ignored or dropped.
// The kinds are the Registry's wire kinds: 201 is Roster and 202 RosterDelta.
// Records from other peers, other kinds, and non-JSON lines are skipped, so a
// truncated tail cannot fail the test on its own.
func secAssertNoRosterFrames(t *testing.T, log []byte, peer string) {
	t.Helper()
	for _, line := range bytes.Split(log, []byte("\n")) {
		var rec struct {
			Event  string `json:"event"`
			Peer   string `json:"peer"`
			Kind   int    `json:"kind"`
			Reason string `json:"reason"`
		}
		if json.Unmarshal(line, &rec) != nil {
			continue
		}
		if rec.Peer != peer || (rec.Kind != 201 && rec.Kind != 202) {
			continue
		}
		switch rec.Event {
		case "topic.frame_ignored", "topic.frame_dropped", "topic.member_dropped":
			t.Fatalf("hostB logged %s for a %d frame from hostA (reason %q): %s",
				rec.Event, rec.Kind, rec.Reason, line)
		}
	}
}

// secForbiddenForms returns the encodings of s that a capture must not
// contain: UTF-8, standard and raw base64, and UTF-16 in both byte orders.
func secForbiddenForms(s string) [][]byte {
	forms := [][]byte{
		[]byte(s),
		[]byte(base64.StdEncoding.EncodeToString([]byte(s))),
		[]byte(base64.RawStdEncoding.EncodeToString([]byte(s))),
	}
	units := utf16.Encode([]rune(s))
	le := make([]byte, 2*len(units))
	be := make([]byte, 2*len(units))
	for i, unit := range units {
		le[2*i] = byte(unit)
		le[2*i+1] = byte(unit >> 8)
		be[2*i] = byte(unit >> 8)
		be[2*i+1] = byte(unit)
	}
	return append(forms, le, be)
}

// secToken returns a short random lowercase hex token that makes a marker
// unique to one run.
func secToken(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generating a random token: %v", err)
	}
	return hex.EncodeToString(b[:])
}
