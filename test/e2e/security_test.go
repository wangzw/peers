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
	secWaitBound   = harness.EventTimeout
	secRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout
	secBrokerPort  = "17443"
	// secLeakWindow is a deliberate observation window for the negative
	// assertions: more than two address-check intervals (500ms each), so an
	// implementation that broadcasts private topic names on a periodic sync
	// has several chances to do so.
	secLeakWindow        = 1500 * time.Millisecond
	secRelayStartBound   = harness.EventTimeout
	secCaptureWriteBound = harness.EventTimeout
)

var secFingerprintPattern = regexp.MustCompile(`[0-9a-f]{64}`)

// TestMITM places a TLS-terminating relay between two hosts and checks that a
// pairing through it is rejected.
func TestMITM(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoMITM)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	secStartBroker(t, aNode, "")
	secStartBroker(t, bNode, "")

	aWan := net.JoinHostPort(c.Addr("hostA", "wan"), secBrokerPort)

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
		r := secConnect(t, opB, address, key.Key)
		r.MustCode(t, "pairing_rejected")
		if !r.HasPrefix("Not connected") {
			t.Fatalf("the MITM pairing error %q does not start with Not connected", r.Text)
		}

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

// TestLinkEncrypted checks that traffic between two paired Brokers is TLS and
// carries no session name, topic name or message body in a common encoding.
func TestLinkEncrypted(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")

	// Capture returns only after tcpdump has attached, so no frame is sent into
	// a capture that is not running yet.
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

		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the two sessions see each other", func(deadline time.Time) (bool, string) {
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

		b1.Call(t, "send_message", map[string]any{"to": frame.FromName, "message": body}).MustOK(t)
		reply := a1.WaitFrame(t, secWaitBound)
		if content := secEnvelopeBody(t, reply.Content); content != body {
			t.Fatalf("hostA received reply body %q, want %q", content, body)
		}

		// tcpdump loses queued packets when interrupted, so wait until it has
		// written the capture before stopping it.
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

// TestPrivateTopicNamesStayHome checks that a topic with no member on the
// peer's side does not reach it in tool results, the status output, the run log
// or the audit file.
func TestPrivateTopicNamesStayHome(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	secStartBroker(t, aNode, secConfig())
	// B runs at debug level so a dropped roster frame leaves a record.
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
		aFingerprint := alIdentityFingerprint(t, aNode)
		// The window opens with the join that creates the private topic, so a
		// roster frame produced by that join is inside it; the offset taken
		// here turns every record appended afterwards into evidence.
		logStart := len(bNode.BrokerLog(t))
		secJoin(t, worker, secretTopic)

		harness.ConsistentlyPoll(t, secLeakWindow, harness.ListAgentsPollBound, "hostB never learns the private topic name or member", nil, func(deadline time.Time) harness.Observation {
			for name, session := range map[string]*harness.Session{"operator": opB, "session": observer} {
				for _, tool := range secReadOnlyTools(name) {
					r, err := session.PollCall(t, deadline, tool, nil)
					if err != nil {
						return harness.Unobservable(name + ": " + tool + ": " + err.Error())
					}
					if r.IsError {
						return harness.Observed(false, name+": "+tool+": "+r.Text)
					}
					for _, marker := range []string{secretTopic, workerName} {
						if strings.Contains(r.Text, marker) || strings.Contains(string(r.Structured), marker) {
							return harness.Observed(false, fmt.Sprintf("%s: %s leaks %q", name, tool, marker))
						}
					}
				}
			}
			return harness.Observed(true, "")
		})

		brokerLog := bNode.BrokerLog(t)
		secAssertNoRosterFrames(t, brokerLog[logStart:], aFingerprint)

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
		assertQuiet(t)

		secJoin(t, observer, secretTopic)
		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "hostB sees the member after joining the topic", func(deadline time.Time) (bool, string) {
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

// TestEchoedTopicDigestsRevealNothing checks that a peer that echoes a Broker's
// own TopicDigests and forges a Roster under an echoed digest learns and injects
// nothing.
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
		digests := f.TopicDigests(t)
		if len(digests.Digests) != 2 {
			t.Fatalf("A sent %d topic digests, want 2 (secret-x and shared-t)", len(digests.Digests))
		}

		// Echo A's set verbatim, then forge a Roster under one of the echoed
		// digests while that set is still A's latest: A knows the digest, so
		// the frame reaches the direction check instead of the branch that
		// drops a digest outside the latest set.
		f.EchoDigests(t)
		f.SendRoster(t, digests.Digests[0], fakePeerWireMember{Name: forgedName, Ref: "abcdef"})

		sent := f.SendTopicDigests(t, sharedTopic)
		if len(sent.Want) != 1 {
			t.Fatalf("digests_sent carried %d expected digests, want 1", len(sent.Want))
		}
		sharedDigest := sent.Want[0]

		// Joining a fresh topic adds a digest the echo never carried: A's
		// Roster under it is a fence the echo cannot produce, and frames are
		// ordered, so every Roster before the fence has arrived and must all
		// be shared-t's.
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

		secAssertAbsent(t, "hostA Broker log", string(a.BrokerLog(t)[logStart:]), secretTopic, forgedName)
		audit := secReadIfExists(t, a.Node, filepath.Join(a.AccountHome(), ".peers", "audit.log"))
		secAssertAbsent(t, "hostA audit log", string(audit), secretTopic, forgedName)
	})
}

// secStartRelayAgent disables ALPN because grpc-go v1.85 enforces it on a
// server built from credentials.NewTLS, while the product's Link client
// completes TLS without advertising h2.
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

func secReadOnlyTools(sessionName string) []string {
	if sessionName == "operator" {
		return []string{"list_brokers"}
	}
	return []string{"list_agents", "list_topics", "list_brokers"}
}

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

func secStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

func secConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

func secOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, secWaitBound)
	return s
}

func secWaitRegistered(t *testing.T, session *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "session "+name+" is registered", func(deadline time.Time) (bool, string) {
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

func secConnect(t *testing.T, session *harness.Session, address, key string) harness.Result {
	t.Helper()
	return session.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
}

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

// secJoin retries while the session is still registering. The poll needs
// harness.TopicCallPollBound because join_topic's handler may spend
// topicRPCTimeout before the update hint runs.
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

func secListAgentsPoll(t *testing.T, deadline time.Time, session *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, session, "list_agents", nil)
}

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

func secFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if secAgentName(agent.Address) == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

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

func secAgentName(address string) string {
	if at := strings.LastIndexByte(address, '@'); at >= 0 {
		return address[:at]
	}
	return address
}

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

func secPeersJSON(t *testing.T, node *harness.Node) []byte {
	t.Helper()
	return secReadIfExists(t, node, filepath.Join(node.AccountHome(), ".peers", "peers.json"))
}

func secReadIfExists(t *testing.T, node *harness.Node, path string) []byte {
	t.Helper()
	res := node.Exec(t, []string{"sh", "-c", "if [ -f " + shellQuote(path) + " ]; then cat " + shellQuote(path) + "; fi"}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("reading %s: exit %d: %s", path, res.ExitCode, res.Stderr)
	}
	return []byte(res.Stdout)
}

func secAssertAbsent(t *testing.T, what, haystack string, markers ...string) {
	t.Helper()
	for _, marker := range markers {
		if strings.Contains(haystack, marker) {
			t.Fatalf("%s contains %q: %s", what, marker, haystack)
		}
	}
}

// secAssertNoRosterFrames checks frames from peer; the Registry's wire kinds
// are 201 for Roster and 202 for RosterDelta.
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

// TestREQBrokerConfig069_RejectedConnectionsMergeBySource checks that unpaired
// dials with fresh certificates merge into one connection_rejected audit record
// keyed by network source.
func TestREQBrokerConfig069_RejectedConnectionsMergeBySource(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	host := c.Node("hostA")
	secStartBroker(t, host, "")

	addr := net.JoinHostPort("127.0.0.1", secBrokerPort)
	since := time.Now()
	const probes = 5
	for index := 0; index < probes; index++ {
		res := host.Exec(t, []string{"/usr/local/bin/e2e-node", "probe-hello", "--addr", addr}, nil)
		if res.ExitCode != 0 {
			t.Fatalf("probe %d exited %d: %s", index, res.ExitCode, strings.TrimSpace(res.Stderr))
		}
	}

	var rejected *localpb.AuditEvent
	harness.Eventually(t, secWaitBound, "one merged connection_rejected record", func() (bool, string) {
		events, detail := host.Oracle().Audit(t, since, 0)
		if detail != nil {
			return false, detail.GetMessage()
		}
		var found *localpb.AuditEvent
		total := 0
		for _, event := range events {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONNECTION_REJECTED {
				total++
				found = event
			}
		}
		if total != 1 {
			return false, fmt.Sprintf("%d connection_rejected records", total)
		}
		if found.GetCount() != probes {
			return false, fmt.Sprintf("the record counts %d attempts", found.GetCount())
		}
		rejected = found
		return true, ""
	})

	if got := rejected.GetDetail(); got != "not a peer from 127.0.0.1/32" {
		t.Errorf("audit detail = %q, want %q", got, "not a peer from 127.0.0.1/32")
	}
	if rejected.GetPeer() == nil {
		t.Error("the merged record carries no peer fingerprint")
	}
}

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

func secToken(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generating a random token: %v", err)
	}
	return hex.EncodeToString(b[:])
}
