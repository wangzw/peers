//go:build e2e

package e2e

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/ctl"
	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	lkRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout
	lkWaitBound   = harness.EventTimeout

	// lkNoRedialWindow is five reconnect intervals (5 * 400ms): a Broker that
	// keeps redialing makes at least four attempts inside it, while a stopped
	// one makes none.
	lkNoRedialWindow = 2000 * time.Millisecond
	// lkBrokerIdleWindow is also five reconnect intervals, forty harness polls
	// at 50ms: a Broker that would exit with its last client is seen still
	// alive, and B keeps seeing the link connected.
	lkBrokerIdleWindow = 2000 * time.Millisecond
	// lkRelinkSlack is the extra second when a link takes one more hop; no
	// product timer governs it.
	lkRelinkSlack = time.Second
	// lkRequirementBound is the 120-second upper bound asserted on a relink.
	// The waits use the shorter lkRelinkBound as their fallback, so the
	// fallback never enters the assertion.
	lkRequirementBound = 120 * time.Second
	lkRequirementSlack = time.Second
)

const lkTopic = "t"

// TestLinks covers link survival, redial after a cut, the acceptor never
// dialing, restart recovery, address-list changes, unpairing and the Broker
// states list_brokers reports.
func TestLinks(t *testing.T) {
	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	lkStartBroker(t, aNode, lkConfig())
	lkStartBroker(t, bNode, lkConfig())
	opA, opB := lkOperator(t, aNode), lkOperator(t, bNode)

	// Port 0 counts every destination port, and the counter is installed
	// before pairing so a dial during setup is counted.
	outSYN := c.SYNCounter(t, "hostA", harness.Outbound, "", 0)
	bSYN := c.SYNCounter(t, "hostB", harness.Outbound, "", 0)

	a1 := lkSession(t, aNode, "a1")
	b1 := lkSession(t, bNode, "b1")
	var (
		a1Ended bool
		b1Ended bool
	)
	defer func() {
		if !a1Ended {
			a1.End(t)
		}
		if !b1Ended {
			b1.End(t)
		}
	}()

	addrA := lkAddr(c, "hostA", "wan")
	key := lkCreateKey(t, opA)
	lkConnect(t, opB, addrA, key.Key).MustOK(t)
	lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
	lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)

	t.Run("REQ-broker-pairing-016 relink without key", func(t *testing.T) {
		lkCutAndWaitDown(t, c, aNode, bNode)
		c.Restore(t)
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)

		r := lkConnect(t, opB, addrA, key.Key)
		r.MustCode(t, "pairing_rejected")
		if !r.HasPrefix("Not connected") {
			t.Fatalf("the reused key error is %q, want it to start with Not connected", r.Text)
		}
	})

	t.Run("REQ-broker-pairing-018 redial after restore", func(t *testing.T) {
		lkCutAndWaitDown(t, c, aNode, bNode)
		c.Restore(t)
		// Start the clock after Restore returns: deleting the rules is the last
		// part of the outage, and charging it as rebuild time would blame the
		// product for the harness's own work.
		start := time.Now()
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)
		if elapsed := time.Since(start); elapsed > lkRequirementBound+lkRequirementSlack {
			t.Fatalf("the link took %s to rebuild, want at most %s (REQ-broker-pairing-018 %s plus %s observation margin)",
				elapsed, lkRequirementBound+lkRequirementSlack, lkRequirementBound, lkRequirementSlack)
		}
	})

	t.Run("REQ-broker-pairing-027 acceptor never dials", func(t *testing.T) {
		if got := bSYN(); got == 0 {
			t.Fatal("B's outbound SYN counter read zero although B dialed A; the counter is blind")
		}

		// The cut and restore cycles above must already show A never dialing:
		// outSYN counts from startup, so this subtest cannot run alone or move
		// earlier.
		if got := outSYN(); got != 0 {
			t.Fatalf("A sent %d outbound SYNs while it was only the acceptor", got)
		}

		undo := c.Block(t, "hostB", c.Addr("hostA", "wan"))
		t.Cleanup(undo)
		lkCutAndWaitDown(t, c, aNode, bNode)
		c.Restore(t)

		harness.Consistently(t, lkNoRedialWindow, "the link stays down and A never dials", func() (bool, string) {
			if got := outSYN(); got != 0 {
				return false, fmt.Sprintf("A sent %d outbound SYNs", got)
			}
			if lkStatus(t, bNode, "hostA.peer") == localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, "B considers the link connected although its path is blocked"
			}
			return true, ""
		})
		undo()
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
	})

	t.Run("REQ-broker-config-014 broker outlives last session", func(t *testing.T) {
		a1.End(t)
		a1Ended = true
		b1.End(t)
		b1Ended = true
		// End the operator sessions too, so no tool client of either Broker
		// remains while the window observes the idle Brokers.
		opA.End(t)
		opB.End(t)

		harness.Consistently(t, lkBrokerIdleWindow, "A's Broker outlives its last session and B still sees it connected", func() (bool, string) {
			if got := lkBrokerProcesses(t, c, "hostA"); got < 1 {
				return false, fmt.Sprintf("A has %d Broker processes after both sessions ended", got)
			}
			if got := lkStatus(t, bNode, "hostA.peer"); got != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, fmt.Sprintf("B sees A as %s", got)
			}
			return true, ""
		})
	})

	t.Run("REQ-broker-pairing-017 survives restart", func(t *testing.T) {
		lkRestart(t, c, "hostA", lkConfig(), opA)
		opA = lkOperator(t, aNode)
		opB = lkOperator(t, bNode)

		if _, ok := lkFindPeer(lkPeers(t, aNode), "hostB.peer"); !ok {
			t.Fatal("A does not list B immediately after the restart")
		}
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)

		a1 = lkSession(t, aNode, "a1")
		b1 = lkSession(t, bNode, "b1")
		a1Ended, b1Ended = false, false
	})

	t.Run("REQ-broker-discovery-012 peers listed", func(t *testing.T) {
		peersB := lkPeers(t, bNode)
		if len(peersB) != 1 {
			t.Fatalf("B lists %d brokers, want one", len(peersB))
		}
		peer := peersB[0]
		if peer.GetAlias() != "hostA.peer" || !peer.GetIsPeer() {
			t.Fatalf("B lists %q (is_peer=%v), want the peer hostA.peer", peer.GetAlias(), peer.GetIsPeer())
		}
		if got := hex.EncodeToString(peer.GetFingerprint()); len(got) != 64 || !lkLowerHex(got) {
			t.Fatalf("B sees A's fingerprint as %q, want 64 lowercase hex digits", got)
		}
		if peer.GetRole() != localpb.PeerRole_PEER_ROLE_DIALER {
			t.Fatalf("B's role is %s, want DIALER", peer.GetRole())
		}
		want := net.JoinHostPort(c.Addr("hostA", "wan"), "17443")
		addrs := peer.GetAdvertisedAddresses()
		if len(addrs) != 1 || addrs[0].GetKind() != "direct" || addrs[0].GetAddr() != want {
			t.Fatalf("B sees A's addresses as %v, want [direct %s]", lkAddressStrings(addrs), want)
		}

		peersA := lkPeers(t, aNode)
		if len(peersA) != 1 {
			t.Fatalf("A lists %d brokers, want one", len(peersA))
		}
		peerB := peersA[0]
		if peerB.GetAlias() != "hostB.peer" || !peerB.GetIsPeer() {
			t.Fatalf("A lists %q (is_peer=%v), want the peer hostB.peer", peerB.GetAlias(), peerB.GetIsPeer())
		}
		if peerB.GetRole() != localpb.PeerRole_PEER_ROLE_ACCEPTOR {
			t.Fatalf("A's role is %s, want ACCEPTOR", peerB.GetRole())
		}
		if len(peerB.GetAdvertisedAddresses()) != 0 {
			t.Fatalf("A invents addresses for the dialer: %v", lkAddressStrings(peerB.GetAdvertisedAddresses()))
		}
	})

	t.Run("REQ-broker-discovery-013 three statuses", func(t *testing.T) {
		c.Cut(t, "hostA", "hostB")
		harness.Eventually(t, lkWaitBound, "the two sides report the cut", func() (bool, string) {
			if got := lkStatus(t, bNode, "hostA.peer"); got != localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE {
				return false, fmt.Sprintf("B sees A as %s, want CONNECTABLE", got)
			}
			if got := lkStatus(t, aNode, "hostB.peer"); got != localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE {
				return false, fmt.Sprintf("A sees B as %s, want NOT_DIRECTLY_REACHABLE", got)
			}
			return true, ""
		})
		if len(lkPeers(t, aNode)) != 1 || len(lkPeers(t, bNode)) != 1 {
			t.Fatal("a peer is listed more than once or missing while the network is cut")
		}

		c.Restore(t)
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)
	})

	t.Run("REQ-broker-pairing-021 disconnect", func(t *testing.T) {
		r := opA.Call(t, "disconnect_broker", map[string]any{"broker": "hostB.peer"})
		r.MustOK(t)

		harness.Eventually(t, harness.EventTimeout, "B no longer sees A as connected", func() (bool, string) {
			got := lkStatus(t, bNode, "hostA.peer")
			return got != localpb.BrokerStatus_BROKER_STATUS_CONNECTED, fmt.Sprintf("B sees A as %s", got)
		})
		harness.Eventually(t, harness.EventTimeout, "A's connection on 17443 is gone", func() (bool, string) {
			if got := establishedOn(t, c, "hostA", 17443); got != 0 {
				return false, fmt.Sprintf("A still has %d established connections on 17443", got)
			}
			return true, ""
		})

		harness.Eventually(t, lkWaitBound, "A forgets B", func() (bool, string) {
			if _, ok := lkFindPeer(lkPeers(t, aNode), "hostB.peer"); ok {
				return false, "A still lists B"
			}
			return true, ""
		})
		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "B's session disappears from A's list", func(deadline time.Time) (bool, string) {
			out, state := lkListAgentsPoll(t, deadline, a1)
			if state != "" {
				return false, state
			}
			if lkHasAgent(out, "b1") {
				return false, "A still lists b1"
			}
			return true, ""
		})
	})

	t.Run("REQ-broker-discovery-014 peer flag", func(t *testing.T) {
		if peerB, ok := lkFindPeer(lkPeers(t, bNode), "hostA.peer"); !ok || !peerB.GetIsPeer() {
			t.Fatalf("B lists A as present=%v is_peer=%v after A unpaired B; B never unpaired A", ok, peerB.GetIsPeer())
		}
		for _, peer := range lkPeers(t, aNode) {
			if peer.GetIsPeer() {
				t.Fatalf("A still marks %q as a peer after unpairing B", peer.GetAlias())
			}
			if peer.GetAlias() == "hostB.peer" && peer.GetIsPeer() {
				t.Fatalf("A lists the unpaired B as a peer")
			}
		}
	})

	t.Run("REQ-broker-pairing-035 rejected dialer stops", func(t *testing.T) {
		harness.Eventually(t, lkWaitBound, "B marks the link to A as rejected", func() (bool, string) {
			peer, ok := lkFindPeer(lkPeers(t, bNode), "hostA.peer")
			if !ok {
				return false, "A is not listed"
			}
			if peer.GetLinkState() != localpb.LinkState_LINK_STATE_REJECTED {
				return false, fmt.Sprintf("link_state is %s", peer.GetLinkState())
			}
			if !strings.Contains(peer.GetStatusDetail(), "unpaired") {
				return false, fmt.Sprintf("status_detail is %q", peer.GetStatusDetail())
			}
			return true, ""
		})

		inSYN := c.SYNCounter(t, "hostA", harness.Inbound, c.Addr("router", "wan"), 0)
		before := inSYN()
		harness.Consistently(t, lkNoRedialWindow, "B stops dialing A", func() (bool, string) {
			if got := inSYN(); got != before {
				return false, fmt.Sprintf("A received %d more SYNs from the router", got-before)
			}
			return true, ""
		})
	})

	t.Run("REQ-broker-pairing-022 re-pair after unpair", func(t *testing.T) {
		fresh := lkCreateKey(t, opA)
		lkConnect(t, opB, addrA, fresh.Key).MustOK(t)
		lkWaitConnected(t, bNode, "hostA.peer", lkWaitBound)
		lkWaitConnected(t, aNode, "hostB.peer", lkWaitBound)
	})

	t.Run("REQ-broker-pairing-034 acceptor addresses change", func(t *testing.T) {
		extra := c.Addr("hostA", "wan2")
		aNode.WriteConfig(t, lkConfig("broker:\n  advertise_extra: ["+extra+"]\n"))

		wan := net.JoinHostPort(c.Addr("hostA", "wan"), "17443")
		wan2 := net.JoinHostPort(extra, "17443")
		harness.Eventually(t, lkWaitBound, "B sees both of A's addresses", func() (bool, string) {
			peer, ok := lkFindPeer(lkPeers(t, bNode), "hostA.peer")
			if !ok {
				return false, "A is not listed"
			}
			addrs := peer.GetAdvertisedAddresses()
			if len(addrs) != 2 || addrs[0].GetAddr() != wan || addrs[1].GetAddr() != wan2 {
				return false, fmt.Sprintf("addresses are %v, want [%s %s]", lkAddressStrings(addrs), wan, wan2)
			}
			return true, ""
		})

		inSYN := c.SYNCounter(t, "hostA", harness.Inbound, c.Addr("hostB", "lanb"), 17443)
		before := inSYN()
		undo := c.Block(t, "hostB", c.Addr("hostA", "wan"))
		t.Cleanup(undo)
		lkCutAndWaitDown(t, c, aNode, bNode)
		c.Restore(t)
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound+lkRelinkSlack)
		if got := inSYN(); got <= before {
			t.Fatalf("B never dialed A's second address: %d SYNs, want more than %d", got, before)
		}
		undo()
	})
}

// TestLinksRestartWithTwoPeers restarts a Broker that is the dialer for one
// peer and the acceptor for another; both links must come back.
func TestLinksRestartWithTwoPeers(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(3))
	xNode, yNode, zNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
	for _, node := range []*harness.Node{xNode, yNode, zNode} {
		lkStartBroker(t, node, lkConfig())
	}
	opX, opY, opZ := lkOperator(t, xNode), lkOperator(t, yNode), lkOperator(t, zNode)

	keyY := lkCreateKey(t, opY)
	lkConnect(t, opX, lkShownAddress(t, c, "hostB", opY), keyY.Key).MustOK(t)
	lkWaitConnected(t, yNode, "hostA.peer", lkRelinkBound)

	keyX := lkCreateKey(t, opX)
	lkConnect(t, opZ, lkShownAddress(t, c, "hostA", opX), keyX.Key).MustOK(t)
	lkWaitConnected(t, xNode, "hostC.peer", lkRelinkBound)

	lkRestart(t, c, "hostA", lkConfig(), opX)

	harness.Eventually(t, lkRelinkBound, "X restores both links after the restart", func() (bool, string) {
		peers := lkPeers(t, xNode)
		y, okY := lkFindPeer(peers, "hostB.peer")
		z, okZ := lkFindPeer(peers, "hostC.peer")
		if !okY || !okZ {
			return false, fmt.Sprintf("X lists %v", lkPeerAliases(peers))
		}
		if y.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED ||
			z.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
			return false, fmt.Sprintf("Y=%s Z=%s", y.GetStatus(), z.GetStatus())
		}
		return true, ""
	})
}

// lkUnauthPerSource and lkUnauthGlobal must match maxUnauthenticatedPerSource
// and maxUnauthenticated in internal/link.
const (
	lkUnauthPerSource = 8
	lkUnauthGlobal    = 64
)

// lkFloodSources holds more than twice the global limit across sources that
// each stay at the per-source limit, so the first eviction is a global one.
const lkFloodSources = 2*lkUnauthGlobal/lkUnauthPerSource + 1

// Staying exactly at the per-source limit keeps source_limit evictions out, so
// a global_limit record proves the global limit fired.
const lkFloodPerSource = lkUnauthPerSource

// TestLinksSourceFlood covers a link rebuild and a one-time-key pairing while
// the acceptor's listener is over its global unauthenticated connection limit.
func TestLinksSourceFlood(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(3))
	aNode, bNode, cNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
	lkStartBroker(t, aNode, lkConfig())
	// B logs at debug level so the test can read the eviction records: the
	// link writes them only at Debug.
	lkStartBroker(t, bNode, lkConfig("log:\n  level: debug\n"))
	lkStartBroker(t, cNode, lkConfig())
	opA, opB, opC := lkOperator(t, aNode), lkOperator(t, bNode), lkOperator(t, cNode)
	defer opA.End(t)
	defer opB.End(t)
	defer opC.End(t)

	addrB := lkAddr(c, "hostB", "net")

	keyA := lkCreateKey(t, opB)
	lkConnect(t, opA, addrB, keyA.Key).MustOK(t)
	lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)
	lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)

	// B generates the key the third source pairs with before the flood starts,
	// so what the flood must not block is the pairing call itself.
	keyC := lkCreateKey(t, opB)

	t.Run("REQ-broker-pairing-039 source flood keeps links and pairing", func(t *testing.T) {
		c.StartFlood(t, "hostB", ctl.FloodStartRequest{
			Addr:      net.JoinHostPort("127.0.0.1", "17443"),
			Sources:   lkFloodSources,
			PerSource: lkFloodPerSource,
		})
		defer c.StopFlood(t, "hostB")

		harness.Eventually(t, lkWaitBound, "the flood overflows B's global unauthenticated limit", func() (bool, string) {
			if strings.Contains(string(bNode.BrokerLog(t)), `"cause":"global_limit"`) {
				return true, ""
			}
			st := c.FloodStats(t, "hostB")
			return false, fmt.Sprintf("no global_limit eviction yet: dialed=%d open=%d max_open=%d closed=%d",
				st.Dialed, st.Open, st.MaxOpen, st.Closed)
		})

		lkCutAndWaitDown(t, c, aNode, bNode)
		c.Restore(t)
		// Start the clock after Restore returns, as in the redial case: the
		// rule deletion is the harness's work, not the rebuild under test.
		start := time.Now()
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)
		if elapsed := time.Since(start); elapsed > lkRequirementBound+lkRequirementSlack {
			t.Fatalf("the link took %s to rebuild under the flood, want at most %s (REQ-broker-pairing-018 %s plus %s observation margin)",
				elapsed, lkRequirementBound+lkRequirementSlack, lkRequirementBound, lkRequirementSlack)
		}

		lkConnect(t, opC, addrB, keyC.Key).MustOK(t)
		lkWaitConnected(t, cNode, "hostB.peer", lkWaitBound)
		lkWaitConnected(t, bNode, "hostC.peer", lkWaitBound)
	})
}

func lkConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

func lkAddr(c *harness.Cluster, node, network string) string {
	return net.JoinHostPort(c.Addr(node, network), "17443")
}

func lkShownAddress(t *testing.T, c *harness.Cluster, node string, op *harness.Session) string {
	t.Helper()
	want := net.JoinHostPort(c.Addr(node, "net"), "17443")
	key := lkCreateKey(t, op)
	for _, address := range key.Addresses {
		if address == want {
			return address
		}
	}
	if len(key.Addresses) > 0 {
		return key.Addresses[0]
	}
	t.Fatalf("create_pairing_key on %s returned no shown address", node)
	return ""
}

func lkStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

func lkRestart(t *testing.T, c *harness.Cluster, node string, config string, stop ...*harness.Session) {
	t.Helper()
	// End the MCP server processes before stopping the Broker.
	for _, s := range stop {
		s.End(t)
	}
	n := c.Node(node)
	n.StopBroker(t, syscall.SIGTERM)
	lkStartBroker(t, n, config)
}

func lkOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, lkWaitBound)
	return s
}

func lkSession(t *testing.T, node *harness.Node, name string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	registrationWaitRegistered(t, s, name)
	s.Call(t, "join_topic", map[string]any{"name": lkTopic}).MustOK(t)
	return s
}

func lkCreateKey(t *testing.T, op *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := op.Call(t, "create_pairing_key", map[string]any{})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

func lkConnect(t *testing.T, op *harness.Session, address, key string) harness.Result {
	t.Helper()
	return op.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
}

func lkPeers(t *testing.T, node *harness.Node) []*localpb.BrokerInfo {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("list_brokers: %s", detail.GetMessage())
	}
	return resp.GetBrokers()
}

func lkFindPeer(peers []*localpb.BrokerInfo, alias string) (*localpb.BrokerInfo, bool) {
	for _, peer := range peers {
		if peer.GetAlias() == alias {
			return peer, true
		}
	}
	return nil, false
}

func lkStatus(t *testing.T, node *harness.Node, alias string) localpb.BrokerStatus {
	t.Helper()
	peer, ok := lkFindPeer(lkPeers(t, node), alias)
	if !ok {
		return localpb.BrokerStatus_BROKER_STATUS_UNSPECIFIED
	}
	return peer.GetStatus()
}

func lkWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the link to "+alias+" is connected", func() (bool, string) {
		return lkStatus(t, node, alias) == localpb.BrokerStatus_BROKER_STATUS_CONNECTED, fmt.Sprintf("status is %s", lkStatus(t, node, alias))
	})
}

func lkCutAndWaitDown(t *testing.T, c *harness.Cluster, aNode, bNode *harness.Node) {
	t.Helper()
	c.Cut(t, "hostA", "hostB")
	harness.Eventually(t, lkWaitBound, "both sides notice the cut", func() (bool, string) {
		a := lkStatus(t, aNode, "hostB.peer")
		b := lkStatus(t, bNode, "hostA.peer")
		if a == localpb.BrokerStatus_BROKER_STATUS_CONNECTED || b == localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
			return false, fmt.Sprintf("A sees B as %s, B sees A as %s", a, b)
		}
		return true, ""
	})
}

func lkListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", map[string]any{})
}

func lkHasAgent(out mcpserver.ListAgentsOut, name string) bool {
	for _, agent := range out.Agents {
		if lkAgentName(agent.Address) == name {
			return true
		}
	}
	return false
}

func lkAgentName(address string) string {
	before, _, _ := strings.Cut(address, "@")
	if i := strings.Index(before, " ["); i >= 0 {
		before = before[:i]
	}
	return strings.TrimSpace(before)
}

// Count Broker processes through the control protocol: node.ListProcs reads
// the host's /proc, the wrong namespace for a container.
func lkBrokerProcesses(t *testing.T, c *harness.Cluster, node string) int {
	t.Helper()
	client := ctl.Dial(filepath.Join(c.CTLDir(t, node), "ctl.sock"))
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), lkWaitBound)
	defer cancel()
	procs, err := client.Procs(ctx, ctl.ProcsRequest{Kind: "broker"})
	if err != nil {
		t.Fatalf("listing the Broker processes of %s: %v", node, err)
	}
	return len(procs)
}

func lkPeerAliases(peers []*localpb.BrokerInfo) []string {
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer.GetAlias())
	}
	return out
}

func lkAddressStrings(addrs []*localpb.AdvertisedAddress) []string {
	out := make([]string, 0, len(addrs))
	for _, address := range addrs {
		out = append(out, address.GetKind()+" "+address.GetAddr())
	}
	return out
}

func lkLowerHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return s != ""
}
