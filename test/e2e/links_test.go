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
	// lkRelinkBound is reconnect_max + dial_timeout plus the harness.EventTimeout
	// margin: the timers that govern a link coming back after a cut.
	lkRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout
	// lkWaitBound is the bound of a wait that no product timer governs.
	lkWaitBound = harness.EventTimeout
	// lkNoRedialWindow is five reconnect intervals (5 * 400ms). A Broker that
	// keeps redialing makes at least four attempts inside it, while a stopped
	// one makes none; the window is a deliberate observation, not a sleep.
	lkNoRedialWindow = 2000 * time.Millisecond
	// lkBrokerIdleWindow is also five reconnect intervals, spanning forty
	// harness polls. A Broker that exits with its last client is gone before
	// the first poll after that disconnect, and a broken link would flip the
	// peer status within one reconnect interval; a healthy idle Broker keeps
	// both assertions true for the whole window.
	lkBrokerIdleWindow = 2000 * time.Millisecond
	// lkRelinkSlack is added to lkRelinkBound when a reconnect has one more
	// hop to make, such as finding a second advertised address. No product
	// timer governs the extra second; it only keeps the bound from turning a
	// slow first address into a second-address failure.
	lkRelinkSlack = time.Second
	// lkRequirementBound is REQ-broker-pairing-018's 120-second upper bound on a
	// relink. The waits use the shorter lkRelinkBound as their fallback, and the
	// measured span is asserted against this requirement value plus a small
	// observation margin, so the fallback never enters the assertion.
	lkRequirementBound = 120 * time.Second
	lkRequirementSlack = time.Second
)

// lkTopic is the topic the sessions use.
const lkTopic = "t"

// TestLinks covers link survival, redial after a cut, the acceptor never
// dialing, restart recovery, address-list changes, unpairing and the Broker
// states reported by list_brokers. A is the acceptor behind no NAT and B is the
// dialer behind the router.
func TestLinks(t *testing.T) {
	c := harness.Up(t, harness.TopoNAT)
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	lkStartBroker(t, aNode, lkConfig())
	lkStartBroker(t, bNode, lkConfig())
	opA, opB := lkOperator(t, aNode), lkOperator(t, bNode)

	// The counter is installed before pairing so any dial A ever makes is
	// counted, including one made while the link is being set up. Port 0
	// watches every destination port: an acceptor that wrongly dials could aim
	// at a port other than the one under test, and the count must not miss it.
	outSYN := c.SYNCounter(t, "hostA", harness.Outbound, "", 0)
	// B is the dialer. Its counter is the positive control: the same rule has
	// to see B's pairing dial, or a zero on A proves nothing.
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

		// No connect_broker was called during the outage; the consumed
		// one-time key must not work a second time.
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
		// product for the harness's own work. Each wait has lkRelinkBound as its
		// fallback and fails on its own if its side does not come back; the summed
		// span is then checked against REQ-broker-pairing-018's 120 seconds, so the
		// requirement's upper bound is asserted with the fallback kept out of it.
		start := time.Now()
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)
		if elapsed := time.Since(start); elapsed > lkRequirementBound+lkRequirementSlack {
			t.Fatalf("the link took %s to rebuild, want at most %s (REQ-broker-pairing-018 %s plus %s observation margin)",
				elapsed, lkRequirementBound+lkRequirementSlack, lkRequirementBound, lkRequirementSlack)
		}
	})

	t.Run("REQ-broker-pairing-027 acceptor never dials", func(t *testing.T) {
		// Positive control: B's dial of A has to show up in the same kind of
		// counter. Without it, a zero on A could mean the rule never matched.
		if got := bSYN(); got == 0 {
			t.Fatal("B's outbound SYN counter read zero although B dialed A; the counter is blind")
		}

		// The two cut/restore cycles above must already show A never dialing.
		if got := outSYN(); got != 0 {
			t.Fatalf("A sent %d outbound SYNs while it was only the acceptor", got)
		}

		// Block B's path to A's only advertised address; then a cut and restore
		// leave B unable to redial. A must not dial in the other direction.
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

		// The peer table is loaded before the Broker answers.
		if _, ok := lkFindPeer(lkPeers(t, aNode), "hostB.peer"); !ok {
			t.Fatal("A does not list B immediately after the restart")
		}
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)

		// Restore the sessions the later cases use.
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
		// Both readings come from the live lists, one case after A unpaired B.
		// B still treats A as a peer and must say so; that is the positive
		// control proving the flag reflects the pairing table rather than a
		// constant. A must no longer mark anything as a peer, and if B's card
		// is still listed it must carry is_peer=false.
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

		// Port 0 watches every destination port: a rejected dialer that starts
		// dialing again may aim anywhere, and the acceptor counter must see it.
		inSYN := c.SYNCounter(t, "hostA", harness.Inbound, c.Addr("router", "wan"), 0)
		before := inSYN()
		harness.Consistently(t, lkNoRedialWindow, "B stops dialing A", func() (bool, string) {
			if got := inSYN(); got != before {
				return false, fmt.Sprintf("A received %d more SYNs from the router", got-before)
			}
			return true, ""
		})
		// "No dial for ten minutes" is covered by
		// internal/link/dial_test.go's
		// TestREQBrokerPairing035_RejectedStopsRedialing; the shortened window
		// here observes the same stop in the real process.
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

		// Block the first address; after the cut and restore B must find the
		// second one, which shows up as an inbound SYN to A from B.
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

	// X dials Y.
	keyY := lkCreateKey(t, opY)
	lkConnect(t, opX, lkShownAddress(t, c, "hostB", opY), keyY.Key).MustOK(t)
	lkWaitConnected(t, yNode, "hostA.peer", lkRelinkBound)

	// Z dials X.
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

// lkUnauthPerSource and lkUnauthGlobal repeat the unauthenticated connection
// limits internal/link keeps unexported. They must match
// maxUnauthenticatedPerSource and maxUnauthenticated in internal/link/new.go.
const (
	lkUnauthPerSource = 8
	lkUnauthGlobal    = 64
)

// lkFloodSources is the number of distinct sources the flood uses: two times
// the global limit divided by the per-source limit, plus one, so the sources
// together hold 136 connections, more than twice the global limit, while no
// source exceeds the per-source limit. The first eviction that happens is
// therefore always a global one.
const lkFloodSources = 2*lkUnauthGlobal/lkUnauthPerSource + 1

// lkFloodPerSource is how many connections each source holds. Staying exactly
// at the per-source limit keeps source_limit evictions out of the test, so a
// global_limit record proves the global limit closed connections.
const lkFloodPerSource = lkUnauthPerSource

// TestLinksSourceFlood proves REQ-broker-pairing-039 on the real network
// stack: while B's listener holds more unauthenticated connections than its
// global limit, A rebuilds its link to B within the same bound
// REQ-broker-pairing-018 uses, and hostC pairs with B using a one-time key.
//
// The flood runs inside hostB, and each of its workers binds to its own
// loopback address, so B sees every source as a distinct /32. That reaches the
// global limit from many sources without adding containers or addresses to the
// topology, and keeps the flooding sources distinct from hostA's and hostC's.
func TestLinksSourceFlood(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(3))
	aNode, bNode, cNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
	lkStartBroker(t, aNode, lkConfig())
	// B logs at debug level so the test can read the eviction records.
	lkStartBroker(t, bNode, lkConfig("log:\n  level: debug\n"))
	lkStartBroker(t, cNode, lkConfig())
	opA, opB, opC := lkOperator(t, aNode), lkOperator(t, bNode), lkOperator(t, cNode)
	defer opA.End(t)
	defer opB.End(t)
	defer opC.End(t)

	addrB := lkAddr(c, "hostB", "net")

	// A is the dialer and B the acceptor, so B generates the key A uses.
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

		// The flood must actually overflow the global limit, not merely reach
		// the per-source one. With every source held at exactly the per-source
		// limit, a global_limit record is the Broker's own proof that it closed
		// connections to enforce the global limit.
		harness.Eventually(t, lkWaitBound, "the flood overflows B's global unauthenticated limit", func() (bool, string) {
			if strings.Contains(string(bNode.BrokerLog(t)), `"cause":"global_limit"`) {
				return true, ""
			}
			st := c.FloodStats(t, "hostB")
			return false, fmt.Sprintf("no global_limit eviction yet: dialed=%d open=%d max_open=%d closed=%d",
				st.Dialed, st.Open, st.MaxOpen, st.Closed)
		})

		// A rebuilds its link while the flood keeps the global limit full. Each
		// side's wait has lkRelinkBound as its fallback; the summed span is then
		// checked against REQ-broker-pairing-018's bound as in the redial case.
		lkCutAndWaitDown(t, c, aNode, bNode)
		c.Restore(t)
		start := time.Now()
		lkWaitConnected(t, bNode, "hostA.peer", lkRelinkBound)
		lkWaitConnected(t, aNode, "hostB.peer", lkRelinkBound)
		if elapsed := time.Since(start); elapsed > lkRequirementBound+lkRequirementSlack {
			t.Fatalf("the link took %s to rebuild under the flood, want at most %s (REQ-broker-pairing-018 %s plus %s observation margin)",
				elapsed, lkRequirementBound+lkRequirementSlack, lkRequirementBound, lkRequirementSlack)
		}

		// A third source pairs during the same flood. It must succeed even
		// though the flooding sources have the stranger pool churning.
		lkConnect(t, opC, addrB, keyC.Key).MustOK(t)
		lkWaitConnected(t, cNode, "hostB.peer", lkWaitBound)
		lkWaitConnected(t, bNode, "hostC.peer", lkWaitBound)
	})
}

// lkConfig merges the shortened timers with the given fragments.
func lkConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

// lkAddr is the address of node's Broker on network.
func lkAddr(c *harness.Cluster, node, network string) string {
	return net.JoinHostPort(c.Addr(node, network), "17443")
}

// lkShownAddress picks the shown address of node on the flat network from a
// key created by op.
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

// lkStartBroker starts a Broker and fails when it does not come up.
func lkStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// lkRestart stops the Broker and starts it with config after ending every MCP
// server process passed in stop.
func lkRestart(t *testing.T, c *harness.Cluster, node string, config string, stop ...*harness.Session) {
	t.Helper()
	for _, s := range stop {
		s.End(t)
	}
	n := c.Node(node)
	n.StopBroker(t, syscall.SIGTERM)
	lkStartBroker(t, n, config)
}

// lkOperator starts an operator session that never registers as an agent.
func lkOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, lkWaitBound)
	return s
}

// lkSession starts a registered session in lkTopic.
func lkSession(t *testing.T, node *harness.Node, name string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	registrationWaitRegistered(t, s, name)
	s.Call(t, "join_topic", map[string]any{"name": lkTopic}).MustOK(t)
	return s
}

// lkCreateKey creates one pairing key.
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

// lkConnect calls connect_broker.
func lkConnect(t *testing.T, op *harness.Session, address, key string) harness.Result {
	t.Helper()
	return op.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
}

// lkPeers lists node's peers through the local interface probe.
func lkPeers(t *testing.T, node *harness.Node) []*localpb.BrokerInfo {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("list_brokers: %s", detail.GetMessage())
	}
	return resp.GetBrokers()
}

// lkFindPeer returns the peer with alias.
func lkFindPeer(peers []*localpb.BrokerInfo, alias string) (*localpb.BrokerInfo, bool) {
	for _, peer := range peers {
		if peer.GetAlias() == alias {
			return peer, true
		}
	}
	return nil, false
}

// lkStatus returns the status of alias in node's list, or UNSPECIFIED.
func lkStatus(t *testing.T, node *harness.Node, alias string) localpb.BrokerStatus {
	t.Helper()
	peer, ok := lkFindPeer(lkPeers(t, node), alias)
	if !ok {
		return localpb.BrokerStatus_BROKER_STATUS_UNSPECIFIED
	}
	return peer.GetStatus()
}

// lkWaitConnected waits until node lists alias as connected.
func lkWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the link to "+alias+" is connected", func() (bool, string) {
		return lkStatus(t, node, alias) == localpb.BrokerStatus_BROKER_STATUS_CONNECTED, fmt.Sprintf("status is %s", lkStatus(t, node, alias))
	})
}

// lkCutAndWaitDown cuts the network and waits until neither side lists the
// other as connected.
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

// lkListAgentsPoll calls list_agents from a wait and returns the decoded answer.
// The call is bounded by the wait's deadline and a transport error is returned
// as state.
func lkListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", map[string]any{})
}

// lkHasAgent reports whether the listing contains an agent named name.
func lkHasAgent(out mcpserver.ListAgentsOut, name string) bool {
	for _, agent := range out.Agents {
		if lkAgentName(agent.Address) == name {
			return true
		}
	}
	return false
}

// lkAgentName returns the name part of an agent address.
func lkAgentName(address string) string {
	before, _, _ := strings.Cut(address, "@")
	if i := strings.Index(before, " ["); i >= 0 {
		before = before[:i]
	}
	return strings.TrimSpace(before)
}

// lkBrokerProcesses counts the Broker processes of node as the node agent sees
// them. node.ListProcs scans the host's /proc, which is the wrong namespace
// for a container, so the count goes through the control protocol.
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

// lkPeerAliases renders peer aliases for failure messages.
func lkPeerAliases(peers []*localpb.BrokerInfo) []string {
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer.GetAlias())
	}
	return out
}

// lkAddressStrings renders advertised addresses for failure messages.
func lkAddressStrings(addrs []*localpb.AdvertisedAddress) []string {
	out := make([]string, 0, len(addrs))
	for _, address := range addrs {
		out = append(out, address.GetKind()+" "+address.GetAddr())
	}
	return out
}

// lkLowerHex reports whether s is a non-empty run of lowercase hex digits.
func lkLowerHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return s != ""
}
