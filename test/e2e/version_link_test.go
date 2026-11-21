//go:build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// vlRelinkBound is reconnect_max (400ms) plus the three-second test allowance.
const vlRelinkBound = 3400 * time.Millisecond

// vlVersionDetail is the status_detail an acceptor one link protocol version
// ahead of the dialer reports. It is derived from the constant so a future
// version bump needs no test change.
var vlVersionDetail = fmt.Sprintf("link protocol version incompatible with this peer; upgrade the older Broker: peer speaks link protocol version %d", linkpb.ProtocolVersion)

// vlStartBroker starts the node's Broker from config and fails when it does not
// come up.
func vlStartBroker(t *testing.T, n *harness.Node, spec harness.BrokerSpec) harness.BrokerStart {
	t.Helper()
	st := n.StartBroker(t, spec)
	if !st.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", st.ExitCode, strings.TrimSpace(st.StderrLast))
	}
	return st
}

// vlOperator starts an operator session that never registers as an agent and
// waits until it reaches its Broker.
func vlOperator(t *testing.T, n *harness.Node, name string) *harness.Session {
	t.Helper()
	s := n.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, 3*time.Second)
	return s
}

// vlPeer returns the node's only peer from Status, or a state string naming
// why it is not exactly one.
func vlPeer(t *testing.T, n *harness.Node) (*localpb.BrokerInfo, string) {
	t.Helper()
	resp, detail := n.Oracle().Status(t)
	if detail != nil {
		return nil, detail.GetMessage()
	}
	peers := resp.GetPeers()
	if len(peers) != 1 {
		return nil, fmt.Sprintf("Status lists %d peers, want 1", len(peers))
	}
	return peers[0], ""
}

// TestLinkVersionMismatchAndRecovery checks the link side of the version
// handshake between two real Brokers: a dialer at the default link protocol
// version is rejected by an acceptor one version ahead, both sides report why,
// both keep the pair listed, and the link recovers when the acceptor returns at
// the default version.
func TestLinkVersionMismatchAndRecovery(t *testing.T) {
	a := harness.NewLocalHost(t, harness.LocalOptions{Name: "a"})
	b := harness.NewLocalHost(t, harness.LocalOptions{Name: "b"})
	addrB := b.FreeAddr(t)
	a.WriteConfig(t, harness.ConfigYAML(harness.FastTimers, "broker:\n  alias: a\n  listen: "+a.FreeAddr(t)+"\n"))
	b.WriteConfig(t, harness.ConfigYAML(harness.FastTimers, "broker:\n  alias: b\n  listen: "+addrB+"\n"))
	vlStartBroker(t, a, harness.BrokerSpec{})
	addrB = vlStartBroker(t, b, harness.BrokerSpec{}).Address(addrB)

	// Pair A, the dialer, with B, the acceptor.
	sb := vlOperator(t, b, "operator-b")
	sa := vlOperator(t, a, "operator-a")
	r := sb.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	key, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	sa.Call(t, "connect_broker", map[string]any{"address": addrB, "key": key.Key}).MustOK(t)
	harness.Eventually(t, 3*time.Second, "A reports B connected", func() (bool, string) {
		peer, state := vlPeer(t, a)
		if peer == nil {
			return false, state
		}
		return peer.GetStatus() == localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
			fmt.Sprintf("status is %s", peer.GetStatus())
	})

	// The sessions are not needed after pairing, and ending them keeps their
	// MCP server processes from racing the Broker restarts below.
	sb.End(t)
	sa.End(t)

	// Restart B one link protocol version ahead; its data directory and the
	// pair survive.
	if code := b.StopBroker(t, syscall.SIGTERM); code != 0 {
		t.Fatalf("B's Broker exited %d on SIGTERM, want 0", code)
	}
	vlStartBroker(t, b, harness.BrokerSpec{Env: map[string]string{
		"PEERS_TEST_LINK_PROTOCOL_VERSION": strconv.FormatUint(uint64(linkpb.ProtocolVersion+1), 10),
	}})

	harness.Eventually(t, vlRelinkBound, "A reports the link version mismatch", func() (bool, string) {
		peer, state := vlPeer(t, a)
		if peer == nil {
			return false, state
		}
		if peer.GetLinkState() == localpb.LinkState_LINK_STATE_ESTABLISHED {
			return false, "link_state is ESTABLISHED"
		}
		if !strings.HasPrefix(peer.GetStatusDetail(), "link protocol version incompatible with this peer") {
			return false, fmt.Sprintf("status_detail is %q", peer.GetStatusDetail())
		}
		return true, ""
	})
	harness.Eventually(t, vlRelinkBound, "B names the peer's link protocol version", func() (bool, string) {
		peer, state := vlPeer(t, b)
		if peer == nil {
			return false, state
		}
		return peer.GetStatusDetail() == vlVersionDetail,
			fmt.Sprintf("status_detail is %q", peer.GetStatusDetail())
	})

	// A version mismatch is not an unpairing: both sides still list the other
	// as a peer.
	for _, n := range []*harness.Node{a, b} {
		resp, detail := n.Oracle().ListBrokers(t)
		if detail != nil {
			t.Fatalf("list_brokers: %s", detail.GetMessage())
		}
		if got := len(resp.GetBrokers()); got != 1 {
			t.Fatalf("a Broker lists %d brokers after the version mismatch, want 1", got)
		}
		if !resp.GetBrokers()[0].GetIsPeer() {
			t.Fatal("the listed Broker is no longer a peer after the version mismatch")
		}
	}

	// Restart B at the default version; A's dialer redials and the link comes
	// back without another connect_broker.
	if code := b.StopBroker(t, syscall.SIGTERM); code != 0 {
		t.Fatalf("B's Broker exited %d on SIGTERM, want 0", code)
	}
	vlStartBroker(t, b, harness.BrokerSpec{})

	harness.Eventually(t, vlRelinkBound, "A reconnects after B returns at the default version", func() (bool, string) {
		peer, state := vlPeer(t, a)
		if peer == nil {
			return false, state
		}
		if peer.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
			return false, fmt.Sprintf("status is %s", peer.GetStatus())
		}
		if detail := peer.GetStatusDetail(); detail != "" {
			return false, fmt.Sprintf("status_detail is %q", detail)
		}
		return true, ""
	})
}

// TestPairingVersionMismatch checks the pairing side of the version check: a
// Broker at the default link protocol version dials an acceptor that reports
// the previous one. The acceptor compares the version in PairRequest before it
// looks up the key, so connect_broker fails with the version-incompatible
// detail and never reports a paired Broker. Without the check the pairing would
// succeed and the link would then fail its own version handshake.
func TestPairingVersionMismatch(t *testing.T) {
	if linkpb.ProtocolVersion < 2 {
		t.Skipf("link protocol version %d has no earlier version to inject", linkpb.ProtocolVersion)
	}
	a := harness.NewLocalHost(t, harness.LocalOptions{Name: "a"})
	b := harness.NewLocalHost(t, harness.LocalOptions{Name: "b"})
	addrB := b.FreeAddr(t)
	a.WriteConfig(t, harness.ConfigYAML(harness.FastTimers, "broker:\n  alias: a\n  listen: "+a.FreeAddr(t)+"\n"))
	b.WriteConfig(t, harness.ConfigYAML(harness.FastTimers, "broker:\n  alias: b\n  listen: "+addrB+"\n"))
	vlStartBroker(t, a, harness.BrokerSpec{})
	addrB = vlStartBroker(t, b, harness.BrokerSpec{Env: map[string]string{
		"PEERS_TEST_LINK_PROTOCOL_VERSION": strconv.FormatUint(uint64(linkpb.ProtocolVersion-1), 10),
	}}).Address(addrB)

	sb := vlOperator(t, b, "operator-b")
	sa := vlOperator(t, a, "operator-a")
	r := sb.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	key, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}

	// Take the audit watermark before connecting: on a slow machine the
	// rejection record can be written before the calls below return, and a
	// watermark taken afterwards would miss it.
	auditSince := time.Now().Add(-time.Second)
	res := sa.Call(t, "connect_broker", map[string]any{"address": addrB, "key": key.Key})
	if !res.IsError {
		t.Fatalf("connect_broker to an acceptor one link protocol version behind succeeded; a pairing must fail: %s", res.Text)
	}
	res.MustCode(t, "pairing_rejected")
	const wantDetail = "link protocol version incompatible with the other Broker; upgrade the older Broker"
	if !strings.Contains(res.Text, wantDetail) {
		t.Fatalf("connect_broker error is %q, want it to name %q", res.Text, wantDetail)
	}

	// Neither side recorded a peer: the pairing never reached PairDone.
	for _, n := range []*harness.Node{a, b} {
		resp, detail := n.Oracle().ListBrokers(t)
		if detail != nil {
			t.Fatalf("list_brokers: %s", detail.GetMessage())
		}
		if got := len(resp.GetBrokers()); got != 0 {
			t.Fatalf("a Broker lists %d brokers after the version rejection, want 0", got)
		}
	}

	// The acceptor audits the rejection as version incompatible, without the
	// peer's version, because it never looked up a key.
	harness.Eventually(t, 3*time.Second, "B audits the version-incompatible pairing failure", func() (bool, string) {
		for _, event := range ctAudit(t, b, auditSince, 0) {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED &&
				event.GetDetail() == "version incompatible" {
				return true, ""
			}
		}
		return false, "no pairing_failed record with detail \"version incompatible\""
	})
}
