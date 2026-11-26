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

const vlRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout

// vlVersionDetail is the status_detail an acceptor one link protocol version
// ahead reports, derived from the constant so a version bump needs no test
// change.
var vlVersionDetail = fmt.Sprintf("link protocol version incompatible with this peer; upgrade the older Broker: peer speaks link protocol version %d", linkpb.ProtocolVersion)

func vlStartBroker(t *testing.T, n *harness.Node, spec harness.BrokerSpec) harness.BrokerStart {
	t.Helper()
	st := n.StartBroker(t, spec)
	if !st.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", st.ExitCode, strings.TrimSpace(st.StderrLast))
	}
	return st
}

func vlOperator(t *testing.T, n *harness.Node, name string) *harness.Session {
	t.Helper()
	s := n.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, harness.EventTimeout)
	return s
}

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

// TestLinkVersionMismatchAndRecovery checks that a dialer at the default link
// protocol version is rejected by an acceptor one version ahead, both sides
// report why and keep the pair, and the link recovers when the acceptor
// returns at the default version.
func TestLinkVersionMismatchAndRecovery(t *testing.T) {
	a := harness.NewLocalHost(t, harness.LocalOptions{Name: "a"})
	b := harness.NewLocalHost(t, harness.LocalOptions{Name: "b"})
	addrB := b.FreeAddr(t)
	a.WriteConfig(t, harness.ConfigYAML(harness.FastTimers, "broker:\n  alias: a\n  listen: "+a.FreeAddr(t)+"\n"))
	b.WriteConfig(t, harness.ConfigYAML(harness.FastTimers, "broker:\n  alias: b\n  listen: "+addrB+"\n"))
	vlStartBroker(t, a, harness.BrokerSpec{})
	addrB = vlStartBroker(t, b, harness.BrokerSpec{}).Address(addrB)

	sb := vlOperator(t, b, "operator-b")
	sa := vlOperator(t, a, "operator-a")
	r := sb.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	key, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	sa.Call(t, "connect_broker", map[string]any{"address": addrB, "key": key.Key}).MustOK(t)
	harness.Eventually(t, harness.EventTimeout, "A reports B connected", func() (bool, string) {
		peer, state := vlPeer(t, a)
		if peer == nil {
			return false, state
		}
		return peer.GetStatus() == localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
			fmt.Sprintf("status is %s", peer.GetStatus())
	})

	// End the sessions before restarting: their MCP server processes would
	// race the Broker restarts.
	sb.End(t)
	sa.End(t)

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

// TestPairingVersionMismatch checks that connect_broker to an acceptor one
// link protocol version behind fails as version incompatible and never reports
// a paired Broker.
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

	// Take the audit watermark before connecting: the rejection record can be
	// written before the call returns.
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

	for _, n := range []*harness.Node{a, b} {
		resp, detail := n.Oracle().ListBrokers(t)
		if detail != nil {
			t.Fatalf("list_brokers: %s", detail.GetMessage())
		}
		if got := len(resp.GetBrokers()); got != 0 {
			t.Fatalf("a Broker lists %d brokers after the version rejection, want 0", got)
		}
	}

	harness.Eventually(t, harness.EventTimeout, "B audits the version-incompatible pairing failure", func() (bool, string) {
		for _, event := range ctAudit(t, b, auditSince, 0) {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED &&
				event.GetDetail() == "version incompatible" {
				return true, ""
			}
		}
		return false, "no pairing_failed record with detail \"version incompatible\""
	})
}
