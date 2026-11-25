package broker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

func statusRegisterAgent(t *testing.T, tb *testBroker, name string) (AgentID, *fakesession.Fake) {
	t.Helper()
	fake := fakesession.New(t, fakesession.Options{Name: name})
	stream := startRegistration(t, tb, fake)
	stream.complete(eventTimeout)
	return mustAgentID(t, fake.SocketPath()), fake
}

func statusCurrentUsername(t *testing.T) string {
	t.Helper()
	uid := strconv.Itoa(os.Geteuid())
	u, err := user.LookupId(uid)
	if err != nil {
		return uid
	}
	return u.Username
}

// TestStatusListsAgentsAndTopics checks that Status lists each agent with its
// account and topics.
func TestStatusListsAgentsAndTopics(t *testing.T) {
	tb := newTestBroker(t)
	alphaID, _ := statusRegisterAgent(t, tb, "alpha")
	betaID, _ := statusRegisterAgent(t, tb, "beta")
	if err := tb.topics.Join(alphaID, "t1"); err != nil {
		t.Fatalf("Join(alpha, t1): %v", err)
	}
	if err := tb.topics.Join(betaID, "t2"); err != nil {
		t.Fatalf("Join(beta, t2): %v", err)
	}

	resp, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(resp.Agents) != 2 {
		t.Fatalf("Status returned %d agents, want 2: %v", len(resp.Agents), resp.Agents)
	}

	byAddress := make(map[string]*localpb.AgentStatusEntry, len(resp.Agents))
	for _, entry := range resp.Agents {
		byAddress[entry.Address] = entry
	}
	account := statusCurrentUsername(t)
	for address, wantTopic := range map[string]string{
		"alpha@hostA": "t1",
		"beta@hostA":  "t2",
	} {
		entry, ok := byAddress[address]
		if !ok {
			t.Fatalf("Status has no entry for %q: %v", address, resp.Agents)
		}
		if len(entry.Topics) != 1 || entry.Topics[0] != wantTopic {
			t.Errorf("%s topics = %v, want [%s]", address, entry.Topics, wantTopic)
		}
		if entry.Account != account {
			t.Errorf("%s account = %q, want %q", address, entry.Account, account)
		}
		if entry.Status != localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED {
			t.Errorf("%s status = %v, want unspecified", address, entry.Status)
		}
	}
}

// TestStatusIncludesPeers checks that Status carries the peer list.
func TestStatusIncludesPeers(t *testing.T) {
	tb := newTestBroker(t)
	info := &localpb.BrokerInfo{Alias: "hostB", IsPeer: true}
	tb.peers.infos = []*localpb.BrokerInfo{info}

	resp, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(resp.Peers) != 1 {
		t.Fatalf("Status returned %d peers, want 1: %v", len(resp.Peers), resp.Peers)
	}
	if resp.Peers[0].Alias != "hostB" || !resp.Peers[0].IsPeer {
		t.Fatalf("Status peer = %+v, want alias hostB and is_peer", resp.Peers[0])
	}
}

// TestStatusListsAllAgents checks that Status does not filter agents by account
// before the permission interceptor decides.
func TestStatusListsAllAgents(t *testing.T) {
	tb := newTestBroker(t)
	mineID, _ := statusRegisterAgent(t, tb, "mine")
	otherID, _ := statusRegisterAgent(t, tb, "other")

	tb.b.mu.Lock()
	tb.b.agents[otherID].caller.UID = 4242
	tb.b.mu.Unlock()

	resp, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(resp.Agents) != 2 {
		t.Fatalf("Status returned %d agents, want both accounts' agents: %v", len(resp.Agents), resp.Agents)
	}
	seen := map[string]bool{}
	for _, entry := range resp.Agents {
		seen[entry.AgentId] = true
	}
	if !seen[string(mineID)] || !seen[string(otherID)] {
		t.Fatalf("Status agents = %v, want IDs %s and %s", resp.Agents, mineID, otherID)
	}
}

// TestStatusDoesNotReread checks that Status does not reread session records.
func TestStatusDoesNotReread(t *testing.T) {
	tb := newTestBroker(t)
	statusRegisterAgent(t, tb, "worker")

	var calls atomic.Int32
	setRefreshOne(tb.b, func(*agent) { calls.Add(1) })

	if _, err := tb.client.Status(context.Background(), &localpb.StatusRequest{}); err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("Status reread %d session records, want 0", got)
	}
}

// TestStatusReportsOffline checks that Status carries the current
// broker.offline value.
func TestStatusReportsOffline(t *testing.T) {
	tb := newTestBroker(t)

	resp, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if resp.GetOffline() {
		t.Fatal("Status reports offline while broker.offline is false")
	}

	tb.cfg.setValue("broker.offline", config.Value{Kind: config.KindBool, Bool: true})
	resp, err = tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status after going offline: %v", err)
	}
	if !resp.GetOffline() {
		t.Fatal("Status does not report offline while broker.offline is true")
	}
}

// TestBrokerOfflineLogsFailedRead checks that a failed broker.offline read is
// logged and still answers online.
func TestBrokerOfflineLogsFailedRead(t *testing.T) {
	c := newWireConfig(t)
	c.fail("broker.offline", errors.New("read failed"))
	var buf bytes.Buffer
	b := &Broker{deps: Deps{Config: c}, log: NewLogger(&buf, new(slog.LevelVar))}

	if b.offline() {
		t.Fatal("offline() reports true after a failed read")
	}
	logged := buf.String()
	if !strings.Contains(logged, evConfigRead) || !strings.Contains(logged, "broker.offline") {
		t.Fatalf("log %q does not record the failed read of broker.offline", logged)
	}
}

// TestStatusNeedsNoAgent checks that Status answers on a Broker with no
// registration.
func TestStatusNeedsNoAgent(t *testing.T) {
	tb := newTestBroker(t)

	resp, err := tb.client.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status without any registration: %v", err)
	}
	if len(resp.Agents) != 0 {
		t.Fatalf("Status returned agents on an empty Broker: %v", resp.Agents)
	}
	if len(resp.Peers) != 0 {
		t.Fatalf("Status returned peers on an empty Broker: %v", resp.Peers)
	}
}
