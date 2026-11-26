//go:build e2e

package e2e

import (
	"encoding/hex"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

const offlineWaitBound = harness.EventTimeout

// TestBrokerOffline covers the offline switch end to end: a peer's delivery is
// refused, links and topic exchange continue, local and outgoing sends still
// work, the state survives a restart and a configuration-file edit, the
// terminal commands and the tool set it, and status shows it.
func TestBrokerOffline(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(2))
	aNode, bNode := c.Node("hostA"), c.Node("hostB")
	lkStartBroker(t, aNode, lkConfig())
	lkStartBroker(t, bNode, lkConfig())

	opA := lkOperator(t, aNode)
	defer opA.End(t)
	opB := lkOperator(t, bNode)
	defer opB.End(t)

	a1 := lkSession(t, aNode, "a1")
	t.Cleanup(func() { a1.End(t) })
	b1 := lkSession(t, bNode, "b1")
	// Registered as a cleanup, not a defer: cleanups run in reverse order, so
	// the online cleanup below still has a live session.
	t.Cleanup(func() { b1.End(t) })

	// Pair last, so both sessions are registered before the rosters exchange.
	addrA := lkAddr(c, "hostA", "net")
	addrB := lkAddr(c, "hostB", "net")
	key := lkCreateKey(t, opA)
	lkConnect(t, opB, addrA, key.Key).MustOK(t)
	lkWaitConnected(t, bNode, "hostA.peer", offlineWaitBound)
	lkWaitConnected(t, aNode, "hostB.peer", offlineWaitBound)
	offWaitAgent(t, a1, "b1")
	offWaitAgent(t, b1, "a1")

	online := func(t *testing.T) {
		t.Helper()
		offSetOnline(t, b1, true)
	}
	t.Cleanup(func() { offSetOnline(t, b1, true) })

	t.Run("REQ-agent-messaging-039 peer delivery is refused", func(t *testing.T) {
		offSetOnline(t, b1, false)
		since := time.Now()

		r := offSend(t, a1, "b1@hostB.peer", "while B is offline")
		if !r.IsError {
			t.Fatalf("send_message to an offline Broker succeeded: %s", r.Text)
		}
		if !strings.HasPrefix(r.Text, "Not sent") {
			t.Errorf("error = %q, want it to start with Not sent", r.Text)
		}
		if !strings.Contains(r.Text, "(code: broker_offline;") {
			t.Errorf("error = %q, want code broker_offline", r.Text)
		}
		if !strings.Contains(r.Text, "is offline and refuses messages from other hosts") {
			t.Errorf("error = %q, want the Broker's explanation", r.Text)
		}
		bPeer, ok := lkFindPeer(lkPeers(t, aNode), "hostB.peer")
		if !ok {
			t.Fatal("A lists no BrokerInfo for hostB.peer")
		}
		wantFP := hex.EncodeToString(bPeer.GetFingerprint())
		if got := r.Origin(); got != wantFP {
			t.Errorf("the refusal is reported by Broker %q, want B's fingerprint %q", got, wantFP)
		}
		if frames := b1.Fake().Frames(); len(frames) != 0 {
			t.Fatalf("the offline target received %d frames, want 0", len(frames))
		}
		if events := ctAudit(t, bNode, since, 0); len(events) != 0 {
			t.Errorf("the refused delivery wrote %d audit records, want 0: %v", len(events), events)
		}

		online(t)
		offSend(t, a1, "b1@hostB.peer", "after B is online").MustOK(t)
		frame, ok := b1.Fake().WaitFrame(offlineWaitBound)
		if !ok {
			t.Fatal("the target received no message after the Broker went online")
		}
		if !strings.Contains(frame.Content, "after B is online") {
			t.Errorf("the delivered frame = %q, want the second message", frame.Content)
		}
	})

	t.Run("REQ-agent-messaging-040 links and topics continue", func(t *testing.T) {
		offSetOnline(t, b1, false)

		harness.Consistently(t, 2*time.Second, "both sides keep the link while B is offline", func() (bool, string) {
			a := lkStatus(t, aNode, "hostB.peer")
			b := lkStatus(t, bNode, "hostA.peer")
			if a != localpb.BrokerStatus_BROKER_STATUS_CONNECTED || b != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, "A sees B as " + a.String() + ", B sees A as " + b.String()
			}
			return true, ""
		})

		lkCutAndWaitDown(t, c, aNode, bNode)
		c.Restore(t)
		lkWaitConnected(t, bNode, "hostA.peer", offlineWaitBound)
		lkWaitConnected(t, aNode, "hostB.peer", offlineWaitBound)

		b2 := lkSession(t, bNode, "b2")
		defer b2.End(t)
		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "A sees the new agent on the offline B", func(deadline time.Time) (bool, string) {
			out, state := pollDecode[mcpserver.ListAgentsOut](t, deadline, a1, "list_agents", map[string]any{})
			if state != "" {
				return false, state
			}
			if _, ok := offFindAgent(out, "b2"); !ok {
				return false, "b2 is not listed"
			}
			return true, ""
		})

		r := offSend(t, a1, "b2@hostB.peer", "after relink")
		if !r.IsError || !strings.Contains(r.Text, "(code: broker_offline;") {
			t.Fatalf("delivery after relink = %q, want broker_offline", r.Text)
		}
		online(t)
	})

	t.Run("REQ-agent-messaging-041 local and outgoing sends continue", func(t *testing.T) {
		offSetOnline(t, b1, false)

		b2 := lkSession(t, bNode, "b2")
		defer b2.End(t)
		selfB2 := offSelf(t, b2)

		offSend(t, b1, selfB2, "local while offline").MustOK(t)
		frame, ok := b2.Fake().WaitFrame(offlineWaitBound)
		if !ok || !strings.Contains(frame.Content, "local while offline") {
			t.Fatalf("the local target received %q, want the local message", frame.Content)
		}

		offSend(t, b1, "a1@hostA.peer", "outgoing while offline").MustOK(t)
		frame, ok = a1.Fake().WaitFrame(offlineWaitBound)
		if !ok || !strings.Contains(frame.Content, "outgoing while offline") {
			t.Fatalf("the remote target received %q, want the outgoing message", frame.Content)
		}

		r := offSend(t, a1, "b1@hostB.peer", "reply into the offline Broker")
		if !r.IsError || !strings.Contains(r.Text, "(code: broker_offline;") {
			t.Fatalf("the reply = %q, want broker_offline", r.Text)
		}
		online(t)
	})

	t.Run("REQ-broker-config-065 state survives restart and a file edit", func(t *testing.T) {
		offSetOnline(t, b1, false)

		bNode.StopBroker(t, syscall.SIGTERM)
		start := bNode.StartBroker(t, harness.BrokerSpec{})
		if !start.Running {
			t.Fatalf("the Broker did not restart: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
		}
		// Wait for re-registration and relink: a send before them would race and report
		// not_found or peer_unreachable.
		registrationWaitRegistered(t, b1, "b1")
		lkWaitConnected(t, aNode, "hostB.peer", offlineWaitBound)
		lkWaitConnected(t, bNode, "hostA.peer", offlineWaitBound)
		if status, detail := bNode.Oracle().Status(t); detail != nil || !status.GetOffline() {
			t.Fatalf("Status after restart = %v (detail %v), want offline", status, detail)
		}
		r := offSend(t, a1, "b1@hostB.peer", "after restart")
		if !r.IsError || !strings.Contains(r.Text, "(code: broker_offline;") {
			t.Fatalf("delivery after restart = %q, want broker_offline", r.Text)
		}

		since := time.Now()
		bNode.WriteConfig(t, lkConfig("broker:\n  offline: false\n"))
		harness.Eventually(t, offlineWaitBound, "broker.offline reads false after the file edit", func() (bool, string) {
			return ctGetConfig(t, bNode, "broker.offline") == "false", "value is " + ctGetConfig(t, bNode, "broker.offline")
		})
		// send_message may spend sendMessageRPCTimeout before its update hint
		// runs, so wait and poll with its own bound.
		harness.EventuallyPoll(t, harness.ToolWaitBound("send_message"), "the reply succeeds after the file edit", func(deadline time.Time) (bool, string) {
			r, err := a1.PollCallWithin(t, deadline, harness.SendMessagePollBound, "send_message",
				map[string]any{"to": "b1@hostB.peer", "message": "after the file edit"})
			if err != nil {
				return false, err.Error()
			}
			if r.IsError {
				return false, r.Text
			}
			return true, ""
		})

		found := false
		for _, ev := range ctAudit(t, bNode, since, 0) {
			if ev.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED && ev.GetKey() == "broker.offline" {
				found = true
			}
		}
		if !found {
			t.Fatal("the configuration-file edit wrote no config_file_changed record for broker.offline")
		}
	})

	t.Run("REQ-broker-config-066 terminal commands", func(t *testing.T) {
		before := time.Now()
		res := bNode.Exec(t, []string{"/usr/local/bin/peers", "service", "offline"}, nil)
		if res.ExitCode != 0 {
			t.Fatalf("peers service offline exited %d\nstdout: %s\nstderr: %s", res.ExitCode, res.Stdout, res.Stderr)
		}
		if res.Stdout != "The Broker is now offline.\n" {
			t.Fatalf("peers service offline printed %q", res.Stdout)
		}
		if status, detail := bNode.Oracle().Status(t); detail != nil || !status.GetOffline() {
			t.Fatalf("Status after peers service offline = %v (detail %v), want offline", status, detail)
		}

		res = bNode.Exec(t, []string{"/usr/local/bin/peers", "service", "online"}, nil)
		if res.ExitCode != 0 {
			t.Fatalf("peers service online exited %d\nstdout: %s\nstderr: %s", res.ExitCode, res.Stdout, res.Stderr)
		}
		if res.Stdout != "The Broker is now online.\n" {
			t.Fatalf("peers service online printed %q", res.Stdout)
		}
		if status, detail := bNode.Oracle().Status(t); detail != nil || status.GetOffline() {
			t.Fatalf("Status after peers service online = %v (detail %v), want online", status, detail)
		}

		var changes []*localpb.AuditEvent
		for _, ev := range ctAudit(t, bNode, before, 0) {
			if ev.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET && ev.GetKey() == "broker.offline" {
				changes = append(changes, ev)
			}
		}
		if len(changes) != 2 {
			t.Fatalf("broker.offline audit records = %d, want 2", len(changes))
		}
		if !changes[0].GetNewValue().GetBoolValue() || changes[1].GetNewValue().GetBoolValue() {
			t.Fatalf("audit values = %v then %v, want true then false",
				changes[0].GetNewValue().GetBoolValue(), changes[1].GetNewValue().GetBoolValue())
		}
		for _, ev := range changes {
			if ev.GetAccount() == "" {
				t.Errorf("the audit record names no account: %v", ev)
			}
		}
	})

	t.Run("REQ-broker-config-067 tool changes the state", func(t *testing.T) {
		out := offSetOnline(t, b1, false)
		if out.Online || !out.Changed {
			t.Fatalf("set_broker_online(false) = %+v, want offline and changed", out)
		}
		if status, detail := bNode.Oracle().Status(t); detail != nil || !status.GetOffline() {
			t.Fatalf("Status after the tool call = %v (detail %v), want offline", status, detail)
		}
		out = offSetOnline(t, b1, true)
		if !out.Online || !out.Changed {
			t.Fatalf("set_broker_online(true) = %+v, want online and changed", out)
		}
	})

	t.Run("REQ-broker-config-068 status shows the state", func(t *testing.T) {
		offSetOnline(t, b1, false)

		resp, detail := bNode.Oracle().ListBrokers(t)
		if detail != nil {
			t.Fatalf("ListBrokers: %s", detail.GetMessage())
		}
		if resp.GetLocal() == nil || !resp.GetLocal().GetOffline() {
			t.Fatalf("ListBrokers local = %v, want offline", resp.GetLocal())
		}
		if status, detail := bNode.Oracle().Status(t); detail != nil || !status.GetOffline() {
			t.Fatalf("Status = %v (detail %v), want offline", status, detail)
		}

		const offlineLine = "Broker: offline; messages from other hosts are refused. Run peers service online to accept them."
		for _, command := range [][]string{{"service", "status"}, {"broker", "list"}} {
			res := bNode.Exec(t, append([]string{"/usr/local/bin/peers"}, command...), nil)
			if res.ExitCode != 0 {
				t.Fatalf("peers %v exited %d\nstdout: %s\nstderr: %s", command, res.ExitCode, res.Stdout, res.Stderr)
			}
			if !strings.Contains(res.Stdout, offlineLine) {
				t.Fatalf("peers %v does not print %q:\n%s", command, offlineLine, res.Stdout)
			}
		}

		offSetOnline(t, b1, true)
		for _, command := range [][]string{{"service", "status"}, {"broker", "list"}} {
			res := bNode.Exec(t, append([]string{"/usr/local/bin/peers"}, command...), nil)
			if res.ExitCode != 0 {
				t.Fatalf("peers %v exited %d\nstdout: %s\nstderr: %s", command, res.ExitCode, res.Stdout, res.Stderr)
			}
			if strings.Contains(res.Stdout, offlineLine) {
				t.Fatalf("peers %v still prints the offline line after going online:\n%s", command, res.Stdout)
			}
		}
	})

	t.Run("pairing works while offline", func(t *testing.T) {
		// Unpair first, because the same two Brokers cannot pair twice: A's
		// disconnect is local, so B still holds A's record and the re-pairing
		// takes the replacement branch of the peer table.
		opA.Call(t, "disconnect_broker", map[string]any{"broker": "hostB.peer"}).MustOK(t)
		harness.Eventually(t, offlineWaitBound, "B no longer sees A as connected", func() (bool, string) {
			status := lkStatus(t, bNode, "hostA.peer")
			return status != localpb.BrokerStatus_BROKER_STATUS_CONNECTED, "status is " + status.String()
		})

		offSetOnline(t, b1, false)
		key := lkCreateKey(t, opB)
		if key.Key == "" {
			t.Fatal("create_pairing_key returned no key")
		}
		lkConnect(t, opA, addrB, key.Key).MustOK(t)
		lkWaitConnected(t, bNode, "hostA.peer", offlineWaitBound)
		lkWaitConnected(t, aNode, "hostB.peer", offlineWaitBound)
		if status, detail := bNode.Oracle().Status(t); detail != nil || !status.GetOffline() {
			t.Fatalf("Status after pairing = %v (detail %v), want still offline", status, detail)
		}
		online(t)
	})
}

// TestBrokerOfflineDirectory covers a third Broker learning an offline public
// Broker's card and a topic it declared after the switch, through their shared
// peer, while the offline Broker keeps its link.
func TestBrokerOfflineDirectory(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(3))
	aNode, bNode, cNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
	const reconcile = "directory:\n  reconcile_interval: 1s"
	lkStartBroker(t, aNode, lkConfig(reconcile))
	lkStartBroker(t, bNode, lkConfig(reconcile, "broker:\n  public: true"))
	lkStartBroker(t, cNode, lkConfig(reconcile))

	opA := lkOperator(t, aNode)
	defer opA.End(t)
	opB := lkOperator(t, bNode)
	defer opB.End(t)
	opC := lkOperator(t, cNode)
	defer opC.End(t)

	// A pairs with the public B first so its BrokerInfo carries B's identity.
	keyB := lkCreateKey(t, opB)
	lkConnect(t, opA, lkAddr(c, "hostB", "net"), keyB.Key).MustOK(t)
	lkWaitConnected(t, bNode, "hostA.peer", offlineWaitBound)
	lkWaitConnected(t, aNode, "hostB.peer", offlineWaitBound)
	bPeer, ok := lkFindPeer(lkPeers(t, aNode), "hostB.peer")
	if !ok {
		t.Fatal("A lists no BrokerInfo for hostB.peer after pairing")
	}
	bFingerprint := hex.EncodeToString(bPeer.GetFingerprint())
	bAddr := lkAddr(c, "hostB", "net")

	offSetOnline(t, opB, false)
	if status, detail := bNode.Oracle().Status(t); detail != nil || !status.GetOffline() {
		t.Fatalf("Status after set_broker_online(false) = %v (detail %v), want offline", status, detail)
	}

	// A pairs with C only after B went offline, so the A-C link is the only source of B's card.
	keyC := lkCreateKey(t, opC)
	lkConnect(t, opA, lkAddr(c, "hostC", "net"), keyC.Key).MustOK(t)
	lkWaitConnected(t, cNode, "hostA.peer", offlineWaitBound)
	lkWaitConnected(t, aNode, "hostC.peer", offlineWaitBound)
	// list_topics needs an agent identity while list_brokers does not, so the
	// topic is checked through this session.
	c1 := lkSession(t, cNode, "c1")
	defer c1.End(t)

	harness.EventuallyPoll(t, discBound(2), "C receives the offline B's card through A", func(deadline time.Time) (bool, string) {
		out, state := pairListBrokersPoll(t, deadline, opC)
		if state != "" {
			return false, state
		}
		broker, ok := pairBrokerByAlias(out, "hostB.peer")
		if !ok {
			return false, "hostB.peer is not listed"
		}
		if broker.IsPeer {
			return false, "B is listed as a peer although C never paired with it"
		}
		if broker.Fingerprint != bFingerprint {
			return false, "fingerprint is " + broker.Fingerprint + ", want " + bFingerprint
		}
		if broker.Status != "connectable" {
			return false, "status is " + broker.Status
		}
		if len(broker.Addresses) != 1 || broker.Addresses[0].Kind != "direct" || broker.Addresses[0].Addr != bAddr {
			return false, fmt.Sprintf("addresses are %v, want [{direct %s}]", broker.Addresses, bAddr)
		}
		return true, ""
	})

	b1 := lkSession(t, bNode, "b1")
	defer b1.End(t)
	b1.Call(t, "join_topic", map[string]any{
		"name": "offline-relay", "public": true, "description": "declared while offline",
	}).MustOK(t)
	harness.EventuallyPoll(t, discBound(2), "C sees the public topic B declared while offline", func(deadline time.Time) (bool, string) {
		out, state := pollDecode[mcpserver.ListTopicsOut](t, deadline, c1, "list_topics", nil)
		if state != "" {
			return false, state
		}
		for _, topic := range out.Public {
			if topic.Name == "offline-relay" && topic.Description == "declared while offline" {
				return true, ""
			}
		}
		return false, fmt.Sprintf("public topics are %v", out.Public)
	})

	if status, detail := bNode.Oracle().Status(t); detail != nil || !status.GetOffline() {
		t.Fatalf("Status after the directory exchange = %v (detail %v), want still offline", status, detail)
	}
}

func offSetOnline(t *testing.T, s *harness.Session, online bool) mcpserver.SetBrokerOnlineOut {
	t.Helper()
	r := s.Call(t, "set_broker_online", map[string]any{"online": online})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.SetBrokerOnlineOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding set_broker_online: %v", err)
	}
	return out
}

func offSend(t *testing.T, s *harness.Session, to, text string) harness.Result {
	t.Helper()
	return s.Call(t, "send_message", map[string]any{"to": to, "message": text})
}

func offSelf(t *testing.T, s *harness.Session) string {
	t.Helper()
	r := s.Call(t, "list_agents", nil)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.ListAgentsOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding list_agents: %v", err)
	}
	return out.Self
}

func offFindAgent(out mcpserver.ListAgentsOut, name string) (mcpserver.AgentOut, bool) {
	for _, agent := range out.Agents {
		if n, _, _ := strings.Cut(agent.Address, "@"); n == name {
			return agent, true
		}
	}
	return mcpserver.AgentOut{}, false
}

func offWaitAgent(t *testing.T, s *harness.Session, name string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the session lists "+name, func(deadline time.Time) (bool, string) {
		out, state := pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", map[string]any{})
		if state != "" {
			return false, state
		}
		if _, ok := offFindAgent(out, name); !ok {
			return false, name + " is not listed"
		}
		return true, ""
	})
}
