//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// tlBrokerPort is the port a container Broker listens on.
const tlBrokerPort = "17443"

// tlRefillWait is longer than the one second the topic token bucket takes to
// add four tokens. The bucket is not observable, so this is a real-time wait
// for a product timer rather than a poll.
const tlRefillWait = 1100 * time.Millisecond

// TestTopicRosterBounds proves REQ-topics-029 against a real Broker in a
// container: a paired peer cannot make the entries the Broker keeps for it grow
// without bound, and a member whose name is one byte over the limit is never
// saved. The Broker runs in the single-host container topology; the fake peer
// is the test-side process the pairing cases already use, dialing the Broker
// over the container network.
//
// The observer Agent shares t2 and t3 with the peer but not the flooded t1, so
// its list_agents walks three remote entries instead of 4096. The flood's own
// bound is proved by the Broker log, which records the dropped frame; the
// member that would have crossed it is never observable because listing t1 with
// a full roster is quadratic in the Broker.
func TestTopicRosterBounds(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	node := c.Node("hostA")
	tlStartBroker(t, node, harness.ConfigYAML(harness.FastTimers))

	token := secToken(t)
	flooderName := "flooder-" + token
	flooder := node.StartSession(t, harness.SessionSpec{Name: flooderName, StartMCP: true})
	defer flooder.End(t)
	secWaitRegistered(t, flooder, flooderName)
	secJoin(t, flooder, "t1")

	observerName := "observer-" + token
	observer := node.StartSession(t, harness.SessionSpec{Name: observerName, StartMCP: true})
	defer observer.End(t)
	secWaitRegistered(t, observer, observerName)
	secJoin(t, observer, "t2")
	secJoin(t, observer, "t3")

	logStart := len(node.BrokerLog(t))

	f := startFakePeer(t)
	f.Pair(t, net.JoinHostPort(c.Addr("hostA", "net"), tlBrokerPort), tlCreateKey(t, node))
	f.WaitLinked(t, secWaitBound)
	f.TopicDigests(t)
	f.SendTopicDigests(t, "t1", "t2", "t3")

	prefix := "fill-" + token + "-"

	// t1: the flood. The first frame fills 4095 entries, the second brings the
	// digest to exactly 4096, and the third would make 4097, so the whole frame
	// is dropped.
	f.SendRosterDeltaBatch(t, "t1", 4095, 0x000000, prefix)
	f.SendRosterDeltaBatch(t, "t1", 1, 0x001000, prefix)
	f.SendRosterDeltaBatch(t, "t1", 1, 0x001001, prefix)

	// t3: one bounded member and a 129-byte name in the same frame. The long
	// entry is dropped on its own while the bounded member is saved.
	longName := strings.Repeat("x", 129)
	savedName := prefix + "003000"
	f.SendRosterDeltaBatch(t, "t3", 1, 0x003000, prefix,
		fakePeerWireMember{Name: longName, Ref: "abcdef"})

	// t2: a member under a third shared topic is a fence. Frames are ordered on
	// the link, so its visibility proves every frame above was already applied
	// or dropped.
	fenceName := "fence-" + token + "-00f000"
	f.SendRosterDeltaBatch(t, "t2", 1, 0x00f000, "fence-"+token+"-")

	harness.EventuallyPoll(t, secWaitBound, "the fence member becomes visible to the observer", func(deadline time.Time) (bool, string) {
		out, state := topicsListAgentsPoll(t, deadline, observer, map[string]any{"name": fenceName})
		if state != "" {
			return false, state
		}
		return len(out.Agents) == 1, fmt.Sprintf("%d agents are named %s", len(out.Agents), fenceName)
	})

	// The bounded member beside the over-long name was saved, so the frame was
	// applied rather than dropped whole.
	out, state := topicsListAgents(t, observer, map[string]any{"name": savedName})
	if state != "" {
		t.Fatalf("list_agents %s: %s", savedName, state)
	}
	if len(out.Agents) != 1 {
		t.Fatalf("the Broker lists %d agents named %s, want the bounded member", len(out.Agents), savedName)
	}

	// The over-long name normalizes to the same 128-byte prefix as the request,
	// so a saved entry would match even though its address is truncated.
	out, state = topicsListAgents(t, observer, map[string]any{"name": longName})
	if state != "" {
		t.Fatalf("list_agents by the over-long name: %s", state)
	}
	if len(out.Agents) != 0 {
		t.Fatalf("the Broker saved the %d-byte member name", len(longName))
	}

	log := node.BrokerLog(t)[logStart:]
	tlRequireLogEvent(t, log, "topic.member_dropped", 202, "invalid_member")
	tlRequireLogEvent(t, log, "topic.frame_dropped", 202, "roster_full")

	peers, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("list_brokers: %s", detail.GetMessage())
	}
	if len(peers.GetBrokers()) != 1 {
		t.Fatalf("the Broker lists %d peers after the flood, want 1", len(peers.GetBrokers()))
	}
	if peers.GetBrokers()[0].GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
		t.Fatalf("the peer link is %v after the flood, want CONNECTED", peers.GetBrokers()[0].GetStatus())
	}
}

// TestTopicChangeLimits proves REQ-topics-028 against a real Broker in a
// container: one account's topic changes are rate limited, a refused change
// leaves the membership unchanged, and tokens come back over time. The global
// topic and per-topic member caps and the system Broker's per-account budget
// are covered by the topic and broker unit tests, because reaching them needs
// thousands of memberships.
func TestTopicChangeLimits(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	node := c.Node("hostA")
	tlStartBroker(t, node, harness.ConfigYAML(harness.FastTimers, "topics:\n  auto_join: false"))

	token := secToken(t)
	name := "limits-" + token
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	defer s.End(t)
	secWaitRegistered(t, s, name)

	joined := 0
	refused := ""
	for i := 0; i < 200; i++ {
		topicName := fmt.Sprintf("rate-%s-%03d", token, i)
		r := s.Call(t, "join_topic", map[string]any{"name": topicName})
		if !r.IsError {
			joined++
			continue
		}
		r.MustCode(t, "quota_exceeded")
		if !strings.Contains(strings.ToLower(r.Text), "try again") {
			t.Fatalf("the rate-limit error does not ask the caller to retry: %s", r.Text)
		}
		refused = topicName
		break
	}
	if refused == "" {
		t.Fatalf("the Broker accepted 200 topic changes, want a quota_exceeded after the burst")
	}
	if joined < 64 {
		t.Fatalf("the Broker refused a topic change after %d successes, want at least the 64-token burst", joined)
	}

	out, state := topicsList(t, s)
	if state != "" {
		t.Fatalf("list_topics: %s", state)
	}
	if len(out.Joined) != joined {
		t.Fatalf("the Agent is in %d topics after %d accepted changes and one refusal, want %d", len(out.Joined), joined, joined)
	}
	if topicsContains(out.Joined, refused) {
		t.Fatalf("the refused topic %q is in the Agent's topics", refused)
	}

	time.Sleep(tlRefillWait)
	for i := 0; i < 4; i++ {
		topicName := fmt.Sprintf("refill-%s-%d", token, i)
		s.Call(t, "join_topic", map[string]any{"name": topicName}).MustOK(t)
	}
}

// tlStartBroker starts a container Broker with config and fails when it does
// not come up.
func tlStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// tlCreateKey creates one pairing key through an operator session.
func tlCreateKey(t *testing.T, node *harness.Node) string {
	t.Helper()
	op := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	defer op.End(t)
	harness.WaitConnected(t, op, 3*time.Second)
	r := op.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out.Key
}

// tlRequireLogEvent fails unless the Broker log holds a record named event with
// the given frame kind and reason.
func tlRequireLogEvent(t *testing.T, log []byte, event string, kind int, reason string) {
	t.Helper()
	for _, line := range strings.Split(string(log), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var rec struct {
			Event  string `json:"event"`
			Kind   int    `json:"kind"`
			Reason string `json:"reason"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if rec.Event == event && rec.Kind == kind && rec.Reason == reason {
			return
		}
	}
	t.Fatalf("the Broker log has no %s record with kind %d and reason %q: %s", event, kind, reason, log)
}
