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

const tlBrokerPort = "17443"

// tlRefillWait exceeds the second the topic token bucket takes to add four
// tokens; the bucket is not observable.
const tlRefillWait = 1100 * time.Millisecond

// TestTopicRosterBounds checks that a paired peer cannot grow the entries a
// Broker keeps for it without bound and that a member one byte over the name
// limit is never saved.
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

	// The observer shares t2 and t3 with the peer but not the flooded t1: its
	// list_agents then walks three remote entries instead of 4096, because
	// listing a full roster is quadratic in the Broker. The flood's own bound
	// is proved by the Broker log instead, since a dropped member cannot be
	// observed.
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

	// The first frame fills 4095 entries, the second reaches exactly 4096, and
	// the third would make 4097, so the whole frame is dropped.
	f.SendRosterDeltaBatch(t, "t1", 4095, 0x000000, prefix)
	f.SendRosterDeltaBatch(t, "t1", 1, 0x001000, prefix)
	f.SendRosterDeltaBatch(t, "t1", 1, 0x001001, prefix)

	longName := strings.Repeat("x", 129)
	savedName := prefix + "003000"
	f.SendRosterDeltaBatch(t, "t3", 1, 0x003000, prefix,
		fakePeerWireMember{Name: longName, Ref: "abcdef"})

	// The t2 member is a fence: frames are ordered on the link, so its
	// visibility proves every frame above was applied or dropped.
	fenceName := "fence-" + token + "-00f000"
	f.SendRosterDeltaBatch(t, "t2", 1, 0x00f000, "fence-"+token+"-")

	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the fence member becomes visible to the observer", func(deadline time.Time) (bool, string) {
		out, state := topicsListAgentsPoll(t, deadline, observer, map[string]any{"name": fenceName})
		if state != "" {
			return false, state
		}
		return len(out.Agents) == 1, fmt.Sprintf("%d agents are named %s", len(out.Agents), fenceName)
	})

	out, state := topicsListAgents(t, observer, map[string]any{"name": savedName})
	if state != "" {
		t.Fatalf("list_agents %s: %s", savedName, state)
	}
	if len(out.Agents) != 1 {
		t.Fatalf("the Broker lists %d agents named %s, want the bounded member", len(out.Agents), savedName)
	}

	// The over-long name normalizes to the same 128-byte prefix, so a saved
	// entry would match even though its address is truncated.
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

// TestTopicChangeLimits checks that one account's topic changes are rate
// limited, that a refused change leaves membership unchanged, and that tokens
// come back over time.
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

func tlStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

func tlCreateKey(t *testing.T, node *harness.Node) string {
	t.Helper()
	op := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	defer op.End(t)
	harness.WaitConnected(t, op, harness.EventTimeout)
	r := op.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out.Key
}

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
